from dataclasses import InitVar, dataclass, field
import datetime
import os
from typing import Dict, List, Set, Tuple
import pandas as pd
from ccloud.connections import CCloudBase, CCloudConnection, EndpointURL
from ccloud.ccloud_api.api_keys import CCloudAPIKeyList
from ccloud.ccloud_api.clusters import CCloudClusterList
from ccloud.ccloud_api.connectors import CCloudConnectorList
from ccloud.ccloud_api.environments import CCloudEnvironmentList
from ccloud.ccloud_api.ksqldb_clusters import CCloudKsqldbClusterList
from ccloud.ccloud_api.service_accounts import CCloudServiceAccountList
from ccloud.ccloud_api.user_accounts import CCloudUserAccountList
from ccloud.metrics_api.billings_api_manager import CCloudBillingManager
from ccloud.metrics_api.chargeback_manager import ChargebackManager
from ccloud.metrics_api.telemetry_manager import CCloudMetricsManager
from ccloud.model import CCMEReq_Granularity
from data_processing.metrics_processing import METRICS_CSV_COLUMNS
from helpers import sanitize_id, BILLING_METRICS_SCOPE
from storage_mgmt import (
    BILLING_PERSISTENCE_STORE,
    CHARGEBACK_PERSISTENCE_STORE,
    METRICS_PERSISTENCE_STORE,
    STORAGE_PATH,
    DirType,
)


@dataclass
class CCloudBillingHandler:
    org_id: str = field(init=True)
    in_ccloud_api_connection = InitVar[CCloudConnection | None] = None
    billing_manager: CCloudBillingManager = field(init=False)
    available_hour_slices_in_dataset: List[str] = field(init=False, default_factory=list)

    def __post_init__(self, in_ccloud_api_connection) -> None:
        self.billing_manager = CCloudBillingManager(in_ccloud_connection=in_ccloud_api_connection)
        self.read_all()

    def read_all(self):
        self.billing_manager.read_all()
        self.available_hour_slices_in_dataset = sorted(list(self.__calculate_daterange_in_all_datasets()))

    def execute_requests(self):
        self.billing_manager.read_all()

    def __calculate_daterange_in_all_datasets(self) -> Set[str]:
        out = set()
        for _, billing_dataframe in self.billing_manager.billing_dataframes.items():
            out = out.union(billing_dataframe.hourly_date_range)
        return out

    # def get_hourly_dataset(self, time_slice: datetime.datetime) -> Tuple[pd.DataFrame, Set[str]]:
    def get_hourly_dataset(self, time_slice: datetime.datetime) -> Dict[str, pd.DataFrame]:
        out_data = dict()
        for filename, billing_dataframe in self.billing_manager.billing_dataframes.items():
            filename_token = os.path.basename(filename)
            if not BILLING_PERSISTENCE_STORE.is_dataset_present(
                org_id=self.org_id, key=(time_slice,), value=filename_token
            ):
                file_level_df = billing_dataframe.get_hourly_dataset(datetime_slice_iso_format=time_slice)
                out_data[filename_token] = file_level_df
            else:
                print(
                    f"Data already captured and processed for file {filename_token} for timeslice {time_slice}. Skipping."
                )
        return out_data


@dataclass
class CCloudMetricsHandler(CCloudBase):
    org_id: str
    cc_objects: object = field(init=True, repr=False)
    in_requests: InitVar[List | None] = None
    days_in_memory: int = field(default=7)

    metrics_basepath: str = field(init=False, repr=False)
    metrics_manager: Dict[str, CCloudMetricsManager] = field(init=False, default_factory=dict)

    def __post_init__(self, in_requests) -> None:
        super().__post_init__()
        self.url = self.in_ccloud_connection.get_endpoint_url(
            key=self.in_ccloud_connection.uri.telemetry_query_metrics
        ).format(dataset="cloud")
        self.metrics_basepath = STORAGE_PATH.get_path(org_id=self.org_id, dir_type=DirType.MetricsData)
        self.read_all(in_requests)

    def read_all(self, **kwargs):
        cb_val = "CHARGEBACK_REQUESTS"
        in_req = kwargs.get("in_requests", None)
        if cb_val in in_req:
            temp = [
                {
                    "id": k,
                    "aggregations": [{"metric": v}],
                    "granularity": "PT1H",
                    "group_by": ["resource.kafka.id", "metric.principal_id"],
                    "limit": 1000,
                    "filter": {"field": "resource.kafka.id", "op": "EQ", "value": ["ALL_CLUSTERS"]},
                }
                for k, v in {
                    "Fetch Request Bytes": "io.confluent.kafka.server/request_bytes",
                    "Fetch Response Bytes": "io.confluent.kafka.server/response_bytes",
                }.items()
            ]
            in_req = temp + in_req
            in_req.remove(cb_val)
        for req in in_req:
            http_req = CCloudMetricsManager(
                org_id=self.org_id,
                _base_payload=req,
                ccloud_url=self.url,
                days_in_memory=self.days_in_memory,
                cc_objects=self.cc_objects,
                metrics_basepath=self.metrics_basepath,
            )
            self.__add_to_cache(http_req=http_req)

    def __add_to_cache(self, http_req: CCloudMetricsManager) -> None:
        if http_req.req_id == "":
            http_req.req_id = str(len(self.metrics_manager))
        self.metrics_manager[http_req.req_id] = http_req

    def execute_requests(self, output_basepath: str):
        for req_name, request in self.metrics_manager.items():
            for req_interval in self.generate_iso8601_dt_intervals(
                granularity=CCMEReq_Granularity.P1D.name, metric_name=request.aggregation_metric, intervals=7
            ):
                request.execute_request(http_connection=self.in_ccloud_connection, date_range=req_interval)
                request.add_dataframes(date_range=req_interval, output_basepath=output_basepath)

    def export_metrics_to_csv(self, output_basepath: str):
        for req_name, request in self.metrics_manager.items():
            for metrics_date, metrics_dataframe in request.metrics_dataframes.items():
                metrics_dataframe.output_to_csv(basepath=output_basepath)

    def generate_iso8601_dt_intervals(self, granularity: str, metric_name: str, intervals: int = 7):
        curr_date = datetime.datetime.now(tz=datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        for _ in range(intervals):
            curr_date = curr_date - datetime.timedelta(days=1)
            curr = (curr_date, curr_date.date(), curr_date.isoformat() + "/" + granularity)
            if not METRICS_PERSISTENCE_STORE.is_dataset_present(
                org_id=self.org_id, key=(str(curr[1]),), value=metric_name
            ):
                yield curr
            else:
                print(f"Dataset already available for metric {metric_name} on {curr[1]}")

    def get_hourly_dataset(self, time_slice: datetime.datetime, billing_mgmt: bool = True):
        out = pd.DataFrame(
            columns=[
                METRICS_CSV_COLUMNS.OUT_TS,
                METRICS_CSV_COLUMNS.OUT_KAFKA_CLUSTER,
                METRICS_CSV_COLUMNS.OUT_PRINCIPAL,
            ]
        )
        data_missing_on_disk = False
        for _, telemetry_dataset in self.metrics_manager.items():
            if billing_mgmt and telemetry_dataset.aggregation_metric not in BILLING_METRICS_SCOPE.values():
                continue
            metric_name, file_level_df = telemetry_dataset.get_hourly_dataset(datetime_slice_iso_format=time_slice)
            if file_level_df is not None and not file_level_df.empty:
                out = out.merge(
                    file_level_df,
                    how="outer",
                    on=[
                        METRICS_CSV_COLUMNS.OUT_TS,
                        METRICS_CSV_COLUMNS.OUT_KAFKA_CLUSTER,
                        METRICS_CSV_COLUMNS.OUT_PRINCIPAL,
                    ],
                )
            else:
                data_missing_on_disk = True
        # TODO: Maybe need to replace NaN items with zero so that maths doesnt fail while calculation.
        return data_missing_on_disk, out


@dataclass
class CCloudObjectsHandler:
    in_ccloud_connection: CCloudConnection

    last_refresh: datetime = field(init=False, default=None)
    min_refresh_gap: datetime.timedelta = field(init=False, default=datetime.timedelta(minutes=30))
    cc_sa: CCloudServiceAccountList = field(init=False)
    cc_users: CCloudUserAccountList = field(init=False)
    cc_api_keys: CCloudAPIKeyList = field(init=False)
    cc_environments: CCloudEnvironmentList = field(init=False)
    cc_clusters: CCloudClusterList = field(init=False)
    cc_connectors: CCloudConnectorList = field(init=False)
    cc_ksqldb_clusters: CCloudKsqldbClusterList = field(init=False)

    def __post_init__(self) -> None:
        self.last_refresh = datetime.datetime.now() - self.min_refresh_gap
        self.read_all()

    def read_all(self):
        if self.min_refresh_gap > datetime.datetime.now() - self.last_refresh:
            print(f"Not refreshing the CCloud Object state  -- TimeDelta is not enough. {self.min_refresh_gap}")
        else:
            self.cc_sa = CCloudServiceAccountList(in_ccloud_connection=self.in_ccloud_connection)
            self.cc_users = CCloudUserAccountList(in_ccloud_connection=self.in_ccloud_connection)
            self.cc_api_keys = CCloudAPIKeyList(in_ccloud_connection=self.in_ccloud_connection)
            self.cc_environments = CCloudEnvironmentList(in_ccloud_connection=self.in_ccloud_connection)
            self.cc_clusters = CCloudClusterList(
                in_ccloud_connection=self.in_ccloud_connection, ccloud_env=self.cc_environments
            )
            self.cc_connectors = CCloudConnectorList(
                in_ccloud_connection=self.in_ccloud_connection,
                ccloud_kafka_clusters=self.cc_clusters,
                ccloud_service_accounts=self.cc_sa,
                ccloud_users=self.cc_users,
                ccloud_api_keys=self.cc_api_keys,
            )
            self.cc_ksqldb_clusters = CCloudKsqldbClusterList(
                in_ccloud_connection=self.in_ccloud_connection,
                ccloud_envs=self.cc_environments,
            )
            self.last_refresh = datetime.datetime.now()

    def execute_requests(self):
        print(f"Trying to refresh CCloud Object Store State for corelation")
        self.read_all()


@dataclass(kw_only=True)
class CCloudChargebackHandler:
    org_id: str
    cc_objects: CCloudObjectsHandler = field(init=True)
    chargeback_basepath: str = field(init=False, repr=False)
    cb_manager: ChargebackManager = field(init=False)

    def __post_init__(self) -> None:
        self.chargeback_basepath = STORAGE_PATH.get_path(org_id=self.org_id, dir_type=DirType.OutputData)
        self.cb_manager = ChargebackManager(org_id=self.org_id, cc_objects=self.cc_objects, days_in_memory=3)

    def export_metrics_to_csv(self, output_basepath: str):
        self.cb_manager.output_to_csv(basepath=output_basepath)

    def get_hourly_dataset(self, date_value: datetime.datetime, billing_mgmt: bool = True):
        out = pd.DataFrame()
        data_missing_on_disk = False
        for _, telemetry_dataset in self.metrics_dataset.items():
            if billing_mgmt and telemetry_dataset.aggregation_metric not in BILLING_METRICS_SCOPE.values():
                continue
            metric_name, file_level_df = telemetry_dataset.get_hourly_dataset(datetime_slice_iso_format=date_value)
            if file_level_df is not None:
                out = out.merge(
                    file_level_df,
                    how="outer",
                    on=[METRICS_CSV_COLUMNS.OUT_TS, METRICS_CSV_COLUMNS.OUT_KAFKA_CLUSTER],
                )
            else:
                data_missing_on_disk = True
        # TODO: Maybe need to replace NaN items with zero so that maths doesnt fail while calculation.
        return data_missing_on_disk, out

    def run_calculations(
        self,
        time_slice: datetime.datetime,
        billing_dataframe: pd.DataFrame,
        metrics_dataframe: pd.DataFrame,
    ):
        self.cb_manager.run_calculations(
            time_slice=time_slice, billing_dataframe=billing_dataframe, metrics_dataframe=metrics_dataframe
        )


@dataclass(kw_only=True)
class CCloudOrg:
    in_org_details: InitVar[List | None] = None
    in_days_in_memory: InitVar[int] = field(default=7)
    org_id: str

    metrics_handler: CCloudMetricsHandler = field(init=False)
    objects_handler: CCloudObjectsHandler = field(init=False)
    billing_handler: CCloudBillingHandler = field(init=False)
    chargeback_handler: CCloudChargebackHandler = field(init=False)

    def __post_init__(self, in_org_details, in_days_in_memory) -> None:
        self.org_id = sanitize_id(in_org_details["id"])
        # Delete the Output Directory for now as the Calculations are not tracked.
        # They need to be re-calculated and set-up again.
        # This will go away in future release when time window and billing CSV based tracking is added.
        # STORAGE_PATH.delete_path(org_id=self.org_id, dir_type=DirType.OutputData)
        STORAGE_PATH.ensure_path(
            org_id=self.org_id,
            dir_type=[
                # DirType.MetricsData,
                # DirType.BillingsData,
                # DirType.OutputData,
                DirType.PersistenceStats,
            ],
        )
        # METRICS_PERSISTENCE_STORE.add_persistence_path(org_id=self.org_id)
        # BILLING_PERSISTENCE_STORE.add_persistence_path(org_id=self.org_id)
        # CHARGEBACK_PERSISTENCE_STORE.add_persistence_path(org_id=self.org_id)
        ccloud_api_http_conn = CCloudConnection(
            api_key=in_org_details["ccloud_details"]["ccloud_api"]["api_key"],
            api_secret=in_org_details["ccloud_details"]["ccloud_api"]["api_secret"],
            base_url=EndpointURL.API_URL,
        )

        # Initialize the CCloud Objects Handler
        self.objects_handler = CCloudObjectsHandler(in_ccloud_connection=ccloud_api_http_conn)

        # Initialize the Metrics Handler
        self.metrics_handler = CCloudMetricsHandler(
            org_id=self.org_id,
            in_requests=in_org_details["requests"],
            days_in_memory=in_days_in_memory,
            cc_objects=self.objects_handler,
            in_ccloud_connection=CCloudConnection(
                api_key=in_org_details["ccloud_details"]["metrics_api"]["api_key"],
                api_secret=in_org_details["ccloud_details"]["metrics_api"]["api_secret"],
                base_url=EndpointURL.TELEMETRY_URL,
            ),
        )

        # Initialize the Billing CSV Handler
        self.billing_handler = CCloudBillingHandler(org_id=self.org_id)

        # Initialize the Chargeback Object Handler
        self.chargeback_handler = CCloudChargebackHandler(org_id=self.org_id, cc_objects=self.objects_handler)

        # Once every initialization step completes, get rid of the requests Dict to save memory.
        # self._requests = None

    def execute_requests(self):
        print(f"Gathering CCloud Existing Objects Data")
        self.objects_handler.execute_requests()
        print(f"Gathering Metrics API Data")
        self.metrics_handler.execute_requests(
            output_basepath=STORAGE_PATH.get_path(org_id=self.org_id, dir_type=DirType.MetricsData)
        )
        print(f"Checking for new Billing CSV Files")
        self.billing_handler.execute_requests()

    def find_available_hour_slices_in_billing_datasets(self) -> datetime.datetime:
        for item in self.billing_handler.available_hour_slices_in_dataset:
            yield datetime.datetime.fromisoformat(item)

    def run_calculations(self):
        curr_date = datetime.datetime.now(tz=None).replace(hour=0, minute=0, second=0, microsecond=0)
        for hour_slice in self.find_available_hour_slices_in_billing_datasets():
            if hour_slice >= curr_date:
                print(f"Skipping Billing Row TS - {hour_slice} as the Metrics are fetched with 1 day delay")
                continue
            billing_data = self.billing_handler.get_hourly_dataset(time_slice=hour_slice)
            if billing_data:
                metrics_found, metrics_data = self.metrics_handler.get_hourly_dataset(
                    time_slice=hour_slice, billing_mgmt=True
                )
                for filename, data_set in billing_data.items():
                    if not data_set.empty:
                        self.chargeback_handler.run_calculations(
                            time_slice=hour_slice,
                            billing_dataframe=data_set,
                            metrics_dataframe=metrics_data,
                        )
                        BILLING_PERSISTENCE_STORE.add_data_to_persistence_store(
                            org_id=self.org_id, key=(hour_slice,), value=filename
                        )
            # TODO: Need to add more status for when data is missing, cannot silently ignore.
            # Bad user experience otherwise.
        self.chargeback_handler.export_metrics_to_csv(
            output_basepath=STORAGE_PATH.get_path(org_id=self.org_id, dir_type=DirType.OutputData, ensure_exists=True)
        )


@dataclass(kw_only=True)
class CCloudOrgList:
    in_orgs: InitVar[List | None] = None
    in_days_in_memory: InitVar[int] = field(default=7)

    orgs: Dict[str, CCloudOrg] = field(default_factory=dict, init=False)

    def __post_init__(self, in_orgs, in_days_in_memory) -> None:
        req_count = 0
        for org_item in in_orgs:
            temp = CCloudOrg(
                in_org_details=org_item,
                in_days_in_memory=in_days_in_memory,
                org_id=org_item["id"] if org_item["id"] else req_count,
            )
            self.__add_org_to_cache(ccloud_org=temp)

    def __add_org_to_cache(self, ccloud_org: CCloudOrg) -> None:
        self.orgs[ccloud_org.org_id] = ccloud_org

    def execute_requests(self):
        for org_item in self.orgs.values():
            org_item.execute_requests()

    def run_calculations(self):
        for org_id, org in self.orgs.items():
            org.run_calculations()
