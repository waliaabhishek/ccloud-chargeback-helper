from copy import deepcopy
from dataclasses import dataclass, field
import datetime
from time import sleep
from typing import Dict, List, Set, Tuple
import pandas as pd
from ccloud.connections import CCloudBase, CCloudConnection, EndpointURL
from ccloud.core_api.api_keys import CCloudAPIKeyList
from ccloud.core_api.clusters import CCloudClusterList
from ccloud.core_api.connectors import CCloudConnectorList
from ccloud.core_api.environments import CCloudEnvironmentList
from ccloud.core_api.ksqldb_clusters import CCloudKsqldbClusterList
from ccloud.core_api.service_accounts import CCloudServiceAccountList
from ccloud.core_api.user_accounts import CCloudUserAccountList
from ccloud.telemetry_api.billings_csv import CCloudBillingDataset
from ccloud.telemetry_api.telemetry import CCloudMetricsDataset
from ccloud.model import CCMEReq_CompareOp, CCMEReq_ConditionalOp, CCMEReq_Granularity, CCMEReq_UnaryOp
from data_processing.metrics_processing import METRICS_CSV_COLUMNS
from helpers import sanitize_id, sanitize_metric_name
from storage_mgmt import METRICS_PERSISTENCE_STORE, STORAGE_PATH, DirType
from workflow_runner import BILLING_METRICS_SCOPE


@dataclass
class CCloudBillingHandler:
    billing_dataset: CCloudBillingDataset = field(init=False)
    available_hour_slices_in_dataset: List[str] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        self.billing_dataset = CCloudBillingDataset()

    def read_all(self):
        self.billing_dataset.read_all()
        self.available_hour_slices_in_dataset = sorted(list(self.__calculate_daterange_in_all_datasets()))

    def execute_requests(self):
        self.billing_dataset.read_all()

    def __calculate_daterange_in_all_datasets(self) -> Set[str]:
        out = set()
        for _, billing_dataframe in self.billing_dataset.billing_dataframes.items():
            out = out.union(billing_dataframe.hourly_date_range)
        return out

    def get_hourly_dataset(self, date_value: datetime.datetime) -> pd.DataFrame:
        out = pd.DataFrame()
        for filename, billing_dataframe in self.billing_dataset.billing_dataframes.items():
            file_level_df = billing_dataframe.get_hourly_dataset(datetime_slice_iso_format=date_value)
            out = pd.concat([out, file_level_df])
        return out


@dataclass
class CCloudMetricsHandler(CCloudBase):
    _requests: List
    days_in_memory: int = field(default=7)

    metrics_dataset: Dict[str, CCloudMetricsDataset] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.url = self._ccloud_connection.get_endpoint_url(
            key=self._ccloud_connection.uri.telemetry_query_metrics
        ).format(dataset="cloud")
        self.read_all()
        self._requests = None

    def read_all(self):
        for req in self._requests:
            http_req = CCloudMetricsDataset(
                _base_payload=req,
                ccloud_url=self.url,
                days_in_memory=self.days_in_memory,
            )
            self.__add_to_cache(http_req=http_req)

    def __add_to_cache(self, http_req: CCloudMetricsDataset) -> None:
        if http_req.req_id == "":
            http_req.req_id = str(len(self.metrics_dataset))
        self.metrics_dataset[http_req.req_id] = http_req

    def execute_requests(self, output_basepath: str):
        for req_name, request in self.metrics_dataset.items():
            for req_interval in self.generate_iso8601_dt_intervals(
                granularity=CCMEReq_Granularity.P1D.name, metric_name=request.aggregation_metric, intervals=7
            ):
                request.execute_request(http_connection=self._ccloud_connection, date_range=req_interval)
                request.add_dataframes(date_range=req_interval, output_basepath=output_basepath)

    def export_metrics_to_csv(self, output_basepath: str):
        for req_name, request in self.metrics_dataset.items():
            for metrics_date, metrics_dataframe in request.metrics_dataframes.items():
                metrics_dataframe.output_to_csv(basepath=output_basepath)

    def generate_iso8601_dt_intervals(self, granularity: str, metric_name: str, intervals: int = 7):
        curr_date = datetime.datetime.now(tz=datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        for _ in range(intervals):
            curr_date = curr_date - datetime.timedelta(days=1)
            curr = (curr_date, curr_date.date(), curr_date.isoformat() + "/" + granularity)
            if not METRICS_PERSISTENCE_STORE.is_dataset_present(date_value=str(curr[1]), metric_name=metric_name):
                yield curr
            else:
                print(f"Dataset already available for metric {metric_name} on {curr[1]}")

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


@dataclass
class CCloudObjectsHandler:
    _ccloud_connection: CCloudConnection

    last_refresh: datetime = field(init=False, default=None)
    min_refresh_gap: datetime.timedelta = field(init=False, default=datetime.timedelta(minutes=60))
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
            print("Not refreshing the CCloud Object state as TimeDelta is not enough.")
        else:
            self.cc_sa = CCloudServiceAccountList(_ccloud_connection=self._ccloud_connection)
            self.cc_users = CCloudUserAccountList(_ccloud_connection=self._ccloud_connection)
            self.cc_api_keys = CCloudAPIKeyList(_ccloud_connection=self._ccloud_connection)
            self.cc_environments = CCloudEnvironmentList(_ccloud_connection=self._ccloud_connection)
            self.cc_clusters = CCloudClusterList(
                _ccloud_connection=self._ccloud_connection, ccloud_env=self.cc_environments
            )
            self.cc_connectors = CCloudConnectorList(
                _ccloud_connection=self._ccloud_connection,
                ccloud_kafka_clusters=self.cc_clusters,
                ccloud_service_accounts=self.cc_sa,
                ccloud_users=self.cc_users,
                ccloud_api_keys=self.cc_api_keys,
            )
            self.cc_ksqldb_clusters = CCloudKsqldbClusterList(
                _ccloud_connection=self._ccloud_connection,
                ccloud_envs=self.cc_environments,
            )
            self.last_refresh = datetime.datetime.now()

    def execute_requests(self):
        print(f"Trying to refresh CCloud Object Store State for corelation")
        self.read_all()


@dataclass(kw_only=True)
class CCloudOrg:
    _org_details: List
    _days_in_memory: int = field(default=7)
    org_id: str

    metrics_handler: CCloudMetricsHandler = field(init=False)
    objects_handler: CCloudObjectsHandler = field(init=False)
    billing_handler: CCloudBillingHandler = field(init=False)

    def __post_init__(self) -> None:
        self.org_id = sanitize_id(self._org_details["id"])
        temp_conn = CCloudConnection(
            api_key=self._org_details["ccloud_details"]["telemetry"]["api_key"],
            api_secret=self._org_details["ccloud_details"]["telemetry"]["api_secret"],
            base_url=EndpointURL.TELEMETRY_URL,
        )
        self.metrics_handler = CCloudMetricsHandler(
            _ccloud_connection=temp_conn,
            _requests=self._org_details["requests"],
            days_in_memory=self._days_in_memory,
        )

        temp_conn = CCloudConnection(
            api_key=self._org_details["ccloud_details"]["metrics"]["api_key"],
            api_secret=self._org_details["ccloud_details"]["metrics"]["api_secret"],
            base_url=EndpointURL.API_URL,
        )
        self.objects_handler = CCloudObjectsHandler(_ccloud_connection=temp_conn)

        self.billing_handler = CCloudBillingHandler()
        self._requests = None

    def execute_requests(self):
        print(f"Gathering Telemetry Data")
        self.metrics_handler.execute_requests(output_basepath=STORAGE_PATH[DirType.MetricsData])
        print(f"Gathering CCloud Existing Objects Data")
        self.objects_handler.execute_requests()
        print(f"Checking for new Billing CSV Files")
        self.billing_handler.execute_requests()

    def find_available_hour_slices_in_billing_datasets(self) -> datetime.datetime:
        for item in self.billing_handler.available_hour_slices_in_dataset:
            yield datetime.datetime.fromisoformat(item)


@dataclass(kw_only=True)
class CCloudOrgList:
    _orgs: List
    _days_in_memory: int = field(default=7)

    orgs: Dict[str, CCloudOrg] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        req_count = 0
        for org_item in self._orgs:
            temp = CCloudOrg(
                _org_details=org_item,
                _days_in_memory=self._days_in_memory,
                org_id=org_item["id"] if org_item["id"] else req_count,
            )
            self.__add_org_to_cache(ccloud_org=temp)
        self._orgs = None

    def __add_org_to_cache(self, ccloud_org: CCloudOrg) -> None:
        self.orgs[ccloud_org.org_id] = ccloud_org

    def execute_requests(self):
        for org_item in self.orgs.values():
            org_item.execute_requests()
