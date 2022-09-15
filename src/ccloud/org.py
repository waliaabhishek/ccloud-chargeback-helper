from dataclasses import dataclass, field
import datetime
from time import sleep
from typing import Dict, List

from ccloud.connections import CCloudBase, CCloudConnection, EndpointURL
from ccloud.core_api.api_keys import CCloudAPIKeyList
from ccloud.core_api.clusters import CCloudClusterList
from ccloud.core_api.connectors import CCloudConnectorList
from ccloud.core_api.environments import CCloudEnvironmentList
from ccloud.core_api.ksqldb_clusters import CCloudKsqldbClusterList
from ccloud.core_api.service_accounts import CCloudServiceAccountList
from ccloud.core_api.user_accounts import CCloudUserAccountList
from ccloud.telemetry_api.billings_csv import CCloudBillingDataset
from ccloud.telemetry_api.telemetry import CCloudTelemetryDataset
from ccloud.model import CCMEReq_Granularity
from helpers import sanitize_id
from storage_mgmt import METRICS_PERSISTENCE_STORE, STORAGE_PATH, DirType


@dataclass
class CCloudBillingHandler:
    billing_data: CCloudBillingDataset = field(init=False)

    def __post_init__(self) -> None:
        self.billing_data = CCloudBillingDataset()

    def read_all(self):
        self.billing_data.read_all()

    def execute_requests(self):
        self.billing_data.read_all()


@dataclass
class CCloudTelemetryHandler(CCloudBase):
    _requests: List
    days_in_memory: int = field(default=7)

    telemetry_requests: Dict[str, CCloudTelemetryDataset] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.url = self._ccloud_connection.get_endpoint_url(
            key=self._ccloud_connection.uri.telemetry_query_metrics
        ).format(dataset="cloud")
        self.read_all()
        self._requests = None

    def read_all(self):
        for req in self._requests:
            http_req = CCloudTelemetryDataset(
                _base_payload=req,
                ccloud_url=self.url,
                days_in_memory=self.days_in_memory,
            )
            self.__add_to_cache(http_req=http_req)

    def __add_to_cache(self, http_req: CCloudTelemetryDataset) -> None:
        if http_req.req_id == "":
            http_req.req_id = str(len(self.telemetry_requests))
        self.telemetry_requests[http_req.req_id] = http_req

    def execute_requests(self, output_basepath: str):
        for req_name, request in self.telemetry_requests.items():
            for req_interval in self.generate_iso8601_dt_intervals(
                granularity=CCMEReq_Granularity.P1D.name, metric_name=request.aggregation_metric, intervals=7
            ):
                request.execute_request(http_connection=self._ccloud_connection, date_range=req_interval)
                request.add_dataframes(date_range=req_interval, output_basepath=output_basepath)

    def export_metrics_to_csv(self, output_basepath: str):
        for req_name, request in self.telemetry_requests.items():
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


@dataclass
class CCloudMetricsHandler:
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

    telemetry_data: CCloudTelemetryHandler = field(init=False)
    metrics_data: CCloudMetricsHandler = field(init=False)
    billing_data: CCloudBillingHandler = field(init=False)

    def __post_init__(self) -> None:
        self.org_id = sanitize_id(self._org_details["id"])
        temp_conn = CCloudConnection(
            api_key=self._org_details["ccloud_details"]["telemetry"]["api_key"],
            api_secret=self._org_details["ccloud_details"]["telemetry"]["api_secret"],
            base_url=EndpointURL.TELEMETRY_URL,
        )
        self.telemetry_data = CCloudTelemetryHandler(
            _ccloud_connection=temp_conn,
            _requests=self._org_details["requests"],
            days_in_memory=self._days_in_memory,
        )

        temp_conn = CCloudConnection(
            api_key=self._org_details["ccloud_details"]["metrics"]["api_key"],
            api_secret=self._org_details["ccloud_details"]["metrics"]["api_secret"],
            base_url=EndpointURL.API_URL,
        )
        self.metrics_data = CCloudMetricsHandler(_ccloud_connection=temp_conn)

        self.billing_data = CCloudBillingHandler()
        self._requests = None

    def execute_requests(self):
        print(f"Gathering Telemetry Data")
        self.telemetry_data.execute_requests(output_basepath=STORAGE_PATH[DirType.MetricsData])
        print(f"Gathering CCloud Existing Objects Data")
        self.metrics_data.execute_requests()
        print(f"Checking for new Billing CSV Files")
        self.billing_data.execute_requests()


@dataclass(kw_only=True)
class CCloudOrgList:
    _orgs: List
    _days_in_memory: int = field(default=7)

    org: Dict[str, CCloudOrg] = field(default_factory=dict, init=False)

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
        self.org[ccloud_org.org_id] = ccloud_org

    def execute_requests(self):
        for org_item in self.org.values():
            org_item.execute_requests()
