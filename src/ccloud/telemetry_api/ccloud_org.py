from dataclasses import dataclass, field
import datetime
from typing import Dict, List

from ccloud.connections import CCloudBase, CCloudConnection
from ccloud.metrics import CCloudHTTPRequest
from ccloud.model import CCMEReq_Granularity
from helpers import sanitize_id
from storage_mgmt import PERSISTENCE_STORE


@dataclass
class CCloudOrg(CCloudBase):
    _requests: List
    org_id: str
    http_request: Dict[str, CCloudHTTPRequest] = field(init=False)
    days_in_memory: int = field(default=7)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.url = self._ccloud_connection.get_endpoint_url(
            key=self._ccloud_connection.uri.telemetry_query_metrics
        ).format(dataset="cloud")
        self.initialize_requests()
        self._requests = None

    def initialize_requests(self):
        req_count = 0
        for req in self._requests:
            http_req = CCloudHTTPRequest(_base_payload=req, days_in_memory=self.days_in_memory)
            self.__add_requests_to_cache(ccloud_http_req=http_req, req_count=req_count)
            req_count += 1
        pass

    def __add_requests_to_cache(self, ccloud_http_req: CCloudHTTPRequest, req_count: int = 0) -> None:
        self.http_request[ccloud_http_req.req_id if ccloud_http_req.req_id else str(req_count)] = ccloud_http_req

    def execute_all_requests(self, output_basepath: str, days_in_memory: int = 7):
        for req_name, request in self.http_request.items():
            for req_interval in self.generate_iso8601_dt_intervals(
                granularity=CCMEReq_Granularity.P1D.name, metric_name=request.aggregation_metric, intervals=7
            ):
                request.execute_request(http_connection=self._ccloud_connection, date_range=req_interval)
                request.add_dataframes(date_range=req_interval, output_basepath=output_basepath)

    def export_metrics_to_csv(self, output_basepath: str):
        for req_name, request in self.http_request.items():
            for metrics_date, metrics_dataframe in request.metrics_dataframes.items():
                metrics_dataframe.output_to_csv(basepath=output_basepath)

    def generate_iso8601_dt_intervals(self, granularity: str, metric_name: str, intervals: int = 7):
        curr_date = datetime.datetime.now(tz=datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        for _ in range(intervals):
            curr_date = curr_date - datetime.timedelta(days=1)
            curr = (curr_date, curr_date.date(), curr_date.isoformat() + "/" + granularity)
            # self.metrics_intervals.append(curr)
            if not PERSISTENCE_STORE.is_dataset_present(date_value=str(curr[1]), metric_name=metric_name):
                yield curr
            else:
                print(f"Dataset already available for metric {metric_name} on {curr[1]}")


@dataclass(kw_only=True)
class CCloudOrgList:
    org: Dict[str, CCloudOrg] = field(default_factory=dict, init=False)

    def initialize_ccloud_entities(self, connections: List, days_in_memory: int = 7) -> Dict[str, CCloudOrg]:
        req_count = 0
        for conn in connections:
            self.__initialize_telemetry_entity(org_details=conn)

    def __initialize_telemetry_entity(self, org_details: List, req_count: int, days_in_emory: int = 7):
        org_id = sanitize_id(org_details["id"]) if sanitize_id(org_details["id"]) else req_count
        telemetry_api_connection = CCloudConnection(
            api_key=org_details["ccloud_details"]["telemetry"]["api_key"],
            api_secret=org_details["ccloud_details"]["telemetry"]["api_key"],
        )
        org = CCloudOrg(
            _ccloud_connection=telemetry_api_connection,
            _requests=org_details["requests"],
            org_id=org_id,
        )
        self.__add_org_to_cache(ccloud_org=org)

    def __add_org_to_cache(self, ccloud_org: CCloudOrg) -> None:
        self.org[ccloud_org.org_id] = ccloud_org
