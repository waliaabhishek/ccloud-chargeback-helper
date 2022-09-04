import datetime
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

import requests
from data_processing.metrics_processing import metrics_dataframe
from helpers import sanitize_id, sanitize_metric_name
from requests.auth import HTTPBasicAuth

from ccloud.model import CCMEReq_CompareOp, CCMEReq_ConditionalOp, CCMEReq_Granularity, CCMEReq_UnaryOp


@dataclass(kw_only=True)
class CCloudConnection:
    api_key: str
    api_secret: str
    CCloudHTTPConnection: HTTPBasicAuth = field(init=False)

    def __post_init__(self) -> None:
        self.CCloudHTTPConnection = HTTPBasicAuth(username=self.api_key, password=self.api_secret)
        self.api_key = None
        self.api_secret = None


@dataclass(kw_only=True)
class CCloudHTTPRequest:
    _base_payload: Dict
    req_id: str = field(init=False)
    ccloud_url: str = field(default="https://api.telemetry.confluent.cloud/v2/metrics/cloud/query")
    aggregation_metric: str = field(init=False)
    days_in_memory: int = field(default=7)
    massaged_request: Dict = field(init=False)
    http_response: Dict[str, Dict] = field(default_factory=dict, init=False)
    metrics_dataframes: Dict[str, metrics_dataframe] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.http_response["data"] = []
        self.create_ccloud_request()
        self.aggregation_metric = sanitize_metric_name(self.massaged_request["aggregations"][0]["metric"])
        self._base_payload = None

    def create_ccloud_request(self) -> Dict:
        req = deepcopy(self._base_payload)
        self.req_id = sanitize_id(str(req.pop("id")))
        req["filter"] = self.generate_filter_struct(req["filter"])
        self.massaged_request = req

    def generate_filter_struct(self, filter: Dict) -> Dict:
        cluster_list = filter["value"]
        if filter["op"] in [member.name for member in CCMEReq_CompareOp]:
            if len(filter["value"]) == 1:
                return {"field": filter["field"], "op": filter["op"], "value": cluster_list[0]}
            elif "ALL_CLUSTERS" in cluster_list:
                # TODO: Add logic to get cluster list and create a compound filter.
                # currently using a list
                temp_cluster_list = ["lkc-pg5gx2", "lkc-pg5gx2"]
                temp_req = [{"field:": filter["field"], "op": filter["op"], "value": temp_cluster_list}]
                self.generate_filter_struct(temp_req)
            elif len(cluster_list) > 1:
                filter_list_1 = [
                    {"field": filter["field"], "op": CCMEReq_CompareOp.EQ.name, "value": c_id} for c_id in cluster_list
                ]
                out_test = {
                    "op": CCMEReq_ConditionalOp.AND.name,
                    "filters": filter_list_1,
                }
                return out_test
        elif filter["op"] in [member.name for member in CCMEReq_ConditionalOp]:
            # TODO: Not sure how to implement it yet.
            pass
        elif filter["op"] in [member.name for member in CCMEReq_UnaryOp]:
            # TODO:: not sure how to implement this yet either.
            pass

    # def inject_interval_into_request(self, date_range: Tuple):
    #     self.massaged_request["intervals"] = [date_range[0]]

    def execute_request(self, http_connection: CCloudConnection, date_range: Tuple, params={}):
        self.massaged_request["intervals"] = [date_range[2]]
        resp = requests.post(
            url=self.ccloud_url,
            auth=http_connection.CCloudHTTPConnection,
            json=self.massaged_request,
            params=params,
        )
        self.massaged_request.pop("intervals")
        if resp.status_code == 200:
            out_json = resp.json()
            self.http_response["data"].extend(out_json["data"])
            if (
                "meta" in out_json
                and "pagination" in out_json["meta"]
                and "next_page_token" in out_json["meta"]["pagination"]
                and out_json["meta"]["pagination"]["next_page_token"] is not None
            ):
                params["page_token"] = str(out_json["meta"]["pagination"]["next_page_token"])
                self.execute_request(http_connection=http_connection, date_range=date_range, params=params)
        else:
            raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)

    def find_datasets_to_evict(self) -> List[str]:
        temp = list(self.metrics_dataframes.keys())
        temp.sort(reverse=True)
        return temp[self.days_in_memory - 1 :]

    def add_dataframes(self, date_range: Tuple, output_basepath: str):
        for dataset in self.find_datasets_to_evict():
            self.metrics_dataframes.pop(dataset).output_to_csv(basepath=output_basepath)
        self.metrics_dataframes[str(date_range[1])] = metrics_dataframe(
            aggregation_metric_name=self.aggregation_metric, _metrics_output=self.http_response
        )
        self.http_response["data"] = []


@dataclass(kw_only=True)
class CCloudOrg:
    org_id: str
    metrics_intervals: List[Tuple] = field(init=False, default_factory=list)
    HTTPConnection: CCloudConnection
    HTTPRequests: Dict[str, CCloudHTTPRequest]

    def execute_all_requests(self, output_basepath: str, days_in_memory: int = 7):
        for req_name, request in self.HTTPRequests.items():
            for req_interval in self.generate_iso8601_dt_intervals(
                granularity=CCMEReq_Granularity.P1D.name, intervals=7
            ):
                request.execute_request(http_connection=self.HTTPConnection, date_range=req_interval)
                request.add_dataframes(date_range=req_interval, output_basepath=output_basepath)

    def export_metrics_to_csv(self, output_basepath: str):
        for req_name, request in self.HTTPRequests.items():
            for metrics_date, metrics_dataframe in request.metrics_dataframes.items():
                metrics_dataframe.output_to_csv(basepath=output_basepath)

    def generate_iso8601_dt_intervals(self, granularity: str, intervals: int = 7):
        curr_date = datetime.datetime.now(tz=datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        for _ in range(intervals):
            curr_date = curr_date - datetime.timedelta(days=1)
            curr = (curr_date, curr_date.date(), curr_date.isoformat() + "/" + granularity)
            self.metrics_intervals.append(curr)
            yield curr


def initialize_ccloud_entities(connections: List, days_in_memory: int = 7) -> Dict[str, CCloudOrg]:
    orgs = {}
    req_count = 0
    for conn in connections:
        org_id = sanitize_id(conn["id"]) if sanitize_id(conn["id"]) else req_count
        api_key = conn["ccloud_details"]["api_key"]
        api_secret = conn["ccloud_details"]["api_secret"]
        http_connection = CCloudConnection(api_key=api_key, api_secret=api_secret)
        http_requests = {}
        for req in conn["requests"]:
            req_count += 1
            http_req = CCloudHTTPRequest(_base_payload=req, days_in_memory=days_in_memory)
            http_requests[http_req.req_id if http_req.req_id else req_count] = http_req
        orgs[org_id] = CCloudOrg(HTTPConnection=http_connection, HTTPRequests=http_requests, org_id=org_id)
    return orgs
