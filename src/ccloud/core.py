from dataclasses import dataclass, field
import requests
from requests.auth import HTTPBasicAuth
from ccloud.model import CCMEReq_CompareOp, CCMEReq_ConditionalOp, CCMEReq_Granularity, CCMEReq_UnaryOp
from helpers import logged_method, timed_method
from typing import Dict, List, Tuple
from copy import deepcopy
import datetime


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
    massaged_request: Dict = field(init=False)

    def __post_init__(self) -> None:
        self.create_ccloud_request(intervals=7)
        self._base_payload = None

    def create_ccloud_request(self, intervals: int = 7) -> Dict:
        req = deepcopy(self._base_payload)
        self.req_id = req.pop("id")
        req["filter"] = self.generate_filter_struct(req["filter"])
        req["intervals"] = self.generate_iso8601_dt_intervals(CCMEReq_Granularity.P1D.name, intervals=intervals)
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

    def generate_iso8601_dt_intervals(self, granularity: str, intervals: int = 7):
        curr_date = datetime.datetime.now(tz=datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        output = []
        for _ in range(intervals):
            curr_date = curr_date - datetime.timedelta(days=1)
            output.append(curr_date.isoformat() + "/" + granularity)
        return output


@dataclass(kw_only=True)
class CCloudOrg:
    HTTPConnection: CCloudConnection
    HTTPRequests: Dict[str, CCloudHTTPRequest]


def initialize_ccloud_entities(connections: List) -> Dict[str, CCloudOrg]:
    orgs = {}
    for conn in connections:
        api_key = conn["ccloud_details"]["api_key"]
        api_secret = conn["ccloud_details"]["api_secret"]
        http_connection = CCloudConnection(api_key=api_key, api_secret=api_secret)
        http_requests = {}
        for req in conn["requests"]:
            http_req = CCloudHTTPRequest(_base_payload=req)
            http_requests[http_req.req_id] = http_req
        orgs[conn["id"]] = CCloudOrg(HTTPConnection=http_connection, HTTPRequests=http_requests)
    return orgs


@timed_method
@logged_method
def execute_ccloud_request(ccloud_url: str, auth: HTTPBasicAuth, payload: Dict, **kwargs) -> Tuple[int, Dict]:
    resp = requests.post(url=ccloud_url, auth=auth, json=payload, **kwargs)
    return resp.status_code, resp.json()
