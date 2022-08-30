import requests
from requests.auth import HTTPBasicAuth
from ccloud.model import CCMEReq_CompareOp, CCMEReq_ConditionalOp, CCMEReq_Granularity, CCMEReq_UnaryOp
from helpers import logged_method, timed_method
from typing import Dict, Tuple
from copy import deepcopy
import datetime


@timed_method
@logged_method
def get_http_connection(ccloud_details: Dict) -> HTTPBasicAuth:
    return HTTPBasicAuth(username=ccloud_details["api_key"], password=ccloud_details["api_secret"])


@timed_method
@logged_method
def generate_filter_struct(filter: Dict) -> Dict:
    cluster_list = filter["value"]
    if filter["op"] in [member.name for member in CCMEReq_CompareOp]:
        if len(filter["value"]) == 1:
            return {"field": filter["field"], "op": filter["op"], "value": cluster_list[0]}
        elif "ALL_CLUSTERS" in cluster_list:
            # TODO: Add logic to get cluster list and create a compound filter.
            # currently using a list
            temp_cluster_list = ["lkc-pg5gx2", "lkc-pg5gx2"]
            temp_req = [{"field:": filter["field"], "op": filter["op"], "value": temp_cluster_list}]
            generate_filter_struct(temp_req)
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


@timed_method
@logged_method
def generate_iso8601_dt_intervals(granularity: str, intervals: int = 7):
    curr_date = datetime.datetime.now(tz=datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    output = []
    for _ in range(intervals):
        curr_date = curr_date - datetime.timedelta(days=1)
        output.append(curr_date.isoformat() + "/" + granularity)
    return output


@timed_method
@logged_method
def create_ccloud_request(request: Dict, intervals: int = 7) -> Dict:
    req = deepcopy(request)
    req.pop("id")
    req["filter"] = generate_filter_struct(req["filter"])
    out_intervals = generate_iso8601_dt_intervals(CCMEReq_Granularity.P1D.name, intervals=intervals)
    print(out_intervals)
    req["intervals"] = out_intervals
    return req


@timed_method
@logged_method
def execute_ccloud_request(ccloud_url: str, auth: HTTPBasicAuth, payload: Dict, **kwargs) -> Tuple[int, Dict]:
    resp = requests.post(url=ccloud_url, auth=auth, json=payload, **kwargs)
    return resp.status_code, resp.json()
