import datetime
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

import requests
from ccloud.connections import CCloudConnection
from data_processing.metrics_processing import metrics_dataframe
from helpers import sanitize_id, sanitize_metric_name
from requests.auth import HTTPBasicAuth

from ccloud.model import CCMEReq_CompareOp, CCMEReq_ConditionalOp, CCMEReq_Granularity, CCMEReq_UnaryOp
from storage_mgmt import METRICS_PERSISTENCE_STORE


@dataclass(kw_only=True)
class CCloudTelemetryDataset:
    _base_payload: Dict
    ccloud_url: str = field(default=None)
    days_in_memory: int = field(default=7)

    req_id: str = field(init=False)
    aggregation_metric: str = field(init=False)
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
        req["filter"] = self.generate_filter_struct(filter=req["filter"])
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
                temp_req = {"field:": filter["field"], "op": filter["op"], "value": temp_cluster_list}
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

    def execute_request(self, http_connection: CCloudConnection, date_range: Tuple, params={}):
        self.massaged_request["intervals"] = [date_range[2]]
        resp = requests.post(
            url=self.ccloud_url,
            auth=http_connection.http_connection,
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
