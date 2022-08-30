from argparse import Namespace
from enum import Enum, auto
from logging import debug
import os
from typing import Dict, List
import yaml
from ccloud.core import create_ccloud_request, execute_ccloud_request, get_http_connection
from data_processing.metrics_processing import metrics_dataframe
import helpers
from helpers import logged_method, timed_method


class DirType(Enum):
    MetricsData = auto()
    BillingsData = auto()
    OutputData = auto()


@logged_method
@timed_method
def try_parse_config_file(config_yaml_path: str) -> Dict:
    debug("Trying to parse Configuration File: " + config_yaml_path)
    with open(config_yaml_path, "r") as config_file:
        core_config = yaml.safe_load(config_file)
    helpers.env_parse_replace(core_config)
    return core_config


@timed_method
@logged_method
def locate_metrics_data(metrics_location: str):
    debug("Trying to locate Metrics files: " + metrics_location)
    pass


def locate_storage_path(
    basepath: str = os.getcwd(), base_dir: str = "output", dir_type: List[DirType] = DirType.MetricsData
) -> Dict[DirType, str]:
    ret = {}
    for item in dir_type:
        temp = os.path.join(basepath, base_dir, item.name, "")
        print(temp)
        if not os.path.exists(temp):
            os.makedirs(temp)
        ret[item] = temp
    return ret


@timed_method
@logged_method
def run_workflow(arg_flags: Namespace):
    core_config = try_parse_config_file(config_yaml_path=arg_flags.config_file)
    storage_path = locate_storage_path(dir_type=[DirType.MetricsData, DirType.BillingsData, DirType.OutputData])
    new_req = create_ccloud_request(request=core_config["configs"]["connection"][0]["requests"][0], intervals=3)
    connection = get_http_connection(ccloud_details=core_config["configs"]["connection"][0]["ccloud_details"])
    resp_code, resp_body = execute_ccloud_request(
        ccloud_url=arg_flags.ccloud_url, auth=connection, payload=new_req, timeout=200
    )
    curr_df = metrics_dataframe(aggregation_metric_name=new_req["aggregations"][0]["metric"], metrics_output=resp_body)
    curr_df.print_sample_df()
    curr_df.output_to_csv(storage_path[DirType.MetricsData])

