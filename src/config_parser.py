from argparse import Namespace
from enum import Enum, auto
from logging import debug
import os
from pprint import pprint
from typing import Dict, List
import yaml
from ccloud.core import CCloudHTTPRequest, initialize_ccloud_entities
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
    
    ccloud_orgs = initialize_ccloud_entities(
        connections=core_config["config"]["connection"], days_in_memory=core_config["system"]["days_in_memory"]
    )
    for org_key, org in ccloud_orgs.items():
        org.execute_all_requests(output_basepath=storage_path[DirType.MetricsData])
        org.export_metrics_to_csv(output_basepath=storage_path[DirType.MetricsData])
