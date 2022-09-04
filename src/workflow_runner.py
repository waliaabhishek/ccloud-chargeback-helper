import threading
from argparse import Namespace
from logging import debug
from typing import Dict

import yaml

import helpers
from ccloud.core import initialize_ccloud_entities
from helpers import logged_method, timed_method
from storage_mgmt import PERSISTENCE_STORE, STORAGE_PATH, DirType, sync_to_file


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
def locate_existing_metrics_data(metrics_location: str):
    debug("Trying to locate Metrics files: " + metrics_location)
    pass


@timed_method
@logged_method
def run_workflow(arg_flags: Namespace):
    core_config = try_parse_config_file(config_yaml_path=arg_flags.config_file)
    existing_metrics_data = locate_existing_metrics_data(STORAGE_PATH[DirType.MetricsData])
    thread = threading.Thread(target=sync_to_file, args=(PERSISTENCE_STORE, 5))
    thread.start()

    ccloud_orgs = initialize_ccloud_entities(
        connections=core_config["config"]["connection"],
        days_in_memory=core_config["config"]["system"]["days_in_memory"],
    )
    for org_key, org in ccloud_orgs.items():
        org.execute_all_requests(
            output_basepath=STORAGE_PATH[DirType.MetricsData],
            days_in_memory=core_config["config"]["system"]["days_in_memory"],
        )
        org.export_metrics_to_csv(output_basepath=STORAGE_PATH[DirType.MetricsData])
    PERSISTENCE_STORE.stop_sync()
    thread.join()
