import threading
from argparse import Namespace
from logging import debug
from typing import Dict

import yaml

import helpers
from ccloud.main import initialize_ccloud_objects
from ccloud.org import CCloudOrgList
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
def execute_workflow(arg_flags: Namespace):
    core_config = try_parse_config_file(config_yaml_path=arg_flags.config_file)
    days_in_memory = core_config["config"]["system"]["days_in_memory"]
    thread = threading.Thread(target=sync_to_file, args=(PERSISTENCE_STORE, 5))
    thread.start()

    ccloud_orgs = CCloudOrgList(
        _orgs=core_config["config"]["org_details"],
        _days_in_memory=days_in_memory,
    )

    ccloud_orgs.execute_requests()

    for org_key, org in ccloud_orgs.items():
        org.export_metrics_to_csv(output_basepath=STORAGE_PATH[DirType.MetricsData])

    PERSISTENCE_STORE.stop_sync()
    thread.join()
