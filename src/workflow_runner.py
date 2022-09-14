import threading
from argparse import Namespace
from logging import debug
from typing import Dict
import yaml

import helpers
from ccloud.org import CCloudOrgList
from helpers import logged_method, timed_method
from storage_mgmt import METRICS_PERSISTENCE_STORE, STORAGE_PATH, DirType, current_memory_usage, sync_to_file


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
    thread1 = threading.Thread(target=sync_to_file, args=(METRICS_PERSISTENCE_STORE, 3))
    thread2 = threading.Thread(target=current_memory_usage, args=(METRICS_PERSISTENCE_STORE, 5))
    thread1.start()
    thread2.start()

    ccloud_orgs = CCloudOrgList(
        _orgs=core_config["config"]["org_details"],
        _days_in_memory=days_in_memory,
    )

    ccloud_orgs.execute_requests()

    # for org_key, org in ccloud_orgs.items():
    #     org.export_metrics_to_csv(output_basepath=STORAGE_PATH[DirType.MetricsData])

    METRICS_PERSISTENCE_STORE.stop_sync()
    print("Waiting for State Sync ticker for Final sync before exit")
    thread1.join()
    thread2.join()
