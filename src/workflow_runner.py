from argparse import Namespace
from dataclasses import dataclass, field
from enum import Enum, auto
from logging import debug
import threading
from time import sleep
from typing import Dict
import prometheus_client
import yaml

from ccloud.org import CCloudOrgList
from helpers import env_parse_replace, logged_method, timed_method
import internal_data_probe
from storage_mgmt import COMMON_THREAD_RUNNER, current_memory_usage


@dataclass(kw_only=True)
class AppProps:
    days_in_memory: int = field(default=30)
    relative_output_dir: str = field(default="output")


def get_app_props(in_config: Dict):
    global APP_PROPS

    if not in_config["system"]:
        APP_PROPS = AppProps()
    else:
        config: Dict = in_config["system"]
        APP_PROPS = AppProps(
            days_in_memory=config.get("days_in_memory", 7),
            relative_output_dir=config.get("output_dir_name", "output"),
        )


class WorkflowStage(Enum):
    GATHER = auto()
    CALCULATE_OUTPUT = auto()
    SLEEP = auto()


@logged_method
@timed_method
def try_parse_config_file(config_yaml_path: str) -> Dict:
    debug("Trying to parse Configuration File: " + config_yaml_path)
    with open(config_yaml_path, "r") as config_file:
        core_config = yaml.safe_load(config_file)
    env_parse_replace(core_config)
    return core_config


@logged_method
@timed_method
def run_gather_cycle(ccloud_orgs: CCloudOrgList):
    # This will try to refresh and read all the data that might be new from the last gather phase.
    # Org Object has built in safeguard to prevent repetitive gathering for the same datasets.
    # for Cloud Objects --> 30 minutes is the minimum.
    # for Metrics API objects --> persistence store knows what all has been cached and written to disk and will not be gathered again.
    # for billing CSV files --> if the data is already read in memory, it wont be read in again.
    ccloud_orgs.execute_requests()


@timed_method
@logged_method
def run_calculate_cycle(ccloud_orgs: CCloudOrgList):
    ccloud_orgs.run_calculations()


@timed_method
@logged_method
def execute_workflow(arg_flags: Namespace):
    core_config = try_parse_config_file(config_yaml_path=arg_flags.config_file)
    get_app_props(core_config["config"])

    thread_configs = [
        # [COMMON_THREAD_RUNNER, current_memory_usage, 5],
        # [METRICS_PERSISTENCE_STORE, sync_to_file, METRICS_PERSISTENCE_STORE.flush_to_disk_interval_sec],
        # [CHARGEBACK_PERSISTENCE_STORE, sync_to_file, CHARGEBACK_PERSISTENCE_STORE.flush_to_disk_interval_sec],
        # [BILLING_PERSISTENCE_STORE, sync_to_file, BILLING_PERSISTENCE_STORE.flush_to_disk_interval_sec],
    ]

    threads_list = list()
    for _, item in enumerate(thread_configs):
        threads_list.append(item[0].get_new_thread(target_func=item[1], tick_duration_secs=item[2]))

    prometheus_client.start_http_server(8000)
    threading.Thread(target=internal_data_probe.internal_api.run, args=("0.0.0.0", "8001")).start()

    try:
        for item in threads_list:
            item.start()

        # This step will initialize the CCloudOrg structure along with all the internal Objects in it.
        # Those will include the first run for all the data gather step as well.
        # There are some safeguards already implemented to prevent request choking, so, it should be safe in most use cases.
        ccloud_orgs = CCloudOrgList(
            in_orgs=core_config["config"]["org_details"],
            in_days_in_memory=APP_PROPS.days_in_memory,
        )
        internal_data_probe.set_readiness(readiness_flag=True)

        while True:
            sleep(10**8)

    finally:
        # Begin shutdown process.
        for item in thread_configs:
            item[0].stop_sync()
        print("Waiting for State Sync ticker for Final sync before exit")
        for item in threads_list:
            item.join()
