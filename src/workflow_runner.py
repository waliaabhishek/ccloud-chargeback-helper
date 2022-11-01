from dataclasses import dataclass, field
from enum import Enum, auto
import threading
from argparse import Namespace
from logging import debug
from typing import Dict
import yaml
from ccloud.org import CCloudOrgList
from helpers import logged_method, sanitize_metric_name, timed_method, env_parse_replace
from storage_mgmt import (
    BILLING_PERSISTENCE_STORE,
    CHARGEBACK_PERSISTENCE_STORE,
    COMMON_THREAD_RUNNER,
    METRICS_PERSISTENCE_STORE,
    STORAGE_PATH,
    DirType,
    current_memory_usage,
    sync_to_file,
)


@dataclass(kw_only=True)
class AppProps:
    days_in_memory: int = field(default=7)
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
    # Org Object has built in safeguard to prevent repetitive gathering. for the same datasets.
    # for Cloud Objects --> 1 hour is the minimum.
    # for telemetry objects --> persistence store knows what all has been cached and written to disk and will not be gathered again.
    # for billing CSV files --> if the data is already read in memory, it wont be read in again.
    ccloud_orgs.execute_requests()

    #  Invoke write to Disk.
    for org in ccloud_orgs.orgs.values():
        org.metrics_handler.export_metrics_to_csv(
            output_basepath=STORAGE_PATH.get_path(org_id=org.org_id, dir_type=DirType.MetricsData, ensure_exists=True)
        )


@timed_method
@logged_method
def run_calculate_cycle(ccloud_orgs: CCloudOrgList):
    ccloud_orgs.run_calculations()


@timed_method
@logged_method
def execute_workflow(arg_flags: Namespace):
    core_config = try_parse_config_file(config_yaml_path=arg_flags.config_file)
    get_app_props(core_config["config"])

    thread1 = COMMON_THREAD_RUNNER.get_new_thread(target_func=current_memory_usage, tick_duration_secs=5)
    thread2 = METRICS_PERSISTENCE_STORE.get_new_thread(
        target_func=sync_to_file, tick_duration_secs=METRICS_PERSISTENCE_STORE.flush_to_disk_interval_sec
    )
    thread3 = CHARGEBACK_PERSISTENCE_STORE.get_new_thread(
        target_func=sync_to_file, tick_duration_secs=CHARGEBACK_PERSISTENCE_STORE.flush_to_disk_interval_sec
    )
    thread4 = BILLING_PERSISTENCE_STORE.get_new_thread(
        target_func=sync_to_file, tick_duration_secs=BILLING_PERSISTENCE_STORE.flush_to_disk_interval_sec
    )
    try:
        thread1.start()
        thread2.start()
        thread3.start()
        thread4.start()

        # This step will initialize the CCloudOrg structure along with all the internal Objects in it.
        # Those will include the first run for all the data gather step as well.
        # There are some safeguards already implemented to prevent request choking, so, it should be safe in most use cases.
        ccloud_orgs = CCloudOrgList(
            _orgs=core_config["config"]["org_details"],
            _days_in_memory=APP_PROPS.days_in_memory,
        )

        run_gather_cycle(ccloud_orgs=ccloud_orgs)

        run_calculate_cycle(ccloud_orgs=ccloud_orgs)
    finally:
        # Begin shutdown process.
        METRICS_PERSISTENCE_STORE.stop_sync()
        CHARGEBACK_PERSISTENCE_STORE.stop_sync()
        BILLING_PERSISTENCE_STORE.stop_sync()
        COMMON_THREAD_RUNNER.stop_sync()
        print("Waiting for State Sync ticker for Final sync before exit")
        thread1.join()
        thread2.join()
        thread3.join()
        thread4.join()
