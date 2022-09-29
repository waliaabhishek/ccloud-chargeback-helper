from enum import Enum, auto
import threading
from argparse import Namespace
from logging import debug
from typing import Dict
import yaml
from data_processing.billing_processing import BillingDataframe
import datetime


import helpers
from ccloud.org import CCloudOrg, CCloudOrgList
from helpers import logged_method, sanitize_metric_name, timed_method
from storage_mgmt import METRICS_PERSISTENCE_STORE, STORAGE_PATH, DirType, current_memory_usage, sync_to_file


BILLING_METRICS_SCOPE = {
    "request_bytes": sanitize_metric_name("io.confluent.kafka.server/request_bytes"),
    "response_bytes": sanitize_metric_name("io.confluent.kafka.server/response_bytes"),
}


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
    helpers.env_parse_replace(core_config)
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
        org.metrics_handler.export_metrics_to_csv(output_basepath=STORAGE_PATH[DirType.MetricsData])


def run_calculate_cycle(ccloud_orgs: CCloudOrgList):
    # for org_id, org in ccloud_orgs.orgs.items():
    #     org: CCloudOrg = org
    #     for file_path, billing_dataframe in org.billing_handler.billing_dataset.billing_dataframes.items():
    #         for _, billing_dict in billing_dataframe.get_all_datasets():
    #             billing_dict: BillingDataframe = billing_dict
    #             for dataset_date, billing_dataframe in billing_dict.get_hourly_dataset():
    #                 # TODO: Read the date and then find the corresponding data from telemetry dataset in files.
    #                 # Read the data and then start calculating the output dataset.
    #                 dataset_date = datetime.datetime.fromisoformat(dataset_date).date()
    #                 pass
    for org_id, org in ccloud_orgs.orgs.items():
        for hour_slice in org.find_available_hour_slices_in_billing_datasets():
            billing_data = org.billing_handler.get_hourly_dataset(hour_slice)
            metrics_found, metrics_data = org.metrics_handler.get_hourly_dataset(
                date_value=hour_slice, billing_mgmt=True
            )


@timed_method
@logged_method
def execute_workflow(arg_flags: Namespace):
    core_config = try_parse_config_file(config_yaml_path=arg_flags.config_file)
    days_in_memory = core_config["config"]["system"]["days_in_memory"]
    thread1 = threading.Thread(target=sync_to_file, args=(METRICS_PERSISTENCE_STORE, 3))
    thread2 = threading.Thread(target=current_memory_usage, args=(METRICS_PERSISTENCE_STORE, 5))
    thread1.start()
    thread2.start()

    # This step will initialize the CCloudOrg structure along with all the internal Objects in it.
    # Those will include the first run for all the data gather step as well.
    # There are some safeguards already implemented to prevent request choking, so, it should be safe in most use cases.
    ccloud_orgs = CCloudOrgList(
        _orgs=core_config["config"]["org_details"],
        _days_in_memory=days_in_memory,
    )

    run_gather_cycle(ccloud_orgs=ccloud_orgs)

    # Begin shutdown process.
    METRICS_PERSISTENCE_STORE.stop_sync()
    print("Waiting for State Sync ticker for Final sync before exit")
    thread1.join()
    thread2.join()
