import logging
import threading
from argparse import Namespace
from dataclasses import dataclass, field
from enum import Enum, auto
from time import sleep
from typing import Dict, List

import prometheus_client
import yaml

import internal_data_probe
from ccloud.org import CCloudOrgList
from helpers import (
    env_parse_replace,
    logged_method,
    set_breadcrumb_flag,
    set_logger_level,
)

LOGGER = logging.getLogger(__name__)


@dataclass(kw_only=True)
class AppProps:
    days_in_memory: int = field(default=30)
    relative_output_dir: str = field(default="output")
    loglevel: str = field(default="INFO")


@logged_method
def get_app_props(in_config: Dict):
    global APP_PROPS

    system_config: Dict = in_config.get("chargeback", {}).get("system", {})

    if not system_config:
        LOGGER.warning("No System Configuration found. Using default values.")
        APP_PROPS = AppProps()
        set_breadcrumb_flag(False)
        set_logger_level(logging.INFO)
        return

    LOGGER.debug("System Configuration found. Using values from config file.")
    LOGGER.debug("Parsing loglevel from config file")
    log_lvl: str = system_config.get("logging",{}).get("log_level", "INFO").upper()
    match log_lvl:
        case "DEBUG":
            LOGGER.info("Setting loglevel to DEBUG")
            loglevel = logging.DEBUG
        case "INFO":
            LOGGER.info("Setting loglevel to INFO")
            loglevel = logging.INFO
        case "WARNING":
            LOGGER.info("Setting loglevel to WARNING")
            loglevel = logging.WARNING
        case "ERROR":
            LOGGER.info("Setting loglevel to ERROR")
            loglevel = logging.ERROR
        case _:
            LOGGER.info(
                f"Cannot understand log level {log_lvl}. Setting loglevel to INFO"
            )
            loglevel = logging.INFO
    set_logger_level(loglevel)
    breadcrumbs = system_config.get("logging", {}).get("enable_method_breadcrumbs", False)
    breadcrumbs = bool(breadcrumbs if breadcrumbs is True else False)
    set_breadcrumb_flag(breadcrumbs)
    LOGGER.info("Parsing Core Application Properties")
    APP_PROPS = AppProps(
        days_in_memory=system_config.get("days_in_memory", 7),
        relative_output_dir=system_config.get("output_dir_name", "output"),
        loglevel=loglevel,
    )


class WorkflowStage(Enum):
    GATHER = auto()
    CALCULATE_OUTPUT = auto()
    SLEEP = auto()


@logged_method
def try_parse_config_file(config_yaml_path: str) -> Dict:
    LOGGER.debug("Trying to parse Configuration File: " + config_yaml_path)
    with open(config_yaml_path, "r") as config_file:
        core_config = yaml.safe_load(config_file)
    LOGGER.debug("Successfully parsed Configuration File: " + config_yaml_path)
    LOGGER.debug("Parsing Environment Variables")
    env_parse_replace(core_config)
    LOGGER.debug("Successfully parsed Environment Variables")
    return core_config


@logged_method
def run_gather_cycle(ccloud_orgs: CCloudOrgList):
    # This will try to refresh and read all the data that might be new from the last gather phase.
    # Org Object has built in safeguard to prevent repetitive gathering for the same datasets.
    # for Cloud Objects --> 30 minutes is the minimum.
    # for Metrics API objects --> persistence store knows what all has been cached and written to disk and will not be
    # gathered again. For billing CSV files --> if the data is already read in memory, it won't be read in again.
    ccloud_orgs.execute_requests()


@logged_method
def run_calculate_cycle(ccloud_orgs: CCloudOrgList):
    ccloud_orgs.run_calculations()


@logged_method
def execute_workflow(arg_flags: Namespace):
    LOGGER.info("Starting Workflow Runner")
    internal_data_probe.set_starting()
    LOGGER.debug("Debug Mode is ON")
    LOGGER.debug("Parsing config file")
    core_config = try_parse_config_file(config_yaml_path=arg_flags.config_file)
    LOGGER.debug("Successfully parsed config file")
    LOGGER.debug("Setting up Core App Properties")
    get_app_props(core_config["config"])

    thread_configs: List = [
        # [COMMON_THREAD_RUNNER, current_memory_usage, 5],
        # [METRICS_PERSISTENCE_STORE, sync_to_file, METRICS_PERSISTENCE_STORE.flush_to_disk_interval_sec],
        # [CHARGEBACK_PERSISTENCE_STORE, sync_to_file, CHARGEBACK_PERSISTENCE_STORE.flush_to_disk_interval_sec],
        # [BILLING_PERSISTENCE_STORE, sync_to_file, BILLING_PERSISTENCE_STORE.flush_to_disk_interval_sec],
    ]

    threads_list = list()
    for _, item in enumerate(thread_configs):
        threads_list.append(
            item[0].get_new_thread(target_func=item[1], tick_duration_secs=item[2])
        )

    try:
        LOGGER.info("Starting all threads")
        for item in threads_list:
            item.start()

        ports_config: Dict = core_config.get("chargeback", {}).get("system", {}).get("exposed_ports", {})

        LOGGER.debug("Starting Prometheus Server")
        prometheus_client.start_http_server(ports_config.get("metrics_export", 8000))

        LOGGER.debug("Starting Internal API Server for state sharing and readiness")
        threading.Thread(
            target=internal_data_probe.internal_api.run,
            kwargs={"host": "0.0.0.0", "port": ports_config.get("readiness_api", 8001)},
        ).start()

        # This step will initialize the CCloudOrg structure along with all the internal Objects in it.
        # Those will include the first run for all the data gather step as well.
        # There are some safeguards already implemented to prevent request choking, it should be safe in most use cases.
        LOGGER.info("Initializing Core CCloudOrgList Object")
        if not core_config.get("config", {}).get("ccloud_org_details", {}):
            LOGGER.error("No CCloud Org Details found in config file. Exiting.")
            raise ValueError("No CCloud Org Details found in config file.")

        CCloudOrgList(
            in_orgs=core_config["config"]["org_details"],
        )

        LOGGER.info("Initialization Complete.")
        internal_data_probe.set_readiness(readiness_flag=True)

        # This is the main loop for the application.
        LOGGER.info("Starting Main Loop")
        while True:
            sleep(10**8)

    finally:
        LOGGER.info("Shutting down all threads")
        # Begin shutdown process.
        for item in thread_configs:
            item[0].stop_sync()
        LOGGER.info("Waiting for State Sync ticker for Final sync before exit")
        for item in threads_list:
            item.join()
