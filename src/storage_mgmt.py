import datetime
import os
import threading
from dataclasses import dataclass, field
from json import dumps, load
from time import sleep
from typing import Dict, List, Tuple

import psutil
from helpers import sanitize_metric_name
from prometheus_processing.custom_collector import TimestampedCollector

# class DirType(Enum):
#     MetricsData = auto()
#     BillingsData = auto()
#     OutputData = auto()
#     PersistenceStats = auto()

# code_run_stats = TimestampedCollector(
#     "python_custom_memory_used_bytes",
#     "Total memory consumed by the process.",
#     [],
#     in_begin_timestamp=datetime.datetime.now(),
# )


# @dataclass(kw_only=True)
# class StoragePathManagement:
#     basepath: str = field(default=os.getcwd())
#     base_dir: str = field(default="output")

#     def __generate_path(self, org_id: str, dir_type: DirType):
#         return os.path.join(self.basepath, self.base_dir, org_id, dir_type.name, "")

#     def ensure_path(self, org_id: str, dir_type: List[DirType]):
#         for item in dir_type:
#             ensure_path(self.__generate_path(org_id=org_id, dir_type=item))

#     def delete_path(self, org_id: str, dir_type: DirType):
#         full_path = self.__generate_path(org_id=org_id, dir_type=dir_type)
#         if os.path.exists(full_path) and full_path.startswith(os.path.join(self.basepath, self.base_dir)):
#             shutil.rmtree(full_path)

#     def get_path(self, org_id: str, dir_type=DirType, ensure_exists: bool = False):
#         if ensure_exists:
#             self.ensure_path(org_id=org_id, dir_type=[dir_type])
#         return self.__generate_path(org_id=org_id, dir_type=dir_type)


# STORAGE_PATH = StoragePathManagement()
# STORAGE_PATH.ensure_path(org_id="common", dir_type=[])


@dataclass
class ThreadableRunner:
    object_lock: threading.Lock = field(init=False)
    sync_runner_status: threading.Event = field(init=False)

    def __post_init__(self):
        self.sync_runner_status = threading.Event()
        self.object_lock = threading.Lock()
        self.start_sync()

    def start_sync(self):
        self.sync_runner_status.set()

    def stop_sync(self):
        self.sync_runner_status.clear()

    def get_new_thread(self, target_func, tick_duration_secs: int):
        temp = threading.Thread(target=target_func, args=(self, tick_duration_secs))
        return temp

    def invoke_custom_func(self, target_func, *args):
        temp = threading.Thread(target=target_func, args=(self, *args))
        return temp


COMMON_THREAD_RUNNER = ThreadableRunner()


@dataclass(kw_only=True)
class PersistenceStore(ThreadableRunner):
    flush_to_disk_interval_sec: int = field(default=3)
    historical_data_to_maintain: int = field(default=7)
    data_type: str = field(init=True)
    persistence_path: Dict[str, Dict[str, object]] = field(init=False, repr=False, default=dict)

    def __post_init__(self):
        super().__post_init__()
        self.data_type = sanitize_metric_name(self.data_type).lower()
        self.persistence_path = {}
        self.add_persistence_path(org_id="common")
        self.rehydrate_persistence_status()

    def stop_sync(self):
        super().stop_sync()
        self.write_file(force_write=True)

    def __encode_key(self, key: Tuple) -> str:
        return "___".join(str(x).replace(" ", "_") for x in key)

    def __decode_key(self, key: str) -> str:
        return tuple([x for x in key.split("___")])

    def add_persistence_path(self, org_id: str, ensure_exists: bool = False):
        temp = self.persistence_path.get(org_id, {})
        # temp = self.persistence_path.get(org_id, dict())
        if not temp:
            with self.object_lock:
                temp["path"] = os.path.join(
                    STORAGE_PATH.get_path(
                        org_id=org_id, dir_type=DirType.PersistenceStats, ensure_exists=ensure_exists
                    ),
                    f"{self.data_type}_{DirType.PersistenceStats.name}.json",
                )
                temp["sync_needed"] = False
                temp["data"] = dict()
                self.persistence_path[org_id] = temp
            self.rehydrate_persistence_status(org_id=org_id)

    def rehydrate_persistence_status(self, org_id: str = "common"):
        org_details = self.persistence_path[org_id]
        path_str = org_details["path"]
        if os.path.exists(path_str) and os.stat(path_str).st_size > 0:
            with open(path_str, "r") as f:
                org_details["data"] = load(f)
        for item in self.__find_datasets_to_evict(org_id=org_id):
            org_details["data"].pop(item)
        # print(org_details["data"])

    def add_data_to_persistence_store(self, org_id: str, key: Tuple, value: str):
        if not org_id in self.persistence_path.keys():
            self.add_persistence_path(org_id=org_id, ensure_exists=True)
        org_data = self.persistence_path[org_id]["data"]
        org_data: Dict[Tuple, str] = org_data
        temp_key = self.__encode_key(key=key)
        if temp_key in org_data.keys():
            temp_val = org_data[temp_key]
            if value not in temp_val:
                with self.object_lock:
                    temp_val.append(value)
                    org_data[temp_key] = temp_val
                    self.persistence_path[org_id]["sync_needed"] = True
        else:
            with self.object_lock:
                org_data[temp_key] = [value]
                self.persistence_path[org_id]["sync_needed"] = True
        # if sync_needed_flag:
        #     with self.object_lock:
        #         self.persistence_path[org_id]["sync_needed"] = True

    def is_dataset_present(self, org_id: str, key: Tuple, value: dict) -> bool:
        temp_key = self.__encode_key(key=key)
        if org_id in self.persistence_path.keys():
            org_data = self.persistence_path[org_id]["data"]
            if temp_key in org_data.keys():
                if value in org_data[temp_key]:
                    return True
        return False

    def write_file(self, force_write: bool = False):
        with self.object_lock:
            for org_id, v in self.persistence_path.items():
                if org_id not in ["common"] and (v["sync_needed"] or force_write):
                    with open(v["path"], "w") as f:
                        f.write(dumps(v["data"], indent=1))

    def __find_datasets_to_evict(self, org_id: str) -> List[str]:
        if self.historical_data_to_maintain == -1:
            return []
        org_data = self.persistence_path[org_id]["data"]
        temp = list(org_data.keys())
        temp.sort(reverse=True)
        return temp[self.historical_data_to_maintain :]

        # if self.historical_data_to_maintain == -1:
        #     return []
        # temp = list(self.__status.keys())
        # temp.sort(reverse=True)
        # return temp[self.historical_data_to_maintain :]


def sync_to_file(persistence_object: PersistenceStore, flush_to_file: int = 5):
    while persistence_object.sync_runner_status.is_set():
        persistence_object.write_file()
        sleep(flush_to_file)


def current_memory_usage(persistence_object: ThreadableRunner, evaluation_interval: int = 5):
    while persistence_object.sync_runner_status.is_set():
        mem_used = psutil.Process().memory_info().rss
        print(f"Current Memory Utilization: {mem_used / (2**20)}")
        # curr_ts = datetime.datetime.utcnow().replace(
        #     hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc
        # )
        # code_run_stats.set_timestamp(curr_timestamp=curr_ts)
        # code_run_stats.set(mem_used)
        sleep(evaluation_interval)


# This will be use to store status for writing Metrics datasets to disk.
# METRICS_PERSISTENCE_STORE = PersistenceStore(data_type="Metrics")

# BILLING_PERSISTENCE_STORE = PersistenceStore(
#     out_path=os.path.join(STORAGE_PATH[DirType.PersistenceStats], f"Billing_{DirType.PersistenceStats.name}.json"),
#     historical_data_to_maintain=-1,
# )
# This will be use to store status for writing chargeback datasets to disk.
# CHARGEBACK_PERSISTENCE_STORE = PersistenceStore(data_type="Chargeback", historical_data_to_maintain=-1)
# This will be use to store status for writing chargeback datasets to disk.
# BILLING_PERSISTENCE_STORE = PersistenceStore(data_type="Billing", historical_data_to_maintain=-1)
