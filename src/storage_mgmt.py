import os
import threading
from dataclasses import dataclass, field
from enum import Enum, auto
from json import dumps, load
from time import sleep
from typing import Dict, List
import psutil
from helpers import ensure_path


class DirType(Enum):
    MetricsData = auto()
    BillingsData = auto()
    OutputData = auto()
    PersistenceStats = auto()


def locate_storage_path(
    basepath: str = os.getcwd(), base_dir: str = "output", dir_type: List[DirType] = DirType.MetricsData
) -> Dict[DirType, str]:
    ret = {}
    for item in dir_type:
        temp = os.path.join(basepath, base_dir, item.name, "")
        print(temp)
        ensure_path(path=temp)
        ret[item] = temp
    return ret


STORAGE_PATH = locate_storage_path(
    dir_type=[DirType.MetricsData, DirType.BillingsData, DirType.OutputData, DirType.PersistenceStats]
)


@dataclass(kw_only=True)
class PersistenceStore:
    flush_to_disk_interval_sec: int = field(default=3)
    historical_data_to_maintain: int = field(default=7)
    out_path: str = field(
        init=True,
        default=os.path.join(STORAGE_PATH[DirType.PersistenceStats], f"{DirType.PersistenceStats.name}.json"),
    )

    sync_needed: bool = field(default=False, init=False)
    sync_runner_status: threading.Event = field(init=False)
    __status: Dict[str, List[str]] = field(default_factory=dict, init=False)

    def __post_init__(self):
        self.rehydrate_persistence_status()
        self.sync_runner_status = threading.Event()
        self.start_sync()

    def start_sync(self):
        self.sync_runner_status.set()

    def stop_sync(self):
        self.sync_runner_status.clear()
        self.write_file()

    def write_file(self):
        with open(self.out_path, "w") as f:
            f.write(dumps(self.__status, indent=1))

    def find_datasets_to_evict(self) -> List[str]:
        if self.historical_data_to_maintain == -1:
            return []
        temp = list(self.__status.keys())
        temp.sort(reverse=True)
        return temp[self.historical_data_to_maintain :]

    def rehydrate_persistence_status(self):
        if os.path.exists(self.out_path):
            with open(self.out_path, "r") as f:
                self.__status = load(f)
        for item in self.find_datasets_to_evict():
            self.__status.pop(item)
        print(self.__status)

    def add_to_persistence(self, date_value: str, metric_name: str):
        date_value = str(date_value)
        if date_value in self.__status:
            if metric_name not in self.__status[date_value]:
                self.__status[date_value].append(metric_name)
                self.sync_needed = True
        else:
            self.__status[date_value] = [str(metric_name)]
            self.sync_needed = True

    def is_dataset_present(self, date_value: str, metric_name: str) -> bool:
        if date_value in self.__status.keys():
            if metric_name in self.__status[date_value]:
                return True
        return False


def sync_to_file(persistence_object: PersistenceStore, flush_to_file: int = 5):
    while persistence_object.sync_runner_status.is_set():
        if persistence_object.sync_needed:
            persistence_object.write_file()
        sleep(flush_to_file)


def current_memory_usage(persistence_object: PersistenceStore, flush_to_file: int = 5):
    while persistence_object.sync_runner_status.is_set():
        print(f"Current Memory Utilization: {psutil.Process().memory_info().rss / (1024 * 1024)}")
        sleep(flush_to_file)


METRICS_PERSISTENCE_STORE = PersistenceStore(
    out_path=os.path.join(STORAGE_PATH[DirType.PersistenceStats], f"Metrics_{DirType.PersistenceStats.name}.json")
)
BILLING_PERSISTENCE_STORE = PersistenceStore(
    out_path=os.path.join(STORAGE_PATH[DirType.PersistenceStats], f"Billing_{DirType.PersistenceStats.name}.json"),
    historical_data_to_maintain=-1,
)
