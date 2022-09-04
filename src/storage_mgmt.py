import os
import threading
from dataclasses import dataclass, field
from enum import Enum, auto
from json import dumps, load
from time import sleep
from typing import Dict, List

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
class persistence_mgmt:
    flush_to_disk_interval_sec: int = field(default=3)
    sync_needed: bool = field(default=False, init=False)
    sync_runner_status: threading.Event = field(init=False)
    __out_path: str = field(init=False)
    __status: Dict[str, List[str]] = field(default_factory=dict, init=False)

    def __post_init__(self):
        self.__out_path = os.path.join(STORAGE_PATH[DirType.PersistenceStats], f"{DirType.PersistenceStats.name}.json")
        self.rehydrate_persistence_status()
        self.sync_runner_status = threading.Event()
        self.start_sync()

    def start_sync(self):
        self.sync_runner_status.set()

    def stop_sync(self):
        self.sync_runner_status.clear()
        self.write_file()

    def write_file(self):
        with open(self.__out_path, "w") as f:
            f.write(dumps(self.__status, indent=1))

    def find_datasets_to_evict(self) -> List[str]:
        temp = list(self.__status.keys())
        temp.sort(reverse=True)
        return temp[6:]

    def rehydrate_persistence_status(self):
        if os.path.exists(self.__out_path):
            with open(self.__out_path, "r") as f:
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
                # self.__change_count += 1
        else:
            self.__status[date_value] = [str(metric_name)]
            self.sync_needed = True
            # self.__change_count += 1


def sync_to_file(persistence_object: persistence_mgmt, flush_to_file: int = 5):
    while persistence_object.sync_runner_status.is_set():
        if persistence_object.sync_needed:
            persistence_object.write_file()
        sleep(flush_to_file)


PERSISTENCE_STORE = persistence_mgmt()
