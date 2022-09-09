from dataclasses import dataclass, field
from pathlib import Path
import threading
from typing import Dict, List

from data_processing.billing_processing import BillingDataset
from storage_mgmt import STORAGE_PATH, DirType


@dataclass(kw_only=True)
class CCloudBillingDataset:
    path_to_monitor: str = field(default=STORAGE_PATH[DirType.BillingsData])
    flush_to_disk_interval_sec: int = field(default=3)

    sync_needed: bool = field(default=False, init=False)
    sync_runner_status: threading.Event = field(init=False)
    available_files: List[str] = field(init=False, default_factory=list)
    billing_dataframes: Dict[str, BillingDataset] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.sync_runner_status = threading.Event()
        self.start_sync()
        self.read_all()

    def start_sync(self):
        self.sync_runner_status.set()

    def stop_sync(self):
        self.sync_runner_status.clear()

    def analyse_directory(self):
        self.available_files = sorted(Path(self.path_to_monitor).glob("*.csv"))

    def read_all(self):
        self.analyse_directory()
        available_file_paths = set([str(x) for x in self.available_files])
        files_in_ds = set(self.billing_dataframes.keys())
        for file_path in available_file_paths - files_in_ds:
            self.__add_to_cache(BillingDataset(file_path=file_path))

    def __add_to_cache(self, bill_ds: BillingDataset):
        self.billing_dataframes[bill_ds.file_path] = bill_ds
