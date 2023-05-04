from dataclasses import dataclass, field
from pathlib import Path
import threading
from typing import Dict, List
from ccloud.connections import CCloudBase

from data_processing.billing_processing import (
    BillingDataframe,
    BILLING_CSV_COLUMNS,
    BillingDatasetNames,
)
from storage_mgmt import STORAGE_PATH, DirType
import pandas as pd


@dataclass(kw_only=True)
class CCloudBillingManager(CCloudBase):
    billing_dataframes: Dict[str, BillingDataframe] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.url = self.in_ccloud_connection.get_endpoint_url(key=self.in_ccloud_connection.uri.get_billing_costs)
        self.read_all()

    def start_sync(self):
        pass

    def stop_sync(self):
        pass

    def analyse_directory(self):
        pass

    def read_all(self):
        self.analyse_directory()
        available_file_paths = set([str(x) for x in self.available_files])
        files_in_ds = set(self.billing_dataframes.keys())
        for file_path in available_file_paths - files_in_ds:
            self.generate_df_from_output(file_path=file_path)
        self.print_sample_df()

    def __add_to_cache(self, file_name: str, ds_name: str, is_shaped: bool, ds: pd.DataFrame):
        self.billing_dataframes[file_name] = BillingDataframe(dataset_name=ds_name, is_shaped=is_shaped, data=ds)

    def generate_df_from_output(self, file_path: str):
        temp = pd.read_csv(
            file_path,
            parse_dates=[BILLING_CSV_COLUMNS.start_date, BILLING_CSV_COLUMNS.end_date],
            infer_datetime_format=True,
        )
        self.__add_to_cache(
            file_name=file_path,
            ds_name=BillingDatasetNames.invoice_csv_representation,
            is_shaped=False,
            ds=temp,
        )

    def print_sample_df(self) -> None:
        for name, billling_ds in self.billing_dataframes.items():
            print(f"Sample Dataset for {name}, is_shaped: {billling_ds.is_shaped}:")
            print(billling_ds.data.info())
            print(billling_ds.data.head(3))
