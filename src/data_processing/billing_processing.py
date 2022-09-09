import os
from csv import QUOTE_NONNUMERIC
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum, auto
from typing import Any, Dict, Tuple

import pandas as pd
from helpers import ensure_path, logged_method, sanitize_metric_name, timed_method
from storage_mgmt import METRICS_PERSISTENCE_STORE, STORAGE_PATH, DirType


class BillingDatasetNames(Enum):
    invoice_csv_representation = auto()


@dataclass(kw_only=True)
class BillingDict:
    is_shaped: bool
    data: pd.DataFrame


@dataclass(kw_only=True)
class BillingDataset:
    file_path: str = field(init=True)

    parsed_datasets: Dict[str, BillingDict] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.generate_df_from_output()

    def append_dataset(self, ds_name: str, is_shaped: bool, ds: pd.DataFrame):
        self.parsed_datasets[ds_name] = BillingDict(is_shaped=is_shaped, data=ds)

    def get_dataset(self, ds_name: str) -> BillingDict:
        return self.parsed_datasets[ds_name]

    def get_all_datasets(self) -> Tuple[str, BillingDict]:
        for item in self.parsed_datasets.keys():
            ds = self.get_dataset(item)
            yield item, ds

    @timed_method
    @logged_method
    def generate_df_from_output(self):
        temp = pd.read_csv(self.file_path, parse_dates=["StartDate", "EndDate"], infer_datetime_format=True)
        self.append_dataset(
            ds_name=BillingDatasetNames.invoice_csv_representation,
            is_shaped=False,
            ds=temp,
        )
        self.print_sample_df()

    def print_sample_df(self) -> None:
        for name, billling_ds in self.get_all_datasets():
            if billling_ds.data is not None:
                print(f"Sample Dataset for {name}, is_shaped: {billling_ds.is_shaped}:")
                print(billling_ds.data.head(3))
                print(billling_ds.data.info())

    def get_date_ranges(self, ts_range, dt_range) -> Tuple[str, pd.Timestamp, pd.Timestamp]:
        for inner_ts_boundary, date_only_str in zip(ts_range, dt_range):
            outer_ts_boundary = inner_ts_boundary + timedelta(days=1)
            yield (date_only_str, inner_ts_boundary, outer_ts_boundary)

    def output_to_csv(self, basepath: str = STORAGE_PATH[DirType.MetricsData]):
        for name, billing_ds in self.get_all_datasets():
            if billing_ds.data is not None and name is BillingDatasetNames.invoice_csv_representation.name:
                ts_range = billing_ds.data["timestamp"].dt.normalize().unique()
                dt_range = ts_range.date
                for dt_val, gt_eq_date, lt_date in self.get_date_ranges(ts_range, dt_range):
                    subset = None
                    out_path = os.path.join(basepath, f"{dt_val}")
                    ensure_path(path=out_path)
                    out_path = os.path.join(basepath, f"{dt_val}", f"{self.aggregation_metric_name}__{name}.csv")
                    subset = billing_ds.data[
                        (billing_ds.data["timestamp"] >= gt_eq_date) & (billing_ds.data["timestamp"] < lt_date)
                    ]
                    subset.to_csv(out_path, index=False, quoting=QUOTE_NONNUMERIC)
                    METRICS_PERSISTENCE_STORE.add_to_persistence(
                        date_value=dt_val, metric_name=self.aggregation_metric_name
                    )
