from dataclasses import dataclass, field
from datetime import timedelta
import os
import pandas as pd
from typing import Any, Dict, Tuple
from helpers import logged_method, timed_method
from csv import QUOTE_NONNUMERIC


@dataclass
class metrics_dataframe:
    aggregation_metric_name: str = field(repr=False, init=True)
    metrics_output: Dict = field(repr=False, init=True)
    parsed_datasets: Dict[str, Dict[str, Any]] = field(init=False, default_factory=dict)
    # parsed_df: pd.DataFrame = field(init=False)
    # req_res_bytes_df: pd.DataFrame = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.generate_df_from_output()
        self.metrics_output = None

    def append_dataset(self, ds_name: str, is_shaped: bool, ds: pd.DataFrame):
        temp = {}
        temp["is_shaped"] = is_shaped
        temp["data"] = ds
        self.parsed_datasets[ds_name] = temp

    def get_dataset(self, ds_name: str) -> Tuple[bool, pd.DataFrame]:
        ret = self.parsed_datasets[ds_name]
        return (ret["is_shaped"], ret["data"])

    def get_all_datasets(self) -> Tuple[str, bool, pd.DataFrame]:
        for item in self.parsed_datasets.keys():
            is_shaped, ds = self.get_dataset(item)
            yield item, is_shaped, ds

    @timed_method
    @logged_method
    def shape_specific_dataframes(self, ds_name_to_shape: str):
        temp = self.get_dataset(ds_name_to_shape)[1].pivot(
            index="timestamp", columns="metric.principal_id", values="value"
        )
        self.append_dataset(ds_name="pivoted_on_timestamp", is_shaped=True, ds=temp)

    @timed_method
    @logged_method
    def generate_df_from_output(self):
        temp = pd.DataFrame(self.metrics_output["data"])
        temp["timestamp"] = pd.to_datetime(temp["timestamp"])
        core_ds_name = "metricsapi_representation"
        self.append_dataset(ds_name=core_ds_name, is_shaped=False, ds=temp)
        print("Trying to shape selective datasets")
        if self.is_shapeable():
            self.shape_specific_dataframes(ds_name_to_shape=core_ds_name)
            print("Successfully re-shaped time-series metrics")
        else:
            print("Did not detect the right aggregation metric. Ignoring.")

    def is_shapeable(self) -> bool:
        if self.aggregation_metric_name in [
            "io.confluent.kafka.server/request_bytes",
            "io.confluent.kafka.server/response_bytes",
        ]:
            return True
        else:
            return False

    def print_sample_df(self) -> None:
        for name, is_shaped, ds in self.get_all_datasets():
            if ds is not None:
                print(f"Sample Dataset for {name}, is_shaped: {is_shaped}:")
                print(ds.head(3))

    def get_date_ranges(self, ts_range, dt_range) -> Tuple[str, pd.Timestamp, pd.Timestamp]:
        for inner_ts_boundary, date_only_str in zip(ts_range, dt_range):
            outer_ts_boundary = inner_ts_boundary + timedelta(days=1)
            yield (date_only_str, inner_ts_boundary, outer_ts_boundary)

    def output_to_csv(self, basepath: str):
        for name, is_shaped, ds in self.get_all_datasets():
            if ds is not None:
                if name is "pivoted_on_timestamp":
                    ts_range = ds.index.normalize().unique()
                    dt_range = ts_range.date
                    for dt_val, gt_eq_date, lt_date in self.get_date_ranges(ts_range, dt_range):
                        subset = None
                        out_path = os.path.join(basepath, f"{dt_val}_{name}.csv")
                        subset = ds[(ds.index >= gt_eq_date) & (ds.index < lt_date)]
                        subset.to_csv(out_path, quoting=QUOTE_NONNUMERIC)
                elif name is "metricsapi_representation":
                    ts_range = ds["timestamp"].dt.normalize().unique()
                    dt_range = ts_range.date
                    for dt_val, gt_eq_date, lt_date in self.get_date_ranges(ts_range, dt_range):
                        subset = None
                        out_path = os.path.join(basepath, f"{dt_val}_{name}.csv")
                        subset = ds[(ds["timestamp"] >= gt_eq_date) & (ds.index < lt_date)]
                        subset.to_csv(out_path, index=False, quoting=QUOTE_NONNUMERIC)
