from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum, auto
import os
import pandas as pd
from typing import Any, Dict, List, Tuple
from helpers import ensure_path, logged_method, timed_method, sanitize_metric_name
from csv import QUOTE_NONNUMERIC


class DatasetNames(Enum):
    metricsapi_representation = auto()
    pivoted_on_timestamp = auto()
    metrics_persistence_status = auto()


@dataclass
class metrics_dataframe:
    aggregation_metric_name: str = field(repr=False, init=True)
    _metrics_output: Dict = field(repr=False, init=True)
    parsed_datasets: Dict[str, Dict[str, Any]] = field(init=False, default_factory=dict)
    persistence_status: Dict[str, List[str]] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        self.generate_df_from_output()
        self._metrics_output = None
        self.aggregation_metric_name = self.aggregation_metric_name

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
        self.append_dataset(ds_name=DatasetNames.pivoted_on_timestamp.name, is_shaped=True, ds=temp)

    @timed_method
    @logged_method
    def generate_df_from_output(self):
        temp = pd.DataFrame(self._metrics_output["data"])
        temp["timestamp"] = pd.to_datetime(temp["timestamp"])
        core_ds_name = DatasetNames.metricsapi_representation.name
        self.append_dataset(ds_name=core_ds_name, is_shaped=False, ds=temp)
        print("Trying to shape selective datasets")
        if self.is_shapeable():
            self.shape_specific_dataframes(ds_name_to_shape=core_ds_name)
            print("Successfully re-shaped time-series metrics")
        else:
            print("Did not detect the right aggregation metric. Ignoring.")

    def is_shapeable(self) -> bool:
        if self.aggregation_metric_name in [
            sanitize_metric_name("io.confluent.kafka.server/request_bytes"),
            sanitize_metric_name("io.confluent.kafka.server/response_bytes"),
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

    def add_to_persistence_list(self, date_value: str):
        # TODO: Implement Persistence list add and send to file.
        pass

    def output_to_csv(self, basepath: str):
        for name, is_shaped, ds in self.get_all_datasets():
            if ds is not None:
                if name is DatasetNames.pivoted_on_timestamp.name:
                    ts_range = ds.index.normalize().unique()
                    dt_range = ts_range.date
                    for dt_val, gt_eq_date, lt_date in self.get_date_ranges(ts_range, dt_range):
                        subset = None
                        out_path = os.path.join(basepath, f"{dt_val}")
                        ensure_path(path=out_path)
                        out_path = os.path.join(basepath, f"{dt_val}", f"{self.aggregation_metric_name}__{name}.csv")
                        subset = ds[(ds.index >= gt_eq_date) & (ds.index < lt_date)]
                        subset.to_csv(out_path, quoting=QUOTE_NONNUMERIC)
                elif name is DatasetNames.metricsapi_representation.name:
                    ts_range = ds["timestamp"].dt.normalize().unique()
                    dt_range = ts_range.date
                    for dt_val, gt_eq_date, lt_date in self.get_date_ranges(ts_range, dt_range):
                        subset = None
                        out_path = os.path.join(basepath, f"{dt_val}")
                        ensure_path(path=out_path)
                        out_path = os.path.join(basepath, f"{dt_val}", f"{self.aggregation_metric_name}__{name}.csv")
                        subset = ds[(ds["timestamp"] >= gt_eq_date) & (ds["timestamp"] < lt_date)]
                        subset.to_csv(out_path, index=False, quoting=QUOTE_NONNUMERIC)
