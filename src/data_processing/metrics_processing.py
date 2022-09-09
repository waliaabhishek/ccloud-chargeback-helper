import os
from csv import QUOTE_NONNUMERIC
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum, auto
from typing import Any, Dict, Tuple

import pandas as pd
from helpers import ensure_path, logged_method, sanitize_metric_name, timed_method
from storage_mgmt import METRICS_PERSISTENCE_STORE, STORAGE_PATH, DirType


class MetricsDatasetNames(Enum):
    metricsapi_representation = auto()
    pivoted_on_timestamp = auto()
    metrics_persistence_status = auto()


@dataclass(kw_only=True)
class MetricsDict:
    is_shaped: bool
    data: pd.DataFrame


@dataclass
class metrics_dataframe:
    aggregation_metric_name: str = field(repr=False, init=True)
    _metrics_output: Dict = field(repr=False, init=True)

    parsed_datasets: Dict[str, MetricsDict] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.generate_df_from_output()
        self._metrics_output = None
        self.aggregation_metric_name = self.aggregation_metric_name

    def append_dataset(self, ds_name: str, is_shaped: bool, ds: pd.DataFrame):
        self.parsed_datasets[ds_name] = MetricsDict(
            is_shaped=is_shaped,
            data=ds,
        )

    def get_dataset(self, ds_name: str) -> MetricsDict:
        return self.parsed_datasets[ds_name]

    def get_all_datasets(self) -> Tuple[str, MetricsDict]:
        for item in self.parsed_datasets.keys():
            dataset = self.get_dataset(item)
            yield item, dataset

    @timed_method
    @logged_method
    def shape_specific_dataframes(self, ds_name_to_shape: str):
        temp = self.get_dataset(ds_name_to_shape).data.pivot(
            index="timestamp", columns="metric.principal_id", values="value"
        )
        self.append_dataset(ds_name=MetricsDatasetNames.pivoted_on_timestamp.name, is_shaped=True, ds=temp)

    @timed_method
    @logged_method
    def generate_df_from_output(self):
        temp = pd.DataFrame(self._metrics_output["data"])
        temp["timestamp"] = pd.to_datetime(temp["timestamp"])
        core_ds_name = MetricsDatasetNames.metricsapi_representation.name
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
        for name, ds in self.get_all_datasets():
            if ds.data is not None:
                print(f"Sample Dataset for {name}, is_shaped: {ds.is_shaped}:")
                print(ds.data.head(3))

    def get_date_ranges(self, ts_range, dt_range) -> Tuple[str, pd.Timestamp, pd.Timestamp]:
        for inner_ts_boundary, date_only_str in zip(ts_range, dt_range):
            outer_ts_boundary = inner_ts_boundary + timedelta(days=1)
            yield (date_only_str, inner_ts_boundary, outer_ts_boundary)

    def output_to_csv(self, basepath: str = STORAGE_PATH[DirType.MetricsData]):
        for name, ds in self.get_all_datasets():
            if ds.data is not None:
                if name is MetricsDatasetNames.pivoted_on_timestamp.name:
                    ts_range = ds.data.index.normalize().unique()
                    dt_range = ts_range.date
                    for dt_val, gt_eq_date, lt_date in self.get_date_ranges(ts_range, dt_range):
                        subset = None
                        out_path = os.path.join(basepath, f"{dt_val}")
                        ensure_path(path=out_path)
                        out_path = os.path.join(basepath, f"{dt_val}", f"{self.aggregation_metric_name}__{name}.csv")
                        subset = ds.data[(ds.data.index >= gt_eq_date) & (ds.data.index < lt_date)]
                        subset.to_csv(out_path, quoting=QUOTE_NONNUMERIC)
                        METRICS_PERSISTENCE_STORE.add_to_persistence(
                            date_value=dt_val, metric_name=self.aggregation_metric_name
                        )
                elif name is MetricsDatasetNames.metricsapi_representation.name:
                    ts_range = ds.data["timestamp"].dt.normalize().unique()
                    dt_range = ts_range.date
                    for dt_val, gt_eq_date, lt_date in self.get_date_ranges(ts_range, dt_range):
                        subset = None
                        out_path = os.path.join(basepath, f"{dt_val}")
                        ensure_path(path=out_path)
                        out_path = os.path.join(basepath, f"{dt_val}", f"{self.aggregation_metric_name}__{name}.csv")
                        subset = ds.data[(ds.data["timestamp"] >= gt_eq_date) & (ds.data["timestamp"] < lt_date)]
                        subset.to_csv(out_path, index=False, quoting=QUOTE_NONNUMERIC)
                        METRICS_PERSISTENCE_STORE.add_to_persistence(
                            date_value=dt_val, metric_name=self.aggregation_metric_name
                        )
