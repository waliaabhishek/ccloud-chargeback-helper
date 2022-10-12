import datetime
import os
from csv import QUOTE_NONNUMERIC
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum, auto
from typing import Dict, Tuple

import pandas as pd
from helpers import ensure_path
from storage_mgmt import METRICS_PERSISTENCE_STORE, STORAGE_PATH, DirType
from helpers import BILLING_METRICS_SCOPE


class MetricsCSVColumnNames:
    IN_TS = "timestamp"
    IN_PRINCIPAL = "metric.principal_id"
    IN_KAFKA_CLUSTER = "resource.kafka.id"
    IN_VALUE = "value"

    OUT_TS = "Interval"
    OUT_PRINCIPAL = "Principal"
    OUT_KAFKA_CLUSTER = "KafkaID"
    OUT_VALUE = "Value"

    def override_column_names(self, key, value):
        object.__setattr__(self, key, value)


METRICS_CSV_COLUMNS = MetricsCSVColumnNames()


class MetricsDatasetNames(Enum):
    metricsapi_representation = auto()
    pivoted_on_timestamp = auto()
    metrics_persistence_status = auto()


@dataclass(kw_only=True)
class MetricsDict:
    is_shaped: bool
    data: pd.DataFrame


@dataclass
class MetricsDataframe:
    org_id: str = field(init=True)
    aggregation_metric_name: str = field(repr=False, init=True)
    _metrics_output: Dict = field(repr=False, init=True)
    filename_for_read_in: str = field(default=None)

    parsed_datasets: Dict[str, MetricsDict] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        if self.filename_for_read_in is None and self._metrics_output is not None:
            self.read_dataframe_from_input()
        else:
            self.read_dataframe_from_file()
        self._metrics_output = None
        # self.aggregation_metric_name = self.aggregation_metric_name

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

    def shape_specific_dataframes(self, ds_name_to_shape: str):
        temp = self.get_dataset(ds_name_to_shape).data.pivot(
            index=[METRICS_CSV_COLUMNS.IN_TS, METRICS_CSV_COLUMNS.IN_KAFKA_CLUSTER],
            columns=METRICS_CSV_COLUMNS.IN_PRINCIPAL,
            values=METRICS_CSV_COLUMNS.IN_VALUE,
        )
        self.append_dataset(ds_name=MetricsDatasetNames.pivoted_on_timestamp.name, is_shaped=True, ds=temp)

    def is_shapeable(self) -> bool:
        if self.aggregation_metric_name in BILLING_METRICS_SCOPE.values():
            return True
        else:
            return False

    def create_dataframe(self, metricsapi_repr_df: pd.DataFrame):
        core_ds_name = MetricsDatasetNames.metricsapi_representation.name
        self.append_dataset(ds_name=core_ds_name, is_shaped=False, ds=metricsapi_repr_df)
        print("Trying to shape selective datasets")
        if self.is_shapeable():
            self.shape_specific_dataframes(ds_name_to_shape=core_ds_name)
            print("Successfully re-shaped time-series metrics")
        else:
            print("Did not detect the right aggregation metric. Ignoring.")
        self.print_sample_df()

    def read_dataframe_from_file(self):
        temp = pd.read_csv(
            self.filename_for_read_in,
            parse_dates=[METRICS_CSV_COLUMNS.IN_TS],
            infer_datetime_format=True,
        )
        self.create_dataframe(metricsapi_repr_df=temp)

    def read_dataframe_from_input(self):
        temp = pd.DataFrame(self._metrics_output["data"])
        temp[METRICS_CSV_COLUMNS.IN_TS] = pd.to_datetime(temp[METRICS_CSV_COLUMNS.IN_TS])
        self.create_dataframe(metricsapi_repr_df=temp)

    def print_sample_df(self) -> None:
        for name, ds in self.get_all_datasets():
            if ds.data is not None:
                print(f"Sample Dataset for {name}, is_shaped: {ds.is_shaped}:")
                print(ds.data.head(3))
                print(ds.data.info())

    def get_date_ranges(self, ts_range, dt_range) -> Tuple[str, pd.Timestamp, pd.Timestamp]:
        for inner_ts_boundary, date_only_str in zip(ts_range, dt_range):
            outer_ts_boundary = inner_ts_boundary + timedelta(days=1)
            yield (date_only_str, inner_ts_boundary, outer_ts_boundary)

    def get_filepath(
        self,
        date_value: datetime.date,
        basepath: str,
        # basepath=STORAGE_PATH[DirType.MetricsData],
        metric_dataset_name: MetricsDatasetNames = MetricsDatasetNames.metricsapi_representation,
    ):
        date_folder_path = os.path.join(basepath, f"{str(date_value)}")
        ensure_path(path=date_folder_path)
        file_path = os.path.join(
            basepath,
            f"{str(date_value)}",
            f"{self.aggregation_metric_name}__{metric_dataset_name.name}.csv",
        )
        return date_folder_path, file_path

    def output_to_csv(self, basepath: str):
        for name, ds in self.get_all_datasets():
            if name is MetricsDatasetNames.metricsapi_representation.name:
                ts_range = ds.data[METRICS_CSV_COLUMNS.IN_TS].dt.normalize().unique()
                dt_range = ts_range.date
                for dt_val, gt_eq_date, lt_date in self.get_date_ranges(ts_range, dt_range):
                    subset = None
                    _, out_file_path = self.get_filepath(
                        date_value=dt_val,
                        metric_dataset_name=MetricsDatasetNames.metricsapi_representation,
                        basepath=basepath,
                    )
                    subset = ds.data[
                        (ds.data[METRICS_CSV_COLUMNS.IN_TS] >= gt_eq_date)
                        & (ds.data[METRICS_CSV_COLUMNS.IN_TS] < lt_date)
                    ]
                    subset.to_csv(out_file_path, index=False, quoting=QUOTE_NONNUMERIC)
                    METRICS_PERSISTENCE_STORE.add_data_to_persistence_store(
                        org_id=self.org_id, key=(dt_val,), value=self.aggregation_metric_name
                    )
                    # METRICS_PERSISTENCE_STORE.add_data_to_persistence_store(
                    #     date_value=dt_val, metric_name=self.aggregation_metric_name
                    # )
