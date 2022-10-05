from csv import QUOTE_NONNUMERIC
import datetime
from copy import deepcopy
from dataclasses import dataclass, field
import os
from typing import Dict, List, Tuple
import pandas as pd
from data_processing.billing_chargeback_processing import ChargebackDataframe
from helpers import ensure_path

from storage_mgmt import CHARGEBACK_PERSISTENCE_STORE, STORAGE_PATH, DirType


@dataclass(kw_only=True)
class ChargebackManager:
    cc_objects: object = field(init=True, repr=False)
    daily_dataset: Dict[str, ChargebackDataframe] = field(init=True, default_factory=dict)
    hourly_dataset: Dict[str, ChargebackDataframe] = field(init=True, default_factory=dict)
    days_in_memory: int = field(default=3)

    def __post_init__(self) -> None:
        # self.daily_dataset = dict()
        # self.hourly_dataset = dict()
        pass

    def run_calculations(
        self, time_slice: datetime.datetime, billing_dataframe: pd.DataFrame, metrics_dataframe: pd.DataFrame
    ):
        self.read_dataset_into_cache(datetime_value=time_slice)
        d_key, t_key = self.__determine_key_names(key=time_slice)
        d, t = self.__is_key_present_in_cache(time_slice)
        if d:
            self.daily_dataset[d_key].compute_output(
                force_data_add=True, addl_billing_dataframe=billing_dataframe, addl_metrics_dataframe=metrics_dataframe
            )
        else:
            self.add_dataset(
                datetime_value=time_slice,
                is_hourly_data=False,
                df=ChargebackDataframe(
                    cc_objects=self.cc_objects,
                    is_hourly_bucket=False,
                    _metrics_dataframe=metrics_dataframe,
                    _billing_dataframe=billing_dataframe,
                ),
            )
        if t:
            self.hourly_dataset[t_key].compute_output(
                force_data_add=True, addl_billing_dataframe=billing_dataframe, addl_metrics_dataframe=metrics_dataframe
            )
        else:
            self.add_dataset(
                datetime_value=time_slice,
                is_hourly_data=True,
                df=ChargebackDataframe(
                    cc_objects=self.cc_objects,
                    is_hourly_bucket=True,
                    _metrics_dataframe=metrics_dataframe,
                    _billing_dataframe=billing_dataframe,
                ),
            )

    def read_dataset_into_cache(self, datetime_value: datetime.datetime):
        d, t = self.__is_key_present_in_cache(key=datetime_value)
        if not d:
            _, _, file_path = self.get_filepath(time_slice=datetime_value, is_hourly_bucket=False)
            if not self.__is_file_present(file_path):
                print(f"Cache Re-hydration for Chargeback was requested. File not found for re-hydration: {file_path}")
            else:
                self.add_dataset(
                    datetime_value=datetime_value,
                    is_hourly_data=False,
                    df=ChargebackDataframe(cc_objects=self.cc_objects, is_hourly_bucket=False, file_path=file_path),
                )
        if not t:
            _, _, file_path = self.get_filepath(time_slice=datetime_value, is_hourly_bucket=True)
            if not self.__is_file_present(file_path):
                print(f"Cache Re-hydration for Chargeback was requested. File not found for re-hydration: {file_path}")
            else:
                self.add_dataset(
                    datetime_value=datetime_value,
                    is_hourly_data=True,
                    df=ChargebackDataframe(cc_objects=self.cc_objects, is_hourly_bucket=False, file_path=file_path),
                )

    def add_dataset(self, datetime_value: datetime.datetime, is_hourly_data: bool, df: ChargebackDataframe):
        date_key, time_key = self.__determine_key_names(key=datetime_value)
        if is_hourly_data:
            self.hourly_dataset[time_key] = df
        else:
            self.daily_dataset[date_key] = df

    def get_filepath(
        self,
        time_slice: datetime.datetime,
        is_hourly_bucket: bool,
        basepath=STORAGE_PATH[DirType.OutputData],
    ):
        date_folder_path = os.path.join(basepath, f"{str(time_slice.date())}")
        ensure_path(path=date_folder_path)

        if is_hourly_bucket:
            file_name = f"{time_slice.strftime('%H_%M_%S')}__chargeback.csv"
        else:
            file_name = f"DailyAggregate__Chargeback.csv"
        file_path = os.path.join(basepath, f"{str(time_slice.date())}", file_name)
        return date_folder_path, file_name, file_path

    def __is_key_present_in_cache(self, key: datetime.datetime) -> Tuple[bool, bool]:
        d, t = self.__determine_key_names(key=key)
        return d in self.daily_dataset.keys(), t in self.hourly_dataset.keys()

    def __is_file_present(self, file_path: str) -> bool:
        return os.path.exists(file_path) and os.path.isfile(file_path)

    def __determine_key_names(self, key: datetime.datetime) -> Tuple[str, str]:
        date_key = key.date().strftime("%Y_%m_%d")
        time_key = key.strftime("%Y_%m_%d_%H_%M_%S")
        return date_key, time_key

    def output_to_csv(self, basepath: str = STORAGE_PATH[DirType.OutputData]):
        for k, v in self.daily_dataset.items():
            df = v.cb_unit.get_dataframe()
            _, file_name, file_path = self.get_filepath(
                time_slice=datetime.datetime.fromisoformat(k),
                is_hourly_bucket=False,
                basepath=basepath,
            )
            df.to_csv(file_path, quoting=QUOTE_NONNUMERIC)
            CHARGEBACK_PERSISTENCE_STORE.add_to_persistence(date_value=k, metric_name=file_name)
        for k, v in self.hourly_dataset.items():
            df = v.cb_unit.get_dataframe()
            _, file_name, file_path = self.get_filepath(
                time_slice=datetime.datetime.fromisoformat(k),
                is_hourly_bucket=True,
                basepath=basepath,
            )
            df.to_csv(file_path, quoting=QUOTE_NONNUMERIC)
            CHARGEBACK_PERSISTENCE_STORE.add_to_persistence(date_value=k, metric_name=file_name)

    def find_datasets_to_evict(self) -> Tuple[List[str], List[str]]:
        out_daily_feed = list(self.daily_dataset.keys()).sort(reverse=True)[self.days_in_memory - 1 :]

        temp_dates = list(
            set().update(
                str(datetime.datetime.strptime(x, "%Y_%m_%d_%H_%M_%S").date()) for x in self.hourly_dataset.keys()
            )
        ).sort(reverse=True)[self.days_in_memory - 1 :]

        out_hourly_feed = [
            x
            for x in self.hourly_dataset.keys()
            if str(datetime.datetime.strptime(x, "%Y_%m_%d_%H_%M_%S").date()) in temp_dates
        ]

        return (out_daily_feed, out_hourly_feed)

    # def get_hourly_dataset(self, datetime_slice_iso_format: datetime.datetime):
    #     able_to_read = self.read_dataset_into_cache(datetime_value=datetime_slice_iso_format)
    #     if not able_to_read:
    #         print(
    #             f"Telemetry Dataset not available on Disk for Metric: {self.aggregation_metric} for Date: {str(datetime_slice_iso_format.date())}"
    #         )
    #         print(f"The data calculations might be skewed.")
    #         return None
    #     target_df = self.metrics_dataframes.get(str(datetime_slice_iso_format.date()))
    #     target_df = target_df.get_dataset(ds_name=MetricsDatasetNames.metricsapi_representation.name)
    #     row_range = target_df[METRICS_CSV_COLUMNS.IN_TS]
    #     row_switcher = row_range.isin([str(datetime_slice_iso_format)])

    #     out = []
    #     for row_val in self.data.itertuples(index=False, name="TelemetryData"):
    #         out.extend(
    #             [
    #                 {
    #                     METRICS_CSV_COLUMNS.OUT_TS: presence_ts,
    #                     METRICS_CSV_COLUMNS.OUT_KAFKA_CLUSTER: row_val["resource.kafka.id"],
    #                     METRICS_CSV_COLUMNS.OUT_PRINCIPAL: row_val.metric.principal_id,
    #                     self.aggregation_metric: row_val.value,
    #                 }
    #                 for presence_flag, presence_ts in zip(row_switcher, row_range)
    #                 if bool(presence_flag) is True
    #             ]
    #         )
    #     return self.aggregation_metric, pd.DataFrame.from_records(
    #         out,
    #         index=[
    #             METRICS_CSV_COLUMNS.OUT_TS,
    #             METRICS_CSV_COLUMNS.OUT_KAFKA_CLUSTER,
    #         ],
    #     )
