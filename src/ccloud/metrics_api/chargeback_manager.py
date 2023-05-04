from csv import QUOTE_NONNUMERIC
import datetime
from copy import deepcopy
from dataclasses import dataclass, field
import os
from typing import Dict, List, Tuple
import pandas as pd
from data_processing.billing_chargeback_processing import ChargebackDataframe, ChargebackTimeSliceType, get_date_format
from helpers import ensure_path

from storage_mgmt import CHARGEBACK_PERSISTENCE_STORE, STORAGE_PATH, DirType


@dataclass(kw_only=True)
class ChargebackManager:
    org_id: str
    cc_objects: object = field(init=True, repr=False)
    days_in_memory: int = field(default=3)
    hourly_dataset: Dict[str, ChargebackDataframe] = field(init=True, repr=False, default_factory=dict)
    daily_dataset: Dict[str, ChargebackDataframe] = field(init=True, repr=False, default_factory=dict)
    monthly_dataset: Dict[str, ChargebackDataframe] = field(init=True, repr=False, default_factory=dict)

    def __post_init__(self) -> None:
        # self.daily_dataset = dict()
        # self.hourly_dataset = dict()
        pass

    def run_calculations(
        self, time_slice: datetime.datetime, billing_dataframe: pd.DataFrame, metrics_dataframe: pd.DataFrame
    ):
        self.read_dataset_into_cache(datetime_value=time_slice)
        m_key, d_key, t_key = self.__determine_key_names(key=time_slice)
        m, d, t = self.__is_key_present_in_cache(time_slice)
        if m:
            self.monthly_dataset[m_key].compute_output(
                force_data_add=True,
                time_slice=time_slice,
                addl_billing_dataframe=billing_dataframe,
                addl_metrics_dataframe=metrics_dataframe,
            )
        else:
            self.add_dataset(
                datetime_value=time_slice,
                data_bucket=ChargebackTimeSliceType.MONTHLY,
                df=ChargebackDataframe(
                    cc_objects=self.cc_objects,
                    time_slice=time_slice,
                    bucket_type=ChargebackTimeSliceType.MONTHLY,
                    _metrics_dataframe=metrics_dataframe,
                    _billing_dataframe=billing_dataframe,
                ),
            )
        if d:
            self.daily_dataset[d_key].compute_output(
                force_data_add=True,
                time_slice=time_slice,
                addl_billing_dataframe=billing_dataframe,
                addl_metrics_dataframe=metrics_dataframe,
            )
        else:
            self.add_dataset(
                datetime_value=time_slice,
                data_bucket=ChargebackTimeSliceType.DAILY,
                df=ChargebackDataframe(
                    cc_objects=self.cc_objects,
                    time_slice=time_slice,
                    bucket_type=ChargebackTimeSliceType.DAILY,
                    _metrics_dataframe=metrics_dataframe,
                    _billing_dataframe=billing_dataframe,
                ),
            )
        if t:
            self.hourly_dataset[t_key].compute_output(
                force_data_add=True,
                time_slice=time_slice,
                addl_billing_dataframe=billing_dataframe,
                addl_metrics_dataframe=metrics_dataframe,
            )
        else:
            self.add_dataset(
                datetime_value=time_slice,
                data_bucket=ChargebackTimeSliceType.HOURLY,
                df=ChargebackDataframe(
                    cc_objects=self.cc_objects,
                    time_slice=time_slice,
                    bucket_type=ChargebackTimeSliceType.HOURLY,
                    _metrics_dataframe=metrics_dataframe,
                    _billing_dataframe=billing_dataframe,
                ),
            )

    def read_dataset_into_cache(self, datetime_value: datetime.datetime):
        m, d, t = self.__is_key_present_in_cache(key=datetime_value)
        basepath = STORAGE_PATH.get_path(org_id=self.org_id, dir_type=DirType.OutputData)
        if not m:
            _, _, file_path = self.get_filepath(
                time_slice=datetime_value, data_bucket=ChargebackTimeSliceType.MONTHLY, basepath=basepath
            )
            if not self.__is_file_present(file_path):
                print(f"Monthly Chargeback Cache Re-hydration requested. File not found for re-hydration: {file_path}")
            else:
                self.add_dataset(
                    datetime_value=datetime_value,
                    data_bucket=ChargebackTimeSliceType.MONTHLY,
                    df=ChargebackDataframe(
                        cc_objects=self.cc_objects,
                        time_slice=datetime_value,
                        bucket_type=ChargebackTimeSliceType.MONTHLY,
                        file_path=file_path,
                        _metrics_dataframe=None,
                        _billing_dataframe=None,
                    ),
                )
        if not d:
            _, _, file_path = self.get_filepath(
                time_slice=datetime_value, data_bucket=ChargebackTimeSliceType.DAILY, basepath=basepath
            )
            if not self.__is_file_present(file_path):
                print(f"Daily Chargeback Cache Re-hydration requested. File not found for re-hydration: {file_path}")
            else:
                self.add_dataset(
                    datetime_value=datetime_value,
                    data_bucket=ChargebackTimeSliceType.DAILY,
                    df=ChargebackDataframe(
                        cc_objects=self.cc_objects,
                        time_slice=datetime_value,
                        bucket_type=ChargebackTimeSliceType.DAILY,
                        file_path=file_path,
                        _metrics_dataframe=None,
                        _billing_dataframe=None,
                    ),
                )
        if not t:
            _, _, file_path = self.get_filepath(
                time_slice=datetime_value, data_bucket=ChargebackTimeSliceType.HOURLY, basepath=basepath
            )
            if not self.__is_file_present(file_path):
                print(f"Hourly Chargeback Cache Re-hydration requested. File not found for re-hydration: {file_path}")
            else:
                self.add_dataset(
                    datetime_value=datetime_value,
                    data_bucket=ChargebackTimeSliceType.HOURLY,
                    df=ChargebackDataframe(
                        cc_objects=self.cc_objects,
                        time_slice=datetime_value,
                        bucket_type=ChargebackTimeSliceType.HOURLY,
                        file_path=file_path,
                        _metrics_dataframe=None,
                        _billing_dataframe=None,
                    ),
                )

    def add_dataset(self, datetime_value: datetime.datetime, data_bucket: str, df: ChargebackDataframe):
        month_key, date_key, time_key = self.__determine_key_names(key=datetime_value)
        if data_bucket == ChargebackTimeSliceType.HOURLY:
            self.hourly_dataset[time_key] = df
        elif data_bucket == ChargebackTimeSliceType.DAILY:
            self.daily_dataset[date_key] = df
        elif data_bucket == ChargebackTimeSliceType.MONTHLY:
            self.monthly_dataset[month_key] = df

    def get_filepath(
        self,
        time_slice: datetime.datetime,
        data_bucket: str,
        basepath: str,
    ):
        date_folder_path = os.path.join(basepath, f"{str(time_slice.date())}")
        ensure_path(path=date_folder_path)

        file_name = f"{time_slice.strftime(get_date_format(data_bucket))}__chargeback.csv"
        if data_bucket == ChargebackTimeSliceType.HOURLY or data_bucket == ChargebackTimeSliceType.DAILY:
            file_path = os.path.join(basepath, f"{str(time_slice.date())}", file_name)
        elif data_bucket == ChargebackTimeSliceType.MONTHLY:
            file_path = os.path.join(basepath, file_name)

        return date_folder_path, file_name, file_path

    def __is_key_present_in_cache(self, key: datetime.datetime) -> Tuple[bool, bool]:
        m, d, t = self.__determine_key_names(key=key)
        return m in self.monthly_dataset.keys(), d in self.daily_dataset.keys(), t in self.hourly_dataset.keys()

    def __is_file_present(self, file_path: str) -> bool:
        return os.path.exists(file_path) and os.path.isfile(file_path)

    def __determine_key_names(self, key: datetime.datetime) -> Tuple[str, str]:
        monthly_key = key.date().strftime(get_date_format(ChargebackTimeSliceType.MONTHLY))
        date_key = key.date().strftime(get_date_format(ChargebackTimeSliceType.DAILY))
        time_key = key.strftime(get_date_format(ChargebackTimeSliceType.HOURLY))
        return monthly_key, date_key, time_key

    def output_to_csv(self, basepath: str):
        basepath = STORAGE_PATH.get_path(org_id=self.org_id, dir_type=DirType.OutputData)
        for k, v in self.monthly_dataset.items():
            df = v.cb_unit.get_dataframe()
            _, file_name, file_path = self.get_filepath(
                time_slice=v.time_slice,
                data_bucket=ChargebackTimeSliceType.MONTHLY,
                basepath=basepath,
            )
            df.to_csv(file_path, quoting=QUOTE_NONNUMERIC)
            CHARGEBACK_PERSISTENCE_STORE.add_data_to_persistence_store(
                org_id=self.org_id, key=(k, ChargebackTimeSliceType.MONTHLY), value=file_name
            )
            # CHARGEBACK_PERSISTENCE_STORE.add_data_to_persistence_store(date_value=k, metric_name=file_name)
        for k, v in self.daily_dataset.items():
            df = v.cb_unit.get_dataframe()
            _, file_name, file_path = self.get_filepath(
                time_slice=v.time_slice,
                data_bucket=ChargebackTimeSliceType.DAILY,
                basepath=basepath,
            )
            df.to_csv(file_path, quoting=QUOTE_NONNUMERIC)
            CHARGEBACK_PERSISTENCE_STORE.add_data_to_persistence_store(
                org_id=self.org_id, key=(k, ChargebackTimeSliceType.DAILY), value=file_name
            )
            # CHARGEBACK_PERSISTENCE_STORE.add_data_to_persistence_store(date_value=k, metric_name=file_name)
        for k, v in self.hourly_dataset.items():
            df = v.cb_unit.get_dataframe()
            _, file_name, file_path = self.get_filepath(
                time_slice=v.time_slice,
                data_bucket=ChargebackTimeSliceType.HOURLY,
                basepath=basepath,
            )
            df.to_csv(file_path, quoting=QUOTE_NONNUMERIC)
            CHARGEBACK_PERSISTENCE_STORE.add_data_to_persistence_store(
                org_id=self.org_id, key=(k, ChargebackTimeSliceType.HOURLY), value=file_name
            )
            # CHARGEBACK_PERSISTENCE_STORE.add_data_to_persistence_store(date_value=k, metric_name=file_name)

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
