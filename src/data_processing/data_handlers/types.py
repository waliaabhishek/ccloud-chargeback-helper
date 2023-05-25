import datetime
from abc import ABC, abstractmethod
from dataclasses import dataclass
from types import NoneType
from typing import Tuple

import pandas as pd


@dataclass
class AbstractDataHandler(ABC):
    @abstractmethod
    def read_all(self, start_date: datetime.datetime, end_date: datetime.datetime, **kwargs):
        pass

    @abstractmethod
    def get_dataset_for_timerange(self, start_datetime: datetime.datetime, end_datetime: datetime.datetime, **kwargs):
        pass

    @abstractmethod
    def read_next_dataset(self):
        pass

    def _generate_date_range_per_row(
        self, start_date: datetime.datetime, end_date: datetime.datetime, freq: str = "1H",
    ):
        start_date = start_date.replace(tzinfo=datetime.timezone.utc).combine(
            date=start_date.date(), time=datetime.time.min
        )
        end_date = end_date.replace(tzinfo=datetime.timezone.utc).combine(date=end_date.date(), time=datetime.time.min)
        end_date = end_date - datetime.timedelta(minutes=1)
        return pd.date_range(start_date, end_date, freq=freq)

    def _generate_next_timestamp(self, curr_date: datetime.datetime, freq: str = "1H",) -> pd.Timestamp:
        start_date = curr_date.replace(minute=0, microsecond=0, tzinfo=datetime.timezone.utc)
        return pd.date_range(start_date, freq=freq, periods=2)[1]

    def _get_dataset_for_timerange(
        self,
        dataset: pd.DataFrame,
        ts_column_name: str,
        start_datetime: datetime.datetime,
        end_datetime: datetime.datetime,
        **kwargs
    ):
        """Converts the chargeback dict stored internally to a dataframe and filter the data using the args

        Args:
            dataset (pd.DataFrame): input pandas Dataframe 
            ts_column_name (str): Column name for the timestamp column in the index
            start_datetime (datetime.datetime): Inclusive start datetime
            end_datetime (datetime.datetime): Exclusive End datetime

        Returns:
            pd.DatFrame: return filtered pandas DataFrame
        """
        start_date = pd.to_datetime(start_datetime)
        end_date = pd.to_datetime(end_datetime)
        return dataset[
            (dataset.index.get_level_values(ts_column_name) >= start_date)
            & (dataset.index.get_level_values(ts_column_name) < end_date)
        ]

    def _get_dataset_for_exact_timestamp(
        self, dataset: pd.DataFrame, ts_column_name: str, time_slice: pd.Timestamp, **kwargs
    ) -> Tuple[pd.DataFrame | None, bool]:
        """used to filter down the data in a dataframe to a specific timestamp that is present in a timestamp index

        Args:
            dataset (pd.DataFrame): The dataframe to filter the data
            ts_column_name (str): The timestamp column name used to filter the data
            time_slice (pd.Timestamp): The exact pandas timestamp used as the filter criterion

        Returns:
            _type_: _description_
        """
        if not isinstance(dataset, NoneType):
            if not dataset.empty:
                return (dataset[(dataset.index.get_level_values(ts_column_name) == time_slice)], False)
            else:
                return (dataset, False)
        else:
            return (None, True)

    def execute_requests(self):
        self.read_next_dataset()
