import datetime
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from types import NoneType
from typing import Tuple

import pandas as pd

from helpers import logged_method

LOGGER = logging.getLogger(__name__)


@dataclass
class EffectiveDates:
    curr_start_date: datetime.datetime
    curr_end_date: datetime.datetime
    next_fetch_start_date: datetime.datetime
    next_fetch_end_date: datetime.datetime
    retention_start_date: datetime.datetime
    retention_end_date: datetime.datetime


@dataclass
class AbstractDataHandler(ABC):
    start_date: datetime.datetime = field(init=True)

    @abstractmethod
    def read_all(self, start_date: datetime.datetime, end_date: datetime.datetime, **kwargs):
        pass

    @abstractmethod
    def get_dataset_for_timerange(self, start_datetime: datetime.datetime, end_datetime: datetime.datetime, **kwargs):
        pass

    @abstractmethod
    def read_next_dataset(self, exposed_timestamp: datetime.datetime):
        pass

    @logged_method
    def _generate_date_range_per_row(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        freq: str = "1H",
    ):
        start_date = start_date.replace(tzinfo=datetime.timezone.utc).combine(
            date=start_date.date(),
            time=datetime.time.min,
            tzinfo=datetime.timezone.utc,
        )
        end_date = end_date.replace(tzinfo=datetime.timezone.utc).combine(
            date=end_date.date(),
            time=datetime.time.min,
            tzinfo=datetime.timezone.utc,
        )
        end_date = end_date - datetime.timedelta(minutes=1)
        return pd.date_range(start_date, end_date, freq=freq)

    @logged_method
    def _generate_next_timestamp(
        self, curr_date: datetime.datetime, freq: str = "1H", position: int = 1
    ) -> pd.Timestamp:
        """Generates the pandas Timestamp item from the provided datetime object based on the frequency provided.
        position == 0 for converting current datetime to the pandas timestamp object
        position == 1 for converting next in freq sequence item to the pandas timestamp object

        Args:
            curr_date (datetime.datetime): datetime object which will be converted
            freq (str, optional): pandas freq object compatible string. Defaults to "1H".
            position (int, optional): which positional item will be converted to the pandas timestamp object. Defaults to 1.

        Returns:
            pd.Timestamp: converted timestamp object
        """
        start_date = curr_date.replace(minute=0, microsecond=0, tzinfo=datetime.timezone.utc)
        return pd.date_range(start_date, freq=freq, periods=2)[position]

    @logged_method
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

        if isinstance(dataset, NoneType):
            return (None, True)

        if dataset.empty:
            return (dataset, False)

        return (
            dataset[
                (dataset.index.get_level_values(ts_column_name) >= start_date)
                & (dataset.index.get_level_values(ts_column_name) < end_date)
            ],
            False,
        )

    @logged_method
    def calculate_effective_dates(
        self,
        last_available_date: datetime.datetime,
        days_per_query: int,
        max_days_in_memory: int,
    ) -> EffectiveDates:
        curr_start_date = last_available_date - datetime.timedelta(days=days_per_query)
        curr_end_date = last_available_date
        next_fetch_start_date = last_available_date
        next_end_fetch_date = last_available_date + datetime.timedelta(days=days_per_query)
        retention_start_date = next_end_fetch_date - datetime.timedelta(days=max_days_in_memory)
        retention_end_date = next_end_fetch_date
        return EffectiveDates(
            curr_start_date,
            curr_end_date,
            next_fetch_start_date,
            next_end_fetch_date,
            retention_start_date,
            retention_end_date,
        )

    @logged_method
    def is_next_fetch_required(
        self,
        curr_exposed_datetime: datetime.datetime,
        last_available_date: datetime.datetime,
        next_fetch_within_days: int = 2,
    ):
        if (
            abs(int((curr_exposed_datetime - last_available_date) / datetime.timedelta(days=1)))
            < next_fetch_within_days
        ):
            LOGGER.debug("Next fetch is required as the data is not available in memory")
            return True
        else:
            LOGGER.debug("Next fetch is not required as the data is available in memory")
            return False

    @logged_method
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
        if isinstance(dataset, NoneType):
            return (None, True)

        if dataset.empty:
            return (dataset, False)

        temp_ds = dataset.index.get_level_values(ts_column_name)
        return (dataset[(dataset.index.get_level_values(ts_column_name) == time_slice)], False)

    @logged_method
    def execute_requests(self, exposed_timestamp: datetime.datetime):
        self.read_next_dataset(exposed_timestamp=exposed_timestamp)
