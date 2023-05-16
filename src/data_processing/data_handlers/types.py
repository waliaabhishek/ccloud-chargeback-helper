from abc import ABC, abstractmethod
import datetime
from dataclasses import dataclass, field
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

    def __generate_date_range_per_row(
        self, start_date: datetime.datetime, end_date: datetime.datetime, freq: str = "1H",
    ):
        start_date = start_date.replace(tzinfo=datetime.timezone.utc).combine(time=datetime.time.min)
        end_date = end_date.replace(tzinfo=datetime.timezone.utc).combine(time=datetime.time.min)
        end_date = end_date - datetime.timedelta(minutes=1)
        return pd.date_range(start_date, end_date, freq=freq)

    def execute_requests(self):
        self.read_next_dataset()
