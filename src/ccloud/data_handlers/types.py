from abc import ABC, abstractmethod
import datetime
from dataclasses import dataclass, field
import pandas as pd


@dataclass
class AbstractDataHandler(ABC):
    org_id: str = field(init=True)

    @abstractmethod
    def read_all(self, start_date: datetime.datetime, end_date: datetime.datetime, **kwargs):
        pass

    @abstractmethod
    def get_dataset_for_timeslot(self, start_datetime: datetime.datetime, end_datetime: datetime.datetime, **kwargs):
        pass

    @abstractmethod
    def read_next_dataset(self):
        pass

    def __generate_date_range_per_row(
        self, start_date: datetime.datetime, end_date: datetime.datetime, freq: str = "1H",
    ):
        start_date = datetime.datetime(year=start_date.year, month=start_date.month, day=start_date.day)
        end_date = datetime.datetime(year=end_date.year, month=end_date.month, day=end_date.day)
        end_date = end_date - datetime.timedelta(minutes=1)
        return pd.date_range(start_date, end_date, freq=freq)

    def execute_requests(self):
        self.read_next_dataset()
