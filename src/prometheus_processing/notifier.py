from __future__ import annotations

import datetime
from abc import ABC, abstractmethod
from typing import List

import pandas as pd

from helpers import LOGGER, logged_method


class Observer(ABC):
    """The Observer Pattern used here is used to signal an observer to a notifier class.
    The notifier class collects all the references to the observer and notifies them when a new event occurs.
    This needs the observer to be registered under the notifier using the attach method of the observer

    """

    @abstractmethod
    def update(self, notifier: NotifierAbstract) -> None:
        """
        Receive update from Notifier and do something.
        """
        pass

    def attach(self, notifier: NotifierAbstract) -> None:
        """
        Attach an observer to the Notifier.
        """
        LOGGER.debug("Observer: Sending request to attach for notifications.")
        notifier.attach(self)

    def _generate_next_timestamp(
        self, curr_date: datetime.datetime, freq: str = "1H", periods: int = 2
    ) -> pd.Timestamp:
        start_date = curr_date.replace(minute=0, microsecond=0, tzinfo=datetime.timezone.utc)
        return pd.date_range(start_date, freq=freq, periods=periods)[1]


class NotifierAbstract(ABC):
    """This class works in conjunction with the Observer Class above to gather all the observers in a list
    and execute their update method when some action is performed.
    It is a custom way to link different objects together while not disturbing any existing functionality.

    """

    _observers: List[Observer]
    _exported_timestamp: datetime.datetime

    def __init__(self) -> None:
        self._observers = []

    def set_timestamp(self, curr_timestamp: datetime.datetime = None):
        """used to set the timestamp that we use to assign timestamp to the Prometheus collector

        Args:
            curr_timestamp (datetime.datetime, optional): Provide the timestamp to be used as timestamp for the next collection cycle. Defaults to None.
        """
        self._exported_timestamp = self.normalize_datetime(in_dt=curr_timestamp)
        return self

    def normalize_datetime(self, in_dt: datetime.datetime = None) -> datetime.datetime:
        """Internal method to normalize a datetime toa  specific targeted format.
        Changes the timezone to UTC and sets the time to midnight for the datetime.

        Args:
            in_dt (datetime.datetime, optional): Input datetime for normalization. Defaults to None.

        Returns:
            datetime.datetime: Output normalized datetime
        """
        if in_dt is not None:
            return in_dt.replace(minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc)

        else:
            return datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc)

    @logged_method
    def attach(self, observer: Observer) -> None:
        """
        Attach an observer to the Notifier.
        """
        LOGGER.debug("Notifier: Attaching an observer.")
        self._observers.append(observer)

    @logged_method
    def detach(self, observer: Observer) -> None:
        """
        Detach an observer from the Notifier.
        """
        LOGGER.debug("Notifier: Detaching an observer.")
        self._observers.remove(observer)

    @abstractmethod
    def notify(self) -> None:
        """
        Notify all observers about an event.
        """
        pass
