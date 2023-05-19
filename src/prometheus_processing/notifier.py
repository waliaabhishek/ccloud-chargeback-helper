from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List

import datetime


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
        print("Observer: Sending request to attach for notifications.")
        notifier.attach(self)


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
            return in_dt.combine(date=in_dt.date(), time=datetime.time.min).replace(tzinfo=datetime.timezone.utc)
        else:
            return datetime.datetime.combine(
                date=datetime.datetime.now(tz=datetime.timezone.utc).date(),
                time=datetime.time.min,
                tzinfo=datetime.timezone.utc,
            )

    def attach(self, observer: Observer) -> None:
        """
        Attach an observer to the Notifier.
        """
        print("Notifier: Attaching an observer.")
        self._observers.append(observer)

    def detach(self, observer: Observer) -> None:
        """
        Detach an observer from the Notifier.
        """
        print("Notifier: Detaching an observer.")
        self._observers.remove(observer)

    @abstractmethod
    def notify(self) -> None:
        """
        Notify all observers about an event.
        """
        pass

