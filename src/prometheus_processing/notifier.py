from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import InitVar, dataclass, field
from typing import List

import datetime


class Observer(ABC):
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
    _observers: List[Observer]
    _exported_timestamp: datetime.datetime = field(init=False)

    def __init__(self) -> None:
        super().__init__()
        self._observers = []

    def set_timestamp(self, curr_timestamp: datetime.datetime = None):
        self.__exported_timestamp = self.normalize_datetime(in_dt=curr_timestamp)

    def normalize_datetime(self, in_dt: datetime.datetime = None) -> datetime.datetime:
        if in_dt is not None:
            return in_dt.combine(time=datetime.time.min).replace(tzinfo=datetime.timezone.utc)
        else:
            return datetime.datetime.now(tz=datetime.timezone.utc).combine(time=datetime.time.min)

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

