from dataclasses import InitVar, dataclass, field
from prometheus_client import start_http_server, Gauge

import datetime

from prometheus_processing.notifier import NotifierAbstract


@dataclass(init=False)
class TimestampedCollector(NotifierAbstract, Gauge):
    in_begin_timestamp: InitVar[datetime.datetime] = None

    def __init__(self, *args, in_begin_timestamp: datetime.datetime, **kwargs):
        super().__init__(*args, **kwargs)
        super(NotifierAbstract, self).__init__()
        super(Gauge, self).__init__(*args, **kwargs)
        self.set_timestamp(curr_timestamp=in_begin_timestamp)

    def collect(self):
        try:
            metrics = super().collect()
            ts_value = int(self.__exported_timestamp.timestamp())
            for metric in metrics:
                metric.samples = [
                    type(sample)(sample.name, sample.labels, sample.value, ts_value, sample.exemplar)
                    for sample in metric.samples
                ]
            return metrics
        finally:
            self.notify()

    def notify(self) -> None:
        for item in self._observers:
            item.update(self)
