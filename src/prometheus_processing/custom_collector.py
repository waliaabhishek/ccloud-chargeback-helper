import datetime

from prometheus_client import Gauge
from prometheus_processing.notifier import NotifierAbstract


class TimestampedCollector(NotifierAbstract, Gauge):
    def __init__(self, *args, in_begin_timestamp: datetime.datetime = None, **kwargs):
        NotifierAbstract.__init__(self)
        Gauge.__init__(self, *args, **kwargs)
        if in_begin_timestamp is not None:
            self.set_timestamp(curr_timestamp=in_begin_timestamp)

    def collect(self):
        try:
            metrics = super().collect()
            ts_value = int(self._exported_timestamp.timestamp()) / 1000
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

    def convert_ts_to_str(self, input_datetime: datetime.datetime) -> str:
        return input_datetime.strftime("%Y_%m_%d_%H_%M_%S")
