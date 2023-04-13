from prometheus_client import start_http_server, Gauge


class TimestampedGauge(Gauge):
    def __init__(self, *args, timestamp=None, **kwargs):
        self._timestamp = timestamp
        super().__init__(*args, **kwargs)

    def set_custom_timestamp(self, timestamp=None):
        self._timestamp = timestamp
        return self

    def collect(self):
        metrics = super().collect()
        for metric in metrics:
            metric.samples = [
                type(sample)(sample.name, sample.labels, sample.value, self._timestamp, sample.exemplar)
                for sample in metric.samples
            ]
        return metrics


# Start up the server to expose the metrics.
start_http_server(8000)

# # Generate some requests.
# while True:
#     time.sleep(3)
#     rand_num = random.randrange(1, 15)
#     test_gauge.set_custom_timestamp(time.time()).set(rand_num)
