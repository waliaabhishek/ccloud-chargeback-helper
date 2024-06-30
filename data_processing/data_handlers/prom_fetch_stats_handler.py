import datetime
import logging
from dataclasses import InitVar, dataclass, field
from enum import Enum, auto
from urllib import parse

import requests

from helpers import logged_method

LOGGER = logging.getLogger(__name__)


class MetricsAPIPrometheusStatusQueries:
    objects_sync_status_name = "ccloud_objects"
    chargeback_sync_status_name = "billing_chargeback"
    status_query = "confluent_cloud_custom_scrape_status"

    def override_column_names(self, key, value):
        object.__setattr__(self, key, value)


class ScrapeType(Enum):
    BillingChargeback = auto()
    CCloudObjects = auto()


METRICS_API_PROMETHEUS_STATUS_QUERIES = MetricsAPIPrometheusStatusQueries()


@dataclass
class PrometheusStatusMetricsDataHandler:
    in_prometheus_url: InitVar[str | None] = field(default="http://localhost:9091")
    in_prometheus_query_endpoint: InitVar[str] = field(default="/api/v1/query")
    # days_per_query: int = field(default=2)
    # max_days_in_memory: int = field(default=30)

    url: str = field(init=False)
    # last_available_date: datetime.datetime = field(init=False)
    # scrape_status_dataset: Dict = field(init=False, repr=False, default_factory=dict)

    def __post_init__(self, in_prometheus_url, in_prometheus_query_endpoint) -> None:
        self.url = parse.urljoin(base=in_prometheus_url, url=in_prometheus_query_endpoint)
        LOGGER.debug(f"Current Prometheus URL: {self.url}")
        # end_date = self.start_date + datetime.timedelta(days=self.days_per_query)
        # Set up params for querying the Billing API
        # for item in [
        #     METRICS_API_PROMETHEUS_STATUS_QUERIES.status_query,
        # ]:
        #     self.read_all(start_date=self.start_date, end_date=end_date, query_type=item)
        # self.last_available_date = end_date

    @logged_method
    def convert_dt_to_ts(self, ts_date: datetime.datetime) -> int:
        return int(ts_date.timestamp())

    @logged_method
    def is_dataset_present(self, scrape_type: ScrapeType, ts_in_millis: int) -> bool:
        # return True if (scrape_type.value, ts_in_millis) in self.scrape_status_dataset.keys() else False
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        post_body = {}
        post_body["time"] = f"{ts_in_millis}"
        post_body[
            "query"
        ] = f'{METRICS_API_PROMETHEUS_STATUS_QUERIES.status_query}{{object_type="{METRICS_API_PROMETHEUS_STATUS_QUERIES.chargeback_sync_status_name}"}}'
        resp = requests.post(url=self.url, headers=headers, data=post_body)
        if resp.status_code == 200:
            out_json = resp.json()
            if out_json is not None and out_json["data"] is not None:
                if out_json["data"]["result"]:
                    for item in out_json["data"]["result"]:
                        if (
                            item["metric"]["object_type"]
                            == METRICS_API_PROMETHEUS_STATUS_QUERIES.chargeback_sync_status_name
                        ):
                            if item["value"][0] == ts_in_millis:
                                return True
        return False
