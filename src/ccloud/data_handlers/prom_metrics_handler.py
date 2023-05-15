from dataclasses import InitVar, dataclass, field
import datetime
from decimal import Decimal
import requests
from time import sleep
from urllib import parse
from typing import Dict, List
import pandas as pd
from ccloud.connections import CCloudBase
from ccloud.data_handlers.types import AbstractDataHandler


class MetricsAPIPrometheusNames:
    request_bytes_name = "request_bytes_value"
    response_bytes_name = "response_bytes_value"
    request_bytes_value = "sum by (kafka_id, principal_id) (confluent_kafka_server_request_bytes)"
    response_bytes_value = "sum by (kafka_id, principal_id) (confluent_kafka_server_response_bytes)"

    def override_column_names(self, key, value):
        object.__setattr__(self, key, value)


class MetricsAPIColumnNames:
    timestamp = "Interval"
    cluster_id = "KafkaClusterID"
    principal_id = "PrincipalID"
    value = "Value"


METRICS_API_PROMETHEUS_NAMES = MetricsAPIPrometheusNames()
METRICS_API_COLUMNS = MetricsAPIColumnNames()


@dataclass
class PrometheusMetricsDataHandler(AbstractDataHandler, CCloudBase):
    in_prometheus_url: InitVar[str | None] = field(default="http://localhost:9090")
    start_date: datetime.datetime = field(init=True)
    days_per_query: int = field(default=7)
    max_days_in_memory: int = field(default=30)

    last_available_date: datetime.datetime = field(init=False)
    url: str = field(init=False)
    metrics_dataset: pd.DataFrame = field(init=False, default=None)

    def __post_init__(self, in_prometheus_url) -> None:
        # Initialize the super classes to set the internal attributes
        super(AbstractDataHandler, self).__post_init__()
        super(CCloudBase, self).__post_init__()
        temp_date = self.start_date.date()
        self.start_date = datetime.datetime(year=temp_date.year, month=temp_date.month, day=temp_date.day)
        # Check if the URL contains the required URI or not
        temp_url = parse.urlparse(in_prometheus_url)
        temp_url.path, temp_url.params, temp_url.query, temp_url.fragment = "/api/v1/query_range", None, None, None
        self.url = parse.urlunparse(temp_url)
        # Calculate the end_date from start_date plus number of days per query
        end_date = self.start_date + datetime.timedelta(days=self.days_per_query)
        # Set up params for querying the Billing API
        self.read_all(
            start_date=self.start_date, end_date=end_date, query_type=METRICS_API_PROMETHEUS_NAMES.request_bytes_name
        )
        self.last_available_date = end_date

    def read_all(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        query_type: str,
        params={"step": 3600},
        **kwargs
    ):
        params["start"] = start_date
        params["end"] = end_date
        params["query"] = METRICS_API_PROMETHEUS_NAMES.__getattribute__(query_type)
        resp = requests.get(url=self.url, auth=self.http_connection, params=params)
        if resp.status_code == 200:
            out_json = resp.json()
            if out_json is not None and out_json["data"] is not None:
                for item in out_json["data"]["result"]:
                    temp_data = [
                        {
                            METRICS_API_COLUMNS.timestamp: pd.to_datetime(in_item[0], unit="s"),
                            METRICS_API_COLUMNS.cluster_id: item["metric"]["kafka_id"],
                            METRICS_API_COLUMNS.principal_id: item["metric"]["principal_id"],
                            METRICS_API_COLUMNS.value: in_item[1],
                        }
                        for in_item in item["values"]
                    ]
                    if temp_data:
                        if self.metrics_dataset is not None:
                            self.metrics_dataset = pd.concat(
                                [
                                    self.metrics_dataset,
                                    pd.DataFrame.from_records(
                                        temp_data,
                                        index=[
                                            METRICS_API_COLUMNS.timestamp,
                                            METRICS_API_COLUMNS.cluster_id,
                                            METRICS_API_COLUMNS.principal_id,
                                        ],
                                    ),
                                ]
                            )
                        else:
                            self.metrics_dataset = pd.DataFrame.from_records(
                                temp_data,
                                index=[
                                    METRICS_API_COLUMNS.timestamp,
                                    METRICS_API_COLUMNS.cluster_id,
                                    METRICS_API_COLUMNS.principal_id,
                                ],
                            )

    def read_next_dataset(self):
        self.start_date = self.last_available_date
        end_date = self.start_date + datetime.timedelta(days=self.days_per_query)
        self.read_all(start_date=self.start_date, end_date=end_date)
        self.last_available_date = end_date
        in_mem_date_cutoff = self.last_available_date - datetime.timedelta(days=self.max_days_in_memory)
        self.metrics_dataset = self.get_dataset_for_timeslot(start_datetime=in_mem_date_cutoff, end_datetime=end_date)

    def get_dataset_for_timeslot(self, start_datetime: datetime.datetime, end_datetime: datetime.datetime, **kwargs):
        start_date = pd.to_datetime(str(start_datetime.date()))
        end_date = pd.to_datetime(str(end_datetime.date()))
        return self.metrics_dataset.loc[
            (self.metrics_dataset[METRICS_API_COLUMNS.timestamp] >= start_date)
            & (self.metrics_dataset[METRICS_API_COLUMNS.timestamp] < end_date)
        ]

