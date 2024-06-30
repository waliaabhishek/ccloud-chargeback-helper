import datetime
from enum import Enum, auto
import logging
from dataclasses import InitVar, dataclass, field
from typing import Callable, Dict
from urllib import parse

import pandas as pd
import requests

from ccloud.connections import CCloudBase
from data_processing.data_handlers.prom_flink_compute_pool_stats_handler import (
    add_resp_to_dataset as add_flink_metrics_to_dataset,
    get_dataset_for_time_slice as get_flink_metrics_for_time_slice,
)
from data_processing.data_handlers.prom_principal_stats_handler import (
    add_resp_to_dataset as add_principal_metrics_to_dataset,
    get_dataset_for_time_slice as get_principal_metrics_for_time_slice,
)
from data_processing.data_handlers.prom_topic_stats_handler import (
    add_resp_to_dataset as add_topic_metrics_to_dataset,
    get_dataset_for_time_slice as get_topic_metrics_for_time_slice,
)
from data_processing.data_handlers.types import AbstractDataHandler
from helpers import logged_method

LOGGER = logging.getLogger(__name__)


class MetricCategory(Enum):
    PRINCIPAL = auto()
    TOPIC = auto()
    FLINK = auto()


class MetricsAPIPrometheusQueries(Enum):
    request_bytes = "sum by (kafka_id, principal_id) (confluent_kafka_server_request_bytes)"
    response_bytes = "sum by (kafka_id, principal_id) (confluent_kafka_server_response_bytes)"
    received_bytes = "sum by (kafka_id, topic) (confluent_kafka_server_received_bytes)"
    sent_bytes = "sum by (kafka_id, topic) (confluent_kafka_server_sent_bytes)"
    num_records_in = "sum by (compute_pool_id, flink_statement_name) (confluent_flink_num_records_in)"
    num_records_out = "sum by (compute_pool_id, flink_statement_name) (confluent_flink_num_records_out)"

    def get_name(self):
        return self.name

    @staticmethod
    def get_metric_category(enum_data):
        if enum_data.name in [
            "request_bytes",
            "response_bytes",
        ]:
            return MetricCategory.PRINCIPAL
        elif enum_data.name in [
            "received_bytes",
            "sent_bytes",
        ]:
            return MetricCategory.TOPIC
        elif enum_data.name in [
            "num_records_in",
            "num_records_out",
        ]:
            return MetricCategory.FLINK

    @staticmethod
    def get_add_method_from_class(metric_category: MetricCategory):
        if metric_category == MetricCategory.PRINCIPAL:
            return add_principal_metrics_to_dataset
        elif metric_category == MetricCategory.TOPIC:
            return add_topic_metrics_to_dataset
        elif metric_category == MetricCategory.FLINK:
            return add_flink_metrics_to_dataset

    def get_add_method(self) -> Callable:
        metric_category = MetricsAPIPrometheusQueries.get_metric_category(self)
        if metric_category == MetricCategory.PRINCIPAL:
            return add_principal_metrics_to_dataset
        elif metric_category == MetricCategory.TOPIC:
            return add_topic_metrics_to_dataset
        elif metric_category == MetricCategory.FLINK:
            return add_flink_metrics_to_dataset
        else:
            LOGGER.error(f"Unknown metric type for query: {self.name}")
            return

    def get_dataset_for_time_slice_method(self) -> Callable:
        metric_category = MetricsAPIPrometheusQueries.get_metric_category(self)
        if metric_category == MetricCategory.PRINCIPAL:
            return get_principal_metrics_for_time_slice
        elif metric_category == MetricCategory.TOPIC:
            return get_topic_metrics_for_time_slice
        elif metric_category == MetricCategory.FLINK:
            return get_flink_metrics_for_time_slice


class MetricsAPIColumnNames:
    timestamp = "Interval"
    query_type = "QueryType"
    cluster_id = "KafkaClusterID"
    principal_id = "PrincipalID"
    value = "Value"


METRICS_API_PROMETHEUS_QUERIES = MetricsAPIPrometheusQueries()
METRICS_API_COLUMNS = MetricsAPIColumnNames()


@dataclass
class PrometheusMetricsDataHandler(AbstractDataHandler, CCloudBase):
    in_prometheus_url: InitVar[str | None] = field(default="http://localhost:9090")
    in_prometheus_query_endpoint: InitVar[str] = field(default="/api/v1/query_range")
    in_connection_kwargs: Dict = field(default=None)
    in_connection_auth: Dict = field(default_factory=dict())
    days_per_query: int = field(default=7)
    max_days_in_memory: int = field(default=14)

    last_available_date: datetime.datetime = field(init=False)
    url: str = field(init=False)
    metrics_dataset: pd.DataFrame = field(init=False, default=None)
    topic_dataset: pd.DataFrame = field(init=False, default=None)
    flink_dataset: pd.DataFrame = field(init=False, default=None)

    def __post_init__(self, in_prometheus_url, in_prometheus_query_endpoint) -> None:
        # Initialize the super classes to set the internal attributes
        AbstractDataHandler.__init__(self, start_date=self.start_date)
        CCloudBase.__post_init__(self)
        LOGGER.info("Setting up Auth Type Supplied by the config file")
        self.override_auth_type_from_yaml(self.in_connection_auth)
        self.url = parse.urljoin(base=in_prometheus_url, url=in_prometheus_query_endpoint)
        LOGGER.debug(f"Prometheus URL: {self.url}")
        end_date = self.start_date + datetime.timedelta(days=self.days_per_query)
        # Set up params for querying the Billing API
        for item in MetricsAPIPrometheusQueries:
            self.read_all(start_date=self.start_date, end_date=end_date, query_type=item)
        self.last_available_date = end_date
        LOGGER.debug(f"Finished Initializing PrometheusMetricsDataHandler")

    @logged_method
    def read_all(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        query_type: MetricsAPIPrometheusQueries,
        params={"step": 3600},
        **kwargs,
    ):
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        post_body = {}
        post_body["start"] = f'{start_date.replace(tzinfo=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")}+00:00'
        post_body["end"] = f'{end_date.replace(tzinfo=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")}+00:00'
        post_body["step"] = params["step"]
        post_body["query"] = query_type.value
        LOGGER.debug(f"Post Body: {post_body}")
        resp = requests.post(
            url=self.url, auth=self.http_connection, headers=headers, data=post_body, **self.in_connection_kwargs
        )
        if resp.status_code != 200:
            raise Exception("Could not connect to Prometheus Server. Please check your settings. " + resp.text)

        LOGGER.debug("Received 200 OK from API")
        out_json: dict = resp.json()
        if out_json is None or out_json["data"] is None:
            LOGGER.debug("No data found in the API response. Response Received is: " + str(out_json))
            return

        if out_json["data"]["result"]:
            LOGGER.info(f"Found {len(out_json['data']['result'])} items in API response.")
            self.add_resp_to_dataset(query_type=query_type, response=out_json)

    @logged_method
    def read_next_dataset(self, exposed_timestamp: datetime.datetime):
        if self.is_next_fetch_required(exposed_timestamp, self.last_available_date, next_fetch_within_days=2):
            effective_dates = self.calculate_effective_dates(
                self.last_available_date, self.days_per_query, self.max_days_in_memory
            )
            for item in MetricsAPIPrometheusQueries:
                self.read_all(
                    start_date=effective_dates.next_fetch_start_date,
                    end_date=effective_dates.next_fetch_end_date,
                    query_type=item,
                )
            self.last_available_date = effective_dates.next_fetch_end_date
            self.metrics_dataset, is_none = self.get_dataset_for_timerange(
                start_datetime=effective_dates.retention_start_date,
                end_datetime=effective_dates.retention_end_date,
                dataset=self.metrics_dataset,
            )
            self.topic_dataset, is_none = self.get_dataset_for_timerange(
                start_datetime=effective_dates.retention_start_date,
                end_datetime=effective_dates.retention_end_date,
                dataset=self.topic_dataset,
            )
            self.flink_dataset, is_none = self.get_dataset_for_timerange(
                start_datetime=effective_dates.retention_start_date,
                end_datetime=effective_dates.retention_end_date,
                dataset=self.flink_dataset,
            )

    @logged_method
    def add_resp_to_dataset(self, query_type: MetricsAPIPrometheusQueries, response: dict):
        """Add the response to the dataset

        Args:
            query_name (str): Name of the query
            response (dict): Response from the API
        """
        metric_type = MetricsAPIPrometheusQueries.get_metric_category(query_type.name)
        add_method = query_type.get_add_method()
        if metric_type is MetricCategory.PRINCIPAL:
            add_method(query_type, response, self.metrics_dataset)
            return
        elif metric_type is MetricCategory.TOPIC:
            add_method(query_type, response, self.topic_dataset)
            return
        elif metric_type is MetricCategory.FLINK:
            add_method(query_type, response, self.flink_dataset)
            return
        else:
            LOGGER.error(f"Unknown metric type for query: {query_type}")
            return

    @logged_method
    def get_dataset_for_timerange(
        self, start_datetime: datetime.datetime, end_datetime: datetime.datetime, api_dataset: pd.DataFrame, **kwargs
    ):
        """Wrapper over the internal method so that cross-imports are not necessary

        Args:
            start_datetime (datetime.datetime): Inclusive Start datetime
            end_datetime (datetime.datetime): Exclusive end datetime

        Returns:
            pd.Dataframe: Returns a pandas dataframe with the filtered data
        """
        return self._get_dataset_for_timerange(
            dataset=api_dataset,
            ts_column_name=METRICS_API_COLUMNS.timestamp,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )

    @logged_method
    def get_dataset_for_time_slice(
        self,
        time_slice: pd.Timestamp,
        api_dataset: pd.DataFrame,
        metrics_category: MetricCategory,
        **kwargs,
    ):
        """Wrapper over the internal method so that cross-imports are not necessary

        Args:
            time_slice (pd.Timestamp): Time slice to be used for fetching the data from datafame for the exact timestamp

        Returns:
            pd.DataFrame: Returns a pandas Dataframe with the filtered data.
        """
        temp_data, is_none = self._get_dataset_for_exact_timestamp(
            dataset=api_dataset, ts_column_name=METRICS_API_COLUMNS.timestamp, time_slice=time_slice
        )
        method_to_call = MetricsAPIPrometheusQueries.get_add_method_from_class(metrics_category)
        return method_to_call(temp_data, is_none)
