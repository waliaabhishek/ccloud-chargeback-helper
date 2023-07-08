import datetime
from dataclasses import InitVar, dataclass, field
from typing import Dict, Tuple
from urllib import parse

import pandas as pd
import requests

from ccloud.connections import CCloudBase
from data_processing.data_handlers.types import AbstractDataHandler


class MetricsAPIPrometheusQueries:
    request_bytes_name = "request_bytes"
    response_bytes_name = "response_bytes"
    request_bytes = "sum by (kafka_id, principal_id) (confluent_kafka_server_request_bytes)"
    response_bytes = "sum by (kafka_id, principal_id) (confluent_kafka_server_response_bytes)"

    def override_column_names(self, key, value):
        object.__setattr__(self, key, value)


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

    def __post_init__(self, in_prometheus_url, in_prometheus_query_endpoint) -> None:
        # Initialize the super classes to set the internal attributes
        AbstractDataHandler.__init__(self, start_date=self.start_date)
        CCloudBase.__post_init__(self)
        self.override_auth_type_from_yaml(self.in_connection_auth)
        self.url = parse.urljoin(base=in_prometheus_url, url=in_prometheus_query_endpoint)
        end_date = self.start_date + datetime.timedelta(days=self.days_per_query)
        # Set up params for querying the Billing API
        for item in [
            METRICS_API_PROMETHEUS_QUERIES.request_bytes_name,
            METRICS_API_PROMETHEUS_QUERIES.response_bytes_name,
        ]:
            self.read_all(start_date=self.start_date, end_date=end_date, query_type=item)
        self.last_available_date = end_date

    def read_all(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        query_type: str,
        params={"step": 3600},
        **kwargs,
    ):
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        post_body = {}
        post_body["start"] = f'{start_date.replace(tzinfo=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")}+00:00'
        post_body["end"] = f'{end_date.replace(tzinfo=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")}+00:00'
        post_body["step"] = params["step"]
        post_body["query"] = METRICS_API_PROMETHEUS_QUERIES.__getattribute__(query_type)
        resp = requests.post(
            url=self.url, auth=self.http_connection, headers=headers, data=post_body, **self.in_connection_kwargs
        )
        if resp.status_code == 200:
            out_json = resp.json()
            if out_json is not None and out_json["data"] is not None:
                if out_json["data"]["result"]:
                    for item in out_json["data"]["result"]:
                        temp_data = [
                            {
                                METRICS_API_COLUMNS.timestamp: pd.to_datetime(in_item[0], unit="s", utc=True),
                                METRICS_API_COLUMNS.query_type: query_type,
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
                                                METRICS_API_COLUMNS.query_type,
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
                                        METRICS_API_COLUMNS.query_type,
                                        METRICS_API_COLUMNS.cluster_id,
                                        METRICS_API_COLUMNS.principal_id,
                                    ],
                                )
        else:
            raise Exception("Could not connect to Prometheus Server. Please check your settings. " + resp.text)

    def read_next_dataset(self, exposed_timestamp: datetime.datetime):
        if self.is_next_fetch_required(exposed_timestamp, self.last_available_date, next_fetch_within_days=2):
            effective_dates = self.calculate_effective_dates(
                self.last_available_date, self.days_per_query, self.max_days_in_memory
            )
            for item in [
                METRICS_API_PROMETHEUS_QUERIES.request_bytes_name,
                METRICS_API_PROMETHEUS_QUERIES.response_bytes_name,
            ]:
                self.read_all(
                    start_date=effective_dates.next_fetch_start_date,
                    end_date=effective_dates.next_fetch_end_date,
                    query_type=item,
                )
            self.last_available_date = effective_dates.next_fetch_end_date
            self.metrics_dataset, is_none = self.get_dataset_for_timerange(
                start_datetime=effective_dates.retention_start_date, end_datetime=effective_dates.retention_end_date
            )

    def get_dataset_for_timerange(self, start_datetime: datetime.datetime, end_datetime: datetime.datetime, **kwargs):
        """Wrapper over the internal method so that cross-imports are not necessary

        Args:
            start_datetime (datetime.datetime): Inclusive Start datetime
            end_datetime (datetime.datetime): Exclusive end datetime

        Returns:
            pd.Dataframe: Returns a pandas dataframe with the filtered data
        """
        return self._get_dataset_for_timerange(
            dataset=self.metrics_dataset,
            ts_column_name=METRICS_API_COLUMNS.timestamp,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )

    def get_dataset_for_time_slice(self, time_slice: pd.Timestamp, **kwargs):
        """Wrapper over the internal method so that cross-imports are not necessary

        Args:
            time_slice (pd.Timestamp): Time slice to be used for fetching the data from datafame for the exact timestamp

        Returns:
            pd.DataFrame: Returns a pandas Dataframe with the filtered data.
        """
        temp_data, is_none = self._get_dataset_for_exact_timestamp(
            dataset=self.metrics_dataset, ts_column_name=METRICS_API_COLUMNS.timestamp, time_slice=time_slice
        )
        if is_none:
            return pd.DataFrame(
                {},
                index=[
                    METRICS_API_COLUMNS.timestamp,
                    METRICS_API_COLUMNS.query_type,
                    METRICS_API_COLUMNS.cluster_id,
                    METRICS_API_COLUMNS.principal_id,
                ],
            )
        else:
            return temp_data
