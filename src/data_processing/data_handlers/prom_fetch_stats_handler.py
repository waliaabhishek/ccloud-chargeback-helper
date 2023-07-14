import datetime
from dataclasses import InitVar, dataclass, field
from urllib import parse
from enum import Enum, auto
import requests


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
        # end_date = self.start_date + datetime.timedelta(days=self.days_per_query)
        # Set up params for querying the Billing API
        # for item in [
        #     METRICS_API_PROMETHEUS_STATUS_QUERIES.status_query,
        # ]:
        #     self.read_all(start_date=self.start_date, end_date=end_date, query_type=item)
        # self.last_available_date = end_date

    def convert_dt_to_ts(self, ts_date: datetime.datetime) -> int:
        return int(ts_date.timestamp())

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

    # def read_all(
    #     self,
    #     start_date: datetime.datetime,
    #     end_date: datetime.datetime,
    #     query_type: str,
    #     params={"step": 3600},
    #     **kwargs,
    # ):
    #     headers = {"Content-Type": "application/x-www-form-urlencoded"}
    #     post_body = {}
    #     post_body["start"] = f'{start_date.replace(tzinfo=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")}+00:00'
    #     post_body["end"] = f'{end_date.replace(tzinfo=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")}+00:00'
    #     post_body["step"] = params["step"]
    #     post_body["query"] = METRICS_API_PROMETHEUS_STATUS_QUERIES.status_query
    #     resp = requests.post(url=self.url, headers=headers, data=post_body)
    #     if resp.status_code == 200:
    #         out_json = resp.json()
    #         if out_json is not None and out_json["data"] is not None:
    #             if out_json["data"]["result"]:
    #                 for item in out_json["data"]["result"]:
    #                     if (
    #                         item["metric"]["object_type"]
    #                         == METRICS_API_PROMETHEUS_STATUS_QUERIES.chargeback_sync_status_name
    #                     ):
    #                         for item_values in item["values"]:
    #                             self.add_to_dataset(
    #                                 scrape_type=ScrapeType.BillingChargeback, ts_in_millis=item_values[0]
    #                             )
    #                     if (
    #                         item["metric"]["object_type"]
    #                         == METRICS_API_PROMETHEUS_STATUS_QUERIES.objects_sync_status_name
    #                     ):
    #                         for item_values in item["values"]:
    #                             self.add_to_dataset(scrape_type=ScrapeType.CCloudObjects, ts_in_millis=item_values[0])

    # def add_to_dataset(self, scrape_type: ScrapeType, ts_in_millis: int):
    #     self.scrape_status_dataset[(scrape_type.value, ts_in_millis)] = None

    # def read_next_dataset(self, exposed_timestamp: datetime.datetime):
    #     if self.is_next_fetch_required(exposed_timestamp, self.last_available_date, next_fetch_within_days=2):
    #         effective_dates = self.calculate_effective_dates(
    #             self.last_available_date, self.days_per_query, self.max_days_in_memory
    #         )
    #         for item in [
    #             METRICS_API_PROMETHEUS_STATUS_QUERIES.objects_sync_status_name,
    #             METRICS_API_PROMETHEUS_STATUS_QUERIES.chargeback_sync_status_name,
    #         ]:
    #             self.read_all(
    #                 start_date=effective_dates.next_fetch_start_date,
    #                 end_date=effective_dates.next_fetch_end_date,
    #                 query_type=item,
    #             )
    #         self.last_available_date = effective_dates.next_fetch_end_date
    #         self.cleanup_old_data(retention_start_date=effective_dates.retention_start_date)
    #         self.metrics_dataset, is_none = self.get_dataset_for_timerange(
    #             start_datetime=effective_dates.retention_start_date, end_datetime=effective_dates.retention_end_date
    #         )

    # def cleanup_old_data(self, retention_start_date: datetime.datetime):
    #     start_ts = self.convert_dt_to_ts(ts_date=retention_start_date)
    #     for (k1, k2), _ in self.scrape_status_dataset.copy().items():
    #         if k2 < start_ts:
    #             del self.scrape_status_dataset[(k1, k2)]

    # def _get_dataset_for_timerange(
    #     self,
    #     start_datetime: datetime.datetime,
    #     end_datetime: datetime.datetime,
    #     **kwargs,
    # ):
    #     start_ts = self.convert_dt_to_ts(ts_date=start_datetime)
    #     end_ts = self.convert_dt_to_ts(ts_date=end_datetime)
    #     for (k1, k2), _ in self.scrape_status_dataset.items():
    #         if k2 < start_ts or k2 > end_ts:
    #             yield self.scrape_status_dataset[(k1, k2)]

    # def get_dataset_for_timerange(self, start_datetime: datetime.datetime, end_datetime: datetime.datetime, **kwargs):
    #     """Wrapper over the internal method so that cross-imports are not necessary

    #     Args:
    #         start_datetime (datetime.datetime): Inclusive Start datetime
    #         end_datetime (datetime.datetime): Exclusive end datetime

    #     Returns:
    #         pd.Dataframe: Returns a pandas dataframe with the filtered data
    #     """
    #     yield self._get_dataset_for_timerange(
    #         start_datetime=start_datetime,
    #         end_datetime=end_datetime,
    #     )

    # def get_dataset_for_time_slice(self, time_slice: pd.Timestamp, **kwargs):
    #     """Wrapper over the internal method so that cross-imports are not necessary

    #     Args:
    #         time_slice (pd.Timestamp): Time slice to be used for fetching the data from datafame for the exact timestamp

    #     Returns:
    #         pd.DataFrame: Returns a pandas Dataframe with the filtered data.
    #     """
    #     temp_data, is_none = self._get_dataset_for_exact_timestamp(
    #         dataset=self.metrics_dataset, ts_column_name=METRICS_API_COLUMNS.timestamp, time_slice=time_slice
    #     )
    #     if is_none:
    #         return pd.DataFrame(
    #             {},
    #             index=[
    #                 METRICS_API_COLUMNS.timestamp,
    #                 METRICS_API_COLUMNS.query_type,
    #                 METRICS_API_COLUMNS.cluster_id,
    #                 METRICS_API_COLUMNS.principal_id,
    #             ],
    #         )
    #     else:
    #         return temp_data
