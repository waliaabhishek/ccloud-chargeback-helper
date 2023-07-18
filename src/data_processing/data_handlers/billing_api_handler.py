import datetime
from dataclasses import dataclass, field
from decimal import Decimal
from typing import List, Tuple

import pandas as pd

from ccloud.connections import CCloudBase
from data_processing.data_handlers.ccloud_api_handler import CCloudObjectsHandler
from data_processing.data_handlers.types import AbstractDataHandler
from helpers import LOGGER
from prometheus_processing.custom_collector import TimestampedCollector
from prometheus_processing.notifier import NotifierAbstract


class BillingAPIColumnNames:
    env_id = "EnvironmentID"
    cluster_id = "LogicalClusterID"
    cluster_name = "LogicalClusterName"
    product_name = "Product"
    product_type = "Type"
    quantity = "Quantity"
    orig_amt = "OriginalAmount"
    total = "Total"
    price = "Price"

    calc_timestamp = "Interval"
    calc_split_quantity = "QuantityAfterSplit"
    calc_split_amt = "AmountAfterSplit"
    calc_split_total = "TotalAfterSplit"


BILLING_API_COLUMNS = BillingAPIColumnNames()

billing_api_prom_metrics = TimestampedCollector(
    "confluent_cloud_billing_details",
    "Confluent Cloud Costs API data distribution details divided on a per hour basis",
    [
        "env_id",
        "kafka_cluster_id",
        "kafka_cluster_unknown_reason",
        "resource_id",
        "product_name",
        "product_line_type",
    ],
    in_begin_timestamp=datetime.datetime.now(),
)


@dataclass
class CCloudBillingHandler(AbstractDataHandler, CCloudBase):
    start_date: datetime.datetime = field(init=True)
    objects_dataset: CCloudObjectsHandler = field(init=True)
    days_per_query: int = field(default=7)
    max_days_in_memory: int = field(default=14)

    billing_dataset: pd.DataFrame = field(init=False, default=None)
    last_available_date: datetime.datetime = field(init=False)
    curr_export_datetime: datetime.datetime = field(init=False)

    def __post_init__(self) -> None:
        # Initialize the super classes to set the internal attributes
        AbstractDataHandler.__init__(self, start_date=self.start_date)
        CCloudBase.__post_init__(self)
        self.url = self.in_ccloud_connection.get_endpoint_url(key=self.in_ccloud_connection.uri.get_billing_costs)
        LOGGER.info(f"Initialized the Billing API Handler with URL: {self.url}")
        # Calculate the end_date from start_date plus number of days per query
        end_date = self.start_date + datetime.timedelta(days=self.days_per_query)
        # Set up params for querying the Billing API
        self.read_all(start_date=self.start_date, end_date=end_date)
        self.curr_export_datetime = self.start_date
        self.update(notifier=billing_api_prom_metrics)

        self.last_available_date = end_date
        LOGGER.info(f"Initialized the Billing API Handler with last available date: {self.last_available_date}")

    def update(self, notifier: NotifierAbstract) -> None:
        """This is the Observer class method implementation that helps us step through the next timestamp in sequence.
        The Data for next timestamp is also populated in the Gauge implementation using this method.
        It also tracks the currently exported timestamp in Observer as well as update it to the Notifier.

        Args:
            notifier (NotifierAbstract): This objects is used to get updates from the notifier that the collection for on timestamp is complete and the dataset should be refreshed for the next timestamp.
        """
        curr_ts = pd.date_range(self.curr_export_datetime, freq="1H", periods=2)[0]
        notifier.set_timestamp(curr_timestamp=self.curr_export_datetime)
        # chargeback_prom_status_metrics.set_timestamp(curr_timestamp=self.curr_export_datetime)
        self.expose_prometheus_metrics(ts_filter=curr_ts)

    def expose_prometheus_metrics(self, ts_filter: pd.Timestamp):
        """Set and expose the metrics to the prom collector as a Gauge.

        Args:
            ts_filter (pd.Timestamp): This Timestamp allows us to filter the data from the entire data set
            to a specific timestamp and expose it to the prometheus collector
        """
        LOGGER.debug(
            "Exposing Prometheus Metrics for Billing dataset for timestamp: " + str(ts_filter.to_pydatetime())
        )
        self.force_clear_prom_metrics()
        out, is_none = self._get_dataset_for_exact_timestamp(
            dataset=self.billing_dataset, ts_column_name=BILLING_API_COLUMNS.calc_timestamp, time_slice=ts_filter
        )
        if not is_none:
            for df_row in out.itertuples(name="BillingData"):
                env_id = df_row[0][1]
                resource_id = df_row[0][2]
                product_name = df_row[0][3]
                product_line_type = df_row[0][4]
                cost = df_row[8]

                kafka_cluster_list, not_found_reason = self.get_connected_kafka_cluster_id(
                    env_id=env_id, resource_id=resource_id
                )
                for item in kafka_cluster_list:
                    billing_api_prom_metrics.labels(
                        env_id,
                        item,
                        not_found_reason,
                        resource_id,
                        product_name,
                        product_line_type,
                    ).set(cost / len(kafka_cluster_list))

    def get_connected_kafka_cluster_id(self, env_id: str, resource_id: str) -> Tuple[List[str], str]:
        """This method is used to get the connected Kafka Cluster ID for a given Billing API resource ID

        Args:
            env_id (str): The environment ID for the resource
            resource_id (str): The resource ID for which the connected Kafka Cluster ID is required

        Returns:
            str: The connected Kafka Cluster ID
        """
        return self.objects_dataset.get_connected_kafka_cluster_id(env_id=env_id, resource_id=resource_id)

    def force_clear_prom_metrics(self):
        billing_api_prom_metrics.clear()

    def read_all(
        self, start_date: datetime.datetime, end_date: datetime.datetime, params={"page_size": 2000}, **kwargs
    ):
        params["start_date"] = str(start_date.date())
        params["end_date"] = str(end_date.date())
        LOGGER.debug(f"Reading from Billing API with params: {params}")
        for item in self.read_from_api(params=params):
            item_start_date = datetime.datetime.strptime(item["start_date"], "%Y-%m-%d")
            item_end_date = datetime.datetime.strptime(item["end_date"], "%Y-%m-%d")
            temp_date_range = self._generate_date_range_per_row(start_date=item_start_date, end_date=item_end_date)
            temp_data = [
                {
                    BILLING_API_COLUMNS.calc_timestamp: x,
                    BILLING_API_COLUMNS.env_id: item["resource"]["environment"]["id"],
                    BILLING_API_COLUMNS.cluster_id: item["resource"]["id"],
                    BILLING_API_COLUMNS.cluster_name: item["resource"]["display_name"],
                    BILLING_API_COLUMNS.product_name: item["product"],
                    BILLING_API_COLUMNS.product_type: item["line_type"],
                    BILLING_API_COLUMNS.quantity: item["quantity"],
                    BILLING_API_COLUMNS.orig_amt: item["original_amount"],
                    BILLING_API_COLUMNS.total: item["amount"],
                    BILLING_API_COLUMNS.price: item["price"],
                    BILLING_API_COLUMNS.calc_split_quantity: Decimal(item["quantity"]) / 24,
                    BILLING_API_COLUMNS.calc_split_amt: Decimal(item["original_amount"]) / 24,
                    BILLING_API_COLUMNS.calc_split_total: Decimal(item["amount"]) / 24,
                }
                for x in temp_date_range
            ]
            if temp_data is not None:
                if self.billing_dataset is not None:
                    LOGGER.debug(f"Appending new Billing data to the existing dataset")
                    self.billing_dataset = pd.concat(
                        [
                            self.billing_dataset,
                            pd.DataFrame.from_records(
                                temp_data,
                                index=[
                                    BILLING_API_COLUMNS.calc_timestamp,
                                    BILLING_API_COLUMNS.env_id,
                                    BILLING_API_COLUMNS.cluster_id,
                                    BILLING_API_COLUMNS.product_name,
                                    BILLING_API_COLUMNS.product_type,
                                ],
                            ),
                        ]
                    )
                else:
                    LOGGER.debug(f"Initializing the Billing dataset with new data")
                    self.billing_dataset = pd.DataFrame.from_records(
                        temp_data,
                        index=[
                            BILLING_API_COLUMNS.calc_timestamp,
                            BILLING_API_COLUMNS.env_id,
                            BILLING_API_COLUMNS.cluster_id,
                            BILLING_API_COLUMNS.product_name,
                            BILLING_API_COLUMNS.product_type,
                        ],
                    )

    def read_next_dataset(self, exposed_timestamp: datetime.datetime):
        LOGGER.debug("Reading the next dataset for Billing API")
        if self.is_next_fetch_required(exposed_timestamp, self.last_available_date, 2):
            effective_dates = self.calculate_effective_dates(
                self.last_available_date, self.days_per_query, self.max_days_in_memory
            )
            self.read_all(
                start_date=effective_dates.next_fetch_start_date, end_date=effective_dates.next_fetch_end_date
            )
            self.last_available_date = effective_dates.next_fetch_end_date
            self.billing_dataset, is_none = self.get_dataset_for_timerange(
                start_datetime=effective_dates.retention_start_date, end_datetime=effective_dates.retention_end_date
            )
        self.curr_export_datetime = exposed_timestamp
        self.update(notifier=billing_api_prom_metrics)

    def get_dataset_for_timerange(self, start_datetime: datetime.datetime, end_datetime: datetime.datetime, **kwargs):
        """Wrapper over the internal method so that cross-imports are not necessary

        Args:
            start_datetime (datetime.datetime): Inclusive Start datetime
            end_datetime (datetime.datetime): Exclusive end datetime

        Returns:
            pd.Dataframe: Returns a pandas dataframe with the filtered data
        """
        return self._get_dataset_for_timerange(
            dataset=self.billing_dataset,
            ts_column_name=BILLING_API_COLUMNS.calc_timestamp,
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
            dataset=self.billing_dataset, ts_column_name=BILLING_API_COLUMNS.calc_timestamp, time_slice=time_slice
        )
        if is_none:
            return pd.DataFrame(
                data={},
                index=[
                    BILLING_API_COLUMNS.calc_timestamp,
                    BILLING_API_COLUMNS.env_id,
                    BILLING_API_COLUMNS.cluster_id,
                    BILLING_API_COLUMNS.product_name,
                    BILLING_API_COLUMNS.product_type,
                ],
            )
        else:
            return temp_data
