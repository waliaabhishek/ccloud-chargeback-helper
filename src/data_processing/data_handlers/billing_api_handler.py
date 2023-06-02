import datetime
from dataclasses import dataclass, field
from decimal import Decimal

import pandas as pd

from ccloud.connections import CCloudBase
from data_processing.data_handlers.types import AbstractDataHandler


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


@dataclass
class CCloudBillingHandler(AbstractDataHandler, CCloudBase):
    last_available_date: datetime.datetime = field(init=False)
    days_per_query: int = field(default=7)
    max_days_in_memory: int = field(default=14)
    billing_dataset: pd.DataFrame = field(init=False, default=None)

    def __post_init__(self) -> None:
        # Initialize the super classes to set the internal attributes
        AbstractDataHandler.__init__(self, start_date=self.start_date)
        CCloudBase.__post_init__(self)
        self.url = self.in_ccloud_connection.get_endpoint_url(key=self.in_ccloud_connection.uri.get_billing_costs)

        # Calculate the end_date from start_date plus number of days per query
        end_date = self.start_date + datetime.timedelta(days=self.days_per_query)
        # Set up params for querying the Billing API
        self.read_all(start_date=self.start_date, end_date=end_date)
        self.last_available_date = end_date

    def read_all(
        self, start_date: datetime.datetime, end_date: datetime.datetime, params={"page_size": 2000}, **kwargs
    ):
        params["start_date"] = str(start_date.date())
        params["end_date"] = str(end_date.date())
        for item in self.read_from_api(params=params):
            temp_date_range = self._generate_date_range_per_row(start_date=start_date, end_date=end_date)
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
                    BILLING_API_COLUMNS.calc_split_quantity: Decimal(item["quantity"]) / temp_date_range.size,
                    BILLING_API_COLUMNS.calc_split_amt: Decimal(item["original_amount"]) / temp_date_range.size,
                    BILLING_API_COLUMNS.calc_split_total: Decimal(item["amount"]) / temp_date_range.size,
                }
                for x in temp_date_range
            ]
            if temp_data is not None:
                if self.billing_dataset is not None:
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
