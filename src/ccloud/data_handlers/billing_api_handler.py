from dataclasses import dataclass, field
import datetime
from decimal import Decimal
import requests
from time import sleep
from urllib import parse
from typing import Dict, List
import pandas as pd
from ccloud.connections import CCloudBase
from ccloud.data_handlers.types import AbstractDataHandler


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
    start_date: datetime.datetime = field(init=True)
    last_available_date: datetime.datetime = field(init=False)
    days_per_query: int = field(default=7)
    max_days_in_memory: int = field(default=30)
    billing_dataset: pd.DataFrame = field(init=False, default=None)

    def __post_init__(self) -> None:
        # Initialize the super classes to set the internal attributes
        super(AbstractDataHandler, self).__post_init__()
        super(CCloudBase, self).__post_init__()
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
        resp = requests.get(url=self.url, auth=self.http_connection, params=params)
        if resp.status_code == 200:
            out_json = resp.json()
            if out_json is not None and out_json["data"] is not None:
                for item in out_json["data"]:
                    temp_date_range = self.__generate_date_range_per_row(start_date=start_date, end_date=end_date)
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
                            BILLING_API_COLUMNS.calc_split_amt: Decimal(item["original_amount"])
                            / temp_date_range.size,
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
            if "next" in out_json["metadata"]:
                if out_json["metadata"]["next"] is not None:
                    query_params = parse.parse_qs(parse.urlsplit(out_json["metadata"]["next"]).query)
                    params["page_token"] = str(query_params["page_token"][0])
                    self.read_all(start_date=start_date, end_date=end_date, params=params)
        elif resp.status_code == 429:
            print(f"CCloud API Per-Minute Limit exceeded. Sleeping for 45 seconds. Error stack: {resp.text}")
            sleep(45)
            print("Timer up. Resuming CCloud API scrape.")
            self.read_all(start_date=start_date, end_date=end_date, params=params)
        else:
            raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)

    def read_next_dataset(self):
        self.start_date = self.last_available_date
        end_date = self.start_date + datetime.timedelta(days=self.days_per_query)
        self.read_all(start_date=self.start_date, end_date=end_date)
        self.last_available_date = end_date
        in_mem_date_cutoff = self.last_available_date - datetime.timedelta(days=self.max_days_in_memory)
        self.billing_dataset = self.get_dataset_for_timeslot(start_datetime=in_mem_date_cutoff, end_datetime=end_date)

    def get_dataset_for_timeslot(self, start_datetime: datetime.datetime, end_datetime: datetime.datetime, **kwargs):
        start_date = pd.to_datetime(str(start_datetime.date()))
        end_date = pd.to_datetime(str(end_datetime.date()))
        return self.billing_dataset.loc[
            (self.billing_dataset[BILLING_API_COLUMNS.calc_timestamp] >= start_date)
            & (self.billing_dataset[BILLING_API_COLUMNS.calc_timestamp] < end_date)
        ]
