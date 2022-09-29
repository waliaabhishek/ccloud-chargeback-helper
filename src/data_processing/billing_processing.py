import datetime
import os
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum, auto
from typing import List, NamedTuple

import pandas as pd


class BillingCSVColumnNames:
    env_id = "EnvironmentID"
    cluster_id = "LogicalClusterID"
    cluster_name = "LogicalClusterName"
    start_date = "StartDate"
    end_date = "EndDate"
    product_name = "Product"
    product_type = "Type"
    price = "Price"
    unit = "UnitForPrice"
    quantity = "Quantity"
    quantity_unit = "Unit"
    orig_amt = "OriginalAmount"
    disc = "Discount"
    total = "Total"

    c_ts = "Interval"
    c_split_quantity = "QuantityAfterSplit"
    c_split_amt = "AmountAfterSplit"
    c_split_total = "TotalAfterSplit"
    c_interval_count = "IntervalCount"
    c_interval_freq = "IntervalFrequency"

    def override_column_names(self, key, value):
        object.__setattr__(self, key, value)


BILLING_CSV_COLUMNS = BillingCSVColumnNames()


class BillingDatasetNames(Enum):
    invoice_csv_representation = auto()


@dataclass(kw_only=True)
class BillingDataframe:
    dataset_name: str
    is_shaped: bool
    data: pd.DataFrame

    hourly_date_range: List = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        self.generate_date_ranges()
        self.data = self.data.sort_values(
            by=[BILLING_CSV_COLUMNS.start_date, BILLING_CSV_COLUMNS.end_date, BILLING_CSV_COLUMNS.env_id],
            ascending=True,
        )

    def generate_date_ranges(self, freq: str = "1H"):
        range_per_row_count, frequency = [], [freq for x in range(len(self.data.index))]
        for item in self.data.itertuples(index=False, name="BillingCSVRow"):
            temp = self.__generate_date_range_per_row(item=item)
            range_per_row_count.append(temp.size)
            self.hourly_date_range.extend([str(x) for x in temp if str(x) not in self.hourly_date_range])
        self.hourly_date_range = sorted(list(set(self.hourly_date_range)))
        self.data[BILLING_CSV_COLUMNS.c_interval_count] = range_per_row_count
        self.data[BILLING_CSV_COLUMNS.c_interval_freq] = frequency

    def __generate_date_range_per_row(
        self, item: NamedTuple, freq: str = "1H", override_start_date: str = None, override_end_date: str = None
    ):
        start_date = datetime.datetime.fromisoformat(override_start_date) if override_start_date else item.StartDate
        end_date = datetime.datetime.fromisoformat(override_end_date) if override_end_date else item.EndDate
        end_date = end_date - timedelta(minutes=10)
        return pd.date_range(start_date, end_date, freq=freq)

    def generate_hourly_dataset_grouped_by_entity(self):
        for item in self.data.itertuples(index=False, name="BillingCSVRow"):
            temp_date_range = self.__generate_date_range_per_row(item)
            temp_dict = [
                {
                    BILLING_CSV_COLUMNS.c_ts: x,
                    BILLING_CSV_COLUMNS.env_id: item.EnvironmentID,
                    BILLING_CSV_COLUMNS.cluster_id: item.LogicalClusterID,
                    BILLING_CSV_COLUMNS.cluster_name: item.LogicalClusterName,
                    BILLING_CSV_COLUMNS.product_name: item.Product,
                    BILLING_CSV_COLUMNS.product_type: item.Type,
                    BILLING_CSV_COLUMNS.quantity: item.Quantity,
                    BILLING_CSV_COLUMNS.orig_amt: item.OriginalAmount,
                    BILLING_CSV_COLUMNS.total: item.Total,
                    BILLING_CSV_COLUMNS.c_split_quantity: item.Quantity / temp_date_range.size,
                    BILLING_CSV_COLUMNS.c_split_amt: item.OriginalAmount / temp_date_range.size,
                    BILLING_CSV_COLUMNS.c_split_total: item.Total / temp_date_range.size,
                }
                for x in temp_date_range
            ]
            yield pd.DataFrame.from_records(temp_dict, index=BILLING_CSV_COLUMNS.c_ts)

    def get_hourly_dataset(self, datetime_slice_iso_format: datetime.datetime):
        # start_date, end_date = str(self.hourly_date_range[0]), str(
        #     self.hourly_date_range[len(self.hourly_date_range) - 1]
        # )
        # inclusive_dates = self.__generate_date_range_per_row(
        #     item=None, override_start_date=start_date, override_end_date=end_date, freq="1D"
        # )
        # for dataset_date in inclusive_dates:
        # per_hour_range = self.__generate_date_range_per_row(
        #     item=None,
        #     override_start_date=str(dataset_date),
        #     override_end_date=str(dataset_date + timedelta(days=1)),
        # )
        out = []
        for row_val in self.data.itertuples(index=False, name="BillingCSVRow"):
            row_range = self.__generate_date_range_per_row(item=row_val)
            row_switcher = row_range.isin(
                [
                    str(datetime_slice_iso_format),
                ]
            )
            out.extend(
                [
                    {
                        BILLING_CSV_COLUMNS.c_ts: presence_ts,
                        BILLING_CSV_COLUMNS.env_id: row_val.EnvironmentID,
                        BILLING_CSV_COLUMNS.cluster_id: row_val.LogicalClusterID,
                        BILLING_CSV_COLUMNS.cluster_name: row_val.LogicalClusterName,
                        BILLING_CSV_COLUMNS.product_name: row_val.Product,
                        BILLING_CSV_COLUMNS.product_type: row_val.Type,
                        BILLING_CSV_COLUMNS.quantity: row_val.Quantity,
                        BILLING_CSV_COLUMNS.orig_amt: row_val.OriginalAmount,
                        BILLING_CSV_COLUMNS.total: row_val.Total,
                        BILLING_CSV_COLUMNS.c_split_quantity: row_val.Quantity / row_range.size,
                        BILLING_CSV_COLUMNS.c_split_amt: row_val.OriginalAmount / row_range.size,
                        BILLING_CSV_COLUMNS.c_split_total: row_val.Total / row_range.size,
                    }
                    for presence_flag, presence_ts in zip(row_switcher, row_range)
                    if bool(presence_flag) is True
                ]
            )
        return pd.DataFrame.from_records(
            out,
            index=[
                BILLING_CSV_COLUMNS.c_ts,
                BILLING_CSV_COLUMNS.env_id,
                BILLING_CSV_COLUMNS.cluster_id,
                BILLING_CSV_COLUMNS.product_name,
                BILLING_CSV_COLUMNS.product_type,
            ],
        )
        # return pd.DataFrame.from_records(out)


# if __name__ == "__main__":
#     filepath = "/Users/abhishek.walia/Documents/Codes/github/ccloud-chargeback-helper/output/BillingsData/invoice_93ca26ff-6d20-483c-9ba4-5474c4db4245_August_2022.csv"

#     temp = BillingDataframe(file_path=filepath)
#     for fn, ds in temp.get_all_datasets():
#         ds: BillingDataframe = ds
#         # print(ds.hourly_date_range)
#         for item in ds.generate_hourly_dataset_grouped_by_entity():
#             print(item)
#         for item in ds.generate_hourly_dataset_grouped_by_date():
#             print(item)
