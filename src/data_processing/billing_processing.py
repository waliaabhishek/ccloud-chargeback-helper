import os
from csv import QUOTE_NONNUMERIC
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum, auto
from timeit import timeit
from typing import Any, Dict, List, NamedTuple, Set, Tuple

import pandas as pd

# from helpers import ensure_path, logged_method, sanitize_metric_name, timed_method


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
class BillingDict:
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
        range_per_row_count, frequency = [], [freq for x in range(self.data.size)]
        for item in self.data.itertuples(index=False, name="BillingCSVRow"):
            temp = self.__generate_date_range_per_row(item=item)
            range_per_row_count.append(temp.size)
            self.hourly_date_range.extend([str(x) for x in temp if str(x) not in self.hourly_date_range])
        self.hourly_date_range = sorted(list(set(self.hourly_date_range)))
        self.data[BILLING_CSV_COLUMNS.c_interval_count] = range_per_row_count
        self.data[BILLING_CSV_COLUMNS.c_interval_freq] = frequency

    def __generate_date_range_per_row(self, item: NamedTuple, freq: str = "1H"):
        start_date, end_date = item.StartDate, item.EndDate - timedelta(minutes=10)
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

    def generate_hourly_dataset_grouped_by_days(self):
        start_date, end_date = self.hourly_date_range[0], self.hourly_date_range[len(self.hourly_date_range - 1)]


@dataclass(kw_only=True)
class BillingDataset:
    file_path: str = field(init=True)

    parsed_datasets: Dict[str, BillingDict] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.generate_df_from_output()

    def append_dataset(self, ds_name: str, is_shaped: bool, ds: pd.DataFrame):
        self.parsed_datasets[ds_name] = BillingDict(is_shaped=is_shaped, data=ds)

    def get_dataset(self, ds_name: str) -> BillingDict:
        return self.parsed_datasets[ds_name]

    def get_all_datasets(self) -> Tuple[str, BillingDict]:
        for item in self.parsed_datasets.keys():
            ds = self.get_dataset(item)
            yield item, ds

    def generate_df_from_output(self):
        temp = pd.read_csv(
            self.file_path,
            parse_dates=[BILLING_CSV_COLUMNS.start_date, BILLING_CSV_COLUMNS.end_date],
            infer_datetime_format=True,
        )
        self.append_dataset(
            ds_name=BillingDatasetNames.invoice_csv_representation,
            is_shaped=False,
            ds=temp,
        )
        self.print_sample_df()

    def print_sample_df(self) -> None:
        for name, billling_ds in self.get_all_datasets():
            if billling_ds.data is not None:
                print(f"Sample Dataset for {name}, is_shaped: {billling_ds.is_shaped}:")
                print(billling_ds.data.head(3))
                print(billling_ds.data.info())

    def transform_to_hourly_slots(self):
        pass

    # def get_date_ranges(self, ts_range, dt_range) -> Tuple[str, pd.Timestamp, pd.Timestamp]:
    #     for inner_ts_boundary, date_only_str in zip(ts_range, dt_range):
    #         outer_ts_boundary = inner_ts_boundary + timedelta(days=1)
    #         yield (date_only_str, inner_ts_boundary, outer_ts_boundary)

    # def output_to_csv(self, basepath: str = STORAGE_PATH[DirType.MetricsData]):
    #     for name, billing_ds in self.get_all_datasets():
    #         if billing_ds.data is not None and name is BillingDatasetNames.invoice_csv_representation.name:
    #             ts_range = billing_ds.data["timestamp"].dt.normalize().unique()
    #             dt_range = ts_range.date
    #             for dt_val, gt_eq_date, lt_date in self.get_date_ranges(ts_range, dt_range):
    #                 subset = None
    #                 out_path = os.path.join(basepath, f"{dt_val}")
    #                 ensure_path(path=out_path)
    #                 out_path = os.path.join(basepath, f"{dt_val}", f"{self.aggregation_metric_name}__{name}.csv")
    #                 subset = billing_ds.data[
    #                     (billing_ds.data["timestamp"] >= gt_eq_date) & (billing_ds.data["timestamp"] < lt_date)
    #                 ]
    #                 subset.to_csv(out_path, index=False, quoting=QUOTE_NONNUMERIC)
    #                 METRICS_PERSISTENCE_STORE.add_to_persistence(
    #                     date_value=dt_val, metric_name=self.aggregation_metric_name
    #                 )


if __name__ == "__main__":
    filepath = "/Users/abhishek.walia/Documents/Codes/github/ccloud-chargeback-helper/output/BillingsData/invoice_93ca26ff-6d20-483c-9ba4-5474c4db4245_August_2022.csv"

    temp = BillingDataset(file_path=filepath)
    for fn, ds in temp.get_all_datasets():
        ds: BillingDict = ds
        # print(ds.hourly_date_range)
        for item in ds.generate_hourly_dataset_grouped_by_entity():
            print(item)
