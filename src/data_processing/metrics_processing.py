from dataclasses import dataclass, field
from datetime import timedelta
import os
import pandas as pd
from typing import Dict, Tuple
from helpers import logged_method, timed_method


@dataclass
class metrics_dataframe:
    aggregation_metric_name: str = field(repr=False, init=True)
    metrics_output: Dict = field(repr=False, init=True)
    parsed_df: pd.DataFrame = field(init=False)
    req_res_bytes_df: pd.DataFrame = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.generate_df_from_output()
        self.metrics_output = None

    @timed_method
    @logged_method
    def shape_specific_dataframes(self):
        self.req_res_bytes_df = self.parsed_df.pivot(index="timestamp", columns="metric.principal_id", values="value")

    @timed_method
    @logged_method
    def generate_df_from_output(self):
        temp = pd.DataFrame(self.metrics_output["data"])
        temp["timestamp"] = pd.to_datetime(temp["timestamp"])
        self.parsed_df = temp
        print("Trying to shape selective datasets")
        if self.is_reshapeable():
            self.shape_specific_dataframes()
            print("Successfully re-shaped time-series metrics")
        else:
            print("Did not detect the right aggregation metric. Ignoring.")

    def is_reshapeable(self) -> bool:
        if self.aggregation_metric_name in [
            "io.confluent.kafka.server/request_bytes",
            "io.confluent.kafka.server/response_bytes",
        ]:
            return True
        else:
            return False

    def print_sample_df(self) -> None:
        for item in [self.parsed_df, self.req_res_bytes_df]:
            if item is not None:
                print(item.head(3))

    def get_unique_dates(self, df: pd.DataFrame) -> Tuple[str, pd.Timestamp, pd.Timestamp]:
        ts_ranges_in_df = df.index.unique().normalize().unique()
        date_only_range = ts_ranges_in_df.date
        for inner_ts_boundary, date_only_str in zip(ts_ranges_in_df, date_only_range):
            outer_ts_boundary = inner_ts_boundary + timedelta(days=1)
            yield (date_only_str, inner_ts_boundary, outer_ts_boundary)

    def output_to_csv(self, basepath: str):
        for item in [self.req_res_bytes_df]:
            if item is not None:
                for dt_str, inner_filter, outer_filter in self.get_unique_dates(item):
                    data_subset = item[(item.index >= inner_filter) & (item.index < outer_filter)]
                    file_path = os.path.join(basepath, f"{dt_str}_{item.Name}.csv")
                    data_subset.to_csv(file_path)
