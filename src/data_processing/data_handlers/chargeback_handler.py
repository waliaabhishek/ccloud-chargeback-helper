import datetime
import decimal
from dataclasses import dataclass, field
from typing import Dict, List
import pandas as pd

from data_processing.data_handlers.billing_api_handler import BILLING_API_COLUMNS, CCloudBillingHandler
from data_processing.data_handlers.ccloud_api_handler import CCloudObjectsHandler
from data_processing.data_handlers.chargeback_handler import CCloudChargebackHandler
from data_processing.data_handlers.prom_metrics_handler import (
    METRICS_API_COLUMNS,
    METRICS_API_PROMETHEUS_QUERIES,
    PrometheusMetricsDataHandler,
)
from data_processing.data_handlers.types import AbstractDataHandler


class ChargebackColumnNames:
    TS = "Timestamp"
    PRINCIPAL = "Principal"
    KAFKA_CLUSTER = "KafkaID"
    USAGE_COST = "UsageCost"
    SHARED_COST = "SharedCost"

    def override_column_names(self, key, value):
        object.__setattr__(self, key, value)

    def all_column_values(self) -> List:
        return [y for x, y in vars(self).items()]


CHARGEBACK_COLUMNS = ChargebackColumnNames()


@dataclass(kw_only=True)
class CCloudChargebackHandler(AbstractDataHandler):
    billing_dataset: CCloudBillingHandler = field(init=True)
    objects_dataset: CCloudObjectsHandler = field(init=True)
    metrics_dataset: PrometheusMetricsDataHandler = field(init=True)
    start_date: datetime.datetime = field(init=True)
    days_per_query: int = field(default=7)

    chargeback_dataset: Dict = field(init=False, repr=False, default_factory=dict)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.start_date = self.start_date.replace(tzinfo=datetime.timezone.utc).combine(time=datetime.time.min)
        # Calculate the end_date from start_date plus number of days per query
        end_date = self.start_date + datetime.timedelta(days=self.days_per_query)
        self.read_all(start_date=self.start_date, end_date=end_date)

    def read_all(self, start_date: datetime.datetime, end_date: datetime.datetime, **kwargs):
        for time_slice_item in self.__generate_date_range_per_row(start_date=start_date, end_date=end_date):
            self.compute_output(start_datetime=start_date, end_datetime=end_date, time_slice=time_slice_item)

    def get_dataset_for_timerange(self, start_datetime: datetime.datetime, end_datetime: datetime.datetime, **kwargs):
        cb_df = self.get_chargeback_dataframe()
        start_date = pd.to_datetime(start_datetime)
        end_date = pd.to_datetime(end_datetime)
        return cb_df.loc[(cb_df[CHARGEBACK_COLUMNS.TS] >= start_date) & (cb_df[CHARGEBACK_COLUMNS.TS] < end_date)]

    def get_dataset_for_time_slice(
        self, dataset: pd.DataFrame, ts_column_name: str, time_slice: pd.Timestamp, **kwargs
    ):
        return dataset.loc[(dataset[ts_column_name] == time_slice)]

    def cleanup_old_data(self):
        for (k1, k2), (_, _, _) in self.chargeback_dataset.copy().items():
            if k2 < self.start_date:
                del self.chargeback_dataset[(k1, k2)]

    def read_next_dataset(self):
        self.start_date = self.start_date + datetime.timedelta(days=self.days_per_query)
        end_date = self.start_date + datetime.timedelta(days=self.days_per_query)
        self.cleanup_old_data()
        self.read_all(start_date=self.start_date, end_date=end_date)

    def __add_cost_to_chargeback_dataset(
        self,
        principal: str,
        time_slice: datetime.datetime,
        product_type_name: str,
        additional_usage_cost: decimal.Decimal = decimal.Decimal(0),
        additional_shared_cost: decimal.Decimal = decimal.Decimal(0),
    ):
        if (principal, time_slice) in self.chargeback_dataset:
            u, s, detailed_split = self.chargeback_dataset[(principal, time_slice)]
            detailed_split[product_type_name] = (
                detailed_split.get(product_type_name, decimal.Decimal(0))
                + additional_shared_cost
                + additional_usage_cost
            )
            self.chargeback_dataset[(principal, time_slice)] = (
                u + additional_usage_cost,
                s + additional_shared_cost,
                detailed_split,
            )
        else:
            detailed_split = dict()
            detailed_split[product_type_name] = additional_shared_cost + additional_usage_cost
            self.chargeback_dataset[(principal, time_slice)] = (
                additional_usage_cost,
                additional_shared_cost,
                detailed_split,
            )

    def get_chargeback_dataset(self):
        for (k1, k2), (usage, shared, extended) in self.chargeback_dataset.items():
            temp_dict = {
                CHARGEBACK_COLUMNS.PRINCIPAL: k1,
                CHARGEBACK_COLUMNS.TS: k2,
                CHARGEBACK_COLUMNS.USAGE_COST: usage,
                CHARGEBACK_COLUMNS.SHARED_COST: shared,
            }
            temp_dict.update(extended)
            yield temp_dict

    def get_chargeback_dataframe(self) -> pd.DataFrame:
        # TODO: Getting this dataframe is amazingly under optimized albeit uses yield.
        # Uses an intermittent list of dict conversion and then another step to convert to dataframe
        # No clue at the moment on how to improve this.
        return pd.DataFrame.from_records(
            self.get_chargeback_dataset(), index=[CHARGEBACK_COLUMNS.PRINCIPAL, CHARGEBACK_COLUMNS.TS]
        )

    def compute_output(
        self, start_datetime: datetime.datetime, end_datetime: datetime.datetime, time_slice: datetime.datetime,
    ):
        billing_data = self.billing_dataset.get_dataset_for_timerange(
            start_datetime=start_datetime, end_datetime=end_datetime
        )
        metrics_data = self.metrics_dataset.get_dataset_for_timerange(
            start_datetime=start_datetime, end_datetime=end_datetime
        )
        for bill_row in billing_data.itertuples(index=True, name="BillingRow"):
            row_ts, row_env, row_cid, row_pname, row_ptype = (
                bill_row.Index[0].to_pydatetime(),
                bill_row.Index[1],
                bill_row.Index[2],
                bill_row.Index[3],
                bill_row.Index[4],
            )

            df_time_slice = pd.Timestamp(time_slice, tz="UTC")

            row_cname = getattr(bill_row, BILLING_API_COLUMNS.cluster_name)
            row_cost = getattr(bill_row, BILLING_API_COLUMNS.calc_split_total)
            if row_ptype == "KafkaBase":
                # GOAL: Split Cost equally across all the SA/Users that have API Keys for that Kafka Cluster
                # Find all active Service Accounts/Users For kafka Cluster using the API Keys in the system.
                sa_count = self.objects_dataset.cc_api_keys.find_sa_count_for_clusters(cluster_id=row_cid)
                if len(sa_count) > 0:
                    splitter = len(sa_count)
                    # Add Shared Cost for all active SA/Users in the cluster and split it equally
                    for sa_name, sa_api_key_count in sa_count.items():
                        self.chargeback_dataset.__add_cost_to_chargeback_dataset(
                            principal=sa_name,
                            time_slice=row_ts,
                            product_type_name=row_ptype,
                            additional_shared_cost=decimal.Decimal(row_cost) / decimal.Decimal(splitter),
                        )
                else:
                    print(
                        f"Row TS: {str(row_ts)} -- No API Keys available for cluster {row_cid}. Attributing {row_ptype} for {row_cid} as Cluster Shared Cost"
                    )
                    self.__add_cost_to_chargeback_dataset(
                        row_cid, row_ts, row_ptype, additional_shared_cost=decimal.Decimal(row_cost)
                    )
            elif row_ptype == "KafkaNetworkRead":
                # GOAL: Split cost across all the consumers to that cluster as a ratio of consumption performed.
                # Read Depends in the Response_Bytes Metric Only
                col_name = METRICS_API_PROMETHEUS_QUERIES.response_bytes_name
                # filter metrics for data that has some consumption > 0 , and then find all rows with index
                # with that timestamp and that specific kafka cluster.
                try:
                    subset = metrics_data[metrics_data[col_name] > 0][
                        [
                            METRICS_API_COLUMNS.timestamp,
                            METRICS_API_COLUMNS.cluster_id,
                            METRICS_API_COLUMNS.principal_id,
                            col_name,
                        ]
                    ]
                    metric_rows = subset[
                        (subset[METRICS_API_COLUMNS.timestamp] == df_time_slice)
                        & (subset[METRICS_API_COLUMNS.cluster_id] == row_cid)
                    ]
                except KeyError:
                    metric_rows = pd.DataFrame()
                if not metric_rows.empty:
                    # Find the total consumption during that time slice
                    agg_data = metric_rows[[col_name]].agg(["sum"])
                    # add the Ratio consumption column by dividing every row by total consumption.
                    metric_rows[f"{col_name}_ratio"] = metric_rows[col_name].transform(
                        lambda x: decimal.Decimal(x) / decimal.Decimal(agg_data.loc[["sum"]][col_name])
                    )
                    # for every filtered Row , add consumption
                    for metric_row in metric_rows.itertuples(index=True, name="MetricsRow"):
                        self.chargeback_dataset.__add_cost_to_chargeback_dataset(
                            getattr(metric_row, METRICS_API_COLUMNS.principal_id),
                            row_ts,
                            row_ptype,
                            additional_usage_cost=decimal.Decimal(row_cost)
                            * decimal.Decimal(getattr(metric_row, f"{col_name}_ratio")),
                        )
                else:
                    print(
                        f"Row TS: {str(row_ts)} -- Could not map {row_ptype} for {row_cid}. Attributing as Cluster Shared Cost for cluster {row_cid}"
                    )
                    self.__add_cost_to_chargeback_dataset(
                        row_cid, row_ts, row_ptype, additional_shared_cost=decimal.Decimal(row_cost),
                    )
            elif row_ptype == "KafkaNetworkWrite":
                # GOAL: Split cost across all the producers to that cluster as a ratio of production performed.
                # Read Depends in the Response_Bytes Metric Only
                col_name = METRICS_API_PROMETHEUS_QUERIES.request_bytes_name
                # filter metrics for data that has some consumption > 0 , and then find all rows with index
                # with that timestamp and that specific kafka cluster.
                try:
                    subset = metrics_data[metrics_data[col_name] > 0][
                        [
                            METRICS_API_COLUMNS.timestamp,
                            METRICS_API_COLUMNS.cluster_id,
                            METRICS_API_COLUMNS.principal_id,
                            col_name,
                        ]
                    ]
                    metric_rows = subset[
                        (subset[METRICS_API_COLUMNS.timestamp] == df_time_slice)
                        & (subset[METRICS_API_COLUMNS.cluster_id] == row_cid)
                    ]
                except KeyError:
                    metric_rows = pd.DataFrame()
                if not metric_rows.empty:
                    # print(metric_rows.info())
                    # Find the total consumption during that time slice
                    agg_value = metric_rows[[col_name]].agg(["sum"]).loc["sum", col_name]
                    # add the Ratio consumption column by dividing every row by total consumption.
                    metric_rows[f"{col_name}_ratio"] = (
                        metric_rows[col_name]
                        .transform(lambda x: decimal.Decimal(x) / decimal.Decimal(agg_value))
                        .to_list()
                    )
                    # for every filtered Row , add consumption
                    for metric_row in metric_rows.itertuples(index=True, name="MetricsRow"):
                        self.__add_cost_to_chargeback_dataset(
                            getattr(metric_row, METRICS_API_COLUMNS.principal_id),
                            row_ts,
                            row_ptype,
                            additional_usage_cost=decimal.Decimal(row_cost)
                            * decimal.Decimal(getattr(metric_row, f"{col_name}_ratio")),
                        )
                else:
                    print(
                        f"Row TS: {str(row_ts)} -- Could not map {row_ptype} for {row_cid}. Attributing as Cluster Shared Cost for cluster {row_cid}"
                    )
                    self.__add_cost_to_chargeback_dataset(
                        row_cid, row_ts, row_ptype, additional_shared_cost=decimal.Decimal(row_cost),
                    )
            elif row_ptype == "KafkaNumCKUs":
                # GOAL: Split into 2 Categories --
                #       Common Charge -- Flat 50% of the cost Divided across all clients active in that duration.
                #       Usage Charge  -- 50% of the cost split variably by the amount of data produced + consumed by the SA/User
                common_charge_ratio = 0.30
                usage_charge_ratio = 0.70
                # Common Charge will be added as a ratio of the count of API Keys created for each service account.
                sa_count = self.objects_dataset.cc_api_keys.find_sa_count_for_clusters(cluster_id=row_cid)
                if len(sa_count) > 0:
                    splitter = len(sa_count)
                    # total_api_key_count = len(
                    #     [x for x in self.cc_objects.cc_api_keys.api_keys.values() if x.cluster_id != "cloud"]
                    # )
                    for sa_name, sa_api_key_count in sa_count.items():
                        self.__add_cost_to_chargeback_dataset(
                            sa_name,
                            row_ts,
                            row_ptype,
                            additional_shared_cost=(decimal.Decimal(row_cost) * decimal.Decimal(common_charge_ratio))
                            / decimal.Decimal(splitter),
                        )
                else:
                    print(
                        f"Row TS: {str(row_ts)} -- No API Keys were found for cluster {row_cid}. Attributing Common Cost component for {row_ptype} as Cluster Shared Cost for cluster {row_cid}"
                    )
                    self.__add_cost_to_chargeback_dataset(
                        row_cid,
                        row_ts,
                        row_ptype,
                        additional_shared_cost=decimal.Decimal(row_cost) * decimal.Decimal(common_charge_ratio),
                    )
                # filter metrics for data that has some consumption > 0 , and then find all rows with index
                # with that timestamp and that specific kafka cluster.
                try:
                    metric_rows = metrics_data[
                        (metrics_data[METRICS_API_COLUMNS.timestamp] == df_time_slice)
                        & (metrics_data[METRICS_API_COLUMNS.cluster_id] == row_cid)
                    ]
                except KeyError:
                    metric_rows = pd.DataFrame()
                # Usage Charge
                if not metric_rows.empty:
                    # Find the total consumption during that time slice
                    query_dataset = [
                        METRICS_API_PROMETHEUS_QUERIES.request_bytes_name,
                        METRICS_API_PROMETHEUS_QUERIES.response_bytes_name,
                    ]

                    agg_data = metric_rows[query_dataset].agg(["sum"])
                    # add the Ratio consumption column by dividing every row by total consumption.
                    for metric_item in query_dataset.values():
                        metric_rows[f"{metric_item}_ratio"] = metric_rows[metric_item].transform(
                            lambda x: decimal.Decimal(x) / decimal.Decimal(agg_data.loc[["sum"]][metric_item])
                        )
                    # for every filtered Row , add consumption
                    for metric_row in metric_rows.itertuples(index=True, name="MetricsRow"):
                        req_cost = (
                            row_cost
                            / len(query_dataset)
                            * getattr(metric_row, f"{METRICS_API_PROMETHEUS_QUERIES.request_bytes_name}_ratio")
                        )
                        res_cost = (
                            row_cost
                            / len(query_dataset)
                            * getattr(metric_row, f"{METRICS_API_PROMETHEUS_QUERIES.response_bytes_name}_ratio")
                        )
                        self.__add_cost_to_chargeback_dataset(
                            getattr(metric_row, METRICS_API_COLUMNS.principal_id),
                            row_ts,
                            row_ptype,
                            # additional_shared_cost=(common_charge_ratio * row_cost) / metric_rows.size,
                            additional_usage_cost=decimal.Decimal(usage_charge_ratio)
                            * (decimal.Decimal(req_cost) + decimal.Decimal(res_cost)),
                        )
                else:
                    if len(sa_count) > 0:
                        print(
                            f"Row TS: {str(row_ts)} -- No Production/Consumption activity for cluster {row_cid}. Splitting Usage Ratio for {row_ptype} across all Service Accounts as Shared Cost"
                        )
                        splitter = len(sa_count)
                        for sa_name, sa_api_key_count in sa_count.items():
                            self.__add_cost_to_chargeback_dataset(
                                sa_name,
                                row_ts,
                                row_ptype,
                                additional_shared_cost=(
                                    decimal.Decimal(row_cost) * decimal.Decimal(usage_charge_ratio)
                                )
                                / decimal.Decimal(splitter),
                            )
                    else:
                        print(
                            f"Row TS: {str(row_ts)} -- No Production/Consumption activity for cluster {row_cid} and no API Keys found for the cluster {row_cid}. Attributing Common Cost component for {row_ptype} as Cluster Shared Cost for cluster {row_cid}"
                        )
                        self.__add_cost_to_chargeback_dataset(
                            row_cid,
                            row_ts,
                            row_ptype,
                            additional_shared_cost=decimal.Decimal(row_cost) * decimal.Decimal(usage_charge_ratio),
                        )
            elif row_ptype in ["KafkaPartition", "KafkaStorage"]:
                # GOAL: Split cost across all the API Key holders for the specific Cluster
                # Find all active Service Accounts/Users For kafka Cluster using the API Keys in the system.
                sa_count = self.objects_dataset.cc_api_keys.find_sa_count_for_clusters(cluster_id=row_cid)
                if len(sa_count) > 0:
                    splitter = len(sa_count)
                    # Add Shared Cost for all active SA/Users in the cluster and split it equally
                    for sa_name, sa_api_key_count in sa_count.items():
                        self.__add_cost_to_chargeback_dataset(
                            sa_name,
                            row_ts,
                            row_ptype,
                            additional_shared_cost=decimal.Decimal(row_cost) / decimal.Decimal(splitter),
                        )
                else:
                    print(
                        f"Row TS: {str(row_ts)} -- No API Keys available for cluster {row_cid}. Attributing {row_ptype}  for {row_cid} as Cluster Shared Cost"
                    )
                    self.__add_cost_to_chargeback_dataset(
                        row_cid, row_ts, row_ptype, additional_shared_cost=decimal.Decimal(row_cost)
                    )
            elif row_ptype == "EventLogRead":
                # GOAL: Split Audit Log read cost across all the Service Accounts + Users that are created in the Org
                # Find all active Service Accounts/Users in the system.
                active_identities = list(self.objects_dataset.cc_sa.sa.keys()) + list(
                    objects_dataset.cc_users.users.keys()
                )
                splitter = len(active_identities)
                # Add Shared Cost for all active SA/Users in the cluster and split it equally
                for identity_item in active_identities:
                    self.__add_cost_to_chargeback_dataset(
                        identity_item,
                        row_ts,
                        row_ptype,
                        additional_shared_cost=decimal.Decimal(row_cost) / decimal.Decimal(splitter),
                    )
            elif row_ptype == "ConnectCapacity":
                # GOAL: Split the Connect Cost across all the connect Service Accounts active in the cluster
                active_identities = set(
                    [
                        y.owner_id
                        for x, y in self.cc_objects.cc_connectors.connectors.items()
                        if y.cluster_id == row_cid
                    ]
                )
                if len(active_identities) > 0:
                    splitter = len(active_identities)
                    for identity_item in active_identities:
                        self.chargeback_dataset.__add_cost_to_chargeback_dataset(
                            identity_item,
                            row_ts,
                            row_ptype,
                            additional_shared_cost=decimal.Decimal(row_cost) / decimal.Decimal(splitter),
                        )
                else:
                    print(
                        f"Row TS: {str(row_ts)} -- No Connector Details were found. Attributing as Shared Cost for Kafka Cluster {row_cid}"
                    )
                    self.__add_cost_to_chargeback_dataset(
                        row_cid, row_ts, row_ptype, additional_shared_cost=decimal.Decimal(row_cost)
                    )
            elif row_ptype in ["ConnectNumTasks", "ConnectThroughput"]:
                # GOAL: Cost will be assumed by the owner of the connector
                # There will be only one active Identity but we will still loop on the identity for consistency
                # The conditions are checking for the specific connector in an environment and trying to find its owner.
                active_identities = set(
                    [
                        y.owner_id
                        for x, y in self.objects_dataset.cc_connectors.connectors.items()
                        if y.env_id == row_env and y.connector_name == row_cname
                    ]
                )
                if len(active_identities) > 0:
                    splitter = len(active_identities)
                    for identity_item in active_identities:
                        self.__add_cost_to_chargeback_dataset(
                            identity_item,
                            row_ts,
                            row_ptype,
                            additional_usage_cost=decimal.Decimal(row_cost) / decimal.Decimal(splitter),
                        )
                else:
                    print(
                        f"Row TS: {str(row_ts)} -- No Connector Details were found. Using the Connector {row_cid} and adding cost as Shared Cost"
                    )
                    self.__add_cost_to_chargeback_dataset(
                        row_cid, row_ts, row_ptype, additional_shared_cost=decimal.Decimal(row_cost)
                    )
            elif row_ptype == "ClusterLinkingPerLink":
                # GOAL: Cost will be assumed by the Logical Cluster ID listed in the Billing API
                self.__add_cost_to_chargeback_dataset(
                    row_cid, row_ts, row_ptype, additional_shared_cost=decimal.Decimal(row_cost)
                )
            elif row_ptype == "ClusterLinkingRead":
                # GOAL: Cost will be assumed by the Logical Cluster ID listed in the Billing API
                self.__add_cost_to_chargeback_dataset(
                    row_cid, row_ts, row_ptype, additional_shared_cost=decimal.Decimal(row_cost)
                )
            elif row_ptype == "ClusterLinkingWrite":
                # GOAL: Cost will be assumed by the Logical Cluster ID listed in the Billing API
                self.__add_cost_to_chargeback_dataset(
                    row_cid, row_ts, row_ptype, additional_shared_cost=decimal.Decimal(row_cost)
                )
            elif row_ptype in ["GovernanceBase", "SchemaRegistry"]:
                # GOAL: Cost will be equally spread across all the Kafka Clusters existing in this CCloud Environment
                active_identities = set(
                    [y.cluster_id for x, y in self.objects_dataset.cc_clusters.cluster.items() if y.env_id == row_env]
                )
                if len(active_identities) > 0:
                    splitter = len(active_identities)
                    for identity_item in active_identities:
                        self.chargeback_dataset.__add_cost_to_chargeback_dataset(
                            identity_item,
                            row_ts,
                            row_ptype,
                            additional_usage_cost=decimal.Decimal(row_cost) / decimal.Decimal(splitter),
                        )
                else:
                    print(
                        f"Row TS: {str(row_ts)} -- No Kafka Clusters present within the environment. Attributing as Shared Cost to {row_env}"
                    )
                    self.__add_cost_to_chargeback_dataset(
                        row_env, row_ts, row_ptype, additional_shared_cost=decimal.Decimal(row_cost)
                    )
            elif row_ptype == "KSQLNumCSUs":
                # GOAL: Cost will be assumed by the ksql Service Account/User being used by the ksqldb cluster
                # There will be only one active Identity but we will still loop on the identity for consistency
                # The conditions are checking for the specific ksqldb cluster in an environment and trying to find its owner.
                active_identities = set(
                    [
                        y.owner_id
                        for x, y in self.objects_dataset.cc_ksqldb_clusters.ksqldb_clusters.items()
                        if y.cluster_id == row_cid
                    ]
                )
                if len(active_identities) > 0:
                    splitter = len(active_identities)
                    for identity_item in active_identities:
                        self.__add_cost_to_chargeback_dataset(
                            identity_item,
                            row_ts,
                            row_ptype,
                            additional_usage_cost=decimal.Decimal(row_cost) / decimal.Decimal(splitter),
                        )
                else:
                    print(
                        f"Row TS: {str(row_ts)} -- No KSQL Cluster Details were found. Attributing as Shared Cost for ksqlDB cluster ID {row_cid}"
                    )
                    self.__add_cost_to_chargeback_dataset(
                        row_cid, row_ts, row_ptype, additional_shared_cost=decimal.Decimal(row_cost)
                    )
            else:
                print("=" * 80)
                print(
                    f"Row TS: {str(row_ts)} -- No Chargeback calculation available for {row_ptype}. Please request for it to be added."
                )
                print("=" * 80)

    ##########################################################################################
    ##########################################################################################
    def export_metrics_to_csv(self, output_basepath: str):
        self.cb_manager.output_to_csv(basepath=output_basepath)

    def get_hourly_dataset(self, date_value: datetime.datetime, billing_mgmt: bool = True):
        out = pd.DataFrame()
        data_missing_on_disk = False
        for _, telemetry_dataset in self.metrics_dataset.items():
            if billing_mgmt and telemetry_dataset.aggregation_metric not in BILLING_METRICS_SCOPE.values():
                continue
            metric_name, file_level_df = telemetry_dataset.get_hourly_dataset(datetime_slice_iso_format=date_value)
            if file_level_df is not None:
                out = out.merge(
                    file_level_df, how="outer", on=[METRICS_CSV_COLUMNS.OUT_TS, METRICS_CSV_COLUMNS.OUT_KAFKA_CLUSTER],
                )
            else:
                data_missing_on_disk = True
        # TODO: Maybe need to replace NaN items with zero so that maths doesnt fail while calculation.
        return data_missing_on_disk, out

    def run_calculations(
        self, time_slice: datetime.datetime, billing_dataframe: pd.DataFrame, metrics_dataframe: pd.DataFrame,
    ):
        self.cb_manager.run_calculations(
            time_slice=time_slice, billing_dataframe=billing_dataframe, metrics_dataframe=metrics_dataframe
        )

