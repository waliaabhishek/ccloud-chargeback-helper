from dataclasses import dataclass, field
import datetime
from enum import Enum
import pandas as pd
from typing import Dict

from ccloud.org import CCloudObjectsHandler
from data_processing.billing_processing import BILLING_CSV_COLUMNS
from data_processing.metrics_processing import METRICS_CSV_COLUMNS
from helpers import sanitize_metric_name
from workflow_runner import BILLING_METRICS_SCOPE


class ChargebackColumnNames:
    TS = "Timestamp"
    PRINCIPAL = "Principal"
    KAFKA_CLUSTER = "KafkaID"
    USAGE_COST = "UsageCost"
    SHARED_COST = "SharedCost"

    def override_column_names(self, key, value):
        object.__setattr__(self, key, value)


CHARGEBACK_COLUMNS = ChargebackColumnNames()


@dataclass(kw_only=True)
class ChargebackUnit:
    cb_dict: Dict = field(init=False, repr=False, default_factory=dict)

    def add_cost(
        self,
        principal: str,
        time_slice: datetime.datetime,
        product_type_name: str,
        additional_usage_cost: float = float(0),
        additional_shared_cost: float = float(0),
    ):
        if (principal, time_slice) in self.cb_dict:
            u, s, detailed_split = self.cb_dict[(principal, time_slice)]
            detailed_split: Dict = detailed_split
            detailed_split[product_type_name] = (
                detailed_split.get(product_type_name, float(0)) + additional_usage_cost + additional_usage_cost
            )
            self.cb_dict[(principal, time_slice)] = (
                u + additional_usage_cost,
                s + additional_shared_cost,
                detailed_split,
            )
        else:
            detailed_split = dict()
            detailed_split[product_type_name] = additional_shared_cost + additional_usage_cost
            self.cb_dict[(principal, time_slice)] = (additional_usage_cost, additional_shared_cost, detailed_split)


CHARGEBACK_UNITS = ChargebackUnit()


@dataclass(kw_only=True)
class ChargebackDataframe:
    cc_objects: CCloudObjectsHandler = field(init=True, repr=False)
    # METRICS_CSV_COLUMNS.OUT_TS: presence_ts, -- Index1
    # METRICS_CSV_COLUMNS.OUT_KAFKA_CLUSTER: row_val["resource.kafka.id"], -- Index2
    # METRICS_CSV_COLUMNS.OUT_PRINCIPAL: row_val.metric.principal_id, -- column
    # self.aggregation_metric: row_val.value, -- column
    _metrics_dataframe: pd.DataFrame = field(init=True, repr=False)
    # BILLING_CSV_COLUMNS.c_ts: presence_ts, -- Index1
    # BILLING_CSV_COLUMNS.env_id: row_val.EnvironmentID, -- Index2
    # BILLING_CSV_COLUMNS.cluster_id: row_val.LogicalClusterID, -- Index3
    # BILLING_CSV_COLUMNS.cluster_name: row_val.LogicalClusterName,
    # BILLING_CSV_COLUMNS.product_name: row_val.Product, -- Index4
    # BILLING_CSV_COLUMNS.product_type: row_val.Type, -- Index5
    # BILLING_CSV_COLUMNS.quantity: row_val.Quantity,
    # BILLING_CSV_COLUMNS.orig_amt: row_val.OriginalAmount,
    # BILLING_CSV_COLUMNS.total: row_val.Total,
    # BILLING_CSV_COLUMNS.c_split_quantity: row_val.Quantity / row_range.size,
    # BILLING_CSV_COLUMNS.c_split_amt: row_val.OriginalAmount / row_range.size,
    # BILLING_CSV_COLUMNS.c_split_total: row_val.Total / row_range.size,
    # index=[
    #     BILLING_CSV_COLUMNS.c_ts,
    #     BILLING_CSV_COLUMNS.env_id,
    #     BILLING_CSV_COLUMNS.cluster_id,
    #     BILLING_CSV_COLUMNS.product_name,
    #     BILLING_CSV_COLUMNS.product_type,
    # ],
    _billings_dataframe: pd.DataFrame = field(init=True, repr=False)

    cb_unit: ChargebackUnit = field(init=False, repr=False)
    active_producers: pd.DataFrame = field(init=False)
    active_consumers: pd.DataFrame = field(init=False)

    def __post_init__(self) -> None:
        self.cb_unit = ChargebackUnit()
        self.compute_output()

    def get_active_client_count(self, df: pd.DataFrame) -> int:
        return int(df[METRICS_CSV_COLUMNS.OUT_PRINCIPAL].count())

    def compute_output(self):
        for bill_row in self._billings_dataframe.itertuples(index=True, name="BillingRow"):
            row_ts, row_env, row_cid, row_pname, row_ptype = (
                bill_row.Index[0],
                bill_row.Index[1],
                bill_row.Index[2],
                bill_row.Index[3],
                bill_row.Index[4],
            )
            row_cname = getattr(bill_row, BILLING_CSV_COLUMNS.cluster_name.name)
            row_cost = getattr(bill_row, BILLING_CSV_COLUMNS.c_split_total.name)
            if row_ptype == "KafkaBase":
                # GOAL: Split Cost equally across all the SA/Users that have API Keys for that Kafka Cluster
                # Find all active Service Accounts/Users For kafka Cluster using the API Keys in the system.
                sa_count = self.cc_objects.cc_api_keys.find_sa_count_for_clusters(cluster_id=row_cid)
                splitter = len(sa_count)
                # Add Shared Cost for all active SA/Users in the cluster and split it equally
                for sa_name, sa_api_key_count in sa_count.items():
                    self.cb_unit.add_cost(sa_name, row_ts, row_ptype, additional_shared_cost=row_cost / splitter)
            elif row_ptype == "KafkaNetworkRead":
                # GOAL: Split cost across all the consumers to that cluster as a ratio of consumption performed.
                # Read Depends in the Response_Bytes Metric Only
                col_name = BILLING_METRICS_SCOPE[1]
                # filter metrics for data that has some consumption > 0 , and then find all rows with index
                # with that timestamp and that specific kafka cluster.
                metric_rows = self._metrics_dataframe[self._metrics_dataframe[col_name] > 0][
                    [METRICS_CSV_COLUMNS.OUT_PRINCIPAL, col_name]
                ].loc[[row_ts, row_cid]]
                # Find the total consumption during that time slice
                agg_data = metric_rows[[col_name]].agg(["sum"])
                # add the Ratio consumption column by dividing every row by total consuption.
                metric_rows[f"{col_name}_ratio"] = metric_rows[col_name].transform(
                    lambda x: x / agg_data.loc[["sum"]][col_name]
                )
                # for every filtered Row , add consumption
                for metric_row in metric_rows.itertuples(index=True, name="MetricsRow"):
                    self.cb_unit.add_cost(
                        getattr(metric_row, METRICS_CSV_COLUMNS.OUT_PRINCIPAL),
                        row_ts,
                        row_ptype,
                        additional_usage_cost=row_cost * getattr(metric_row, f"{col_name}_ratio"),
                    )
            elif row_ptype == "KafkaNetworkWrite":
                # GOAL: Split cost across all the producers to that cluster as a ratio of production performed.
                # Read Depends in the Response_Bytes Metric Only
                col_name = BILLING_METRICS_SCOPE["request_bytes"]
                # filter metrics for data that has some consumption > 0 , and then find all rows with index
                # with that timestamp and that specific kafka cluster.
                metric_rows = self._metrics_dataframe[self._metrics_dataframe[col_name] > 0][
                    [METRICS_CSV_COLUMNS.OUT_PRINCIPAL, col_name]
                ].loc[[row_ts, row_cid]]
                # Find the total consumption during that time slice
                agg_data = metric_rows[[col_name]].agg(["sum"])
                # add the Ratio consumption column by dividing every row by total consuption.
                metric_rows[f"{col_name}_ratio"] = metric_rows[col_name].transform(
                    lambda x: x / agg_data.loc[["sum"]][col_name]
                )
                # for every filtered Row , add consumption
                for metric_row in metric_rows.itertuples(index=True, name="MetricsRow"):
                    self.cb_unit.add_cost(
                        getattr(metric_row, METRICS_CSV_COLUMNS.OUT_PRINCIPAL),
                        row_ts,
                        row_ptype,
                        additional_usage_cost=row_cost * getattr(metric_row, f"{col_name}_ratio"),
                    )
            elif row_ptype == "KafkaNumCKUs":
                # GOAL: Split into 2 Categories --
                #       Common Charge -- Flat 20% of the cost Divided across all clients active in that duration.
                #       Usage Charge  -- 80% of the cost split variably by the amount of data produced + consumed by the SA/User
                common_charge_ratio = 0.20
                usage_charge_ratio = 0.80
                # Read Depends in the Response_Bytes Metric Only
                # col_name = BILLING_METRICS_SCOPE['request_bytes']
                # filter metrics for data that has some consumption > 0 , and then find all rows with index
                # with that timestamp and that specific kafka cluster.
                metric_rows = self._metrics_dataframe.loc[[row_ts, row_cid]]
                # Find the total consumption during that time slice
                agg_data = metric_rows[[list(BILLING_METRICS_SCOPE.values())]].agg(["sum"])
                # add the Ratio consumption column by dividing every row by total consuption.
                for metric_item in BILLING_METRICS_SCOPE.values():
                    metric_rows[f"{metric_item}_ratio"] = metric_rows[metric_item].transform(
                        lambda x: x / agg_data.loc[["sum"]][metric_item]
                    )
                # for every filtered Row , add consumption
                for metric_row in metric_rows.itertuples(index=True, name="MetricsRow"):
                    self.cb_unit.add_cost(
                        getattr(metric_row, METRICS_CSV_COLUMNS.OUT_PRINCIPAL),
                        row_ts,
                        row_ptype,
                        additional_shared_cost=(common_charge_ratio * row_cost) / metric_rows.size,
                        additional_usage_cost=usage_charge_ratio
                        * (
                            (
                                (
                                    row_cost
                                    / len(BILLING_METRICS_SCOPE)
                                    * getattr(metric_row, f"{BILLING_METRICS_SCOPE['request_bytes']}_ratio")
                                )
                                + (
                                    (row_cost / len(BILLING_METRICS_SCOPE))
                                    * getattr(metric_row, f"{BILLING_METRICS_SCOPE['response_bytes']}_ratio")
                                )
                            ),
                        ),
                    )
            elif row_ptype in ["KafkaPartition", "KafkaStorage"]:
                # GOAL: Split cost across all the API Key holders for the specific Cluster
                # Find all active Service Accounts/Users For kafka Cluster using the API Keys in the system.
                sa_count = self.cc_objects.cc_api_keys.find_sa_count_for_clusters(cluster_id=row_cid)
                splitter = len(sa_count)
                # Add Shared Cost for all active SA/Users in the cluster and split it equally
                for sa_name, sa_api_key_count in sa_count.items():
                    self.cb_unit.add_cost(sa_name, row_ts, row_ptype, additional_shared_cost=row_cost / splitter)
            elif row_ptype == "EventLogRead":
                # GOAL: Split Audit Log read cost across all the Service Accounts + Users that are created in the Org
                # Find all active Service Accounts/Users in the system.
                active_identities = list(self.cc_objects.cc_sa.sa.keys()) + list(self.cc_objects.cc_users.users.keys())
                splitter = len(active_identities)
                # Add Shared Cost for all active SA/Users in the cluster and split it equally
                for identity_item in active_identities:
                    self.cb_unit.add_cost(identity_item, row_ts, row_ptype, additional_shared_cost=row_cost / splitter)
            elif row_ptype == "ConnectCapacity":
                # GOAL: Split the Connect Cost across all the connect Service Accounts active in the cluster
                active_identities = set(
                    [
                        y.owner_id.resource_id
                        for x, y in self.cc_objects.cc_connectors.connectors.items()
                        if y.cluster_id == row_cid
                    ]
                )
                splitter = len(active_identities)
                for identity_item in active_identities:
                    self.cb_unit.add_cost(identity_item, row_ts, row_ptype, additional_shared_cost=row_cost / splitter)
            elif row_ptype in ["ConnectNumTasks", "ConnectThroughput"]:
                # GOAL: Cost will be assumed by the owner of the connector
                # There will be only one active Identity but we will still loop on the identity for consistency
                # The conditions are checking for the specific connector in an environment and trying to find its owner.
                active_identities = set(
                    [
                        y.owner_id
                        for x, y in self.cc_objects.cc_connectors.connectors.items()
                        if y.env_id == row_env and y.connector_name == row_cname
                    ]
                )
                splitter = len(active_identities)
                for identity_item in active_identities:
                    self.cb_unit.add_cost(identity_item, row_ts, row_ptype, additional_usage_cost=row_cost / splitter)
            elif row_ptype == "ClusterLinkingPerLink":
                # GOAL: Cost will be assumed by the Logical Cluster ID listed in the Billing API
                self.cb_unit.add_cost(row_cid, row_ts, row_ptype, additional_shared_cost=row_cost)
            elif row_ptype == "ClusterLinkingRead":
                # GOAL: Cost will be assumed by the Logical Cluster ID listed in the Billing API
                self.cb_unit.add_cost(row_cid, row_ts, row_ptype, additional_shared_cost=row_cost)
            elif row_ptype == "ClusterLinkingWrite":
                # GOAL: Cost will be assumed by the Logical Cluster ID listed in the Billing API
                self.cb_unit.add_cost(row_cid, row_ts, row_ptype, additional_shared_cost=row_cost)
            elif row_ptype == "KSQLNumCSUs":
                # GOAL: Cost will be assumed by the ksql Service Account/User being used by the ksqldb cluster
                # There will be only one active Identity but we will still loop on the identity for consistency
                # The conditions are checking for the specific ksqldb cluster in an environment and trying to find its owner.
                active_identities = set(
                    [
                        y.owner_id
                        for x, y in self.cc_objects.cc_ksqldb_clusters.ksqldb_clusters.items()
                        if y.cluster_id == row_cid
                    ]
                )
                splitter = len(active_identities)
                for identity_item in active_identities:
                    self.cb_unit.add_cost(identity_item, row_ts, row_ptype, additional_usage_cost=row_cost / splitter)
