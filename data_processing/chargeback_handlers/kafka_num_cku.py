from decimal import Decimal

import pandas as pd

from data_processing.chargeback_handlers.types import ChargebackExecutorOutputObject
from data_processing.data_handlers.prom_metrics_api_handler import METRICS_API_COLUMNS, METRICS_API_PROMETHEUS_QUERIES


def KafkaNumCKUChargeback(
    cb_handler_input,
    cb_input_row,
    cb_append_function,
):
    """
    GOAL: Split into 2 Categories --
        Shared Charge -- Some flat percentage of the cost Divided across all clients active in that duration.
        Usage Charge  -- Some flat percentage of the cost split variably by the amount of data produced + consumed by the SA/User
    """
    common_charge_ratio = 0.30
    usage_charge_ratio = 0.70

    # Common Charge will be added as a ratio of the count of API Keys created for each service account.
    sa_count = cb_handler_input.ccloud_objects_handler.cc_api_keys.find_sa_count_for_clusters(
        cluster_id=cb_input_row.row_cluster_id
    )
    df_time_slice = pd.Timestamp(cb_input_row.input_time_slice)

    if len(sa_count) > 0:
        splitter = len(sa_count)
        # total_api_key_count = len(
        #     [x for x in self.cc_objects.cc_api_keys.api_keys.values() if x.cluster_id != "cloud"]
        # )
        for sa_name, sa_api_key_count in sa_count.items():
            calc_data = ChargebackExecutorOutputObject(
                principal=sa_name,
                time_slice=cb_input_row.row_timestamp,
                product_type_name=cb_input_row.row_product_type,
                env_id=cb_input_row.row_env_id,
                additional_shared_cost=(Decimal(cb_input_row.row_billing_cost) * Decimal(common_charge_ratio))
                / Decimal(splitter),
            )
            cb_append_function(calc_data, cb_handler_input.ccloud_chargeback_handler)
    else:
        calc_data = ChargebackExecutorOutputObject(
            principal=cb_input_row.row_cluster_id,
            time_slice=cb_input_row.row_timestamp,
            product_type_name=cb_input_row.row_product_type,
            env_id=cb_input_row.row_env_id,
            additional_shared_cost=Decimal(cb_input_row.row_billing_cost) * Decimal(common_charge_ratio),
        )
        cb_append_function(calc_data, cb_handler_input.ccloud_chargeback_handler)

    # filter metrics for data that has some consumption > 0 , and then find all rows with index
    # with that timestamp and that specific kafka cluster.
    try:
        metric_rows = cb_input_row.metrics_dataframe[
            (cb_input_row.metrics_dataframe[METRICS_API_COLUMNS.timestamp] == df_time_slice)
            & (cb_input_row.metrics_dataframe[METRICS_API_COLUMNS.cluster_id] == cb_input_row.row_cluster_id)
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
                lambda x: Decimal(x) / Decimal(agg_data.loc[["sum"]][metric_item])
            )
        # for every filtered Row , add consumption
        for metric_row in metric_rows.itertuples(index=True, name="MetricsRow"):
            req_cost = (
                cb_input_row.row_billing_cost
                / len(query_dataset)
                * getattr(metric_row, f"{METRICS_API_PROMETHEUS_QUERIES.request_bytes_name}_ratio")
            )
            res_cost = (
                cb_input_row.row_billing_cost
                / len(query_dataset)
                * getattr(metric_row, f"{METRICS_API_PROMETHEUS_QUERIES.response_bytes_name}_ratio")
            )
            calc_data = ChargebackExecutorOutputObject(
                principal=getattr(metric_row, METRICS_API_COLUMNS.principal_id),
                time_slice=cb_input_row.row_timestamp,
                product_type_name=cb_input_row.row_product_type,
                env_id=cb_input_row.row_env_id,
                additional_usage_cost=Decimal(usage_charge_ratio) * (Decimal(req_cost) + Decimal(res_cost)),
            )
            cb_append_function(calc_data, cb_handler_input.ccloud_chargeback_handler)
    else:
        if len(sa_count) > 0:
            splitter = len(sa_count)
            for sa_name, sa_api_key_count in sa_count.items():
                calc_data = ChargebackExecutorOutputObject(
                    principal=sa_name,
                    time_slice=cb_input_row.row_timestamp,
                    product_type_name=cb_input_row.row_product_type,
                    env_id=cb_input_row.row_env_id,
                    additional_shared_cost=(Decimal(cb_input_row.row_billing_cost) * Decimal(usage_charge_ratio))
                    / Decimal(splitter),
                )
                cb_append_function(calc_data, cb_handler_input.ccloud_chargeback_handler)
        else:
            calc_data = ChargebackExecutorOutputObject(
                principal=cb_input_row.row_cluster_id,
                time_slice=cb_input_row.row_timestamp,
                product_type_name=cb_input_row.row_product_type,
                env_id=cb_input_row.row_env_id,
                additional_shared_cost=Decimal(cb_input_row.row_billing_cost) * Decimal(usage_charge_ratio),
            )
            cb_append_function(calc_data, cb_handler_input.ccloud_chargeback_handler)
