from decimal import Decimal

import pandas as pd

from data_processing.chargeback_handlers.types import ChargebackExecutorOutputObject
from data_processing.data_handlers.prom_metrics_api_handler import METRICS_API_COLUMNS, METRICS_API_PROMETHEUS_QUERIES


def KafkaNetworkReadChargeback(
    cb_handler_input,
    cb_input_row,
    cb_append_function,
):
    """
    GOAL: Split cost across all the consumers to that cluster as a ratio of consumption performed.
    Read Depends in the Response_Bytes Metric Only
    """

    response_bytes_column_name = METRICS_API_PROMETHEUS_QUERIES.response_bytes_name
    df_time_slice = pd.Timestamp(cb_input_row.input_time_slice)

    # filter metrics for data that has some consumption > 0 , and then find all rows with index
    # with that timestamp and that specific kafka cluster.
    try:
        subset = cb_input_row.principal_metrics_dataframe[
            cb_input_row.principal_metrics_dataframe[response_bytes_column_name] > 0
        ][
            [
                METRICS_API_COLUMNS.timestamp,
                METRICS_API_COLUMNS.cluster_id,
                METRICS_API_COLUMNS.principal_id,
                response_bytes_column_name,
            ]
        ]
        metric_rows = subset[
            (subset[METRICS_API_COLUMNS.timestamp] == df_time_slice)
            & (subset[METRICS_API_COLUMNS.cluster_id] == cb_input_row.row_cluster_id)
        ]
    except KeyError:
        metric_rows = pd.DataFrame()

    if metric_rows.empty:
        calc_data = ChargebackExecutorOutputObject(
            principal=cb_input_row.row_cluster_id,
            time_slice=cb_input_row.row_timestamp,
            product_type_name=cb_input_row.row_product_type,
            env_id=cb_input_row.row_env_id,
            additional_shared_cost=Decimal(cb_input_row.row_billing_cost),
        )
        cb_append_function(calc_data, cb_handler_input.ccloud_chargeback_handler)
        return

    # Find the total consumption during that time slice
    agg_data = metric_rows[[response_bytes_column_name]].agg(["sum"])
    # add the Ratio consumption column by dividing every row by total consumption.
    metric_rows[f"{response_bytes_column_name}_ratio"] = metric_rows[response_bytes_column_name].transform(
        lambda x: Decimal(x) / Decimal(agg_data.loc[["sum"]][response_bytes_column_name])
    )
    # for every filtered Row , add consumption
    for metric_row in metric_rows.itertuples(index=True, name="MetricsRow"):
        calc_data = ChargebackExecutorOutputObject(
            principal=getattr(metric_row, METRICS_API_COLUMNS.principal_id),
            time_slice=cb_input_row.row_timestamp,
            product_type_name=cb_input_row.row_product_type,
            env_id=cb_input_row.row_env_id,
            additional_usage_cost=Decimal(cb_input_row.row_billing_cost)
            * Decimal(getattr(metric_row, f"{response_bytes_column_name}_ratio")),
        )
        cb_append_function(calc_data, cb_handler_input.ccloud_chargeback_handler)

    return
