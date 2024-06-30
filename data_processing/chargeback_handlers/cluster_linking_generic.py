from decimal import Decimal

from data_processing.chargeback_handlers.types import ChargebackExecutorOutputObject


def ClusterLinkingGenericChargeback(
    cb_handler_input,
    cb_input_row,
    cb_append_function,
):
    """
    GOAL: Cost will be assumed by the Logical Cluster ID listed in the Billing API
    """
    calc_data = ChargebackExecutorOutputObject(
        principal=cb_input_row.row_cluster_id,
        time_slice=cb_input_row.row_timestamp,
        product_type_name=cb_input_row.row_product_type,
        env_id=cb_input_row.row_env_id,
        additional_shared_cost=Decimal(cb_input_row.row_billing_cost),
    )
    cb_append_function(calc_data, cb_handler_input.ccloud_chargeback_handler)
