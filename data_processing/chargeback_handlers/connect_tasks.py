from decimal import Decimal

from data_processing.chargeback_handlers.types import ChargebackExecutorOutputObject


def ConnectTasksChargeback(
    cb_handler_input,
    cb_input_row,
    cb_append_function,
):
    """
    # GOAL: Cost will be assumed by the owner of the connector
    """
    # There will be only one active Identity but we will still loop on the identity for consistency
    # The conditions are checking for the specific connector in an environment and trying to find its owner.
    active_identities = set(
        [
            y.owner_id
            for x, y in cb_handler_input.ccloud_objects_handler.cc_connectors.connectors.items()
            if y.env_id == cb_input_row.row_env_id and y.connector_name == cb_input_row.row_cluster_name
        ]
    )
    if len(active_identities) > 0:
        splitter = len(active_identities)
        for identity_item in active_identities:
            calc_data = ChargebackExecutorOutputObject(
                principal=identity_item,
                time_slice=cb_input_row.row_timestamp,
                product_type_name=cb_input_row.row_product_type,
                env_id=cb_input_row.row_env_id,
                additional_usage_cost=Decimal(cb_input_row.row_billing_cost) / Decimal(splitter),
            )
            cb_append_function(calc_data, cb_handler_input.ccloud_chargeback_handler)
    else:
        calc_data = ChargebackExecutorOutputObject(
            principal=cb_input_row.row_cluster_id,
            time_slice=cb_input_row.row_timestamp,
            product_type_name=cb_input_row.row_product_type,
            env_id=cb_input_row.row_env_id,
            additional_usage_cost=Decimal(cb_input_row.row_billing_cost),
        )
        cb_append_function(calc_data, cb_handler_input.ccloud_chargeback_handler)
