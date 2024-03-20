from decimal import Decimal

from data_processing.chargeback_handlers.types import ChargebackExecutorOutputObject


def ConnectCapacityChargeback(
    cb_handler_input,
    cb_input_row,
    cb_append_function,
):
    """
    # GOAL: Split the Connect Cost across all the connect Service Accounts active in the cluster
    """
    active_identities = set(
        [
            y.owner_id
            for x, y in cb_handler_input.ccloud_objects_handler.cc_connectors.connectors.items()
            if y.cluster_id == cb_input_row.row_cluster_id
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
                additional_shared_cost=Decimal(cb_input_row.row_billing_cost) / Decimal(splitter),
            )
            cb_append_function(calc_data, cb_handler_input.ccloud_chargeback_handler)
    else:
        calc_data = ChargebackExecutorOutputObject(
            principal=cb_input_row.row_cluster_id,
            time_slice=cb_input_row.row_timestamp,
            product_type_name=cb_input_row.row_product_type,
            env_id=cb_input_row.row_env_id,
            additional_shared_cost=Decimal(cb_input_row.row_billing_cost),
        )
        cb_append_function(calc_data, cb_handler_input.ccloud_chargeback_handler)
