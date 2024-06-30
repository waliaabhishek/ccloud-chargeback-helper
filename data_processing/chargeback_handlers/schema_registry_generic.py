from decimal import Decimal

from data_processing.chargeback_handlers.types import ChargebackExecutorOutputObject


def SchemaRegistryGenericChargeback(
    cb_handler_input,
    cb_input_row,
    cb_append_function,
):
    """
    GOAL: Cost will be equally spread across all owners of API Keys in this CCloud Environment.
    Condition - If lsrc keys exist, then split cost across all lsrc keys active in the environment.
    Condition - If no lsrc keys exist, then split cost across all keys active in the environment.
    """

    # Check for active API Keys for Schema Registry in the environment
    active_identities = set(
        [
            y.owner_id
            for x, y in cb_handler_input.ccloud_objects_handler.cc_api_keys.api_keys.items()
            if y.env_id == cb_input_row.row_env_id and y.cluster_id.startswith("lsrc-")
        ]
    )

    # If no active API Keys found, then split cost across all identities active in the environment
    env_identities = set(
        [
            y.owner_id
            for x, y in cb_handler_input.ccloud_objects_handler.cc_api_keys.api_keys.items()
            if y.env_id == cb_input_row.row_env_id
        ]
    )

    total_identities = set(
        list(cb_handler_input.ccloud_objects_handler.cc_sa.sa.keys())
        + list(cb_handler_input.ccloud_objects_handler.cc_users.users.keys())
    )

    if len(total_identities) == 0:
        calc_data = ChargebackExecutorOutputObject(
            principal=cb_input_row.row_cluster_id,
            time_slice=cb_input_row.row_timestamp,
            product_type_name=cb_input_row.row_product_type,
            env_id=cb_input_row.row_env_id,
            additional_shared_cost=Decimal(cb_input_row.row_billing_cost),
        )
        cb_append_function(calc_data, cb_handler_input.ccloud_chargeback_handler)
        return

    if len(env_identities) == 0:
        splitter = len(total_identities)
        for identity_item in total_identities:
            calc_data = ChargebackExecutorOutputObject(
                principal=identity_item,
                time_slice=cb_input_row.row_timestamp,
                product_type_name=cb_input_row.row_product_type,
                env_id=cb_input_row.row_env_id,
                additional_shared_cost=Decimal(cb_input_row.row_billing_cost) / Decimal(splitter),
            )
            cb_append_function(calc_data, cb_handler_input.ccloud_chargeback_handler)
        return

    if len(active_identities) == 0:
        splitter = len(env_identities)
        for identity_item in env_identities:
            calc_data = ChargebackExecutorOutputObject(
                principal=identity_item,
                time_slice=cb_input_row.row_timestamp,
                product_type_name=cb_input_row.row_product_type,
                env_id=cb_input_row.row_env_id,
                additional_shared_cost=Decimal(cb_input_row.row_billing_cost) / Decimal(splitter),
            )
            cb_append_function(calc_data, cb_handler_input.ccloud_chargeback_handler)
        return

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
    return
