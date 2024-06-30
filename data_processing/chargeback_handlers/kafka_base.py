from decimal import Decimal

from data_processing.chargeback_handlers.types import ChargebackExecutorOutputObject


def KafkaBaseChargeback(
    cb_handler_input,
    cb_input_row,
    cb_append_function,
):
    """
    GOAL: Split Cost equally across all the SA/Users that have API Keys for that Kafka Cluster
    Find all active Service Accounts/Users For kafka Cluster using the API Keys in the system.
    """
    active_identities = cb_handler_input.ccloud_objects_handler.cc_api_keys.find_sa_count_for_clusters(
        cluster_id=cb_input_row.row_cluster_id
    )

    total_identities = cb_handler_input.ccloud_objects_handler.cc_api_keys.find_sa_count_for_clusters(cluster_id=None)

    # If there are no entities in the Org but some clusters exist, then the cost is allotted to the cluster
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

    # If there are no active entities for specific cluster but identities do exist, then the cost is split across all identities
    # as a penalty for not using the cluster
    if len(active_identities) == 0:
        splitter = len(total_identities)
        for identity_name, sa_api_key_count in total_identities.items():
            calc_data = ChargebackExecutorOutputObject(
                principal=identity_name,
                time_slice=cb_input_row.row_timestamp,
                product_type_name=cb_input_row.row_product_type,
                env_id=cb_input_row.row_env_id,
                additional_shared_cost=Decimal(cb_input_row.row_billing_cost) / Decimal(splitter),
            )
            cb_append_function(calc_data, cb_handler_input.ccloud_chargeback_handler)
        return

    # If there are active entities in the Org for specific cluster, then the cost is split across all active identities.
    splitter = len(active_identities)
    for sa_name, sa_api_key_count in active_identities.items():
        calc_data = ChargebackExecutorOutputObject(
            principal=sa_name,
            time_slice=cb_input_row.row_timestamp,
            product_type_name=cb_input_row.row_product_type,
            env_id=cb_input_row.row_env_id,
            additional_usage_cost=Decimal(cb_input_row.row_billing_cost) / Decimal(splitter),
        )
        cb_append_function(calc_data, cb_handler_input.ccloud_chargeback_handler)
    return
