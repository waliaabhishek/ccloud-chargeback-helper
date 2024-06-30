from decimal import Decimal

from data_processing.chargeback_handlers.types import ChargebackExecutorOutputObject


def KafkaPartitionChargeback(
    cb_handler_input,
    cb_input_row,
    cb_append_function,
):
    """
    GOAL: Split cost across all the API Key holders for the specific Cluster
    Find all active Service Accounts/Users For kafka Cluster using the API Keys in the system.
    """
    sa_count = cb_handler_input.ccloud_objects_handler.cc_api_keys.find_sa_count_for_clusters(
        cluster_id=cb_input_row.row_cluster_id
    )
    if len(sa_count) > 0:
        splitter = len(sa_count)
        # Add Shared Cost for all active SA/Users in the cluster and split it equally
        for sa_name, sa_api_key_count in sa_count.items():
            calc_data = ChargebackExecutorOutputObject(
                principal=sa_name,
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
