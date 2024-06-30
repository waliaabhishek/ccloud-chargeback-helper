from decimal import Decimal
import logging
from data_processing.chargeback_handlers.types import ChargebackExecutorOutputObject

LOGGER = logging.getLogger(__name__)


def KSQLNumCSUChargeback(
    cb_handler_input,
    cb_input_row,
    cb_append_function,
):
    """
    GOAL: Cost will be assumed by the ksql Service Account/User being used by the ksqldb cluster.
    There will be only one active Identity but we will still loop on the identity for consistency.
    The conditions are checking for the specific ksqldb cluster in an environment and trying to find its owner.
    """
    active_identities = set(
        [
            y.owner_id
            for x, y in cb_handler_input.ccloud_objects_handler.cc_ksqldb_clusters.ksqldb_clusters.items()
            if y.cluster_id == cb_input_row.row_cluster_id
        ]
    )

    # This state should never happen but we are handling it just in case.
    if len(active_identities) == 0:
        LOGGER.warning(
            f"No active identities found for KSQL cluster ID {cb_input_row.row_cluster_id}. This only happens if the identity assigned to ksql clusters is missing. As the data is missing, I am assigning cost to the cluster instead."
        )
        LOGGER.debug(f"Data Received: {cb_input_row}")
        LOGGER.debug(
            f"KSQL cluster details received: {cb_handler_input.ccloud_objects_handler.cc_ksqldb_clusters.ksqldb_clusters}"
        )
        calc_data = ChargebackExecutorOutputObject(
            principal=cb_input_row.row_cluster_id,
            time_slice=cb_input_row.row_timestamp,
            product_type_name=cb_input_row.row_product_type,
            env_id=cb_input_row.row_env_id,
            additional_shared_cost=Decimal(cb_input_row.row_billing_cost),
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
