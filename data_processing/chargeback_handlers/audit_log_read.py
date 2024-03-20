from __future__ import annotations

from decimal import Decimal

from data_processing.chargeback_handlers.types import ChargebackExecutorOutputObject


def AuditLogReadChargeback(
    cb_handler_input,
    cb_input_row,
    cb_append_function,
):
    """
    # GOAL: Split Audit Log read cost across all the Service Accounts + Users that are created in the Org
    # Find all active Service Accounts/Users in the system.
    """
    active_identities = list(cb_handler_input.ccloud_objects_handler.cc_sa.sa.keys()) + list(
        cb_handler_input.ccloud_objects_handler.cc_users.users.keys()
    )
    splitter = len(active_identities)
    # Add Shared Cost for all active SA/Users in the cluster and split it equally
    for identity_item in active_identities:
        calc_data = ChargebackExecutorOutputObject(
            principal=identity_item,
            time_slice=cb_input_row.row_timestamp,
            product_type_name=cb_input_row.row_product_type,
            env_id=cb_input_row.row_env_id,
            additional_shared_cost=Decimal(cb_input_row.row_billing_cost) / Decimal(splitter),
        )
        cb_append_function(calc_data, cb_handler_input.ccloud_chargeback_handler)
