from __future__ import annotations

import datetime
import decimal
from dataclasses import dataclass

import pandas as pd


@dataclass
class ChargebackExecutorInputObject:
    input_time_slice: datetime.datetime
    billing_dataframe: pd.DataFrame
    principal_metrics_dataframe: pd.DataFrame
    topic_metrics_dataframe: pd.DataFrame
    flink_metrics_dataframe: pd.DataFrame
    row_timestamp: datetime.datetime
    row_env_id: str
    row_cluster_id: str
    row_cluster_name: str
    row_product_name: str
    row_product_type: str
    row_billing_cost: decimal.Decimal


@dataclass
class ChargebackExecutorOutputObject:
    principal: str
    time_slice: datetime.datetime
    env_id: str
    product_type_name: str
    additional_usage_cost: decimal.Decimal = decimal.Decimal(0)
    additional_shared_cost: decimal.Decimal = decimal.Decimal(0)
