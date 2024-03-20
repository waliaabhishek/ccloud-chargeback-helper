from __future__ import annotations

import datetime
import decimal
import logging
from dataclasses import dataclass, field
from typing import Callable
from typing import Dict, List

import pandas as pd

from data_processing.chargeback_handlers.audit_log_read import AuditLogReadChargeback
from data_processing.chargeback_handlers.cluster_linking_generic import ClusterLinkingGenericChargeback
from data_processing.chargeback_handlers.connect_capacity import ConnectCapacityChargeback
from data_processing.chargeback_handlers.connect_tasks import ConnectTasksChargeback
from data_processing.chargeback_handlers.kafka_base import KafkaBaseChargeback
from data_processing.chargeback_handlers.kafka_network_read import KafkaNetworkReadChargeback
from data_processing.chargeback_handlers.kafka_network_write import KafkaNetworkWriteChargeback
from data_processing.chargeback_handlers.kafka_num_cku import KafkaNumCKUChargeback
from data_processing.chargeback_handlers.kafka_partition import KafkaPartitionChargeback
from data_processing.chargeback_handlers.ksql_num_csu import KSQLNumCSUChargeback
from data_processing.chargeback_handlers.schema_registry_generic import SchemaRegistryGenericChargeback
from data_processing.chargeback_handlers.types import ChargebackExecutorOutputObject, ChargebackExecutorInputObject
from data_processing.data_handlers.billing_api_handler import BILLING_API_COLUMNS, CCloudBillingHandler
from data_processing.data_handlers.ccloud_api_handler import CCloudObjectsHandler
from data_processing.data_handlers.prom_metrics_api_handler import (
    PrometheusMetricsDataHandler,
)
from data_processing.data_handlers.types import AbstractDataHandler
from helpers import logged_method
from prometheus_processing.custom_collector import TimestampedCollector
from prometheus_processing.notifier import NotifierAbstract, Observer

LOGGER = logging.getLogger(__name__)


class ChargebackColumnNames:
    TS = "Timestamp"
    PRINCIPAL = "Principal"
    ENV_ID = "EnvironmentID"
    KAFKA_CLUSTER = "KafkaID"
    PRODUCT_TYPE = "ProductType"
    USAGE_COST = "UsageCost"
    SHARED_COST = "SharedCost"

    @logged_method
    def override_column_names(self, key, value):
        object.__setattr__(self, key, value)

    @logged_method
    def all_column_values(self) -> List:
        return [y for x, y in vars(self).items()]


CHARGEBACK_COLUMNS = ChargebackColumnNames()

chargeback_prom_metrics = TimestampedCollector(
    "confluent_cloud_chargeback_details",
    "Approximate Chargeback Distribution details for costs w.r.t contextual access within CCloud",
    [
        "principal",
        "product_type",
        "env_id",
        "cost_type",
    ],
    in_begin_timestamp=datetime.datetime.now(),
)


CHARGEBACK_EXECUTORS = {
    # This is a dict of all the chargeback executors that are available
    # The key is the product type and the value is the class that handles the chargeback
    "KAFKA_BASE": KafkaBaseChargeback,
    "KAFKA_NETWORK_READ": KafkaNetworkReadChargeback,
    "KAFKA_NETWORK_WRITE": KafkaNetworkWriteChargeback,
    "KAFKA_NUM_CKU": KafkaNumCKUChargeback,
    "KAFKA_NUM_CKUS": KafkaNumCKUChargeback,
    "KAFKA_PARTITION": KafkaPartitionChargeback,
    "KAFKA_STORAGE": KafkaPartitionChargeback,
    "AUDIT_LOG_READ": AuditLogReadChargeback,
    "CONNECT_CAPACITY": ConnectCapacityChargeback,
    "CONNECT_NUM_TASKS": ConnectTasksChargeback,
    "CONNECT_THROUGHPUT": ConnectTasksChargeback,
    "CLUSTER_LINKING_PER_LINK": ClusterLinkingGenericChargeback,
    "CLUSTER_LINKING_READ": ClusterLinkingGenericChargeback,
    "CLUSTER_LINKING_WRITE": ClusterLinkingGenericChargeback,
    "GOVERNANCE_BASE": SchemaRegistryGenericChargeback,
    "SCHEMA_REGISTRY": SchemaRegistryGenericChargeback,
    "KSQL_NUM_CSU": KSQLNumCSUChargeback,
    "KSQL_NUM_CSUS": KSQLNumCSUChargeback,
}


@dataclass(kw_only=True)
class CCloudChargebackHandler(AbstractDataHandler):
    billing_dataset: CCloudBillingHandler = field(init=True)
    objects_dataset: CCloudObjectsHandler = field(init=True)
    metrics_dataset: PrometheusMetricsDataHandler = field(init=True)
    start_date: datetime.datetime = field(init=True)
    days_per_query: int = field(default=7)
    max_days_in_memory: int = field(default=14)

    last_available_date: datetime.datetime = field(init=False)
    chargeback_dataset: Dict = field(init=False, repr=False, default_factory=dict)
    curr_export_datetime: datetime.datetime = field(init=False)
    metrics_collector: TimestampedCollector = field(init=False)

    def __post_init__(self) -> None:
        """Initialize the Chargeback handler:
        * Set the Start Date as UTC zero and time.min to convert the date to midnight of UTC
        * Calculate the data and save it in memory as a dict
        * attach this class to the Prom scraper which is also a notifier
        * set the exported datetime in memory for stepping through the data every scrape
        """
        AbstractDataHandler.__init__(self, start_date=self.start_date)
        Observer.__init__(self)
        # self.start_date = datetime.datetime.combine(
        #     date=self.start_date.date(), time=datetime.time.min, tzinfo=datetime.timezone.utc
        # )
        # Calculate the end_date from start_date plus number of days per query
        self.last_available_date = self.start_date + datetime.timedelta(days=self.days_per_query)
        self.read_all(start_date=self.start_date, end_date=self.last_available_date)
        # self.attach(chargeback_prom_metrics)
        self.curr_export_datetime = self.start_date
        self.metrics_collector = chargeback_prom_metrics
        self.update(notifier=self.metrics_collector)

    @logged_method
    def update(self, notifier: NotifierAbstract) -> None:
        """This is the Observer class method implementation that helps us step through the next timestamp in sequence.
        The Data for next timestamp is also populated in the Gauge implementation using this method.
        It also tracks the currently exported timestamp in Observer as well as update it to the Notifier.

        Args:
            notifier (NotifierAbstract): This objects is used to get updates from the notifier that the collection for on timestamp is complete and the dataset should be refreshed for the next timestamp.
        """
        curr_ts = pd.date_range(self.curr_export_datetime, freq="1H", periods=2)[0]
        notifier.set_timestamp(curr_timestamp=self.curr_export_datetime)
        # chargeback_prom_status_metrics.set_timestamp(curr_timestamp=self.curr_export_datetime)
        self.expose_prometheus_metrics(ts_filter=curr_ts)

    @logged_method
    def expose_prometheus_metrics(self, ts_filter: pd.Timestamp):
        """Set and expose the metrics to the prom collector as a Gauge.

        Args:
            ts_filter (pd.Timestamp): This Timestamp allows us to filter the data from the entire data set
            to a specific timestamp and expose it to the prometheus collector
        """
        LOGGER.info(f"Currently reading the Chargeback dataset for Timestamp: {ts_filter.to_pydatetime()}")
        # chargeback_prom_status_metrics.clear()
        # chargeback_prom_status_metrics.set(1)
        self.force_clear_prom_metrics()
        out, is_none = self._get_dataset_for_exact_timestamp(
            dataset=self.get_chargeback_dataframe(), ts_column_name=CHARGEBACK_COLUMNS.TS, time_slice=ts_filter
        )
        if not is_none:
            for df_row in out.itertuples(name="ChargeBackData"):
                principal_id = df_row[0][0]
                product_type = df_row[0][2]
                env_id = df_row[0][3]
                usage_cost = df_row[1]
                shared_cost = df_row[2]

                chargeback_prom_metrics.labels(principal_id, product_type, env_id, CHARGEBACK_COLUMNS.USAGE_COST).set(
                    usage_cost
                )
                chargeback_prom_metrics.labels(principal_id, product_type, env_id, CHARGEBACK_COLUMNS.SHARED_COST).set(
                    shared_cost
                )

    @logged_method
    def force_clear_prom_metrics(self):
        chargeback_prom_metrics.clear()

    @logged_method
    def read_all(self, start_date: datetime.datetime, end_date: datetime.datetime, **kwargs):
        """Iterate through all the timestamps in the datetime range and calculate the chargeback for that timestamp

        Args:
            start_date (datetime.datetime): Inclusive datetime for the period beginning
            end_date (datetime.datetime): Exclusive datetime for the period ending
        """
        for time_slice_item in self._generate_date_range_per_row(start_date=start_date, end_date=end_date):
            self.compute_output(time_slice=time_slice_item)

    @logged_method
    def cleanup_old_data(self, retention_start_date: datetime.datetime):
        """Cleanup the older dataset from the chargeback object and prevent it from using too much memory"""
        for (k1, k2, k3, k4), (_, _) in self.chargeback_dataset.copy().items():
            if k2 < retention_start_date:
                del self.chargeback_dataset[(k1, k2, k3, k4)]

    @logged_method
    def read_next_dataset(self, exposed_timestamp: datetime.datetime):
        """Calculate chargeback data fom the next timeslot. This should be used when the current_export_datetime is running very close to the days_per_query end_date."""
        if self.is_next_fetch_required(exposed_timestamp, self.last_available_date, 2):
            effective_dates = self.calculate_effective_dates(
                self.last_available_date, self.days_per_query, self.max_days_in_memory
            )
            self.read_all(effective_dates.next_fetch_start_date, effective_dates.next_fetch_end_date)
            self.last_available_date = effective_dates.next_fetch_end_date
            self.cleanup_old_data(retention_start_date=effective_dates.retention_start_date)
        self.curr_export_datetime = exposed_timestamp
        self.update(notifier=self.metrics_collector)

    @logged_method
    def get_dataset_for_timerange(self, start_datetime: datetime.datetime, end_datetime: datetime.datetime, **kwargs):
        """Wrapper over the internal method so that cross-imports are not necessary

        Args:
            start_datetime (datetime.datetime): Inclusive Start datetime
            end_datetime (datetime.datetime): Exclusive end datetime

        Returns:
            pd.Dataframe: Returns a pandas dataframe with the filtered data
        """
        return self._get_dataset_for_timerange(
            dataset=self.get_chargeback_dataframe(),
            ts_column_name=CHARGEBACK_COLUMNS.TS,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )

    @logged_method
    def add_cost_to_chargeback_dataset(
        self,
        principal: str,
        time_slice: datetime.datetime,
        product_type_name: str,
        env_id: str,
        additional_usage_cost: decimal.Decimal = decimal.Decimal(0),
        additional_shared_cost: decimal.Decimal = decimal.Decimal(0),
    ):
        """Internal chargeback Data structure to hold all the calculated chargeback data in memory.
        As the column names & values were needed to be dynamic, we did not use a dataframe here for ease of use.

        Args:
            principal (str): The Principal used for Chargeback Aggregation -- Primary Complex key
            time_slice (datetime.datetime): datetime of the Hour used for chargeback aggregation -- Primary complex key
            product_type_name (str): The different product names available in CCloud for aggregation
            additional_usage_cost (decimal.Decimal, optional): Is the cost Usage cost for that product type and what is the total usage cost for that duration? Defaults to decimal.Decimal(0).
            additional_shared_cost (decimal.Decimal, optional): Is the cost Shared cost for that product type and what is the total shared cost for that duration. Defaults to decimal.Decimal(0).
        """
        row_key = (principal, time_slice, product_type_name, env_id)
        if row_key in self.chargeback_dataset:
            u, s = self.chargeback_dataset[row_key]
            self.chargeback_dataset[row_key] = (
                u + additional_usage_cost,
                s + additional_shared_cost,
            )
        else:
            self.chargeback_dataset[row_key] = (
                additional_usage_cost,
                additional_shared_cost,
            )

    @logged_method
    def get_chargeback_dataset(self):
        temp_ds = []
        for (principal, ts, product_type, env_id), (usage, shared) in self.chargeback_dataset.items():
            next_ts = self._generate_next_timestamp(curr_date=ts, position=0)
            temp_dict = {
                CHARGEBACK_COLUMNS.PRINCIPAL: principal,
                CHARGEBACK_COLUMNS.TS: next_ts,
                CHARGEBACK_COLUMNS.PRODUCT_TYPE: product_type,
                CHARGEBACK_COLUMNS.ENV_ID: env_id,
                CHARGEBACK_COLUMNS.USAGE_COST: usage,
                CHARGEBACK_COLUMNS.SHARED_COST: shared,
            }
            temp_ds.append(temp_dict)
        return temp_ds

    @logged_method
    def get_chargeback_dataframe(self) -> pd.DataFrame:
        """Generate pandas Dataframe for the Chargeback data available in memory within attribute chargeback_dataset

        Returns:
            pd.DataFrame: _description_
        """
        # TODO: Getting this dataframe is amazingly under optimized albeit uses yield.
        # Uses an intermittent list of dict conversion and then another step to convert to dataframe
        # No clue at the moment on how to improve this.
        out_ds = self.get_chargeback_dataset()
        temp = pd.DataFrame.from_records(
            out_ds,
            index=[
                CHARGEBACK_COLUMNS.PRINCIPAL,
                CHARGEBACK_COLUMNS.TS,
                CHARGEBACK_COLUMNS.PRODUCT_TYPE,
                CHARGEBACK_COLUMNS.ENV_ID,
            ],
        )
        return temp

    @logged_method
    def compute_output(
        self,
        time_slice: datetime.datetime,
    ):
        """The core calculation method. This method aggregates all the costs on
           a per product type basis for every principal per hour and appends that
           calculated dataset in chargeback_dataset object attribute

        Args:
            time_slice (datetime.datetime): The exact timestamp for which the compute will happen
        """

        handlers_base = CCloudChargebackHandlersInputBase(
            ccloud_billing_handler=self.billing_dataset,
            prometheus_metrics_data_handler=self.metrics_dataset,
            ccloud_objects_handler=self.objects_dataset,
            ccloud_chargeback_handler=self,
        )

        billing_data = self.billing_dataset.get_dataset_for_time_slice(time_slice=time_slice)
        metrics_data = self.metrics_dataset.get_dataset_for_time_slice(time_slice=time_slice)
        for bill_row in billing_data.itertuples(index=True, name="BillingRow"):
            row_ts, row_env, row_cid, row_pname, row_ptype = (
                bill_row.Index[0].to_pydatetime(),
                bill_row.Index[1],
                bill_row.Index[2],
                bill_row.Index[3],
                bill_row.Index[4],
            )

            df_time_slice = pd.Timestamp(time_slice)

            row_cname = getattr(bill_row, BILLING_API_COLUMNS.cluster_name)
            row_cost = getattr(bill_row, BILLING_API_COLUMNS.calc_split_total)

            row_input_object = ChargebackExecutorInputObject(
                input_time_slice=time_slice,
                billing_dataframe=billing_data,
                metrics_dataframe=metrics_data,
                row_timestamp=row_ts,
                row_env_id=row_env,
                row_cluster_id=row_cid,
                row_cluster_name=row_cname,
                row_product_name=row_pname,
                row_product_type=row_ptype,
                row_billing_cost=row_cost,
            )

            LOGGER.debug(f"Locating Chargeback Executor for {row_ptype}")
            chargeback_executor = CHARGEBACK_EXECUTORS.get(row_ptype, None)
            LOGGER.debug(f"Found Chargeback Executor for {row_ptype} is {chargeback_executor}")
            if chargeback_executor is None:
                LOGGER.warning(
                    f"No Chargeback calculation available for {row_ptype}. Please request for it to be added. The data might be an inaccurate split for {row_ptype}"
                )
                continue

            chargeback_executor(
                cb_handler_input=handlers_base,
                cb_input_row=row_input_object,
                cb_append_function=CCloudChargebackCostAppender,
            )


@dataclass
class CCloudChargebackHandlersInputBase:
    ccloud_billing_handler: CCloudBillingHandler
    prometheus_metrics_data_handler: PrometheusMetricsDataHandler
    ccloud_objects_handler: CCloudObjectsHandler
    ccloud_chargeback_handler: CCloudChargebackHandler


# This Append Function is used to add the calculated cost to the Chargeback Dataset.
# The implementation of this function is provided by the CCloudChargebackCostAppender function.
# This is done to avoid everything in the Chargeback Handler and make it too rigid.
# Keeping this modular will allow me to switch the implementation in the future if needed.
CCloudChargebackAppendFunction = Callable[[ChargebackExecutorOutputObject, CCloudChargebackHandler], None]


def CCloudChargebackCostAppender(
    cb_calc_row: ChargebackExecutorOutputObject, cb_handler: CCloudChargebackHandler
) -> None:
    # Implementation for CCloudChargebackAppendFunction
    cb_handler.add_cost_to_chargeback_dataset(
        principal=cb_calc_row.principal,
        time_slice=cb_calc_row.time_slice,
        product_type_name=cb_calc_row.product_type_name,
        env_id=cb_calc_row.env_id,
        additional_shared_cost=cb_calc_row.additional_shared_cost,
        additional_usage_cost=cb_calc_row.additional_usage_cost,
    )


# This is the Main calculator function that will have multiple implementations based on the Chargeback Type.
# The implementation of this function is provided by multiple functions defined elsewhere.
# The main goal is to keep everything modular for ease of maintenance and future changes.
CCloudChargebackCalculatorFunction = Callable[
    [CCloudChargebackHandlersInputBase, ChargebackExecutorInputObject, CCloudChargebackAppendFunction], None
]
