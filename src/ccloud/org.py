import datetime
from dataclasses import InitVar, dataclass, field
from typing import Dict, List

import pandas as pd


from ccloud.connections import CCloudConnection, EndpointURL
from data_processing.data_handlers.billing_api_handler import CCloudBillingHandler
from data_processing.data_handlers.ccloud_api_handler import CCloudObjectsHandler
from data_processing.data_handlers.chargeback_handler import CCloudChargebackHandler
from data_processing.data_handlers.prom_fetch_stats_handler import PrometheusStatusMetricsDataHandler, ScrapeType
from data_processing.data_handlers.prom_metrics_api_handler import PrometheusMetricsDataHandler
from helpers import sanitize_id
from prometheus_processing.custom_collector import TimestampedCollector
from prometheus_processing.notifier import NotifierAbstract, Observer

scrape_status_metrics = TimestampedCollector(
    "confluent_cloud_custom_scrape_status",
    "CCloud Scrape Status for various object types",
    ["object_type"],
    in_begin_timestamp=datetime.datetime.now(),
)


@dataclass(kw_only=True)
class CCloudOrg(Observer):
    in_org_details: InitVar[List | None] = None
    in_days_in_memory: InitVar[int] = field(default=7)
    org_id: str

    objects_handler: CCloudObjectsHandler = field(init=False)
    metrics_handler: PrometheusMetricsDataHandler = field(init=False)
    status_metrics_handler: PrometheusStatusMetricsDataHandler = field(init=False)
    billing_handler: CCloudBillingHandler = field(init=False)
    chargeback_handler: CCloudChargebackHandler = field(init=False)
    exposed_metrics_datetime: datetime.datetime = field(init=False)
    exposed_end_date: datetime.datetime = field(init=False)

    def __post_init__(self, in_org_details, in_days_in_memory) -> None:
        Observer.__init__(self)
        self.org_id = sanitize_id(in_org_details["id"])
        # This start date is calculated from the now time to rewind back 365 days as that is the
        # time limit of the billing dataset which is available to us. We will need the metrics handler
        # to try and get the data from that far back as well.
        # start_date = datetime.datetime.utcnow().replace(
        #     minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc
        # ) + datetime.timedelta(days=-30, hours=+1)

        # This following action is required as for the first run we need to derive the start date.
        # So we step back by 1 hour, so that the current hour slice is returned.
        self.exposed_metrics_datetime = datetime.datetime.utcnow().replace(
            minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc
        ) + datetime.timedelta(days=-30, hours=+1)

        self.exposed_end_date = datetime.datetime.utcnow().replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc
        ) - datetime.timedelta(days=1)

        # Initialize the Scrape Metrics Handler
        # This is used for checking when was the last scrape stored in Prometheus
        # and where to resume the scrape
        self.status_metrics_handler = PrometheusStatusMetricsDataHandler(
            in_prometheus_url="http://localhost:9091",
        )

        next_fetch_date = self.locate_next_fetch_date(start_date=self.exposed_metrics_datetime)
        print(f"Initial Fetch Date after checking chargeback status in Prometheus: {next_fetch_date}")

        # Initialize the CCloud Objects Handler
        self.objects_handler = CCloudObjectsHandler(
            in_ccloud_connection=CCloudConnection(
                in_api_key=in_org_details["ccloud_details"]["ccloud_api"]["api_key"],
                in_api_secret=in_org_details["ccloud_details"]["ccloud_api"]["api_secret"],
                base_url=EndpointURL.API_URL,
            ),
            start_date=next_fetch_date,
        )

        # Initialize the Billing API Handler
        self.billing_handler = CCloudBillingHandler(
            in_ccloud_connection=CCloudConnection(
                in_api_key=in_org_details["ccloud_details"]["billing_api"]["api_key"],
                in_api_secret=in_org_details["ccloud_details"]["billing_api"]["api_secret"],
                base_url=EndpointURL.API_URL,
            ),
            start_date=next_fetch_date,
        )

        # Initialize the Metrics Handler
        self.metrics_handler = PrometheusMetricsDataHandler(
            in_ccloud_connection=CCloudConnection(
                in_api_key=in_org_details["ccloud_details"]["metrics_api"]["api_key"],
                in_api_secret=in_org_details["ccloud_details"]["metrics_api"]["api_secret"],
            ),
            in_prometheus_url="http://localhost:9090",
            start_date=next_fetch_date,
        )

        # Initialize the Chargeback Object Handler
        self.chargeback_handler = CCloudChargebackHandler(
            billing_dataset=self.billing_handler,
            objects_dataset=self.objects_handler,
            metrics_dataset=self.metrics_handler,
            start_date=next_fetch_date,
        )

        self.attach(notifier=scrape_status_metrics)
        # self.update(notifier=scrape_status_metrics)

    def update(self, notifier: NotifierAbstract):
        self.exposed_end_date = datetime.datetime.utcnow().replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc
        ) - datetime.timedelta(days=1)
        next_ts_in_dt = self.locate_next_fetch_date(start_date=self.exposed_metrics_datetime, is_notifier_update=True)
        if next_ts_in_dt < self.exposed_end_date:
            if next_ts_in_dt == self.exposed_metrics_datetime:
                notifier.clear()
                self.objects_handler.force_clear_prom_metrics()
                self.chargeback_handler.force_clear_prom_metrics()
            else:
                notifier.clear()
                notifier.set_timestamp(curr_timestamp=next_ts_in_dt)
                # self.expose_prometheus_metrics(ts_filter=next_ts)
                print(f"Refreshing CCloud Existing Objects Data")
                self.objects_handler.execute_requests(exposed_timestamp=next_ts_in_dt)
                notifier.labels("ccloud_objects").set(1)
                print(f"Gathering Metrics API Data")
                self.metrics_handler.execute_requests(exposed_timestamp=next_ts_in_dt)
                print(f"Checking for new Billing CSV Files")
                self.billing_handler.execute_requests(exposed_timestamp=next_ts_in_dt)
                print("Calculating next dataset for chargeback")
                self.chargeback_handler.execute_requests(exposed_timestamp=next_ts_in_dt)
                notifier.labels("billing_chargeback").set(1)
                self.exposed_metrics_datetime = next_ts_in_dt
        else:
            print(
                f"""Chargeback calculation is fully caught up to the point where it needs to be. 
                More processing will continue after the day passes and the data for the day is finalized in the Billing API."""
            )

    def locate_next_fetch_date(
        self, start_date: datetime.datetime, is_notifier_update: bool = False
    ) -> datetime.datetime:
        # TODO; Current fetch is totally naive and finds the first gap in Chargeback dataset only.
        # Long term thought is to diverge datetimes for multiple objects as necessary.
        # This will prevent naive re-calculations for other objects as well.
        # The reason we do it with CB object right now is 2 fold:
        # 1. simple :)
        # 2. as we scrape all the datasets in the same scrape and align the same dates, right now everything will work.
        #    Once we diverge scrapes on their own dates, we will need to enhance this method to return specific values.
        next_ts_in_dt = start_date
        for next_date in pd.date_range(
            start=start_date,
            end=self.exposed_end_date,
            freq="1H",
            inclusive="neither" if is_notifier_update else "left",
        ):
            next_ts_in_dt = next_date.to_pydatetime(warn=False)
            if not self.status_metrics_handler.is_dataset_present(
                scrape_type=ScrapeType.BillingChargeback,
                ts_in_millis=self.status_metrics_handler.convert_dt_to_ts(next_ts_in_dt),
            ):
                # return the immediate gap datetime in the series
                return next_ts_in_dt
        # if no dates are found, use the last fetch date
        return next_ts_in_dt


@dataclass(kw_only=True)
class CCloudOrgList:
    in_orgs: InitVar[List | None] = None
    in_days_in_memory: InitVar[int] = field(default=7)

    orgs: Dict[str, CCloudOrg] = field(default_factory=dict, init=False)

    def __post_init__(self, in_orgs, in_days_in_memory) -> None:
        req_count = 0
        for org_item in in_orgs:
            temp = CCloudOrg(
                in_org_details=org_item,
                in_days_in_memory=in_days_in_memory,
                org_id=org_item["id"] if org_item["id"] else req_count,
            )
            self.__add_org_to_cache(ccloud_org=temp)

    def __add_org_to_cache(self, ccloud_org: CCloudOrg) -> None:
        self.orgs[ccloud_org.org_id] = ccloud_org

    def execute_requests(self):
        for org_item in self.orgs.values():
            org_item.execute_requests()

    def run_calculations(self):
        for org_id, org in self.orgs.items():
            org.run_calculations()
