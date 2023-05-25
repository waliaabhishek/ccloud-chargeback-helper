import datetime
from dataclasses import InitVar, dataclass, field
from typing import Dict, List


from ccloud.connections import CCloudConnection, EndpointURL
from data_processing.data_handlers.billing_api_handler import CCloudBillingHandler
from data_processing.data_handlers.ccloud_api_handler import CCloudObjectsHandler
from data_processing.data_handlers.chargeback_handler import CCloudChargebackHandler
from data_processing.data_handlers.prom_metrics_handler import PrometheusMetricsDataHandler
from helpers import sanitize_id


@dataclass(kw_only=True)
class CCloudOrg:
    in_org_details: InitVar[List | None] = None
    in_days_in_memory: InitVar[int] = field(default=7)
    org_id: str

    objects_handler: CCloudObjectsHandler = field(init=False)
    metrics_handler: PrometheusMetricsDataHandler = field(init=False)
    billing_handler: CCloudBillingHandler = field(init=False)
    chargeback_handler: CCloudChargebackHandler = field(init=False)

    def __post_init__(self, in_org_details, in_days_in_memory) -> None:
        self.org_id = sanitize_id(in_org_details["id"])
        # Initialize the CCloud Objects Handler
        self.objects_handler = CCloudObjectsHandler(
            in_ccloud_connection=CCloudConnection(
                in_api_key=in_org_details["ccloud_details"]["ccloud_api"]["api_key"],
                in_api_secret=in_org_details["ccloud_details"]["ccloud_api"]["api_secret"],
                base_url=EndpointURL.API_URL,
            ),
        )

        # This start date is calculated from the now time to rewind back 365 days as that is the
        # time limit of the billing dataset which is available to us. We will need the metrics handler
        # to try and get the data from that far back as well.
        start_date = datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0) + datetime.timedelta(
            days=-364
        )

        # Initialize the Billing API Handler
        self.billing_handler = CCloudBillingHandler(
            in_ccloud_connection=CCloudConnection(
                in_api_key=in_org_details["ccloud_details"]["billing_api"]["api_key"],
                in_api_secret=in_org_details["ccloud_details"]["billing_api"]["api_secret"],
                base_url=EndpointURL.API_URL,
            ),
            start_date=start_date,
        )

        # Initialize the Metrics Handler
        self.metrics_handler = PrometheusMetricsDataHandler(
            in_ccloud_connection=CCloudConnection(
                in_api_key=in_org_details["ccloud_details"]["metrics_api"]["api_key"],
                in_api_secret=in_org_details["ccloud_details"]["metrics_api"]["api_secret"],
            ),
            in_prometheus_url="http://localhost:9090",
            start_date=start_date,
        )

        # Initialize the Chargeback Object Handler
        self.chargeback_handler = CCloudChargebackHandler(
            billing_dataset=self.billing_handler,
            objects_dataset=self.objects_handler,
            metrics_dataset=self.metrics_handler,
            start_date=start_date,
        )

    def execute_requests(self):
        print(f"Gathering CCloud Existing Objects Data")
        self.objects_handler.execute_requests()
        print(f"Gathering Metrics API Data")
        self.metrics_handler.execute_requests()
        print(f"Checking for new Billing CSV Files")
        self.billing_handler.execute_requests()
        print("Calculating next dataset for chargeback")
        self.chargeback_handler.execute_requests()

    # def find_available_hour_slices_in_billing_datasets(self) -> datetime.datetime:
    #     for item in self.billing_handler.available_hour_slices_in_dataset:
    #         yield datetime.datetime.fromisoformat(item)

    # def run_calculations(self):
    #     curr_date = datetime.datetime.now(tz=None).replace(hour=0, minute=0, second=0, microsecond=0)
    #     for hour_slice in self.find_available_hour_slices_in_billing_datasets():
    #         if hour_slice >= curr_date:
    #             print(f"Skipping Billing Row TS - {hour_slice} as the Metrics are fetched with 1 day delay")
    #             continue
    #         billing_data = self.billing_handler.get_hourly_dataset(time_slice=hour_slice)
    #         if billing_data:
    #             metrics_found, metrics_data = self.metrics_handler.get_hourly_dataset(
    #                 time_slice=hour_slice, billing_mgmt=True
    #             )
    #             for filename, data_set in billing_data.items():
    #                 if not data_set.empty:
    #                     self.chargeback_handler.run_calculations(
    #                         time_slice=hour_slice, billing_dataframe=data_set, metrics_dataframe=metrics_data,
    #                     )
    #                     BILLING_PERSISTENCE_STORE.add_data_to_persistence_store(
    #                         org_id=self.org_id, key=(hour_slice,), value=filename
    #                     )
    #         # TODO: Need to add more status for when data is missing, cannot silently ignore.
    #         # Bad user experience otherwise.
    #     self.chargeback_handler.export_metrics_to_csv(
    #         output_basepath=STORAGE_PATH.get_path(org_id=self.org_id, dir_type=DirType.OutputData, ensure_exists=True)
    #     )


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
