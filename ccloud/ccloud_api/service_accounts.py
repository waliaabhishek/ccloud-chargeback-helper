import datetime
import logging
from dataclasses import InitVar, dataclass, field
from typing import Dict

from dateutil import parser

from ccloud.connections import CCloudBase
from helpers import logged_method
from prometheus_processing.custom_collector import TimestampedCollector

LOGGER = logging.getLogger(__name__)


@dataclass
class CCloudServiceAccount:
    resource_id: str
    name: str
    description: str
    created_at: str
    updated_at: str


sa_prom_metrics = TimestampedCollector(
    "confluent_cloud_sa",
    "Environment Details for every Environment created within CCloud",
    ["sa_id", "display_name"],
    in_begin_timestamp=datetime.datetime.now(),
)
# sa_prom_status_metrics = TimestampedCollector(
#     "confluent_cloud_sa_scrape_status",
#     "CCloud Service Accounts scrape status",
#     in_begin_timestamp=datetime.datetime.now(),
# )


@dataclass(kw_only=True)
class CCloudServiceAccountList(CCloudBase):
    exposed_timestamp: InitVar[datetime.datetime] = field(init=True)
    sa: Dict[str, CCloudServiceAccount] = field(default_factory=dict, init=False)

    def __post_init__(self, exposed_timestamp: datetime.datetime) -> None:
        super().__post_init__()
        self.url = self.in_ccloud_connection.get_endpoint_url(key=self.in_ccloud_connection.uri.service_accounts)
        LOGGER.debug(f"Service Account URL: {self.url}")
        self.read_all()
        LOGGER.debug("Exposing Prometheus Metrics for Service Accounts")
        self.expose_prometheus_metrics(exposed_timestamp=exposed_timestamp)
        LOGGER.info("CCloud Service Accounts initialized successfully")

    @logged_method
    def expose_prometheus_metrics(self, exposed_timestamp: datetime.datetime):
        LOGGER.debug("Exposing Prometheus Metrics for Service Accounts for timestamp: " + str(exposed_timestamp))
        self.force_clear_prom_metrics()
        sa_prom_metrics.set_timestamp(curr_timestamp=exposed_timestamp)
        for _, v in self.sa.items():
            if v.created_at >= exposed_timestamp:
                sa_prom_metrics.labels(v.resource_id, v.name).set(1)
        # sa_prom_status_metrics.set_timestamp(curr_timestamp=exposed_timestamp).set(1)

    @logged_method
    def force_clear_prom_metrics(self):
        sa_prom_metrics.clear()

    def __str__(self) -> str:
        for item in self.sa.values():
            print("{:<15} {:<40} {:<50}".format(item.resource_id, item.name, item.description))

    # Read ALL Service Account details from Confluent Cloud
    @logged_method
    def read_all(self, params={"page_size": 100}):
        LOGGER.debug("Reading all Service Accounts from Confluent Cloud")
        for item in self.read_from_api(params=params):
            self.__add_to_cache(
                CCloudServiceAccount(
                    resource_id=item["id"],
                    name=item["display_name"],
                    description=item["description"],
                    created_at=parser.isoparse(item["metadata"]["created_at"]),
                    updated_at=parser.isoparse(item["metadata"]["updated_at"]),
                )
            )
            LOGGER.debug(f"Found Service Account: {item['id']}; Name {item['display_name']}")

    @logged_method
    def __add_to_cache(self, ccloud_sa: CCloudServiceAccount) -> None:
        self.sa[ccloud_sa.resource_id] = ccloud_sa

    # Read/Find one SA from the cache
    @logged_method
    def find_sa(self, sa_name):
        for item in self.sa.values():
            if sa_name == item.name:
                return item
        return None

    # def __delete_from_cache(self, res_id):
    #     self.sa.pop(res_id, None)

    # Create/Find one SA and add it to the cache, so that we do not have to refresh the cache manually
    # def create_sa(self, sa_name, description=None) -> Tuple[CCloudServiceAccount, bool]:
    #     temp = self.find_sa(sa_name)
    #     if temp:
    #         return temp, False
    #     # print("Creating a new Service Account with name: " + sa_name)
    #     payload = {
    #         "display_name": sa_name,
    #         "description": str("Account for " + sa_name + " created by CI/CD framework")
    #         if not description
    #         else description,
    #     }
    #     resp = requests.post(
    #         url=self.url,
    #         auth=self.http_connection,
    #         json=payload,
    #     )
    #     if resp.status_code == 201:
    #         sa_details = resp.json()
    #         sa_value = CCloudServiceAccount(
    #             resource_id=sa_details["id"],
    #             name=sa_details["display_name"],
    #             description=sa_details["description"],
    #             created_at=sa_details["metadata"]["created_at"],
    #             updated_at=sa_details["metadata"]["updated_at"],
    #             is_ignored=False,
    #         )
    #         self.__add_to_cache(ccloud_sa=sa_value)
    #         return (sa_value, True)
    #     else:
    #         raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)

    # def delete_sa(self, sa_name) -> bool:
    #     temp = self.find_sa(sa_name)
    #     if not temp:
    #         print("Did not find Service Account with name '" + sa_name + "'. Not deleting anything.")
    #         return False
    #     else:
    #         resp = requests.delete(url=str(self.url + "/" + temp.resource_id), auth=self.http_connection)
    #         if resp.status_code == 204:
    #             self.__delete_from_cache(temp.resource_id)
    #             return True
    #         else:
    #             raise Exception("Could not perform the DELETE operation. Please check your settings. " + resp.text)

    # def __try_detect_internal_service_accounts(self, sa_name: str) -> bool:
    #     if sa_name.startswith(("Connect.lcc-", "KSQL.lksqlc-")):
    #         return True
    #     else:
    #         return False
