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
class CCloudIdentityProvider:
    resource_id: str
    name: str
    state: str
    created_at: str
    updated_at: str
    deleted_at: str


id_provider_prom_metrics = TimestampedCollector(
    "confluent_cloud_id_provider",
    "Details for every ID Provider created within CCloud",
    ["provider_id", "display_name"],
    in_begin_timestamp=datetime.datetime.now(),
)


@dataclass(kw_only=True)
class CCloudIdentityProvidersList(CCloudBase):
    exposed_timestamp: InitVar[datetime.datetime] = field(init=True)
    id_providers: Dict[str, CCloudIdentityProvider] = field(default_factory=dict, init=False)

    def __post_init__(self, exposed_timestamp: datetime.datetime) -> None:
        super().__post_init__()
        self.url = self.in_ccloud_connection.get_endpoint_url(key=self.in_ccloud_connection.uri.identity_providers)
        LOGGER.debug(f"CCloud ID Providers Fetch URL: {self.url}")
        self.read_all()
        LOGGER.debug("Exposing Prometheus Metrics for CCloud ID Providers")
        self.expose_prometheus_metrics(exposed_timestamp=exposed_timestamp)
        LOGGER.info("CCloud ID Providers initialized successfully")

    @logged_method
    def expose_prometheus_metrics(self, exposed_timestamp: datetime.datetime):
        LOGGER.debug("Exposing Prometheus Metrics for ID Providers for timestamp: " + str(exposed_timestamp))
        self.force_clear_prom_metrics()
        id_provider_prom_metrics.set_timestamp(curr_timestamp=exposed_timestamp)
        for _, v in self.id_providers.items():
            if v.created_at >= exposed_timestamp:
                id_provider_prom_metrics.labels(v.resource_id, v.name).set(1)

    @logged_method
    def force_clear_prom_metrics(self):
        id_provider_prom_metrics.clear()

    def __str__(self) -> str:
        for item in self.id_providers.values():
            print("{:<15} {:<40} {:<50}".format(item.resource_id, item.name, item.description))

    # Read ALL Service Account details from Confluent Cloud
    @logged_method
    def read_all(self, params={"page_size": 100}):
        LOGGER.debug("Reading all CCloud ID providers from Confluent Cloud")
        for item in self.read_from_api(params=params):
            LOGGER.debug(f"Found ID Provider: {item['id']}; Name {item['display_name']}; State {item['state']}")
            self.__add_to_cache(
                CCloudIdentityProvider(
                    resource_id=item["id"],
                    name=item["display_name"],
                    created_at=parser.isoparse(item["metadata"]["created_at"]),
                    updated_at=parser.isoparse(item["metadata"]["updated_at"]),
                    deleted_at=parser.isoparse(item["metadata"]["deleted_at"]),
                )
            )

    @logged_method
    def __add_to_cache(self, ccloud_id_provider: CCloudIdentityProvider) -> None:
        self.id_providers[ccloud_id_provider.resource_id] = ccloud_id_provider

    # Read/Find one SA from the cache
    @logged_method
    def find_user(self, ccloud_id_provider):
        for item in self.id_providers.values():
            if ccloud_id_provider == item.name:
                return item
        return None
