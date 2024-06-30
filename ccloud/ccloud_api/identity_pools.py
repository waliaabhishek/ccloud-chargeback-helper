import datetime
import logging
from dataclasses import InitVar, dataclass, field
from typing import Dict

from dateutil import parser
from ccloud.ccloud_api.identity_providers import CCloudIdentityProvider, CCloudIdentityProvidersList

from ccloud.connections import CCloudBase
from helpers import logged_method
from prometheus_processing.custom_collector import TimestampedCollector

LOGGER = logging.getLogger(__name__)


@dataclass
class CCloudIdentityPool:
    resource_id: str
    name: str
    principal: str
    id_provider: CCloudIdentityProvider
    created_at: str
    updated_at: str
    deleted_at: str


id_pool_prom_metrics = TimestampedCollector(
    "confluent_cloud_id_pool",
    "Details for every ID pool created within CCloud",
    ["resource_id", "display_name", "principal"],
    in_begin_timestamp=datetime.datetime.now(),
)


@dataclass(kw_only=True)
class CCloudIdentityPoolsList(CCloudBase):
    ccloud_id_providers: CCloudIdentityProvidersList

    exposed_timestamp: InitVar[datetime.datetime] = field(init=True)
    id_pools: Dict[str, CCloudIdentityPool] = field(default_factory=dict, init=False)

    def __post_init__(self, exposed_timestamp: datetime.datetime) -> None:
        super().__post_init__()
        self.url = self.in_ccloud_connection.get_endpoint_url(key=self.in_ccloud_connection.uri.identity_pools)
        LOGGER.debug(f"CCloud ID Pools Fetch URL: {self.url}")
        self.read_all()
        LOGGER.debug("Exposing Prometheus Metrics for CCloud ID Pools")
        self.expose_prometheus_metrics(exposed_timestamp=exposed_timestamp)
        LOGGER.info("CCloud ID Pools initialized successfully")

    @logged_method
    def expose_prometheus_metrics(self, exposed_timestamp: datetime.datetime):
        LOGGER.debug("Exposing Prometheus Metrics for ID Pools for timestamp: " + str(exposed_timestamp))
        self.force_clear_prom_metrics()
        id_pool_prom_metrics.set_timestamp(curr_timestamp=exposed_timestamp)
        for _, v in self.id_pools.items():
            if v.created_at >= exposed_timestamp:
                id_pool_prom_metrics.labels(v.resource_id, v.name, v.principal).set(1)

    @logged_method
    def force_clear_prom_metrics(self):
        id_pool_prom_metrics.clear()

    def __str__(self) -> str:
        for item in self.id_pools.values():
            print("{:<15} {:<40} {:<50}".format(item.resource_id, item.name, item.principal))

    @logged_method
    def read_all(self, params={"page_size": 50}):
        LOGGER.debug("Reading all CCloud ID pools from Confluent Cloud")
        for provider in self.ccloud_id_providers.id_providers.values():
            temp_url = self.url.format(provider_id=provider.resource_id)
            for item in self.read_from_api(params=params):
                LOGGER.debug(f"Found ID Pool: {item['id']}; Name {item['display_name']}; State {item['principal']}")
                self.__add_to_cache(
                    CCloudIdentityPool(
                        resource_id=item["id"],
                        name=item["display_name"],
                        principal=item["principal"],
                        id_provider=provider,
                        created_at=parser.isoparse(item["metadata"]["created_at"]),
                        updated_at=parser.isoparse(item["metadata"]["updated_at"]),
                        deleted_at=parser.isoparse(item["metadata"]["deleted_at"]),
                    )
                )

    @logged_method
    def __add_to_cache(self, ccloud_id_provider: CCloudIdentityPool) -> None:
        self.id_pools[ccloud_id_provider.resource_id] = ccloud_id_provider

    # Read/Find one SA from the cache
    @logged_method
    def find_user(self, ccloud_id_provider):
        for item in self.id_pools.values():
            if ccloud_id_provider == item.name:
                return item
        return None
