import datetime
import logging
from dataclasses import InitVar, dataclass, field
from time import sleep
from typing import Dict

import requests

from ccloud.ccloud_api.api_keys import CCloudAPIKeyList
from ccloud.ccloud_api.clusters import CCloudCluster, CCloudClusterList
from ccloud.ccloud_api.service_accounts import CCloudServiceAccountList
from ccloud.ccloud_api.user_accounts import CCloudUserAccountList
from ccloud.connections import CCloudBase
from helpers import logged_method
from prometheus_processing.custom_collector import TimestampedCollector

LOGGER = logging.getLogger(__name__)


@dataclass
class CCloudConnector:
    env_id: str
    cluster_id: str
    connector_id: str
    connector_name: str
    connector_class: str
    owner_id: str


kafka_connectors_prom_metrics = TimestampedCollector(
    "confluent_cloud_connector",
    "Connector Details for every Fully Managed Connector created within CCloud",
    ["connector_id", "cluster_id", "env_id"],
    in_begin_timestamp=datetime.datetime.now(),
)
# kafka_connectors_prom_status_metrics = TimestampedCollector(
#     "confluent_cloud_kafka_connector_scrape_status",
#     "CCloud Kafka Connectors scrape status",
#     in_begin_timestamp=datetime.datetime.now(),
# )


@dataclass
class CCloudConnectorList(CCloudBase):
    ccloud_kafka_clusters: CCloudClusterList
    ccloud_service_accounts: CCloudServiceAccountList
    ccloud_users: CCloudUserAccountList
    ccloud_api_keys: CCloudAPIKeyList
    exposed_timestamp: InitVar[datetime.datetime] = field(init=True)

    connectors: Dict[str, CCloudConnector] = field(default_factory=dict, init=False)
    url_get_connector_config: str = field(init=False)

    def __post_init__(self, exposed_timestamp: datetime.datetime) -> None:
        super().__post_init__()
        self.url = self.in_ccloud_connection.get_endpoint_url(key=self.in_ccloud_connection.uri.list_connector_names)
        LOGGER.debug(f"Kafka Connector URL: {self.url}")
        self.url_get_connector_config = self.in_ccloud_connection.get_endpoint_url(
            key=self.in_ccloud_connection.uri.get_connector_config
        )
        LOGGER.debug(f"Kafka Get Connector Config URL: {self.url}")
        self.read_all()
        LOGGER.debug("Exposing Prometheus Metrics for Kafka Connector")
        self.expose_prometheus_metrics(exposed_timestamp=exposed_timestamp)
        LOGGER.info("CCloud Kafka Connectors initialized successfully")

    @logged_method
    def expose_prometheus_metrics(self, exposed_timestamp: datetime.datetime):
        LOGGER.debug("Exposing Prometheus Metrics for Kafka Connector for timestamp: " + str(exposed_timestamp))
        self.force_clear_prom_metrics()
        kafka_connectors_prom_metrics.set_timestamp(curr_timestamp=exposed_timestamp)
        for _, v in self.connectors.items():
            # TODO: created datetime is missing from connector creation date.
            kafka_connectors_prom_metrics.labels(v.connector_id, v.cluster_id, v.env_id).set(1)

    @logged_method
    def force_clear_prom_metrics(self):
        kafka_connectors_prom_metrics.clear()

    @logged_method
    def __str__(self):
        for v in self.cluster.values():
            print(
                "{:<15} {:<15} {:<25} {:<10} {:<25} {:<50}".format(
                    v.env_id, v.cluster_id, v.cluster_name, v.cloud, v.availability, v.bootstrap_url
                )
            )

    @logged_method
    def read_all(self):
        LOGGER.debug("Reading all Kafka Connector from Confluent Cloud")
        for kafka_cluster in self.ccloud_kafka_clusters.clusters.values():
            LOGGER.info("Checking Environment " + kafka_cluster.env_id + " for any provisioned connectors.")
            for connector_item in self.read_all_connector_details(kafka_cluster=kafka_cluster):
                LOGGER.debug(
                    f'Found Connector {connector_item["status"]["name"]} linked to Kafka Cluster ID {kafka_cluster.cluster_id} with Cluster Name {kafka_cluster.cluster_name}'
                )
                self.read_connector_config(kafka_cluster=kafka_cluster, connector_details=connector_item)

    def read_all_connector_details(self, kafka_cluster: CCloudCluster, params={}):
        temp_url = self.url.format(environment_id=kafka_cluster.env_id, kafka_cluster_id=kafka_cluster.cluster_id)
        LOGGER.debug(f"Reading from CCloud API: {temp_url}")
        resp = requests.get(url=temp_url, auth=self.http_connection, timeout=10, params=params)
        if resp.status_code == 200:
            LOGGER.debug(f"Successfully fetched the Connector details for Kafka Cluster {kafka_cluster.cluster_id}")
            out_json = resp.json()
            # if out_json is not None and out_json["data"] is not None:
            for item in out_json.values():
                yield item
        elif resp.status_code == 429:
            LOGGER.info(f"CCloud API Per-Minute Limit exceeded. Sleeping for 45 seconds. Error stack: {resp.text}")
            sleep(45)
            LOGGER.info("Timer up. Resuming CCloud API scrape.")
        elif resp.status_code >= 400:
            LOGGER.error(
                f"Cannot fetch the Connector details. API Error Code: {resp.status_code} API Error Message: {resp.text}"
            )

    @logged_method
    def read_connector_config(self, kafka_cluster: dict, connector_details:dict):
        connector_id = str(connector_details["id"]["id"]).strip().replace(" ", "")
        connector_config = connector_details["info"]["config"]
        connector_name=str(connector_config["name"]).strip().replace(" ", "")
        LOGGER.debug("Found connector config for connector " + connector_config["name"])
        owner_id = None
        auth_mode = connector_config["kafka.auth.mode"]
        match auth_mode:
            case "KAFKA_API_KEY":
                api_key = connector_config["kafka.api.key"]
                # Check if all the API_KEY value is protected or not
                if not all([ch == "*" for ch in api_key]):
                    # Locate the API Key details
                    if self.ccloud_api_keys.api_keys.get(api_key) is not None:
                        owner_id = self.ccloud_api_keys.api_keys[api_key].owner_id
                    else:
                        LOGGER.warn(f"Connector API Key Not found in the Active API Keys. Connector {connector_config['name']} returned {api_key} as the executioner.")
                        owner_id = "connector_api_key_cannot_be_mapped"
                        LOGGER.debug(
                            f"API Key is unavailable for Mapping Connector {connector_config['name']} to its corresponding holder. Connector Ownership defaulted to {owner_id}."
                        )
                else:
                    LOGGER.warn(f"Connector API Key Masked. Found API Key {api_key} for Connector {connector_config['name']}.")
                    owner_id = "connector_api_key_masked"
                    LOGGER.debug(
                        f"API Key is unavailable for Mapping Connector {connector_config['name']} to its corresponding Service Account. Connector Ownership defaulted to {owner_id}."
                    )
            case "SERVICE_ACCOUNT":
                owner_id = connector_config["kafka.service.account.id"]
        self.__add_to_cache(
            CCloudConnector(
                env_id=kafka_cluster.env_id,
                cluster_id=kafka_cluster.cluster_id,
                connector_id=connector_id,
                connector_name=connector_name,
                connector_class=connector_config["connector.class"],
                owner_id=owner_id,
            )
        )

    @logged_method
    def __add_to_cache(self, connector: CCloudConnector) -> None:
        self.connectors[f"{connector.connector_id}"] = connector

    # def locate_api_key_owner(self, api_key: str) -> CCloudUserAccount | CCloudServiceAccount:
    #     key = self.ccloud_api_keys.api_keys[api_key]
    #     if key.owner_id in self.ccloud_service_accounts.sa.keys():
    #         return self.ccloud_service_accounts.sa[key.owner_id]
    #     elif key.owner_id in self.ccloud_users.users.keys():
    #         return self.ccloud_users.users[key.owner_id]

    # Read/Find one Cluster from the cache
    @logged_method
    def find_cluster(self, cluster_id):
        return self.cluster[cluster_id]
