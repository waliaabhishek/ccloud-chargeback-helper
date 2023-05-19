from dataclasses import dataclass, field
from time import sleep, time
from typing import Dict

import requests
from ccloud.ccloud_api.api_keys import CCloudAPIKeyList
from ccloud.ccloud_api.clusters import CCloudCluster, CCloudClusterList
import datetime
from ccloud.connections import CCloudBase
from ccloud.ccloud_api.service_accounts import CCloudServiceAccount, CCloudServiceAccountList
from ccloud.ccloud_api.user_accounts import CCloudUserAccount, CCloudUserAccountList
from prometheus_processing.custom_collector import TimestampedCollector


@dataclass
class CCloudConnector:
    env_id: str
    cluster_id: str
    connector_name: str
    connector_class: str
    owner_id: str


kafka_connectors_prom_metrics = TimestampedCollector(
    "confluent_cloud_connector",
    "Connector Details for every Fully Managed Connector created within CCloud",
    ["connector_id", "cluster_id", "env_id"],
    in_begin_timestamp=datetime.datetime.now(),
)


@dataclass
class CCloudConnectorList(CCloudBase):
    ccloud_kafka_clusters: CCloudClusterList
    ccloud_service_accounts: CCloudServiceAccountList
    ccloud_users: CCloudUserAccountList
    ccloud_api_keys: CCloudAPIKeyList

    connectors: Dict[str, CCloudConnector] = field(default_factory=dict, init=False)
    url_get_connector_config: str = field(init=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.url = self.in_ccloud_connection.get_endpoint_url(key=self.in_ccloud_connection.uri.list_connector_names)
        self.url_get_connector_config = self.in_ccloud_connection.get_endpoint_url(
            key=self.in_ccloud_connection.uri.get_connector_config
        )
        self.read_all()
        self.expose_prometheus_metrics()

    def expose_prometheus_metrics(self):
        for _, v in self.connectors.items():
            kafka_connectors_prom_metrics.labels(v.connector_name, v.cluster_id, v.env_id).set(1)

    def __str__(self):
        for v in self.cluster.values():
            print(
                "{:<15} {:<15} {:<25} {:<10} {:<25} {:<50}".format(
                    v.env_id, v.cluster_id, v.cluster_name, v.cloud, v.availability, v.bootstrap_url
                )
            )

    def read_all(self):
        for kafka_cluster in self.ccloud_kafka_clusters.cluster.values():
            print("Checking Kafka Cluster " + kafka_cluster.env_id + " for any provisioned connectors.")
            for connector_name in self.read_all_connector_names(kafka_cluster=kafka_cluster):
                print(
                    f"Found Connector {connector_name} linked to Kafka Cluster ID {kafka_cluster.cluster_id} with Cluster Name {kafka_cluster.cluster_name}"
                )
                temp_url = self.url_get_connector_config.format(
                    environment_id=kafka_cluster.env_id,
                    kafka_cluster_id=kafka_cluster.cluster_id,
                    connector_name=connector_name,
                )
                self.read_connector_config(kafka_cluster=kafka_cluster, call_url=temp_url)

    def read_all_connector_names(self, kafka_cluster: CCloudCluster, params={}):
        temp_url = self.url.format(environment_id=kafka_cluster.env_id, kafka_cluster_id=kafka_cluster.cluster_id)
        resp = requests.get(url=temp_url, auth=self.http_connection, params=params)
        if resp.status_code == 200:
            out_json = resp.json()
            # if out_json is not None and out_json["data"] is not None:
            for item in out_json:
                yield item
        elif resp.status_code == 429:
            print(f"CCloud API Per-Minute Limit exceeded. Sleeping for 45 seconds. Error stack: {resp.text}")
            sleep(45)
            print("Timer up. Resuming CCloud API scrape.")
        elif resp.status_code >= 400:
            print(
                f"Cannot fetch the Connector details. API Error Code: {resp.status_code} API Error Message: {resp.text}"
            )

    def read_connector_config(self, kafka_cluster: CCloudCluster, call_url: str, params={}):
        resp = requests.get(url=call_url, auth=self.http_connection, params=params)
        if resp.status_code == 200:
            item = resp.json()
            print("Found connector config for connector " + item["name"])
            owner_id = None
            if "kafka.api.key" in item.keys():
                api_key = item["kafka.api.key"]
                if not all([ch == "*" for ch in api_key]):
                    owner_id = api_key
                else:
                    print(f"Connector API Key Masked. Found API Key {api_key} for Connector {item['name']}.")
                    print(
                        f"API Key is unavailable for Mapping Connector {item['name']} to its corresponding Service Account. Connector Ownership will default to the Kafka Cluster {kafka_cluster} instead."
                    )
                    owner_id = kafka_cluster.cluster_id
            elif "kafka.service.account.id" in item.keys():
                owner_id = item["kafka.service.account.id"]
            self.__add_to_cache(
                CCloudConnector(
                    env_id=kafka_cluster.env_id,
                    cluster_id=kafka_cluster.cluster_id,
                    connector_name=str(item["name"]).strip().replace(" ", ""),
                    connector_class=item["connector.class"],
                    owner_id=owner_id,
                )
            )
        else:
            raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)

    def __add_to_cache(self, connector: CCloudConnector) -> None:
        self.connectors[f"{connector.cluster_id}__{connector.connector_name}"] = connector

    # def locate_api_key_owner(self, api_key: str) -> CCloudUserAccount | CCloudServiceAccount:
    #     key = self.ccloud_api_keys.api_keys[api_key]
    #     if key.owner_id in self.ccloud_service_accounts.sa.keys():
    #         return self.ccloud_service_accounts.sa[key.owner_id]
    #     elif key.owner_id in self.ccloud_users.users.keys():
    #         return self.ccloud_users.users[key.owner_id]

    # Read/Find one Cluster from the cache
    def find_cluster(self, cluster_id):
        return self.cluster[cluster_id]
