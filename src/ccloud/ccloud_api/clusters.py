from dataclasses import dataclass, field
from time import sleep, time
from typing import Dict
from urllib import parse

import requests

from ccloud.connections import CCloudBase
from ccloud.ccloud_api.environments import CCloudEnvironmentList
from prometheus_processing.metrics_server import TimestampedGauge


@dataclass
class CCloudCluster:
    env_id: str
    cluster_id: str
    cluster_name: str
    cloud: str
    availability: str
    region: str
    bootstrap_url: str


kafka_cluster_prom_metrics = TimestampedGauge(
    "confluent_cloud_kafka_cluster",
    "Cluster Details for every Kafka Cluster created within CCloud",
    ["cluster_id", "env_id"],
    timestamp=time(),
)


@dataclass
class CCloudClusterList(CCloudBase):
    ccloud_envs: CCloudEnvironmentList

    cluster: Dict[str, CCloudCluster] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.url = self.in_ccloud_connection.get_endpoint_url(key=self.in_ccloud_connection.uri.clusters)
        for item in self.ccloud_envs.env.values():
            print("Checking Environment " + item.env_id + " for any provisioned clusters.")
            self.read_all(env_id=item.env_id, params={"page_size": 50})
        self.expose_prometheus_metrics()

    def expose_prometheus_metrics(self):
        for _, v in self.cluster.items():
            kafka_cluster_prom_metrics.labels(v.cluster_id, v.env_id).set(1)

    def __str__(self):
        for v in self.cluster.values():
            print(
                "{:<15} {:<15} {:<25} {:<10} {:<25} {:<50}".format(
                    v.env_id, v.cluster_id, v.cluster_name, v.cloud, v.availability, v.bootstrap_url
                )
            )

    def read_all(self, params={"page_size": 100}):
        for env_item in self.ccloud_envs.env.values():
            print("Checking CCloud Environment " + env_item.env_id + " for any provisioned ksqlDB Clusters.")
            params["environment"] = env_item.env_id
            for item in self.read_from_api(params=params):
                self.__add_to_cache(
                    CCloudCluster(
                        env_id=env_item.env_id,
                        cluster_id=item["id"],
                        cluster_name=item["spec"]["display_name"],
                        cloud=item["spec"]["cloud"],
                        availability=item["spec"]["availability"],
                        region=item["spec"]["region"],
                        bootstrap_url=item["spec"]["kafka_bootstrap_endpoint"],
                    )
                )
                print("Found cluster " + item["id"] + " with name " + item["spec"]["display_name"])

        # params["environment"] = env_id
        # resp = requests.get(url=self.url, auth=self.http_connection, params=params)
        # if resp.status_code == 200:
        #     out_json = resp.json()
        #     if out_json is not None and out_json["data"] is not None:
        #         for item in out_json["data"]:
        #             print("Found cluster " + item["id"] + " with name " + item["spec"]["display_name"])
        #             self.__add_to_cache(
        #                 CCloudCluster(
        #                     env_id=env_id,
        #                     cluster_id=item["id"],
        #                     cluster_name=item["spec"]["display_name"],
        #                     cloud=item["spec"]["cloud"],
        #                     availability=item["spec"]["availability"],
        #                     region=item["spec"]["region"],
        #                     bootstrap_url=item["spec"]["kafka_bootstrap_endpoint"],
        #                 )
        #             )
        #     if "next" in out_json["metadata"]:
        #         query_params = parse.parse_qs(parse.urlsplit(out_json["metadata"]["next"]).query)
        #         params["page_token"] = str(query_params["page_token"][0])
        #         self.read_all(env_id, params)
        # elif resp.status_code == 429:
        #     print(f"CCloud API Per-Minute Limit exceeded. Sleeping for 45 seconds. Error stack: {resp.text}")
        #     sleep(45)
        #     print("Timer up. Resuming CCloud API scrape.")
        # else:
        #     raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)

    def __add_to_cache(self, ccloud_cluster: CCloudCluster) -> None:
        self.cluster[ccloud_cluster.cluster_id] = ccloud_cluster

    # Read/Find one Cluster from the cache
    def find_cluster(self, cluster_id):
        return self.cluster[cluster_id]
