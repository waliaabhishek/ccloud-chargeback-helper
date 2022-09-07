from dataclasses import dataclass, field
from typing import Dict
from urllib import parse

import requests

from ccloud.connections import CCloudBase
from ccloud.core_api.environments import CCloudEnvironmentList


@dataclass
class CCloudCluster:
    env_id: str
    cluster_id: str
    cluster_name: str
    cloud: str
    availability: str
    region: str
    bootstrap_url: str


@dataclass
class CCloudClusterList(CCloudBase):
    ccloud_env: CCloudEnvironmentList
    cluster: Dict[str, CCloudCluster] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.url = self._ccloud_connection.get_endpoint_url(key=self._ccloud_connection.uri.clusters)
        for item in self.ccloud_env.env.values():
            print("Checking Environment " + item.env_id + " for any provisioned clusters.")
            self.read_all(env_id=item.env_id, params={"page_size": 50})

    def __str__(self):
        for v in self.cluster.values():
            print(
                "{:<15} {:<15} {:<25} {:<10} {:<25} {:<50}".format(
                    v.env_id, v.cluster_id, v.cluster_name, v.cloud, v.availability, v.bootstrap_url
                )
            )

    def read_all(self, env_id: str, params={"page_size": 50}):
        params["environment"] = env_id
        resp = requests.get(url=self.url, auth=self.http_connection, params=params)
        if resp.status_code == 200:
            out_json = resp.json()
            for item in out_json["data"]:
                print("Found cluster " + item["id"] + " with name " + item["spec"]["display_name"])
                self.__add_to_cache(
                    CCloudCluster(
                        env_id=env_id,
                        cluster_id=item["id"],
                        cluster_name=item["spec"]["display_name"],
                        cloud=item["spec"]["cloud"],
                        availability=item["spec"]["availability"],
                        region=item["spec"]["region"],
                        bootstrap_url=item["spec"]["kafka_bootstrap_endpoint"],
                    )
                )
            if "next" in out_json["metadata"]:
                query_params = parse.parse_qs(parse.urlsplit(out_json["metadata"]["next"]).query)
                params["page_token"] = str(query_params["page_token"][0])
                self.read_all(env_id, params)
        else:
            raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)

    def __add_to_cache(self, ccloud_cluster: CCloudCluster) -> None:
        self.cluster[ccloud_cluster.cluster_id] = ccloud_cluster

    # Read/Find one Cluster from the cache
    def find_cluster(self, cluster_id):
        return self.cluster[cluster_id]
