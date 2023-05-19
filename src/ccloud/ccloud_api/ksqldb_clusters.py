import pprint
from dataclasses import dataclass, field
from time import sleep, time
from typing import Dict
from urllib import parse
import requests
import datetime
from dateutil import parser
from ccloud.connections import CCloudBase
from ccloud.ccloud_api.environments import CCloudEnvironmentList
from prometheus_processing.custom_collector import TimestampedCollector

pp = pprint.PrettyPrinter(indent=2)


@dataclass
class CCloudKsqldbCluster:
    cluster_id: str
    cluster_name: str
    csu_count: str
    env_id: str
    kafka_cluster_id: str
    owner_id: str
    created_at: str


ksqldb_prom_metrics = TimestampedCollector(
    "confluent_cloud_ksqldb_cluster",
    "Environment Details for every Environment created within CCloud",
    ["cluster_id", "env_id", "kafka_cluster_id", "created_at"],
    in_begin_timestamp=datetime.datetime.now(),
)


@dataclass
class CCloudKsqldbClusterList(CCloudBase):
    ccloud_envs: CCloudEnvironmentList

    ksqldb_clusters: Dict[str, CCloudKsqldbCluster] = field(default_factory=dict, init=False)

    # This init function will initiate the base object and then check CCloud
    # for all the active API Keys. All API Keys that are listed in CCloud are
    # the added to a cache.
    def __post_init__(self) -> None:
        super().__post_init__()
        self.url = self.in_ccloud_connection.get_endpoint_url(key=self.in_ccloud_connection.uri.list_ksql_clusters)
        print("Gathering list of all ksqlDB Clusters for all Service Account(s) in CCloud.")
        self.read_all()
        self.expose_prometheus_metrics()

    def expose_prometheus_metrics(self):
        for _, v in self.ksqldb_clusters.items():
            ksqldb_prom_metrics.labels(v.cluster_id, v.env_id, v.kafka_cluster_id, v.created_at).set(1)

    # This method will help reading all the API Keys that are already provisioned.
    # Please note that the API Secrets cannot be read back again, so if you do not have
    # access to the secret , you will need to generate new api key/secret pair.
    def read_all(self, params={"page_size": 100}):
        for env_item in self.ccloud_envs.env.values():
            print("Checking CCloud Environment " + env_item.env_id + " for any provisioned ksqlDB Clusters.")
            params["environment"] = env_item.env_id
            for item in self.read_from_api(params=params):
                self.__add_to_cache(
                    CCloudKsqldbCluster(
                        cluster_id=item["id"],
                        cluster_name=item["spec"]["display_name"],
                        csu_count=item["spec"]["csu"],
                        env_id=item["spec"]["environment"]["id"],
                        kafka_cluster_id=item["spec"]["kafka_cluster"]["id"],
                        owner_id=item["spec"]["credential_identity"]["id"],
                        created_at=parser.isoparse(item["metadata"]["created_at"]),
                    )
                )
                print("Found ksqlDB Cluster " + item["id"] + " with name " + item["spec"]["display_name"])
            # resp = requests.get(url=self.url, auth=self.http_connection, params=params)
            # if resp.status_code == 200:
            #     out_json = resp.json()
            #     if out_json is not None and out_json["data"] is not None:
            #         for item in out_json["data"]:
            #             self.__add_to_cache(
            #                 CCloudKsqldbCluster(
            #                     cluster_id=item["id"],
            #                     cluster_name=item["spec"]["display_name"],
            #                     csu_count=item["spec"]["csu"],
            #                     env_id=item["spec"]["environment"]["id"],
            #                     kafka_cluster_id=item["spec"]["kafka_cluster"]["id"],
            #                     owner_id=item["spec"]["credential_identity"]["id"],
            #                     created_at=parser.isoparse(item["metadata"]["created_at"]),
            #                 )
            #             )
            #             print("Found ksqlDB Cluster " + item["id"] + " with name " + item["spec"]["display_name"])
            #     if "next" in out_json["metadata"]:
            #         query_params = parse.parse_qs(parse.urlsplit(out_json["metadata"]["next"]).query)
            #         params["page_token"] = str(query_params["page_token"][0])
            #         self.read_all(params)
            # elif resp.status_code == 429:
            #     print(f"CCloud API Per-Minute Limit exceeded. Sleeping for 45 seconds. Error stack: {resp.text}")
            #     sleep(45)
            #     print("Timer up. Resuming CCloud API scrape.")
            # else:
            #     raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)

    def __add_to_cache(self, ksqldb_cluster: CCloudKsqldbCluster) -> None:
        self.ksqldb_clusters[ksqldb_cluster.cluster_id] = ksqldb_cluster
