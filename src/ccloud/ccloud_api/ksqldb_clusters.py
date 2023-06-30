import datetime
import pprint
from dataclasses import InitVar, dataclass, field
from typing import Dict

from dateutil import parser

from ccloud.ccloud_api.environments import CCloudEnvironmentList
from ccloud.connections import CCloudBase
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
    [
        "cluster_id",
        "env_id",
        "kafka_cluster_id",
    ],
    in_begin_timestamp=datetime.datetime.now(),
)
# ksqldb_prom_status_metrics = TimestampedCollector(
#     "confluent_cloud_ksqldb_scrape_status", "CCloud ksqlDB scrape status", in_begin_timestamp=datetime.datetime.now(),
# )


@dataclass
class CCloudKsqldbClusterList(CCloudBase):
    ccloud_envs: CCloudEnvironmentList
    exposed_timestamp: InitVar[datetime.datetime] = field(init=True)

    ksqldb_clusters: Dict[str, CCloudKsqldbCluster] = field(default_factory=dict, init=False)

    # This init function will initiate the base object and then check CCloud
    # for all the active API Keys. All API Keys that are listed in CCloud are
    # the added to a cache.
    def __post_init__(self, exposed_timestamp: datetime.datetime) -> None:
        super().__post_init__()
        self.url = self.in_ccloud_connection.get_endpoint_url(key=self.in_ccloud_connection.uri.list_ksql_clusters)
        print("Gathering list of all ksqlDB Clusters for all Service Account(s) in CCloud.")
        self.read_all()
        self.expose_prometheus_metrics(exposed_timestamp=exposed_timestamp)

    def expose_prometheus_metrics(self, exposed_timestamp: datetime.datetime):
        self.force_clear_prom_metrics()
        ksqldb_prom_metrics.set_timestamp(curr_timestamp=exposed_timestamp)
        for _, v in self.ksqldb_clusters.items():
            if v.created_at >= exposed_timestamp:
                ksqldb_prom_metrics.labels(v.cluster_id, v.env_id, v.kafka_cluster_id).set(1)
        # ksqldb_prom_status_metrics.set_timestamp(curr_timestamp=exposed_timestamp).set(1)

    def force_clear_prom_metrics(self):
        ksqldb_prom_metrics.clear()

    # This method will help reading all the API Keys that are already provisioned.
    # Please note that the API Secrets cannot be read back again, so if you do not have
    # access to the secret , you will need to generate new api key/secret pair.
    def read_all(self, params={"page_size": 100}):
        for env_item in self.ccloud_envs.env.values():
            print("Checking CCloud Environment " + env_item.env_id + " for any provisioned ksqlDB Clusters.")
            params["environment"] = env_item.env_id
            for item in self.read_from_api(params=params):
                owner_id = None
                if item["spec"]["credential_identity"]["id"]:
                    owner_id = item["spec"]["credential_identity"]["id"]
                else:
                    owner_id = "ksqldb_owner_id_missing_in_api_response"
                    print(
                        f'ksqlDB API does not provide any Owner ID for cluster {item["id"]}. ksqlDB cluster Ownership will default to a static string'
                    )
                self.__add_to_cache(
                    CCloudKsqldbCluster(
                        cluster_id=item["id"],
                        cluster_name=item["spec"]["display_name"],
                        csu_count=item["spec"]["csu"],
                        env_id=item["spec"]["environment"]["id"],
                        kafka_cluster_id=item["spec"]["kafka_cluster"]["id"],
                        owner_id=owner_id,
                        created_at=parser.isoparse(item["metadata"]["created_at"]),
                    )
                )
                print("Found ksqlDB Cluster " + item["id"] + " with name " + item["spec"]["display_name"])

    def __add_to_cache(self, ksqldb_cluster: CCloudKsqldbCluster) -> None:
        self.ksqldb_clusters[ksqldb_cluster.cluster_id] = ksqldb_cluster
