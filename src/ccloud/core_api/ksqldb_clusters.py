import pprint
from dataclasses import dataclass, field
from typing import Dict
from urllib import parse
import requests

from ccloud.connections import CCloudBase
from ccloud.core_api.environments import CCloudEnvironmentList

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


@dataclass
class CCloudKsqldbClusterList(CCloudBase):
    ccloud_envs: CCloudEnvironmentList

    ksqldb_clusters: Dict[str, CCloudKsqldbCluster] = field(default_factory=dict, init=False)

    # This init function will initiate the base object and then check CCloud
    # for all the active API Keys. All API Keys that are listed in CCloud are
    # the added to a cache.
    def __post_init__(self) -> None:
        super().__post_init__()
        self.url = self._ccloud_connection.get_endpoint_url(key=self._ccloud_connection.uri.list_ksql_clusters)
        print("Gathering list of all ksqlDB Clusters for all Service Account(s) in CCloud.")
        self.read_all()

    # This method will help reading all the API Keys that are already provisioned.
    # Please note that the API Secrets cannot be read back again, so if you do not have
    # access to the secret , you will need to generate new api key/secret pair.
    def read_all(self, params={"page_size": 25}):
        for env_item in self.ccloud_envs.env.values():
            print("Checking CCloud Environment " + env_item.env_id + " for any provisioned ksqlDB Clusters.")
            params["environment"] = env_item.env_id
            resp = requests.get(url=self.url, auth=self.http_connection, params=params)
            if resp.status_code == 200:
                out_json = resp.json()
                if out_json is not None and out_json["data"] is not None:
                    for item in out_json["data"]:
                        print("Found ksqlDB Cluster " + item["id"] + " with name " + item["spec"]["display_name"])
                        self.__add_to_cache(
                            CCloudKsqldbCluster(
                                cluster_id=item["id"],
                                cluster_name=item["spec"]["display_name"],
                                csu_count=item["spec"]["csu"],
                                env_id=item["spec"]["environment"]["id"],
                                kafka_cluster_id=item["spec"]["kafka_cluster"]["id"],
                                owner_id=item["spec"]["credential_identity"]["id"],
                                created_at=item["metadata"]["created_at"],
                            )
                        )
                if "next" in out_json["metadata"]:
                    query_params = parse.parse_qs(parse.urlsplit(out_json["metadata"]["next"]).query)
                    params["page_token"] = str(query_params["page_token"][0])
                    self.read_all(params)
            else:
                raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)

    def __add_to_cache(self, ksqldb_cluster: CCloudKsqldbCluster) -> None:
        self.ksqldb_clusters[ksqldb_cluster.cluster_id] = ksqldb_cluster

    # def find_keys_with_sa(self, sa_id: str) -> List[CCloudKsqldbCluster]:
    #     output = []
    #     for item in self.ksqldb_clusters.values():
    #         if sa_id == item.owner_id:
    #             output.append(item)
    #     return output

    # def find_keys_with_sa_and_cluster(self, sa_id: str, cluster_id: str) -> List[CCloudKsqldbCluster]:
    #     output = []
    #     for item in self.ksqldb_clusters.values():
    #         if cluster_id == item.cluster_id and sa_id == item.owner_id:
    #             output.append(item)
    #     return output

    # def print_cluster_details(
    #     self, ccloud_sa: service_account.CCloudServiceAccountList, api_keys: List[CCloudKsqldbCluster] = None
    # ):
    #     print(
    #         "{:<20} {:<25} {:<25} {:<20} {:<20} {:<50}".format(
    #             "API Key",
    #             "API Key Cluster ID",
    #             "Created",
    #             "API Key Owner ID",
    #             "API Key Owner Name",
    #             "API Key Description",
    #         )
    #     )
    #     if api_keys:
    #         iter_data = api_keys
    #     else:
    #         iter_data = [v for v in self.ksqldb_clusters.values()]
    #     for item in iter_data:
    #         sa_details = ccloud_sa.sa[item.owner_id]
    #         print(
    #             "{:<20} {:<25} {:<25} {:<20} {:<20} {:<50}".format(
    #                 item.api_key,
    #                 item.cluster_id,
    #                 item.created_at,
    #                 item.owner_id,
    #                 sa_details.name,
    #                 item.api_key_description,
    #             )
    #         )

    # def delete_keys_from_cache(self, sa_name) -> int:
    #     count = 0
    #     for item in self.api_keys.values():
    #         if sa_name == item.owner_id:
    #             self.api_keys.pop(item.api_key, None)
    #             count += 1
    #     return count

    # def __delete_key_from_cache(self, key_id: str) -> int:
    #     self.api_keys.pop(key_id, None)

    # def create_api_key(self, env_id: str, cluster_id: str, sa_id: str, sa_name: str, description: str = None):
    #     self.__confluent_cli_set_env(env_id)
    #     self.__confluent_cli_set_cluster(cluster_id)
    #     api_key_description = (
    #         "API Key for " + sa_name + " created by CI/CD framework." if not description else description
    #     )
    #     cmd_create_api_key = (
    #         "confluent api-key create -o json --service-account "
    #         + sa_id
    #         + " --resource "
    #         + cluster_id
    #         + ' --description "'
    #         + api_key_description
    #         + '"'
    #         + self.__CMD_STDERR_TO_STDOUT
    #     )
    #     output = loads(self.__execute_subcommand(cmd_create_api_key))
    #     # TODO: Add exception handling for not being able to create the API Key.
    #     self.__add_to_cache(
    #         CCloudAPIKey(
    #             api_key=output["key"],
    #             api_secret=output["secret"],
    #             api_key_description=api_key_description,
    #             owner_id=sa_id,
    #             cluster_id=cluster_id,
    #             created_at=str(datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")),
    #         )
    #     )
    #     return (output, True)

    # def delete_api_key(self, api_key: str) -> bool:
    #     cmd_delete_api_key = "confluent api-key delete " + api_key
    #     output = self.__execute_subcommand(cmd_delete_api_key)
    #     if not output.startswith("Deleted API key "):
    #         raise Exception("Could not delete the API Key.")
    #     else:
    #         self.__delete_key_from_cache(api_key)
    #     return True
