import pprint
from dataclasses import dataclass, field
from time import sleep
from typing import Dict, List
from urllib import parse
import requests
from ccloud.connections import CCloudBase

pp = pprint.PrettyPrinter(indent=2)


@dataclass
class CCloudAPIKey:
    api_key: str
    api_secret: str
    api_key_description: str
    owner_id: str
    cluster_id: str
    created_at: str


@dataclass
class CCloudAPIKeyList(CCloudBase):
    # ccloud_sa: service_account.CCloudServiceAccountList
    api_keys: Dict[str, CCloudAPIKey] = field(default_factory=dict, init=False)

    # This init function will initiate the base object and then check CCloud
    # for all the active API Keys. All API Keys that are listed in CCloud are
    # the added to a cache.
    def __post_init__(self) -> None:
        super().__post_init__()
        self.url = self._ccloud_connection.get_endpoint_url(key=self._ccloud_connection.uri.api_keys)
        print("Gathering list of all API Key(s) for all Service Account(s) in CCloud.")
        self.read_all()

    # This method will help reading all the API Keys that are already provisioned.
    # Please note that the API Secrets cannot be read back again, so if you do not have
    # access to the secret , you will need to generate new api key/secret pair.
    def read_all(self, params={"page_size": 100}):
        resp = requests.get(url=self.url, auth=self.http_connection, params=params)
        if resp.status_code == 200:
            out_json = resp.json()
            if out_json is not None and out_json["data"] is not None:
                for item in out_json["data"]:
                    print("Found API Key " + item["id"] + " with owner " + item["spec"]["owner"]["id"])
                    self.__add_to_cache(
                        CCloudAPIKey(
                            api_key=item["id"],
                            api_secret=None,
                            api_key_description=item["spec"]["description"],
                            owner_id=item["spec"]["owner"]["id"],
                            cluster_id=item["spec"]["resource"]["id"],
                            created_at=item["metadata"]["created_at"],
                        )
                    )
            if "next" in out_json["metadata"]:
                query_params = parse.parse_qs(parse.urlsplit(out_json["metadata"]["next"]).query)
                params["page_token"] = str(query_params["page_token"][0])
                self.read_all(params)
        elif resp.status_code == 429:
            print(f"CCloud API Per-Minute Limit exceeded. Sleeping for 45 seconds. Error stack: {resp.text}")
            sleep(45)
            print("Timer up. Resuming CCloud API scrape.")
        else:
            raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)

    def __add_to_cache(self, api_key: CCloudAPIKey) -> None:
        self.api_keys[api_key.api_key] = api_key

    def find_keys_with_sa(self, sa_id: str) -> List[CCloudAPIKey]:
        output = []
        for item in self.api_keys.values():
            if sa_id == item.owner_id:
                output.append(item)
        return output

    def find_sa_count_for_clusters(self, cluster_id: str) -> Dict[str, int]:
        out = {}
        for item in self.api_keys.values():
            if item.cluster_id == cluster_id:
                count = out.get(item.owner_id, int(0))
                out[item.owner_id] = count + 1
        return out

    def find_keys_with_sa_and_cluster(self, sa_id: str, cluster_id: str) -> List[CCloudAPIKey]:
        output = []
        for item in self.api_keys.values():
            if cluster_id == item.cluster_id and sa_id == item.owner_id:
                output.append(item)
        return output

    # def print_api_keys(self, ccloud_sa: service_account.CCloudServiceAccountList, api_keys: List[CCloudAPIKey] = None):
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
    #         iter_data = [v for v in self.api_keys.values()]
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
