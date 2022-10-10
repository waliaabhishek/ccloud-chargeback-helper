from dataclasses import dataclass, field
from typing import Dict
from urllib import parse

import requests

from ccloud.connections import CCloudBase


@dataclass
class CCloudServiceAccount:
    resource_id: str
    name: str
    description: str
    created_at: str
    updated_at: str


@dataclass(kw_only=True)
class CCloudServiceAccountList(CCloudBase):
    sa: Dict[str, CCloudServiceAccount] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.url = self._ccloud_connection.get_endpoint_url(key=self._ccloud_connection.uri.service_accounts)
        self.read_all()

    def __str__(self) -> str:
        for item in self.sa.values():
            print("{:<15} {:<40} {:<50}".format(item.resource_id, item.name, item.description))

    # Read ALL Service Account details from Confluent Cloud
    def read_all(self, params={"page_size": 100}):
        resp = requests.get(url=self.url, auth=self.http_connection, params=params)
        if resp.status_code == 200:
            out_json = resp.json()
            if out_json is not None and out_json["data"] is not None:
                for item in out_json["data"]:
                    self.__add_to_cache(
                        CCloudServiceAccount(
                            resource_id=item["id"],
                            name=item["display_name"],
                            description=item["description"],
                            created_at=item["metadata"]["created_at"],
                            updated_at=item["metadata"]["updated_at"],
                        )
                    )
                    print(f"Found SA: {item['id']}; Name {item['display_name']}")
            if "next" in out_json["metadata"]:
                query_params = parse.parse_qs(parse.urlsplit(out_json["metadata"]["next"]).query)
                params["page_token"] = str(query_params["page_token"][0])
                self.read_all(params)
        else:
            raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)

    def __add_to_cache(self, ccloud_sa: CCloudServiceAccount) -> None:
        self.sa[ccloud_sa.resource_id] = ccloud_sa

    # Read/Find one SA from the cache
    def find_sa(self, sa_name):
        for item in self.sa.values():
            if sa_name == item.name:
                return item
        return None

    # def __delete_from_cache(self, res_id):
    #     self.sa.pop(res_id, None)

    # Create/Find one SA and add it to the cache, so that we do not have to refresh the cache manually
    # def create_sa(self, sa_name, description=None) -> Tuple[CCloudServiceAccount, bool]:
    #     temp = self.find_sa(sa_name)
    #     if temp:
    #         return temp, False
    #     # print("Creating a new Service Account with name: " + sa_name)
    #     payload = {
    #         "display_name": sa_name,
    #         "description": str("Account for " + sa_name + " created by CI/CD framework")
    #         if not description
    #         else description,
    #     }
    #     resp = requests.post(
    #         url=self.url,
    #         auth=self.http_connection,
    #         json=payload,
    #     )
    #     if resp.status_code == 201:
    #         sa_details = resp.json()
    #         sa_value = CCloudServiceAccount(
    #             resource_id=sa_details["id"],
    #             name=sa_details["display_name"],
    #             description=sa_details["description"],
    #             created_at=sa_details["metadata"]["created_at"],
    #             updated_at=sa_details["metadata"]["updated_at"],
    #             is_ignored=False,
    #         )
    #         self.__add_to_cache(ccloud_sa=sa_value)
    #         return (sa_value, True)
    #     else:
    #         raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)

    # def delete_sa(self, sa_name) -> bool:
    #     temp = self.find_sa(sa_name)
    #     if not temp:
    #         print("Did not find Service Account with name '" + sa_name + "'. Not deleting anything.")
    #         return False
    #     else:
    #         resp = requests.delete(url=str(self.url + "/" + temp.resource_id), auth=self.http_connection)
    #         if resp.status_code == 204:
    #             self.__delete_from_cache(temp.resource_id)
    #             return True
    #         else:
    #             raise Exception("Could not perform the DELETE operation. Please check your settings. " + resp.text)

    # def __try_detect_internal_service_accounts(self, sa_name: str) -> bool:
    #     if sa_name.startswith(("Connect.lcc-", "KSQL.lksqlc-")):
    #         return True
    #     else:
    #         return False
