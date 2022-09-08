from dataclasses import dataclass, field
from typing import Dict, Tuple
from urllib import parse

import requests

from ccloud.connections import CCloudBase


@dataclass
class CCloudUserAccount:
    resource_id: str
    name: str
    created_at: str
    updated_at: str
    is_ignored: bool


@dataclass(kw_only=True)
class CCloudUserAccountList(CCloudBase):
    users: Dict[str, CCloudUserAccount] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.url = self._ccloud_connection.get_endpoint_url(key=self._ccloud_connection.uri.user_accounts)
        self.read_all()

    def __str__(self) -> str:
        for item in self.users.values():
            print("{:<15} {:<40} {:<50}".format(item.resource_id, item.name, item.description))

    # Read ALL Service Account details from Confluent Cloud
    def read_all(self, params={"page_size": 50}):
        resp = requests.get(url=self.url, auth=self.http_connection, params=params)
        if resp.status_code == 200:
            out_json = resp.json()
            for item in out_json["data"]:
                self.__add_to_cache(
                    CCloudUserAccount(
                        resource_id=item["id"],
                        name=item["full_name"],
                        created_at=item["metadata"]["created_at"],
                        updated_at=item["metadata"]["updated_at"],
                    )
                )
                print(f"Found User: {item['id']}; Name {item['display_name']}")
            if "next" in out_json["metadata"]:
                query_params = parse.parse_qs(parse.urlsplit(out_json["metadata"]["next"]).query)
                params["page_token"] = str(query_params["page_token"][0])
                self.read_all(params)
        else:
            raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)

    def __add_to_cache(self, ccloud_user: CCloudUserAccount) -> None:
        self.users[ccloud_user.resource_id] = ccloud_user

    # Read/Find one SA from the cache
    def find_user(self, ccloud_user):
        for item in self.users.values():
            if ccloud_user == item.name:
                return item
        return None

    # def __delete_from_cache(self, res_id):
    #     self.users.pop(res_id, None)

    # Create/Find one SA and add it to the cache, so that we do not have to refresh the cache manually
    # def create_user(self, ccloud_user) -> Tuple[CCloudUserAccount, bool]:
    #     temp = self.find_user(ccloud_user)
    #     if temp:
    #         return temp, False
    #     # print("Creating a new Service Account with name: " + sa_name)
    #     payload = {
    #         "display_name": ccloud_user,
    #     }
    #     resp = requests.post(
    #         url=self.url,
    #         auth=self.http_connection,
    #         json=payload,
    #     )
    #     if resp.status_code == 201:
    #         user_details = resp.json()
    #         user_value = CCloudUserAccount(
    #             resource_id=user_details["id"],
    #             name=user_details["full_name"],
    #             created_at=user_details["metadata"]["created_at"],
    #             updated_at=user_details["metadata"]["updated_at"],
    #             is_ignored=False,
    #         )
    #         self.__add_to_cache(ccloud_user=user_value)
    #         return (user_value, True)
    #     else:
    #         raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)

    # def delete_sa(self, ccloud_user) -> bool:
    #     temp = self.find_user(ccloud_user)
    #     if not temp:
    #         print("Did not find CCloud user with name '" + ccloud_user + "'. Not deleting anything.")
    #         return False
    #     else:
    #         resp = requests.delete(url=str(self.url + "/" + temp.resource_id), auth=self.http_connection)
    #         if resp.status_code == 204:
    #             self.__delete_from_cache(temp.resource_id)
    #             return True
    #         else:
    #             raise Exception("Could not perform the DELETE operation. Please check your settings. " + resp.text)
