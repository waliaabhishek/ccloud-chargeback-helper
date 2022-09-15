from dataclasses import dataclass, field
from typing import Dict
from urllib import parse

import requests

from ccloud.connections import CCloudBase


@dataclass
class CCloudEnvironment:
    env_id: str
    display_name: str
    created_at: str


@dataclass(kw_only=True)
class CCloudEnvironmentList(CCloudBase):
    env: Dict[str, CCloudEnvironment] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.url = self._ccloud_connection.get_endpoint_url(key=self._ccloud_connection.uri.environments)
        self.read_all()

    def __str__(self):
        print("Found " + str(len(self.env)) + " environments.")
        for v in self.env.values():
            print("{:<15} {:<40}".format(v.env_id, v.display_name))

    def read_all(self, params={"page_size": 50}):
        resp = requests.get(url=self.url, auth=self.http_connection, params=params)
        if resp.status_code == 200:
            out_json = resp.json()
            if out_json is not None and out_json["data"] is not None:
                for item in out_json["data"]:
                    print("Found environment " + item["id"] + " with name " + item["display_name"])
                    self.__add_env_to_cache(
                        CCloudEnvironment(
                            env_id=item["id"],
                            display_name=item["display_name"],
                            created_at=item["metadata"]["created_at"],
                        )
                    )
            if "next" in out_json["metadata"]:
                query_params = parse.parse_qs(parse.urlsplit(out_json["metadata"]["next"]).query)
                params["page_token"] = str(query_params["page_token"][0])
                self.read_all(params)
        else:
            raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)

    def __add_env_to_cache(self, ccloud_env: CCloudEnvironment) -> None:
        self.env[ccloud_env.env_id] = ccloud_env

    # Read/Find one Cluster from the cache
    def find_environment(self, env_id):
        return self.env[env_id]
