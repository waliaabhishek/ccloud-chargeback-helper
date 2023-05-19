from dataclasses import dataclass, field
from time import sleep, time
from typing import Dict
from urllib import parse
import datetime
import requests
from dateutil import parser
from ccloud.connections import CCloudBase
from prometheus_processing.custom_collector import TimestampedCollector


@dataclass
class CCloudEnvironment:
    env_id: str
    display_name: str
    created_at: str


env_prom_metrics = TimestampedCollector(
    "confluent_cloud_environment",
    "Environment Details for every Environment created within CCloud",
    ["env_id", "created_at"],
    in_begin_timestamp=datetime.datetime.now(),
)


@dataclass(kw_only=True)
class CCloudEnvironmentList(CCloudBase):
    env: Dict[str, CCloudEnvironment] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.url = self.in_ccloud_connection.get_endpoint_url(key=self.in_ccloud_connection.uri.environments)
        self.read_all()
        self.expose_prometheus_metrics()

    def expose_prometheus_metrics(self):
        for _, v in self.env.items():
            env_prom_metrics.labels(v.env_id, v.created_at).set(1)

    def __str__(self):
        print("Found " + str(len(self.env)) + " environments.")
        for v in self.env.values():
            print("{:<15} {:<40}".format(v.env_id, v.display_name))

    def read_all(self, params={"page_size": 100}):
        for item in self.read_from_api(params=params):
            self.__add_env_to_cache(
                CCloudEnvironment(
                    env_id=item["id"],
                    display_name=item["display_name"],
                    created_at=parser.isoparse(item["metadata"]["created_at"]),
                )
            )
            print("Found environment " + item["id"] + " with name " + item["display_name"])
        # resp = requests.get(url=self.url, auth=self.http_connection, params=params)
        # if resp.status_code == 200:
        #     out_json = resp.json()
        #     if out_json is not None and out_json["data"] is not None:
        #         for item in out_json["data"]:
        #             self.__add_env_to_cache(
        #                 CCloudEnvironment(
        #                     env_id=item["id"],
        #                     display_name=item["display_name"],
        #                     created_at=parser.isoparse(item["metadata"]["created_at"]),
        #                 )
        #             )
        #             print("Found environment " + item["id"] + " with name " + item["display_name"])
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

    def __add_env_to_cache(self, ccloud_env: CCloudEnvironment) -> None:
        self.env[ccloud_env.env_id] = ccloud_env

    # Read/Find one Cluster from the cache
    def find_environment(self, env_id):
        return self.env[env_id]
