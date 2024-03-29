import datetime
import logging
from dataclasses import InitVar, dataclass, field
from typing import Dict

from dateutil import parser

from ccloud.connections import CCloudBase
from helpers import logged_method
from prometheus_processing.custom_collector import TimestampedCollector

LOGGER = logging.getLogger(__name__)


@dataclass
class CCloudEnvironment:
    env_id: str
    display_name: str
    created_at: str


env_prom_metrics = TimestampedCollector(
    "confluent_cloud_environment",
    "Environment Details for every Environment created within CCloud",
    ["env_id", "display_name"],
    in_begin_timestamp=datetime.datetime.now(),
)
# env_prom_status_metrics = TimestampedCollector(
#     "confluent_cloud_env_scrape_status",
#     "CCloud Environments scrape status",
#     in_begin_timestamp=datetime.datetime.now(),
# )


@dataclass(kw_only=True)
class CCloudEnvironmentList(CCloudBase):
    env: Dict[str, CCloudEnvironment] = field(default_factory=dict, init=False)
    exposed_timestamp: InitVar[datetime.datetime] = field(init=True)

    def __post_init__(self, exposed_timestamp: datetime.datetime) -> None:
        super().__post_init__()
        self.url = self.in_ccloud_connection.get_endpoint_url(key=self.in_ccloud_connection.uri.environments)
        LOGGER.debug(f"Environment List URL: {self.url}")
        self.read_all()
        LOGGER.debug("Exposing Prometheus Metrics for Environment List")
        self.expose_prometheus_metrics(exposed_timestamp=exposed_timestamp)
        LOGGER.info("CCloud Environment List initialized successfully")

    @logged_method
    def expose_prometheus_metrics(self, exposed_timestamp: datetime.datetime):
        LOGGER.debug("Exposing Prometheus Metrics for Environment List for timestamp: " + str(exposed_timestamp))
        self.force_clear_prom_metrics()
        env_prom_metrics.set_timestamp(curr_timestamp=exposed_timestamp)
        for _, v in self.env.items():
            if v.created_at >= exposed_timestamp:
                env_prom_metrics.labels(v.env_id, v.display_name).set(1)
        # env_prom_status_metrics.set_timestamp(curr_timestamp=exposed_timestamp).set(1)

    @logged_method
    def force_clear_prom_metrics(self):
        env_prom_metrics.clear()

    def __str__(self):
        LOGGER.debug("Found " + str(len(self.env)) + " environments.")
        for v in self.env.values():
            print("{:<15} {:<40}".format(v.env_id, v.display_name))

    @logged_method
    def read_all(self, params={"page_size": 100}):
        LOGGER.debug("Reading all Environment List from Confluent Cloud")
        for item in self.read_from_api(params=params):
            self.__add_env_to_cache(
                CCloudEnvironment(
                    env_id=item["id"],
                    display_name=item["display_name"],
                    created_at=parser.isoparse(item["metadata"]["created_at"]),
                )
            )
            LOGGER.debug("Found environment " + item["id"] + " with name " + item["display_name"])
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

    @logged_method
    def __add_env_to_cache(self, ccloud_env: CCloudEnvironment) -> None:
        self.env[ccloud_env.env_id] = ccloud_env

    # Read/Find one Cluster from the cache
    @logged_method
    def find_environment(self, env_id):
        return self.env[env_id]
