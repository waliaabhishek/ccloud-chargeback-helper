import datetime
import logging
import pprint
from dataclasses import InitVar, dataclass, field
from typing import Dict, List

from dateutil import parser

from ccloud.connections import CCloudBase
from helpers import logged_method
from prometheus_processing.custom_collector import TimestampedCollector

pp = pprint.PrettyPrinter(indent=2)
LOGGER = logging.getLogger(__name__)


@dataclass
class CCloudAPIKey:
    api_key: str
    api_secret: str
    api_key_description: str
    owner_id: str
    cluster_id: str
    created_at: datetime.datetime


api_key_prom_metrics = TimestampedCollector(
    "confluent_cloud_api_key",
    "API Key details for every API Key created within CCloud",
    ["api_key", "owner_id", "resource_id"],
    in_begin_timestamp=datetime.datetime.now(),
)
# api_key_prom_status_metrics = TimestampedCollector(
#     "confluent_cloud_api_key_scrape_status",
#     "CCloud API Keys scrape status",
#     in_begin_timestamp=datetime.datetime.now(),
# )


@dataclass
class CCloudAPIKeyList(CCloudBase):
    exposed_timestamp: InitVar[datetime.datetime] = field(init=True)

    # ccloud_sa: service_account.CCloudServiceAccountList
    api_keys: Dict[str, CCloudAPIKey] = field(default_factory=dict, init=False)

    # This init function will initiate the base object and then check CCloud
    # for all the active API Keys. All API Keys that are listed in CCloud are
    # the added to a cache.
    @logged_method
    def __post_init__(self, exposed_timestamp: datetime.datetime) -> None:
        super().__post_init__()
        self.url = self.in_ccloud_connection.get_endpoint_url(key=self.in_ccloud_connection.uri.api_keys)
        LOGGER.debug(f"API Keys URL: {self.url}")
        self.read_all()
        LOGGER.debug("Exposing Prometheus Metrics for API Keys")
        self.expose_prometheus_metrics(exposed_timestamp=exposed_timestamp)
        LOGGER.info("CCloud API Keys initialized successfully")

    def expose_prometheus_metrics(self, exposed_timestamp: datetime.datetime):
        LOGGER.debug("Exposing Prometheus Metrics for API Keys for timestamp: " + str(exposed_timestamp))
        self.force_clear_prom_metrics()
        api_key_prom_metrics.set_timestamp(curr_timestamp=exposed_timestamp)
        for _, v in self.api_keys.items():
            if v.created_at >= exposed_timestamp:
                api_key_prom_metrics.labels(v.api_key, v.owner_id, v.cluster_id).set(1)
        # api_key_prom_status_metrics.set_timestamp(curr_timestamp=exposed_timestamp).set(1)

    @logged_method
    def force_clear_prom_metrics(self):
        api_key_prom_metrics.clear()

    # This method will help reading all the API Keys that are already provisioned.
    # Please note that the API Secrets cannot be read back again, so if you do not have
    # access to the secret , you will need to generate new api key/secret pair.
    @logged_method
    def read_all(self, params={"page_size": 100}):
        LOGGER.debug("Reading all API Keys from Confluent Cloud")
        for item in self.read_from_api(params=params):
            self.__add_to_cache(
                CCloudAPIKey(
                    api_key=item["id"],
                    api_secret=None,
                    api_key_description=item["spec"]["description"],
                    owner_id=item["spec"]["owner"]["id"],
                    cluster_id=item["spec"]["resource"]["id"],
                    created_at=parser.isoparse(item["metadata"]["created_at"]),
                )
            )
            LOGGER.debug("Found API Key " + item["id"] + " with owner " + item["spec"]["owner"]["id"])

        # resp = requests.get(url=self.url, auth=self.http_connection, params=params)
        # if resp.status_code == 200:
        #     out_json = resp.json()
        #     if out_json is not None and out_json["data"] is not None:
        #         for item in out_json["data"]:
        #             print("Found API Key " + item["id"] + " with owner " + item["spec"]["owner"]["id"])
        #             self.__add_to_cache(
        #                 CCloudAPIKey(
        #                     api_key=item["id"],
        #                     api_secret=None,
        #                     api_key_description=item["spec"]["description"],
        #                     owner_id=item["spec"]["owner"]["id"],
        #                     cluster_id=item["spec"]["resource"]["id"],
        #                     created_at=parser.isoparse(item["metadata"]["created_at"]),
        #                 )
        #             )
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
    def __add_to_cache(self, api_key: CCloudAPIKey) -> None:
        self.api_keys[api_key.api_key] = api_key

    @logged_method
    def find_keys_with_sa(self, sa_id: str) -> List[CCloudAPIKey]:
        output = []
        for item in self.api_keys.values():
            if sa_id == item.owner_id:
                output.append(item)
        return output

    @logged_method
    def find_sa_count_for_clusters(self, cluster_id: str) -> Dict[str, int]:
        out = {}
        for item in self.api_keys.values():
            if item.cluster_id == cluster_id:
                count = out.get(item.owner_id, int(0))
                out[item.owner_id] = count + 1
        return out

    @logged_method
    def find_keys_with_sa_and_cluster(self, sa_id: str, cluster_id: str) -> List[CCloudAPIKey]:
        output = []
        for item in self.api_keys.values():
            if cluster_id == item.cluster_id and sa_id == item.owner_id:
                output.append(item)
        return output
