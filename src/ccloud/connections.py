from dataclasses import InitVar, dataclass, field
from enum import Enum, auto
from time import sleep
from typing import Dict
from urllib import parse

import requests
from requests.auth import HTTPBasicAuth, HTTPDigestAuth

from helpers import LOGGER


class EndpointURL(Enum):
    API_URL = auto()
    TELEMETRY_URL = auto()


class URIDetails:
    API_URL = "https://api.confluent.cloud"
    environments = "/org/v2/environments"
    clusters = "/cmk/v2/clusters"
    service_accounts = "/iam/v2/service-accounts"
    user_accounts = "/iam/v2/users"
    api_keys = "/iam/v2/api-keys"
    list_connector_names = (
        "/connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors?expand=info,status,id"
    )
    get_connector_config = (
        "/connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/config"
    )
    list_ksql_clusters = "/ksqldbcm/v2/clusters"
    get_billing_costs = "/billing/v1/costs"

    TELEMETRY_URL = "https://api.telemetry.confluent.cloud"
    telemetry_query_metrics = "/v2/metrics/{dataset}/query"

    prometheus_query_range = "/api/v1/query_range"

    def override_column_names(self, key, value):
        object.__setattr__(self, key, value)


@dataclass(
    frozen=True,
    kw_only=True,
)
class CCloudConnection:
    in_api_key: InitVar[str] = None
    in_api_secret: InitVar[str] = None

    base_url: EndpointURL = field(default=EndpointURL.API_URL)
    uri: URIDetails = field(default=URIDetails(), init=False)
    http_connection: HTTPBasicAuth = field(init=False)

    def __post_init__(self, in_api_key, in_api_secret) -> None:
        object.__setattr__(self, "http_connection", HTTPBasicAuth(in_api_key, in_api_secret))

    def get_endpoint_url(self, key="/") -> str:
        if self.base_url is EndpointURL.API_URL:
            return self.uri.API_URL + key
        else:
            return self.uri.TELEMETRY_URL + key


@dataclass
class CCloudBase:
    in_ccloud_connection: CCloudConnection

    url: str = field(init=False)
    http_connection: HTTPBasicAuth = field(init=False)

    def __post_init__(self) -> None:
        self.http_connection = self.in_ccloud_connection.http_connection

    def override_auth_type_from_yaml(self, auth_dict: Dict):
        if auth_dict.get("enable_auth", False):
            if auth_dict.get("auth_type") == "HTTPBasicAuth":
                self.http_connection = HTTPBasicAuth(**auth_dict.get("auth_args"))
            elif auth_dict.get("auth_type") == "HTTPDigestAuth":
                self.http_connection = HTTPDigestAuth(**auth_dict.get("auth_args"))
            else:
                # Other AUTH Types are not implemented yet.
                self.http_connection = None
        else:
            self.http_connection = None

    def read_from_api(self, params={"page_size": 500}, **kwagrs):
        LOGGER.info(f"Reading from API: {self.url}")
        resp = requests.get(url=self.url, auth=self.http_connection, timeout=10, params=params)
        if resp.status_code == 200:
            LOGGER.debug("Received 200 OK from API")
            out_json = resp.json()
            if out_json is not None and out_json["data"] is not None:
                LOGGER.info(f"Found {len(out_json['data'])} items in API response.")
                for item in out_json["data"]:
                    yield item
            if "next" in out_json["metadata"] and out_json["metadata"]["next"]:
                query_params = parse.parse_qs(parse.urlsplit(out_json["metadata"]["next"]).query)
                params["page_token"] = str(query_params["page_token"][0])
                LOGGER.info(f"Found next page token: {params['page_token']}. Grabbing next page.")
                self.read_from_api(params)
        elif resp.status_code == 429:
            LOGGER.info(f"CCloud API Per-Minute Limit exceeded. Sleeping for 45 seconds. Error stack: {resp.text}")
            sleep(45)
            LOGGER.info("Timer up. Resuming CCloud API scrape.")
            self.read_from_api(params)
        else:
            LOGGER.error("Error stack: " + resp.text)
            raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)
