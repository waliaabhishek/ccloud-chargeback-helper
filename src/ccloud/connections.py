from dataclasses import InitVar, dataclass, field
from enum import Enum, auto

from requests.auth import HTTPBasicAuth


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
    list_connector_names = "/connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors"
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
    frozen=True, kw_only=True,
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
