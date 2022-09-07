from dataclasses import dataclass, field
from enum import Enum, auto

from helpers import mandatory_check
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

    TELEMETRY_URL = "https://api.telemetry.confluent.cloud"
    telemetry_query_metrics = "/v2/metrics/{dataset}/query"


@dataclass(
    frozen=True,
    kw_only=True,
)
class CCloudConnection:
    api_key: str
    api_secret: str
    uri: URIDetails = field(default_factory=URIDetails, init=False)
    base_url: EndpointURL = field(default=EndpointURL.API_URL)
    http_connection: HTTPBasicAuth = field(init=False)

    def __post_init__(self) -> None:
        self.http_connection = HTTPBasicAuth(username=self.api_key, password=self.api_secret)
        self.api_key = None
        self.api_secret = None

    def get_endpoint_url(self, key="/") -> str:
        return self.uri[self.base_url.name] + key


@dataclass
class CCloudBase:
    _ccloud_connection: CCloudConnection
    url: str = field(init=False)
    http_connection: HTTPBasicAuth = field(init=False)

    def __post_init__(self) -> None:
        self.http_connection = self._ccloud_connection.http_connection
