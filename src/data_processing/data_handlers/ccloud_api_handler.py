import datetime
from dataclasses import dataclass, field
from typing import List, Tuple

from ccloud.ccloud_api.api_keys import CCloudAPIKeyList
from ccloud.ccloud_api.clusters import CCloudClusterList
from ccloud.ccloud_api.connectors import CCloudConnectorList
from ccloud.ccloud_api.environments import CCloudEnvironmentList
from ccloud.ccloud_api.ksqldb_clusters import CCloudKsqldbClusterList
from ccloud.ccloud_api.service_accounts import CCloudServiceAccountList
from ccloud.ccloud_api.user_accounts import CCloudUserAccountList
from ccloud.connections import CCloudBase
from data_processing.data_handlers.types import AbstractDataHandler
from helpers import LOGGER


@dataclass
class CCloudObjectsHandler(AbstractDataHandler, CCloudBase):
    last_refresh: datetime.datetime | None = field(init=False, default=None)
    min_refresh_gap: datetime.timedelta = field(init=False, default=datetime.timedelta(minutes=30))
    cc_sa: CCloudServiceAccountList = field(init=False)
    cc_users: CCloudUserAccountList = field(init=False)
    cc_api_keys: CCloudAPIKeyList = field(init=False)
    cc_environments: CCloudEnvironmentList = field(init=False)
    cc_clusters: CCloudClusterList = field(init=False)
    cc_connectors: CCloudConnectorList = field(init=False)
    cc_ksqldb_clusters: CCloudKsqldbClusterList = field(init=False)

    def __post_init__(self) -> None:
        LOGGER.debug(f"Initializing CCloudObjectsHandler")
        # Initialize the super classes to set the internal attributes
        AbstractDataHandler.__init__(self, start_date=self.start_date)
        CCloudBase.__post_init__(self)
        self.last_refresh = datetime.datetime.now() - self.min_refresh_gap
        effective_dates = self.calculate_effective_dates(
            last_available_date=self.start_date, days_per_query=1, max_days_in_memory=1
        )
        # self.read_all(exposed_timestamp=effective_dates.curr_end_date)
        self.read_next_dataset(exposed_timestamp=effective_dates.curr_end_date)
        LOGGER.debug(f"Finished Initializing CCloudObjectsHandler")

    def read_all(self, exposed_timestamp: datetime.datetime = None):
        if self.min_refresh_gap > datetime.datetime.now() - self.last_refresh:
            # TODO: Add Refresh gap as a configurable value in YAML file
            LOGGER.info(f"Not refreshing the CCloud Object state  -- TimeDelta is not enough. {self.min_refresh_gap}")
        else:
            LOGGER.info(f"Starting CCloud Object refresh now -- {datetime.datetime.now()}")
            LOGGER.info(f"Refreshing CCloud Service Accounts")
            self.cc_sa = CCloudServiceAccountList(
                in_ccloud_connection=self.in_ccloud_connection,
                exposed_timestamp=exposed_timestamp,
            )
            LOGGER.info(f"Refreshing CCloud User Accounts")
            self.cc_users = CCloudUserAccountList(
                in_ccloud_connection=self.in_ccloud_connection,
                exposed_timestamp=exposed_timestamp,
            )
            LOGGER.info(f"Refreshing CCloud API Keys")
            self.cc_api_keys = CCloudAPIKeyList(
                in_ccloud_connection=self.in_ccloud_connection,
                exposed_timestamp=exposed_timestamp,
            )
            LOGGER.info(f"Refreshing CCloud Environments")
            self.cc_environments = CCloudEnvironmentList(
                in_ccloud_connection=self.in_ccloud_connection,
                exposed_timestamp=exposed_timestamp,
            )
            LOGGER.info(f"Refreshing CCloud Kafka Clusters")
            self.cc_clusters = CCloudClusterList(
                in_ccloud_connection=self.in_ccloud_connection,
                ccloud_envs=self.cc_environments,
                exposed_timestamp=exposed_timestamp,
            )
            LOGGER.info(f"Refreshing CCloud Connectors")
            self.cc_connectors = CCloudConnectorList(
                in_ccloud_connection=self.in_ccloud_connection,
                ccloud_kafka_clusters=self.cc_clusters,
                ccloud_service_accounts=self.cc_sa,
                ccloud_users=self.cc_users,
                ccloud_api_keys=self.cc_api_keys,
                exposed_timestamp=exposed_timestamp,
            )
            LOGGER.info(f"Refreshing CCloud KSQLDB Clusters")
            self.cc_ksqldb_clusters = CCloudKsqldbClusterList(
                in_ccloud_connection=self.in_ccloud_connection,
                ccloud_envs=self.cc_environments,
                exposed_timestamp=exposed_timestamp,
            )
            self.last_refresh = datetime.datetime.now()
            LOGGER.info(f"Finished CCloud Object refresh -- {self.last_refresh}")

    def read_next_dataset(self, exposed_timestamp: datetime.datetime):
        self.read_all(exposed_timestamp=exposed_timestamp)
        print(f"Currently reading the Objects dataset for Timestamp: {exposed_timestamp}")
        self.cc_sa.expose_prometheus_metrics(exposed_timestamp=exposed_timestamp)
        self.cc_users.expose_prometheus_metrics(exposed_timestamp=exposed_timestamp)
        self.cc_api_keys.expose_prometheus_metrics(exposed_timestamp=exposed_timestamp)
        self.cc_environments.expose_prometheus_metrics(exposed_timestamp=exposed_timestamp)
        self.cc_clusters.expose_prometheus_metrics(exposed_timestamp=exposed_timestamp)
        self.cc_connectors.expose_prometheus_metrics(exposed_timestamp=exposed_timestamp)
        self.cc_ksqldb_clusters.expose_prometheus_metrics(exposed_timestamp=exposed_timestamp)

    def force_clear_prom_metrics(self):
        self.cc_sa.force_clear_prom_metrics()
        self.cc_users.force_clear_prom_metrics()
        self.cc_api_keys.force_clear_prom_metrics()
        self.cc_environments.force_clear_prom_metrics()
        self.cc_clusters.force_clear_prom_metrics()
        self.cc_connectors.force_clear_prom_metrics()
        self.cc_ksqldb_clusters.force_clear_prom_metrics()

    def get_dataset_for_timerange(self, start_datetime: datetime.datetime, end_datetime: datetime.datetime, **kwargs):
        # TODO: Do we want to narrow down the active dataset for the timelines ?
        pass

    def get_connected_kafka_cluster_id(self, env_id: str, resource_id: str) -> Tuple[List[str], str]:
        cluster_list = []
        error_string = None
        LOGGER.debug(f"Getting connected Kafka cluster(s) for resource_id: {resource_id} in env_id: {env_id}")
        if resource_id.startswith("lcc"):
            if resource_id in self.cc_connectors.connectors.keys():
                cluster_list.append(self.cc_connectors.connectors[resource_id].cluster_id)
                error_string = None
            else:
                cluster_list.append("unknown")
                error_string = "no_data_in_api"
        elif resource_id.startswith("lksql"):
            if resource_id in self.cc_ksqldb_clusters.ksqldb_clusters.keys():
                cluster_list.append(self.cc_ksqldb_clusters.ksqldb_clusters[resource_id].kafka_cluster_id)
                error_string = None
            else:
                cluster_list.append("unknown")
                error_string = "no_data_in_api"
        elif resource_id.startswith("lkc"):
            cluster_list.append(resource_id)
            error_string = None
        elif resource_id.startswith("lsr"):
            temp_cluster_list = [x.cluster_id for x in self.cc_clusters.clusters.values() if x.env_id == env_id]
            if len(temp_cluster_list) > 0:
                cluster_list += temp_cluster_list
                error_string = None
            else:
                cluster_list.append(None)
                error_string = "no_cluster_in_env"
        else:
            cluster_list.append("unknown")
            error_string = "unknown_resource_type"
        LOGGER.debug(
            f"Found cluster_list: {cluster_list} and error_string: {error_string} for resource_id: {resource_id} in env_id: {env_id}"
        )
        return (cluster_list, error_string)
