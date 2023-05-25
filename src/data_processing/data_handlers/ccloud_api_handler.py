import datetime
from dataclasses import dataclass, field

from ccloud.ccloud_api.api_keys import CCloudAPIKeyList
from ccloud.ccloud_api.clusters import CCloudClusterList
from ccloud.ccloud_api.connectors import CCloudConnectorList
from ccloud.ccloud_api.environments import CCloudEnvironmentList
from ccloud.ccloud_api.ksqldb_clusters import CCloudKsqldbClusterList
from ccloud.ccloud_api.service_accounts import CCloudServiceAccountList
from ccloud.ccloud_api.user_accounts import CCloudUserAccountList
from ccloud.connections import CCloudBase
from data_processing.data_handlers.types import AbstractDataHandler


@dataclass
class CCloudObjectsHandler(AbstractDataHandler, CCloudBase):

    last_refresh: datetime = field(init=False, default=None)
    min_refresh_gap: datetime.timedelta = field(init=False, default=datetime.timedelta(minutes=30))
    cc_sa: CCloudServiceAccountList = field(init=False)
    cc_users: CCloudUserAccountList = field(init=False)
    cc_api_keys: CCloudAPIKeyList = field(init=False)
    cc_environments: CCloudEnvironmentList = field(init=False)
    cc_clusters: CCloudClusterList = field(init=False)
    cc_connectors: CCloudConnectorList = field(init=False)
    cc_ksqldb_clusters: CCloudKsqldbClusterList = field(init=False)

    def __post_init__(self) -> None:
        # Initialize the super classes to set the internal attributes
        AbstractDataHandler.__init__(self)
        CCloudBase.__post_init__(self)
        self.last_refresh = datetime.datetime.now() - self.min_refresh_gap
        self.read_all()

    def read_all(self):
        if self.min_refresh_gap > datetime.datetime.now() - self.last_refresh:
            # TODO: Add Refresh gap as a configurable value in YAML file
            print(f"Not refreshing the CCloud Object state  -- TimeDelta is not enough. {self.min_refresh_gap}")
        else:
            print(f"Starting CCloud Object refresh now -- {datetime.datetime.now()}")
            self.cc_sa = CCloudServiceAccountList(in_ccloud_connection=self.in_ccloud_connection)
            self.cc_users = CCloudUserAccountList(in_ccloud_connection=self.in_ccloud_connection)
            self.cc_api_keys = CCloudAPIKeyList(in_ccloud_connection=self.in_ccloud_connection)
            self.cc_environments = CCloudEnvironmentList(in_ccloud_connection=self.in_ccloud_connection)
            self.cc_clusters = CCloudClusterList(
                in_ccloud_connection=self.in_ccloud_connection, ccloud_envs=self.cc_environments
            )
            self.cc_connectors = CCloudConnectorList(
                in_ccloud_connection=self.in_ccloud_connection,
                ccloud_kafka_clusters=self.cc_clusters,
                ccloud_service_accounts=self.cc_sa,
                ccloud_users=self.cc_users,
                ccloud_api_keys=self.cc_api_keys,
            )
            self.cc_ksqldb_clusters = CCloudKsqldbClusterList(
                in_ccloud_connection=self.in_ccloud_connection, ccloud_envs=self.cc_environments,
            )
            self.last_refresh = datetime.datetime.now()
            print(f"Finished CCloud Object refresh -- {self.last_refresh}")

    def read_next_dataset(self):
        self.read_all()

    def get_dataset_for_timerange(self, start_datetime: datetime.datetime, end_datetime: datetime.datetime, **kwargs):
        # TODO: Do we want to narrow down the active dataset for the timelines ?
        pass