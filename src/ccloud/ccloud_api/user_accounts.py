import datetime
from dataclasses import InitVar, dataclass, field
from typing import Dict

from dateutil import parser

from ccloud.connections import CCloudBase
from prometheus_processing.custom_collector import TimestampedCollector


@dataclass
class CCloudUserAccount:
    resource_id: str
    name: str
    created_at: str
    updated_at: str


users_prom_metrics = TimestampedCollector(
    "confluent_cloud_user",
    "Environment Details for every Environment created within CCloud",
    ["sa_id", "display_name"],
    in_begin_timestamp=datetime.datetime.now(),
)
# users_prom_status_metrics = TimestampedCollector(
#     "confluent_cloud_users_scrape_status",
#     "CCloud User Accounts scrape status",
#     in_begin_timestamp=datetime.datetime.now(),
# )


@dataclass(kw_only=True)
class CCloudUserAccountList(CCloudBase):
    exposed_timestamp: InitVar[datetime.datetime] = field(init=True)
    users: Dict[str, CCloudUserAccount] = field(default_factory=dict, init=False)

    def __post_init__(self, exposed_timestamp: datetime.datetime) -> None:
        super().__post_init__()
        self.url = self.in_ccloud_connection.get_endpoint_url(key=self.in_ccloud_connection.uri.user_accounts)
        self.read_all()
        self.expose_prometheus_metrics(exposed_timestamp=exposed_timestamp)

    def expose_prometheus_metrics(self, exposed_timestamp: datetime.datetime):
        users_prom_metrics.clear()
        users_prom_metrics.set_timestamp(curr_timestamp=exposed_timestamp)
        for _, v in self.users.items():
            if v.created_at >= exposed_timestamp:
                users_prom_metrics.labels(v.resource_id, v.name).set(1)
        # users_prom_status_metrics.set_timestamp(curr_timestamp=exposed_timestamp).set(1)

    def __str__(self) -> str:
        for item in self.users.values():
            print("{:<15} {:<40} {:<50}".format(item.resource_id, item.name, item.description))

    # Read ALL Service Account details from Confluent Cloud
    def read_all(self, params={"page_size": 100}):
        for item in self.read_from_api(params=params):
            self.__add_to_cache(
                CCloudUserAccount(
                    resource_id=item["id"],
                    name=item["full_name"],
                    created_at=parser.isoparse(item["metadata"]["created_at"]),
                    updated_at=parser.isoparse(item["metadata"]["updated_at"]),
                )
            )
            print(f"Found User: {item['id']}; Name {item['full_name']}")
        # resp = requests.get(url=self.url, auth=self.http_connection, params=params)
        # if resp.status_code == 200:
        #     out_json = resp.json()
        #     if out_json is not None and out_json["data"] is not None:
        #         for item in out_json["data"]:
        #             self.__add_to_cache(
        #                 CCloudUserAccount(
        #                     resource_id=item["id"],
        #                     name=item["full_name"],
        #                     created_at=parser.isoparse(item["metadata"]["created_at"]),
        #                     updated_at=parser.isoparse(item["metadata"]["updated_at"]),
        #                 )
        #             )
        #             print(f"Found User: {item['id']}; Name {item['full_name']}")
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
