import logging

import pandas as pd
from data_processing.data_handlers.types import AbstractDataHandler

from helpers import logged_method


LOGGER = logging.getLogger(__name__)


class MetricsAPIPrincipalDataColumnNames:
    timestamp = "Interval"
    query_type = "QueryType"
    cluster_id = "KafkaClusterID"
    principal_id = "PrincipalID"
    value = "Value"


METRICS_API_COLUMNS = MetricsAPIPrincipalDataColumnNames()


@logged_method
def add_resp_to_dataset(query_name: str, response: dict, dataset: pd.DataFrame):
    """Add the response to the dataset

    Args:
        query_name (str): Name of the query
        response (dict): Response from the API
    """
    for item in response["data"]["result"]:
        temp_data = [
            {
                METRICS_API_COLUMNS.timestamp: pd.to_datetime(in_item[0], unit="s", utc=True),
                METRICS_API_COLUMNS.query_type: query_name,
                METRICS_API_COLUMNS.cluster_id: item["metric"]["kafka_id"],
                METRICS_API_COLUMNS.principal_id: item["metric"]["principal_id"],
                METRICS_API_COLUMNS.value: in_item[1],
            }
            for in_item in item["values"]
        ]
        if temp_data:
            if dataset is not None:
                dataset = pd.concat(
                    [
                        dataset,
                        pd.DataFrame.from_records(
                            temp_data,
                            index=[
                                METRICS_API_COLUMNS.timestamp,
                                METRICS_API_COLUMNS.query_type,
                                METRICS_API_COLUMNS.cluster_id,
                                METRICS_API_COLUMNS.principal_id,
                            ],
                        ),
                    ]
                )
            else:
                dataset = pd.DataFrame.from_records(
                    temp_data,
                    index=[
                        METRICS_API_COLUMNS.timestamp,
                        METRICS_API_COLUMNS.query_type,
                        METRICS_API_COLUMNS.cluster_id,
                        METRICS_API_COLUMNS.principal_id,
                    ],
                )


@logged_method
def get_dataset_for_time_slice(dataset: pd.DataFrame, is_none: bool, **kwargs):
    """Wrapper over the internal method so that cross-imports are not necessary

    Args:
        time_slice (pd.Timestamp): Time slice to be used for fetching the data from datafame for the exact timestamp

    Returns:
        pd.DataFrame: Returns a pandas Dataframe with the filtered data.
    """
    if is_none:
        return pd.DataFrame(
            {},
            index=[
                METRICS_API_COLUMNS.timestamp,
                METRICS_API_COLUMNS.query_type,
                METRICS_API_COLUMNS.cluster_id,
                METRICS_API_COLUMNS.principal_id,
            ],
        )
    else:
        return dataset
