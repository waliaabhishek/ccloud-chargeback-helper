import logging
import os
import pprint
import timeit
from functools import wraps
from os import environ

ENV_PREFIX = "env::"
pretty = pprint.PrettyPrinter(indent=2)

# logging.basicConfig(level=logging.INFO, format="{asctime} {name:25s} {levelname:8s} {message}", style="{")
LOG_LEVEL = logging.INFO


def set_logger_level(log_level: int):
    global LOG_LEVEL
    LOG_LEVEL = log_level
    logging.basicConfig(level=LOG_LEVEL, format="{asctime} {name:25s} {levelname:8s} {message}", style="{", force=True)


LOGGER = logging.getLogger(__name__)
METHOD_BREADCRUMBS = False


def set_breadcrumb_flag(breadcrumbs: bool):
    global METHOD_BREADCRUMBS
    METHOD_BREADCRUMBS = breadcrumbs


def logged_method(func):
    """
    This Method allows you to add breadcrumbs to logging for the method.
    :rtype: object
    """

    @wraps(func)
    def add_entry_exit_logs(*args, **kwargs):
        if METHOD_BREADCRUMBS:
            LOGGER.info(f"Begin method execution:\t\t{str(func.__name__)}")
        ret = func(*args, **kwargs)
        if METHOD_BREADCRUMBS:
            LOGGER.info(f"End method execution:\t\t{str(func.__name__)}")
        return ret

    return add_entry_exit_logs


@logged_method
def get_env_var(var_name: str):
    if environ.get(var_name.strip()) is None:
        raise Exception(f"Cannot find environment variable {var_name}")
    else:
        return environ[var_name]


@logged_method
def find_replace_env_vars(input: str, env_prefix=ENV_PREFIX):
    if input.startswith(env_prefix):
        input = input.split(env_prefix)[1]
        return get_env_var(input)
    else:
        return input


def env_parse_replace(input):
    if isinstance(input, dict):
        for k, v in input.items():
            if isinstance(v, dict) or isinstance(v, list):
                env_parse_replace(v)
            elif isinstance(v, str):
                input[k] = find_replace_env_vars(v)
    elif isinstance(input, list):
        for k, v in enumerate(input):
            if isinstance(v, dict) or isinstance(v, list):
                env_parse_replace(v)
            elif isinstance(v, str):
                input[k] = find_replace_env_vars(v)


@logged_method
def ensure_path(path: str):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Directory Created: {path}")
    # else:
    #     print(f"Path already present: {path}")


@logged_method
def sanitize_id(input: str) -> str:
    return input.strip().replace(" ", "_").lower()


@logged_method
def sanitize_metric_name(input: str) -> str:
    return input.strip().replace("/", "_").replace(" ", "_").replace(".", "_")


@logged_method
def mandatory_check(key, value):
    if not value:
        raise Exception(key + " is a mandatory attribute. Please populate to ensure correct functionality.")


@logged_method
def check_pair(key1Name, key1Value, key2Name, key2Value):
    if (key1Value and not key2Value) or (not key1Value and key2Value) or (not key1Value and not key2Value):
        raise Exception("Both " + key1Name + " & " + key2Name + " must be present in the configuration.")
    return


def printline():
    print("=" * 80)


def timed_method(func):
    @wraps(func)
    def add_timer(*args, **kwargs):
        start = timeit.default_timer()
        ret = func(*args, **kwargs)
        stop = timeit.default_timer()
        LOGGER.debug(f"Time to execute method:\t\t{func.__name__}:\t\t\t", stop - start)
        return ret

    return add_timer


# BILLING_METRICS_SCOPE = {
#     "request_bytes": sanitize_metric_name("io.confluent.kafka.server/request_bytes"),
#     "response_bytes": sanitize_metric_name("io.confluent.kafka.server/response_bytes"),
# }


if __name__ == "__main__":
    test = ["env:safdsaf", "regular", ENV_PREFIX + "CONFLUENT_CLOUD_EMAIL"]

    for item in test:
        print(find_replace_env_vars(item))
