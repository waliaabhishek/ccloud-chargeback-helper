import logging
from datetime import datetime

from flask import Flask

from helpers import logged_method

LOGGER = logging.getLogger(__name__)

internal_api = Flask(__name__)

READINESS_FLAG = False
CURRENT_EXPOSED_DATE: datetime = None


@logged_method
def set_readiness(readiness_flag: bool):
    global READINESS_FLAG
    READINESS_FLAG = readiness_flag


@internal_api.route("/is_ready", methods=["GET"])
def is_ready():
    global READINESS_FLAG
    return str(READINESS_FLAG)


def set_current_exposed_date(exposed_date: datetime):
    global CURRENT_EXPOSED_DATE
    CURRENT_EXPOSED_DATE = exposed_date


@internal_api.route("/current_exposed_date", methods=["GET"])
def current_exposed_date():
    global CURRENT_EXPOSED_DATE
    return str(CURRENT_EXPOSED_DATE)


@internal_api.route("/current_timestamp", methods=["GET"])
def current_timestamp():
    global CURRENT_EXPOSED_DATE
    return str(int(CURRENT_EXPOSED_DATE.timestamp()))
