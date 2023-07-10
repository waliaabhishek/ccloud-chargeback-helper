import threading
from flask import Flask

internal_api = Flask(__name__)

READINESS_FLAG = False


def set_readiness(readiness_flag: bool):
    global READINESS_FLAG
    READINESS_FLAG = readiness_flag


@internal_api.route("/is_ready", methods=["GET"])
def is_ready():
    global READINESS_FLAG
    return {"is_ready": READINESS_FLAG}
