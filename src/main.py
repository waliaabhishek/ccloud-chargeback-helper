from pprint import pprint
from workflow_runner import run_workflow
import ccloud.model as model
import argparse
from helpers import timed_method

parser = argparse.ArgumentParser(
    description="Command line arguments for controlling the application",
    add_help=True,
)

wf_args = parser.add_argument_group("workflow-args", "Workflow Selection Arguments")
wf_args.add_argument(
    "--config-file",
    type=str,
    default="./config/config.yaml",
    help="Provide the Service Account Display Name with which the account will be created.",
)
wf_args.add_argument(
    "--ccloud-url",
    type=str,
    default="https://api.telemetry.confluent.cloud/v2/metrics/cloud/query",
    help="CCloud URL that will be called for collecting the metrics",
)

arg_flags = parser.parse_args()

run_workflow(arg_flags)
