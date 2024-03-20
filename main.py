import argparse
import logging
import os

import dotenv

from workflow_runner import execute_workflow

parser = argparse.ArgumentParser(
    description="Command line arguments for controlling the application",
    add_help=True,
)

wf_args = parser.add_argument_group("workflow-args", "Workflow Selection Arguments")
wf_args.add_argument(
    "--config-file",
    type=str,
    default="./config/config_internal.yaml",
    help="Provide the path to the config file. Default is ./config/config_internal.yaml.",
)

arg_flags = parser.parse_args()

# Load dev.env file if in development mode
if os.environ.get("IS_DEV") == "True":
    dotenv.load_dotenv(dotenv.find_dotenv("dev.env"))

# Set the log level based on the environment variable
if os.environ.get("DEBUG") == "True":
    logging.basicConfig(
        level=logging.DEBUG,
        format="{asctime} {name:25s} {levelname:8s} {message}",
        style="{",
    )
    os.environ["LOG_LEVEL"] = "DEBUG"
else:
    logging.basicConfig(
        level=logging.INFO,
        format="{asctime} {name:25s} {levelname:8s} {message}",
        style="{",
    )


execute_workflow(arg_flags)
