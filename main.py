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
    default="./configs/config_with_no_env_vars.yaml",
    help="Provide the path to the config file. Default is ./configs/config_with_no_env_vars.yaml",
)

wf_args.add_argument(
    "--env-file",
    type=str,
    required=False,
    help="Provide the path to ENV file for ease of env variable handling. Default is None and discouraged as it is not scalable",
)


arg_flags = parser.parse_args()

# Load dev.env file if in development mode
if os.getenv("IS_DEV", "False") == "True":
    dotenv.load_dotenv(dotenv.find_dotenv("dev.env"))

# Load the env file if provided
if arg_flags.env_file:
    dotenv.load_dotenv(dotenv.find_dotenv(arg_flags.env_file))

# Set the log level based on the environment variable
if os.getenv("DEBUG", "False") == "True":
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
