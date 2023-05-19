from workflow_runner import execute_workflow
import argparse

parser = argparse.ArgumentParser(description="Command line arguments for controlling the application", add_help=True,)

wf_args = parser.add_argument_group("workflow-args", "Workflow Selection Arguments")
wf_args.add_argument(
    "--config-file",
    type=str,
    default="./config/config_internal.yaml",
    help="Provide the Service Account Display Name with which the account will be created.",
)

arg_flags = parser.parse_args()

execute_workflow(arg_flags)
