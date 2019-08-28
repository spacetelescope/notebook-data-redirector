#!/usr/bin/env python3
import argparse
import json
import subprocess
import boto3
from pathlib import Path

OUTPUT_TEMPLATE = "packaged.yaml"

def parse_args():
    parser = argparse.ArgumentParser("deploy.py", description="redirector deploy script")
    parser.add_argument("--config", help="path to config file", required=True)
    return parser.parse_args()

def read_config(path):
    with path.open("r") as file:
        return json.loads(file.read())

config = read_config(Path(parse_args().config))

print(f"Building and deploying notebook-data-redirector to stack '{config['stack_name']}'")

print("Building...")
subprocess.check_call(["sam", "build"])

print("Packaging...")
subprocess.check_call(["sam", "package", "--output-template", OUTPUT_TEMPLATE, "--s3-bucket", config["deploy_bucket"]])

print("Deploying...")
deploy_args = ["sam", "deploy", "--template-file", "packaged.yaml", "--capabilities", "CAPABILITY_IAM"]
deploy_args.extend(["--stack-name", config["stack_name"]])
deploy_args.append("--parameter-overrides")
deploy_args.append(f"SecretArn={config['secret_arn']}")
deploy_args.append(f"BoxFolderId={config['box_folder_id']}")
subprocess.check_call(deploy_args)

print("Deploy complete, outputs:")
cf = boto3.client("cloudformation")
stack = cf.describe_stacks(StackName=config["stack_name"])["Stacks"][0]
for output in stack["Outputs"]:
    print(f"{output['OutputKey']}: {output['OutputValue']}")
