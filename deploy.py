#!/usr/bin/env python3
import argparse
import json
import subprocess
import boto3
from pathlib import Path

OUTPUT_TEMPLATE = "packaged.yaml"

def parse_args():
    parser = argparse.ArgumentParser("deploy.py", description="redirector deploy script")
    parser.add_argument("--stack-name", help="name of Cloud Formation stack to deploy")
    parser.add_argument("--secret-arn", help="ARN of Secrets Manager secret containing Box credentials (generate with create_secret.py)", required=True)
    parser.add_argument("--deploy-bucket", help="name of an S3 bucket that will store deployment artifacts", required=True)
    parser.add_argument("--box-folder-id", help="ID of the shared Box folder", required=True)

    return parser.parse_args()

args = parse_args()

print(f"Building and deploying notebook-data-redirector to stack '{args.stack_name}'")

print("Building...")
subprocess.check_call(["sam", "build"])

print("Packaging...")
subprocess.check_call(["sam", "package", "--output-template", OUTPUT_TEMPLATE, "--s3-bucket", args.deploy_bucket])

print("Deploying...")
deploy_args = ["sam", "deploy", "--template-file", "packaged.yaml", "--capabilities", "CAPABILITY_IAM"]
deploy_args.extend(["--stack-name", args.stack_name])
deploy_args.append("--parameter-overrides")
deploy_args.append(f"SecretArn={args.secret_arn}")
deploy_args.append(f"BoxFolderId={args.box_folder_id}")
subprocess.check_call(deploy_args)

print("Deploy complete, outputs:")
cf = boto3.client("cloudformation")
stack = cf.describe_stacks(StackName=args.stack_name)["Stacks"][0]
for output in stack["Outputs"]:
    print(f"{output['OutputKey']}: {output['OutputValue']}")
