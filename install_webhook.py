#!/usr/bin/env python3
import argparse
import json
import subprocess
import boto3
from pathlib import Path
import os

def parse_args():
    parser = argparse.ArgumentParser("install_webhook.py", description="install a webhook on a Box folder that points to a notebook-data-redirector stack")
    parser.add_argument("--stack-name", help="name of Cloud Formation stack", required=True)
    parser.add_argument("--secret-arn", help="RN of Secrets Manager secret containing Box credentials", required=True)
    parser.add_argument("--box-folder-id", help="ID of the shared Box folder", required=True)
    parser.add_argument("--force", help="overwrite any existing webhooks", action="store_true")
    return parser.parse_args()

args = parse_args()

cf = boto3.client("cloudformation")
stack = cf.describe_stacks(StackName=args.stack_name)["Stacks"][0]
webhook_url = next(o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "BoxWebhookURL")

os.environ["SECRET_ARN"] = args.secret_arn
os.environ["MANIFEST_TABLE_NAME"] = "dummy"
os.environ["BOX_FOLDER_ID"] = args.box_folder_id

from redirector import common

client, _ = common.get_box_client()
folder = client.folder(args.box_folder_id)
webhook = next((w for w in client.get_webhooks() if w.target.id == folder.object_id), None)

if webhook is not None:
    if args.force:
        webhook.delete()
    else:
        raise RuntimeError("A webhook already exists on the specified folder, use --force to overwrite it")

client.create_webhook(folder, list(common.HANDLED_TRIGGERS), webhook_url)

print("Webhook created")