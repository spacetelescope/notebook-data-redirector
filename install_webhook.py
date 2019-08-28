#!/usr/bin/env python3
import argparse
import json
import subprocess
import boto3
from pathlib import Path
import os

def parse_args():
    parser = argparse.ArgumentParser("deploy.py", description="redirector deploy script")
    parser.add_argument("--config", help="path to config file", required=True)
    parser.add_argument("--force", help="overwrite any existing webhooks", action="store_true")
    return parser.parse_args()

def read_config(path):
    with path.open("r") as file:
        return json.loads(file.read())

args = parse_args()
config = read_config(Path(args.config))

cf = boto3.client("cloudformation")
stack = cf.describe_stacks(StackName=config["stack_name"])["Stacks"][0]
webhook_url = next(o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "BoxWebhookURL")

os.environ["SECRET_ARN"] = config["secret_arn"]
os.environ["MANIFEST_TABLE_NAME"] = "dummy"
os.environ["BOX_FOLDER_ID"] = config["box_folder_id"]

from redirector import common

client, _ = common.get_box_client()
folder = client.folder(config["box_folder_id"])
webhook = next((w for w in client.get_webhooks() if w.target.id == folder.object_id), None)

if webhook is not None:
    if args.force:
        webhook.delete()
    else:
        raise RuntimeError("A webhook already exists on the specified folder, use --force to overwrite it")

client.create_webhook(folder, list(common.HANDLED_TRIGGERS), webhook_url)

print("Webhook created")