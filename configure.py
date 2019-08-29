#!/usr/bin/env python3
import boto3
import argparse
import json
from pathlib import Path
from boxsdk import JWTAuth

def parse_args():
    parser = argparse.ArgumentParser("configure.py", description="redirector config script")
    parser.add_argument("--config", help="path to config file", required=True)
    return parser.parse_args()

def read_config(path):
    with path.open("r") as file:
        return json.loads(file.read())

def write_config(path, config):
    with path.open("w") as file:
        file.write(json.dumps(config))

def check_box_auth(secret):
    try:
        JWTAuth(
            client_id=secret["box_client_id"],
            client_secret=secret["box_client_secret"],
            enterprise_id=secret["box_enterprise_id"],
            jwt_key_id=secret["box_jwt_key_id"],
            rsa_private_key_data=secret["box_rsa_private_key_data"],
            rsa_private_key_passphrase=secret["box_rsa_private_key_passphrase"],
        ).authenticate_instance()
    except Exception as e:
        raise RuntimeError("Box failed to authenticate, check credentials and try again") from e

def create_secret(config, secret):
    secret_name = config["stack_name"] + "-secret"
    serialized_secret = json.dumps(secret)

    client = boto3.client('secretsmanager')
    try:
        arn = client.describe_secret(SecretId=secret_name)["ARN"]
    except client.exceptions.ResourceNotFoundException:
        arn = client.create_secret(
            Name=secret_name,
            Description=f"Credentials for notebook-data-redirector stack '{config['stack_name']}'",
            SecretString=serialized_secret
        )["ARN"]
    else:
        client.put_secret_value(SecretId=arn, SecretString=serialized_secret)

    return arn

config_path = Path(parse_args().config)
if config_path.exists():
    config = read_config(config_path)
else:
    config = {}

print("Configuring notebook-data-redirector\n")

if config.get("stack_name"):
    print(f"Using CloudFormation stack name '{config['stack_name']}'")
else:
    config["stack_name"] = input("Enter CloudFormation stack name: ")
    write_config(config_path, config)

if config.get("secret_arn"):
    print(f"Using secret ARN '{config['secret_arn']}'")
else:
    print("Enter Box API credentials")

    secret = {}
    secret["box_client_id"] = input("Client ID: ")
    secret["box_client_secret"] = input("Client secret: ")
    secret["box_enterprise_id"] = input("Enterprise ID: ")
    secret["box_jwt_key_id"] = input("JWT key ID: ")
    secret["box_rsa_private_key_passphrase"] = input("RSA private key passphrase: ")
    secret["box_webhook_signature_key"] = input("Webhook signature key: ")

    while True:
        rsa_path = Path(input("Path to RSA private key data: "))
        try:
            with rsa_path.open() as file:
                secret["box_rsa_private_key_data"] = file.read()
            break
        except Exception:
            print("Unable to read private key, please try again")

    check_box_auth(secret)
    config["secret_arn"] = create_secret(config, secret)
    write_config(config_path, config)
    print(f"Created secret ARN '{config['secret_arn']}'")

if config.get("box_folder_id"):
    print(f"Using Box folder ID {config['box_folder_id']}")
else:
    config["box_folder_id"] = input("Enter Box folder ID: ")
    write_config(config_path, config)

if config.get("deploy_bucket"):
    print(f"Using S3 bucket '{config['deploy_bucket']}' for staging the Lambda archive")
else:
    config["deploy_bucket"] = input("Enter deployment S3 bucket name: ")
    write_config(config_path, config)

print("Configuration complete")
