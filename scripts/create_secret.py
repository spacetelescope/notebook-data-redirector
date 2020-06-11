#!/usr/bin/env python3
import boto3
import argparse
import json
from pathlib import Path
from boxsdk import JWTAuth


def parse_args():
    parser = argparse.ArgumentParser(
        "create_secret.py", description="create a Secrets Manager secret for use with notebook-data-redirector"
    )
    parser.add_argument("--secret-name", help="the name of the Secrets Manager secret to create", required=True)
    parser.add_argument("--box-client-id", help="your Box client ID", required=True)
    parser.add_argument("--box-client-secret", help="your Box client secret", required=True)
    parser.add_argument("--box-enterprise-id", help="your Box enterprise ID", required=True)
    parser.add_argument("--box-jwt-key-id", help="your Box JWT key ID", required=True)
    parser.add_argument(
        "--box-rsa-private-key-path", help="path to a file containing your Box RSA private key", required=True
    )
    parser.add_argument("--box-rsa-private-key-passphrase", help="your Box RSA private key passphrase", required=True)
    parser.add_argument("--box-webhook-signature-key", help="your Box webhook signature key", required=True)
    parser.add_argument(
        "--tags", nargs="+", type=str, default=[], help="space separated list of key value tag pairs", required=False
    )
    parser.add_argument("--force", help="overwrite an existing Secrets Manager secret", action="store_true")
    return parser.parse_args()


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


def create_secret(secret_name, secret, tags=[], force=False):
    serialized_secret = json.dumps(secret)

    client = boto3.client("secretsmanager")

    try:
        arn = client.describe_secret(SecretId=secret_name)["ARN"]
    # on the STSci control tower account, a generic ClientError is raised if the resource doesn't exist.
    # this is likely due to describe secret only being allowed on your own tagged secrets
    except (client.exceptions.ResourceNotFoundException, client.exceptions.ClientError):
        arn = client.create_secret(
            Name=secret_name,
            Description="Credentials for notebook-data-redirector",
            SecretString=serialized_secret,
            Tags=tags,
        )["ARN"]
    else:
        if force:
            client.put_secret_value(SecretId=arn, SecretString=serialized_secret)
            client.tag_resource(SecretId=arn, Tags=tags)
        else:
            raise RuntimeError("A secret already exists with the specified name, use --force to overwrite it")

    return arn


args = parse_args()

secret = {}
secret["box_client_id"] = args.box_client_id
secret["box_client_secret"] = args.box_client_secret
secret["box_enterprise_id"] = args.box_enterprise_id
secret["box_jwt_key_id"] = args.box_jwt_key_id
secret["box_rsa_private_key_passphrase"] = args.box_rsa_private_key_passphrase
secret["box_webhook_signature_key"] = args.box_webhook_signature_key
with Path(args.box_rsa_private_key_path).open() as file:
    secret["box_rsa_private_key_data"] = file.read().replace("\\n", "\n")

check_box_auth(secret)

if (len(args.tags) >= 2) and (len(args.tags) % 2 == 0):
    tags = [{"Key": k, "Value": v} for k, v in zip(args.tags[0::2], args.tags[1::2])]
elif len(args.tags) > 0:
    raise ValueError(
        "tags argument must be a space-separated list of key value pairs, and therefore should have an even number of entries"
    )
else:
    tags = []

arn = create_secret(args.secret_name, secret, tags=tags, force=args.force)

print(f"Created Secrets Manager secret with ARN: {arn}")
