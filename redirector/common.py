import os
import json
import logging

import boto3
from boxsdk import Client, JWTAuth
from boxsdk.exception import BoxAPIException


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

SECRET_ARN = os.environ["SECRET_ARN"]
MANIFEST_TABLE_NAME = os.environ["MANIFEST_TABLE_NAME"]
BOX_FOLDER_ID = os.environ["BOX_FOLDER_ID"]


def get_box_client():
    secret = _get_secret()

    # TODO(eslavich): We should pick one of snake_case or camelCase
    # TODO(eslavich): Document the secret format and setup
    client_id = secret["clientID"]
    client_secret = secret["clientSecret"]
    public_key_id = secret["publicKeyID"]
    private_key = secret["privateKey"].replace("\\n", "\n")
    passphrase = secret["passphrase"]
    enterprise_id = secret["enterpriseID"]
    webhook_key = secret["webhook_key"]

    auth = JWTAuth(
        client_id,
        client_secret,
        enterprise_id,
        public_key_id,
        rsa_private_key_data=private_key,
        rsa_private_key_passphrase=passphrase,
    )
    auth.authenticate_instance()

    client = Client(auth)

    users = client.users()
    try:
        app_user = users.next()
    except StopIteration:
        LOGGER.warning(
            "no app user exists, so the service account will be used as the box api client"
        )
        return client, webhook_key

    app_client = client.as_user(app_user)

    return app_client, webhook_key


def is_box_file_public(file):
    assert hasattr(
        file, "shared_link"
    ), "cannot operate on summary file, call get() first"

    return (
        file.shared_link
        and file.shared_link["effective_access"] == "open"
        and file.shared_link["effective_permission"] == "can_download"
    )


def get_ddb_table():
    return boto3.resource("dynamodb").Table(MANIFEST_TABLE_NAME)


def put_file_item(ddb_table, file):
    assert is_box_file_public(
        file
    ), "cannot put a file that hasn't been shared publicly"

    item = {
        "filename": file.name,
        "box_file_id": file.id,
        "download_url": file.shared_link["download_url"],
    }

    ddb_table.put_item(Item=item)


def delete_file_item(ddb_table, file):
    ddb_table.delete_item(Key={"filename": file.name})


def get_download_url(ddb_table, filename):
    result = ddb_table.get_item(Key={"filename": filename})
    if result.get('Item'):
        return result['Item']['download_url']
    else:
        return None


def get_file(client, box_file_id):
    return _get_box_resource(lambda: client.file(box_file_id).get())


def get_folder(client, box_folder_id):
    return _get_box_resource(lambda: client.folder(box_folder_id))


def _get_box_resource(callback):
    try:
        return callback()
    except BoxAPIException as e:
        if e.status == 404:
            return None
        else:
            raise e


def iterate_files(folder):
    for item in folder.get_items():
        if item.object_type == "folder":
            # Here we're recursively calling iterate_files on a nested folder and
            # receiving an iterator that contains all of its files.  "yield from"
            # will yield each value from that iterator in turn.
            yield from iterate_files(item)
        elif item.object_type == "file":
            yield item.get()


def _get_secret():
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager")
    response = client.get_secret_value(SecretId=SECRET_ARN)

    if "SecretString" in response:
        secret = response["SecretString"]
        return json.loads(secret)
    else:
        raise NotImplementedError("Binary secret not implemented")
