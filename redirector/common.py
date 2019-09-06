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

HANDLED_FILE_TRIGGERS = {
    "SHARED_LINK.CREATED",
    "SHARED_LINK.UPDATED",
    "SHARED_LINK.DELETED",
    "FILE.TRASHED",
    "FILE.RESTORED",
}

HANDLED_FOLDER_TRIGGERS = {"FOLDER.RESTORED", "FOLDER.TRASHED"}

HANDLED_TRIGGERS = HANDLED_FILE_TRIGGERS | HANDLED_FOLDER_TRIGGERS


def get_box_client():
    secret = _get_secret()

    client_id = secret["box_client_id"]
    client_secret = secret["box_client_secret"]
    enterprise_id = secret["box_enterprise_id"]
    jwt_key_id = secret["box_jwt_key_id"]
    rsa_private_key_data = secret["box_rsa_private_key_data"]
    rsa_private_key_passphrase = secret["box_rsa_private_key_passphrase"]
    webhook_signature_key = secret["box_webhook_signature_key"]

    auth = JWTAuth(
        client_id=client_id,
        client_secret=client_secret,
        enterprise_id=enterprise_id,
        jwt_key_id=jwt_key_id,
        rsa_private_key_data=rsa_private_key_data,
        rsa_private_key_passphrase=rsa_private_key_passphrase,
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
        return client, webhook_signature_key

    app_client = client.as_user(app_user)

    return app_client, webhook_signature_key


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


def get_file_pathname(file):
    # want to start the path after "All Files/<BoxFolderName>/"
    file_path_collection = file.path_collection
    start_index = [e.id for e in file_path_collection["entries"]].index(BOX_FOLDER_ID) + 1
    file_path_names = [fp.get().name for fp in file_path_collection["entries"][start_index:]] + [file.name]
    # note: lists with len < 1 are not handled. They shouldn't happen.
    if len(file_path_names) > 1:
        return "/".join(file_path_names)
    elif len(file_path_names) == 1:
        return file_path_names
    


def put_file_item(ddb_table, file):
    assert is_box_file_public(
        file
    ), "cannot put a file that hasn't been shared publicly"

    item = {
        "filepath": get_file_pathname(file),
        "box_file_id": file.id,
        "download_url": file.shared_link["download_url"],
    }

    ddb_table.put_item(Item=item)


def delete_file_item(ddb_table, file):
    ddb_table.delete_item(Key={"filepath": get_file_pathname(file)})


def get_download_url(ddb_table, filepath):
    result = ddb_table.get_item(Key={"filepath": filepath})
    if result.get("Item"):
        return result["Item"]["download_url"]
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
