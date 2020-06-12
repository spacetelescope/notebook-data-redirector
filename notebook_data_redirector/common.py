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
SECRET_ROLE_ARN = os.environ["SECRET_ROLE_ARN"]


HANDLED_FILE_TRIGGERS = {
    "SHARED_LINK.CREATED",
    "SHARED_LINK.UPDATED",
    "SHARED_LINK.DELETED",
    "FILE.TRASHED",
    "FILE.RESTORED",
    "FILE.MOVED",
}

HANDLED_FOLDER_TRIGGERS = {
    "SHARED_LINK.CREATED",
    "SHARED_LINK.UPDATED",
    "SHARED_LINK.DELETED",
    "FOLDER.RESTORED",
    "FOLDER.TRASHED",
    "FOLDER.MOVED",
}

HANDLED_TRIGGERS = HANDLED_FILE_TRIGGERS | HANDLED_FOLDER_TRIGGERS

GET_ITEMS_FIELDS = {"name", "path_collection", "shared_link"}

GET_ITEMS_LIMIT = 1000


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
        app_user = next(users)
    except StopIteration:
        LOGGER.warning("no app user exists, so the service account will be used as the box api client")
        return client, webhook_signature_key

    app_client = client.as_user(app_user)

    return app_client, webhook_signature_key


def is_box_object_public(file):
    if not hasattr(file, "shared_link"):
        raise RuntimeError("cannot operate on summary file, call get() first")

    return (
        file.shared_link is not None
        and file.shared_link["effective_access"] == "open"
        and file.shared_link["effective_permission"] == "can_download"
    )


def is_any_parent_public(client, file):
    # checks if any parent folder of the file is public
    # necessary due to changes in the Box API when a folder is shared
    filepath_collection = file.path_collection
    start_index = [e["id"] for e in filepath_collection["entries"]].index(BOX_FOLDER_ID)
    for fpc in filepath_collection["entries"][start_index:]:
        folder = get_folder(client, fpc["id"]).get()
        if is_box_object_public(folder):
            return True

    return False


def create_shared_link(client, file, **boxargs):
    if not hasattr(file, "shared_link"):
        raise RuntimeError("cannot operate on summary file, call get() first")
    # technically this could be a file or a folder
    # create_shared_link returns a new object with the shared link; the original object is not modified
    # see boxsdk docstring
    return file.create_shared_link(**boxargs)


def remove_shared_link(client, file):
    if not hasattr(file, "shared_link"):
        raise RuntimeError("cannot operate on summary file, call get() first")
    # unlike create_shared_link, remove_shared_link returns a boolean indicating whether the operation was successful
    # to avoid confusion, I'm going to get and return the new file without the shared link
    response = file.remove_shared_link()
    if not response:
        # not sure how to reach this in testing
        raise RuntimeError("boxsdk API call to remove_shared_link returned False")
    return file.get()


def get_ddb_table():
    return boto3.resource("dynamodb").Table(MANIFEST_TABLE_NAME)


def get_filepath(file):
    # want to start the path after "All Files/<BoxFolderName>/"
    filepath_collection = file.path_collection
    start_index = [e["id"] for e in filepath_collection["entries"]].index(BOX_FOLDER_ID) + 1
    filepath_tokens = [fp["name"] for fp in filepath_collection["entries"][start_index:]] + [file.name]
    return "/".join(filepath_tokens)


def make_ddb_item(file):
    return {"filepath": get_filepath(file), "box_file_id": file.id, "download_url": file.shared_link["download_url"]}


def put_file_item(ddb_table, file):
    if not is_box_object_public(file):
        raise ValueError("cannot put a file that hasn't been shared publicly")

    ddb_table.put_item(Item=make_ddb_item(file))


def delete_file_item(ddb_table, file):
    ddb_table.delete_item(Key={"filepath": get_filepath(file)})


def get_download_url(ddb_table, filepath):
    result = ddb_table.get_item(Key={"filepath": filepath})
    if result.get("Item"):
        return result["Item"]["download_url"]
    else:
        return None


def get_file(client, box_file_id):
    return _get_box_resource(lambda: client.file(box_file_id).get())


def get_folder(client, box_folder_id):
    return _get_box_resource(lambda: client.folder(box_folder_id).get())


def _get_box_resource(callback):
    try:
        return callback()
    except BoxAPIException as e:
        if e.status == 404:
            return None
        else:
            raise e


def iterate_files(folder, shared=False):
    offset = 0
    while True:
        count = 0
        for item in folder.get_items(limit=GET_ITEMS_LIMIT, offset=offset, fields=GET_ITEMS_FIELDS):
            count += 1
            if item.object_type == "folder":
                # Here we're recursively calling iterate_files on a nested folder and
                # receiving an iterator that contains all of its files.  "yield from"
                # will yield each value from that iterator in turn.
                yield from iterate_files(item, shared=shared or is_box_object_public(item))
            elif item.object_type == "file":
                yield item, shared
        if count >= GET_ITEMS_LIMIT:
            offset += count
        else:
            # unclear why pytest reports this break is never tested...
            break


def _get_secret():
    client = boto3.client("secretsmanager")
    try:
        response = client.get_secret_value(SecretId=SECRET_ARN)
    except client.exceptions.ClientError:  # pragma: no cover

        sts_client = boto3.client("sts")
        assumed_role_object = sts_client.assume_role(RoleArn=SECRET_ROLE_ARN, RoleSessionName="AssumeRoleSession1")

        credentials = assumed_role_object["Credentials"]

        client = boto3.client("secretsmanager",
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )

        response = client.get_secret_value(
            SecretId=SECRET_ARN,
        )

    if "SecretString" in response:
        secret = response["SecretString"]
        return json.loads(secret)
    else:
        raise NotImplementedError("Binary secret not implemented")
