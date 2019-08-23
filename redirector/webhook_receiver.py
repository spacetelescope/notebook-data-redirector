import logging
import json

import common


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

HANDLED_FILE_TRIGGERS = {
    "SHARED_LINK.CREATED",
    "SHARED_LINK.UPDATED",
    "SHARED_LINK.DELETED",
    "FILE.TRASHED",
    "FILE.RESTORED",
}

HANDLED_FOLDER_TRIGGERS = {"FOLDER.RESTORED", "FOLDER.TRASHED"}

HANDLED_TRIGGERS = HANDLED_FILE_TRIGGERS | HANDLED_FOLDER_TRIGGERS

STATUS_SUCCESS = {"statusCode": 200}


def lambda_handler(event, context):
    LOGGER.info(json.dumps(event))

    raw_body = event["body"]
    body = json.loads(raw_body)
    trigger = body["trigger"]
    webhook_id = body["webhook"]["id"]
    source = body["source"]

    # The event structure varies by trigger
    if "item" in source:
        box_id = source["item"]["id"]
        box_type = source["item"]["type"]
    elif "id" in source:
        box_id = source["id"]
        box_type = source["type"]
    else:
        raise RuntimeError("Missing id field")

    LOGGER.info("Received trigger %s on %s id %s", trigger, box_type, box_id)

    # only get a box client if we're actually going to need one
    if trigger not in HANDLED_TRIGGERS:
        LOGGER.info("%s is not supported by this endpoint", trigger)
        return STATUS_SUCCESS

    client, webhook_key = common.get_box_client()
    ddb = common.get_ddb_table()

    webhook = client.webhook(webhook_id)
    is_valid = webhook.validate_message(
        bytes(raw_body, "utf-8"), event["headers"], webhook_key
    )
    if not is_valid:
        LOGGER.critical("Received invalid webhook request")
        return STATUS_SUCCESS

    if trigger in HANDLED_FILE_TRIGGERS:
        file = common.get_file(client, box_id)
        if not file:
            LOGGER.warning("File %s is missing (trashed or deleted)", box_id)
            common.delete_file_item(ddb, file)
            return STATUS_SUCCESS

        if common.is_box_file_public(file):
            common.put_file_item(ddb, file)
        else:
            common.delete_file_item(ddb, file)
    elif trigger in HANDLED_FOLDER_TRIGGERS:
        folder = common.get_folder(client, box_id)
        if not folder:
            LOGGER.warning("Folder %s is missing (trashed or deleted)", box_id)
            # NOTE(eslavich): The Box API doesn't appear to give us a way to
            # list the contents of a trashed folder, so we're just going to have
            # to let the sync lambda clean up the relevant DynamoDB rows.
            return STATUS_SUCCESS

        for file in common.iterate_files(folder):
            if common.is_box_file_public(file):
                common.put_file_item(ddb, file)
            else:
                common.delete_file_item(ddb, file)

    return STATUS_SUCCESS
