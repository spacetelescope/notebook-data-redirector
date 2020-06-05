import logging
import json

import common


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

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
        # not covered by tests
        box_id = source["id"]
        box_type = source["type"]
    else:
        # not covered by tests
        raise RuntimeError("Missing id field")

    LOGGER.info("Received trigger %s on %s id %s", trigger, box_type, box_id)

    # only get a box client if we're actually going to need one
    if trigger not in common.HANDLED_TRIGGERS:
        LOGGER.info("%s is not supported by this endpoint", trigger)
        return STATUS_SUCCESS

    client, webhook_key = common.get_box_client()
    ddb = common.get_ddb_table()

    webhook = client.webhook(webhook_id)
    is_valid = webhook.validate_message(bytes(raw_body, "utf-8"), event["headers"], webhook_key)
    if not is_valid:
        LOGGER.critical("Received invalid webhook request")
        return STATUS_SUCCESS

    if (trigger in common.HANDLED_FILE_TRIGGERS) and (box_type == "file"):
        file = common.get_file(client, box_id)
        if not file:
            LOGGER.warning("File %s is missing (trashed or deleted)", box_id)
            # We don't know what the file's path was, so we'll just have to
            # let the sync lambda clean up DynamoDB.
            return STATUS_SUCCESS

        # if the file isn't public but any parent directory is, make a shared link
        if (not common.is_box_object_public(file)) and (common.is_any_parent_public(client, file)):
            # this includes an api call
            file = common.create_shared_link(client, file, access="open", allow_download=True)
        # if the file is public but no parent directory is, delete the shared link
        if (common.is_box_object_public(file)) and (not common.is_any_parent_public(client, file)):
            file = common.remove_shared_link(client, file)

        if common.is_box_object_public(file):
            common.put_file_item(ddb, file)
        else:
            common.delete_file_item(ddb, file)
    elif (trigger in common.HANDLED_FOLDER_TRIGGERS) and (box_type == "folder"):
        folder = common.get_folder(client, box_id)
        if not folder:
            LOGGER.warning("Folder %s is missing (trashed or deleted)", box_id)
            # The Box API doesn't appear to give us a way to list the contents of
            # a trashed folder, so we're just going to have to let the sync lambda
            # clean up the relevant DynamoDB rows.
            return STATUS_SUCCESS

        folder_shared = common.is_box_object_public(folder)
        for file, shared in common.iterate_files(folder, shared=folder_shared):

            # if the file isn't public but any parent directory is
            if (not common.is_box_object_public(file)) and shared:
                # this includes an api call
                file = common.create_shared_link(client, file, access="open", allow_download=True)
            elif (common.is_box_object_public(file)) and (not shared):
                file = common.remove_shared_link(client, file)

            if common.is_box_object_public(file):
                common.put_file_item(ddb, file)
            else:
                common.delete_file_item(ddb, file)

    return STATUS_SUCCESS
