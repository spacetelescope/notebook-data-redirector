import json
from datetime import datetime, timezone

from botocore.exceptions import ClientError

import common

STATUS_SUCCESS = {"statusCode": 200}


def lambda_handler(event, context):
    try:
        return _handle_event(event)
    except Exception as e:
        common.log_action("ERROR", "webhook_receiver", "webhook_handler_error",
                          error_type=type(e).__name__, error=str(e))
        return STATUS_SUCCESS


def _handle_event(event):
    common.log_action("INFO", "webhook_receiver", "event_received")

    raw_body = event["body"]
    body = json.loads(raw_body)
    trigger = body["trigger"]
    source = body["source"]
    created_by = body.get("created_by", {})

    # Cascade prevention: skip events triggered by our own service account
    created_by_id = created_by.get("id", "") if isinstance(created_by, dict) else ""
    if common.SERVICE_ACCOUNT_USER_ID and created_by_id == common.SERVICE_ACCOUNT_USER_ID:
        common.log_action("INFO", "webhook_receiver", "cascade_event_skipped",
                          trigger=trigger, created_by_id=created_by_id)
        return STATUS_SUCCESS

    # The event structure varies by trigger
    if "item" in source:
        box_id = source["item"]["id"]
        box_type = source["item"]["type"]
    elif "id" in source:
        box_id = source["id"]
        box_type = source["type"]
    else:
        raise RuntimeError("Missing id field")

    common.log_action("INFO", "webhook_receiver", "trigger_received", box_file_id=box_id)

    # only get a box client if we're actually going to need one
    if trigger not in common.HANDLED_TRIGGERS:
        common.log_action("INFO", "webhook_receiver", "trigger_unsupported")
        return STATUS_SUCCESS

    # Event deduplication: conditional write to dedup table
    event_id = body.get("id", "")
    if event_id:
        dedup_table = common.get_event_dedup_table()
        now = datetime.now(timezone.utc)
        try:
            dedup_table.put_item(
                Item={
                    "event_id": event_id,
                    "processed_at": now.isoformat(),
                    "expires_at": int(now.timestamp()) + common.EVENT_DEDUP_TTL_SECONDS,
                },
                ConditionExpression="attribute_not_exists(event_id)",
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                common.log_action("INFO", "webhook_receiver", "duplicate_event_skipped",
                                  event_id=event_id)
                return STATUS_SUCCESS
            else:
                common.log_action("WARNING", "webhook_receiver", "dedup_write_failed",
                                  event_id=event_id, error_type=type(e).__name__)
        except Exception as e:
            common.log_action("WARNING", "webhook_receiver", "dedup_write_failed",
                              event_id=event_id, error_type=type(e).__name__)

    # Signature verification uses only the webhook key — no Box client needed
    webhook_key = common.get_webhook_signature_key()

    is_valid = common.validate_webhook_message(bytes(raw_body, "utf-8"), event["headers"], webhook_key)
    if not is_valid:
        common.log_action("ERROR", "webhook_receiver", "invalid_signature")
        return STATUS_SUCCESS

    # Box client only initialized after signature is validated
    client = common.get_box_client()
    ddb = common.get_ddb_table()

    if (trigger in common.HANDLED_FILE_TRIGGERS) and (box_type == "file"):
        file = common.get_file(client, box_id)
        if not file:
            common.log_action("WARNING", "webhook_receiver", "file_missing", box_file_id=box_id)
            # We don't know what the file's path was, so we'll just have to
            # let the sync lambda clean up DynamoDB.
            return STATUS_SUCCESS

        # if the file isn't public but any parent directory is, make a shared link
        parent_public = common.is_any_parent_public(client, file)
        if (not common.is_box_object_public(file)) and parent_public:
            # this includes an api call
            file = common.create_shared_link(client, file, access="open", allow_download=True)
        # if the file is public but no parent directory is, delete the shared link
        if (common.is_box_object_public(file)) and (not parent_public):
            file = common.remove_shared_link(client, file)

        if common.is_box_object_public(file):
            common.put_file_item(ddb, file)
        else:
            common.delete_file_item(ddb, file)
    elif (trigger in common.HANDLED_FOLDER_TRIGGERS) and (box_type == "folder"):
        folder = common.get_folder(client, box_id)
        if not folder:
            common.log_action("WARNING", "webhook_receiver", "folder_missing", box_file_id=box_id)
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
