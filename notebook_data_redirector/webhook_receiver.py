import json
from datetime import datetime, timezone

import common

STATUS_SUCCESS = {"statusCode": 200}


def _share_and_store_if_public(client, ddb, file):
    """Check parent sharing, share file if needed, store in manifest if public."""
    parent_public = common.is_any_parent_public(client, file)
    if (not common.is_box_object_public(file)) and parent_public:
        file = common.with_box_retry(common.create_shared_link, client, file, access="open", allow_download=True)
    if common.is_box_object_public(file):
        common.put_file_item(ddb, file)
        return file, True
    return file, False


def _handle_file_event(trigger, box_id):
    ddb = common.get_ddb_table()

    if trigger in ("FILE.DELETED", "FILE.TRASHED"):
        item = common.get_manifest_item_by_box_file_id(ddb, box_id)
        if item:
            ddb.delete_item(Key={"filepath": item["filepath"]})
            common.log_action(
                "INFO",
                "webhook_receiver",
                "file_event_processed",
                trigger=trigger,
                box_file_id=box_id,
                filepath=item["filepath"],
            )
        else:
            common.log_action(
                "INFO",
                "webhook_receiver",
                "file_event_processed",
                trigger=trigger,
                box_file_id=box_id,
                note="no_manifest_entry",
            )
        return

    client = common.get_box_client()

    if trigger in ("FILE.RENAMED", "FILE.MOVED"):
        old_item = common.get_manifest_item_by_box_file_id(ddb, box_id)
        if old_item:
            ddb.delete_item(Key={"filepath": old_item["filepath"]})
        file = common.with_box_retry(common.get_file, client, box_id)
        if not file:
            common.log_action("WARNING", "webhook_receiver", "file_missing", box_file_id=box_id)
            return
        file, stored = _share_and_store_if_public(client, ddb, file)
        common.log_action(
            "INFO",
            "webhook_receiver",
            "file_event_processed",
            trigger=trigger,
            box_file_id=box_id,
            filepath=common.get_filepath(file),
            note=None if stored else "not_public",
        )
        return

    if trigger in ("FILE.UPLOADED", "FILE.COPIED"):
        file = common.with_box_retry(common.get_file, client, box_id)
        if not file:
            common.log_action("WARNING", "webhook_receiver", "file_missing", box_file_id=box_id)
            return
        file, stored = _share_and_store_if_public(client, ddb, file)
        common.log_action(
            "INFO",
            "webhook_receiver",
            "file_event_processed",
            trigger=trigger,
            box_file_id=box_id,
            filepath=common.get_filepath(file),
            note=None if stored else "not_public",
        )
        return

    # Existing logic for SHARED_LINK.*, FILE.RESTORED
    file = common.with_box_retry(common.get_file, client, box_id)
    if not file:
        common.log_action("WARNING", "webhook_receiver", "file_missing", box_file_id=box_id)
        return

    parent_public = common.is_any_parent_public(client, file)
    if (not common.is_box_object_public(file)) and parent_public:
        file = common.with_box_retry(common.create_shared_link, client, file, access="open", allow_download=True)
    if (common.is_box_object_public(file)) and (not parent_public):
        file = common.with_box_retry(common.remove_shared_link, client, file)

    if common.is_box_object_public(file):
        common.put_file_item(ddb, file)
    else:
        common.delete_file_item(ddb, file)

    common.log_action("INFO", "webhook_receiver", "file_event_processed", trigger=trigger, box_file_id=box_id)


def _handle_folder_event(trigger, box_id, event_id, source):
    # Folder SHARED_LINK events: check/fix sharing before queuing
    if trigger in ("SHARED_LINK.CREATED", "SHARED_LINK.UPDATED", "SHARED_LINK.DELETED"):
        client = common.get_box_client()
        folder = common.with_box_retry(common.get_folder, client, box_id)
        if folder:
            if not common.is_box_object_public(folder):
                common.with_box_retry(folder.create_shared_link, access="open", allow_download=True)
                common.log_action("INFO", "webhook_receiver", "fix_folder_sharing", trigger=trigger, folder_id=box_id)
        else:
            common.log_action("WARNING", "webhook_receiver", "folder_missing", trigger=trigger, folder_id=box_id)

    # Queue the work item for the webhook worker
    now = datetime.now(timezone.utc)
    work_item = {
        "work_id": f"{box_id}:{event_id}",
        "folder_id": box_id,
        "trigger": trigger,
        "source_data": source,
        "status": "pending",
        "created_at": now.isoformat(),
        "expires_at": int(now.timestamp()) + common.WEBHOOK_QUEUE_TTL_SECONDS,
    }
    common.get_webhook_queue_table().put_item(Item=work_item)
    common.log_action(
        "INFO",
        "webhook_receiver",
        "folder_event_queued",
        trigger=trigger,
        folder_id=box_id,
        work_id=work_item["work_id"],
    )


def lambda_handler(event, context):
    common.log_action("INFO", "webhook_receiver", "event_received")

    raw_body = event["body"]
    body = json.loads(raw_body)
    trigger = body["trigger"]
    source = body["source"]
    created_by = body.get("created_by", {})

    # Cascade prevention: skip events triggered by our own service account
    created_by_id = created_by.get("id", "") if isinstance(created_by, dict) else ""
    if common.SERVICE_ACCOUNT_USER_ID and created_by_id == common.SERVICE_ACCOUNT_USER_ID:
        common.log_action(
            "INFO", "webhook_receiver", "cascade_event_skipped", trigger=trigger, created_by_id=created_by_id
        )
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

    # Event deduplication: check if we've already processed this event.
    # Note: read-then-write has a TOCTOU race for concurrent invocations,
    # but processing is idempotent so duplicate work is harmless.
    event_id = body.get("id", "")
    dedup_table = None
    if event_id:
        dedup_table = common.get_event_dedup_table()
        try:
            result = dedup_table.get_item(Key={"event_id": event_id})
            if result.get("Item"):
                common.log_action("INFO", "webhook_receiver", "duplicate_event_skipped", event_id=event_id)
                return STATUS_SUCCESS
        except Exception as e:
            common.log_action(
                "WARNING",
                "webhook_receiver",
                "dedup_check_failed",
                event_id=event_id,
                error_type=type(e).__name__,
                error=str(e),
            )

    # Signature verification uses only the webhook key — no Box client needed
    webhook_key = common.get_webhook_signature_key()

    is_valid = common.validate_webhook_message(bytes(raw_body, "utf-8"), event["headers"], webhook_key)
    if not is_valid:
        common.log_action("ERROR", "webhook_receiver", "invalid_signature")
        return STATUS_SUCCESS

    if (trigger in common.HANDLED_FILE_TRIGGERS) and (box_type == "file"):
        _handle_file_event(trigger, box_id)
    elif (trigger in common.HANDLED_FOLDER_TRIGGERS) and (box_type == "folder"):
        _handle_folder_event(trigger, box_id, event_id, source)

    # Write dedup record after successful processing so retries work on failure
    if event_id and dedup_table is not None:
        now = datetime.now(timezone.utc)
        try:
            dedup_table.put_item(
                Item={
                    "event_id": event_id,
                    "processed_at": now.isoformat(),
                    "expires_at": int(now.timestamp()) + common.EVENT_DEDUP_TTL_SECONDS,
                },
            )
            common.log_action("INFO", "webhook_receiver", "dedup_record_written", event_id=event_id)
        except Exception as e:
            common.log_action(
                "WARNING",
                "webhook_receiver",
                "dedup_write_failed",
                event_id=event_id,
                error_type=type(e).__name__,
                error=str(e),
            )

    return STATUS_SUCCESS
