from datetime import datetime, timezone, timedelta

from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

import common

LOCK_EXPIRY_MINUTES = 16
TIMEOUT_BUFFER_MS = 120000  # 2 minutes

_deserializer = TypeDeserializer()


def _deserialize_stream_image(image):
    """Convert DynamoDB Streams wire format to Python types."""
    return {k: _deserializer.deserialize(v) for k, v in image.items()}


def _parse_stream_records(event):
    """Parse and filter DDB Streams records.

    Yields (work_item, checkpoint) tuples for actionable records.
    """
    for record in event.get("Records", []):
        event_name = record.get("eventName")
        if event_name not in ("INSERT", "MODIFY"):
            common.log_action(
                "INFO",
                "webhook_worker",
                "stream_record_skipped",
                reason=f"eventName={event_name}",
            )
            continue

        new_image = record.get("dynamodb", {}).get("NewImage")
        if not new_image:
            common.log_action(
                "INFO",
                "webhook_worker",
                "stream_record_skipped",
                reason="no_new_image",
            )
            continue

        work_item = _deserialize_stream_image(new_image)
        status = work_item.get("status")

        if status == "pending":
            yield work_item, None
        elif status == "continuing":
            yield work_item, work_item.get("checkpoint")
        else:
            common.log_action(
                "INFO",
                "webhook_worker",
                "stream_record_skipped",
                reason=f"status={status}",
                work_id=work_item.get("work_id"),
            )


def _acquire_lock(queue_table, work_id):
    """Attempt to acquire processing lock on a work item.

    Returns True if lock acquired, False if already held by another worker.
    """
    now = datetime.now(timezone.utc).isoformat()
    expired = (datetime.now(timezone.utc) - timedelta(minutes=LOCK_EXPIRY_MINUTES)).isoformat()
    try:
        queue_table.update_item(
            Key={"work_id": work_id},
            UpdateExpression="SET processing_lock = :now, #s = :processing",
            ConditionExpression="attribute_not_exists(processing_lock) OR processing_lock < :expired",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":now": now,
                ":processing": "processing",
                ":expired": expired,
            },
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def _update_work_item(queue_table, work_id, **fields):
    """Update a work item with the given fields plus updated_at timestamp."""
    fields["updated_at"] = datetime.now(timezone.utc).isoformat()

    set_parts = []
    names = {}
    values = {}
    for i, (key, value) in enumerate(fields.items()):
        attr_name = f"#a{i}"
        attr_value = f":v{i}"
        names[attr_name] = key
        values[attr_value] = value
        set_parts.append(f"{attr_name} = {attr_value}")

    queue_table.update_item(
        Key={"work_id": work_id},
        UpdateExpression="SET " + ", ".join(set_parts),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def _extract_folder_path(source_data):
    """Extract the folder path from work item source_data.

    source_data is the webhook source object — contains name and path_collection
    directly (not nested under an 'item' key).
    """
    path_collection = source_data.get("path_collection", {})
    entries = path_collection.get("entries", [])
    item_name = source_data.get("name")

    if entries and item_name:
        folder_ids = [e.get("id") for e in entries]
        if common.BOX_FOLDER_ID in folder_ids:
            start_index = folder_ids.index(common.BOX_FOLDER_ID) + 1
            path_tokens = [e["name"] for e in entries[start_index:]] + [item_name]
            return "/".join(path_tokens)
    return None


def _delete_stale_entries(manifest_table, folder_path):
    """Scan ManifestTable and delete entries under the given folder path."""
    deleted_count = 0
    scan_kwargs = {}
    while True:
        response = manifest_table.scan(**scan_kwargs)
        for item in response.get("Items", []):
            if item["filepath"].startswith(folder_path + "/") or item["filepath"] == folder_path:
                manifest_table.delete_item(Key={"filepath": item["filepath"]})
                deleted_count += 1

        if response.get("LastEvaluatedKey"):
            scan_kwargs = {"ExclusiveStartKey": response["LastEvaluatedKey"]}
        else:
            break
    return deleted_count


def _process_folder_deleted(work_item, manifest_table, queue_table):
    """Handle FOLDER.DELETED: scan ManifestTable and delete matching entries."""
    work_id = work_item["work_id"]
    source_data = work_item.get("source_data", {})

    folder_path = _extract_folder_path(source_data)

    if not folder_path:
        common.log_action(
            "WARNING",
            "webhook_worker",
            "folder_deleted_no_path",
            work_id=work_id,
        )
        _update_work_item(queue_table, work_id, status="completed", processed_count=0)
        return

    deleted_count = _delete_stale_entries(manifest_table, folder_path)

    common.log_action(
        "INFO",
        "webhook_worker",
        "folder_deleted_cleanup",
        work_id=work_id,
        deleted_count=deleted_count,
        folder_path=folder_path,
    )
    _update_work_item(queue_table, work_id, status="completed", processed_count=deleted_count)


def _process_folder_enumeration(work_item, checkpoint, manifest_table, queue_table, folder_cache_table, context):
    """Enumerate folder children and update ManifestTable entries."""
    work_id = work_item["work_id"]
    folder_id = work_item["folder_id"]

    client = common.get_box_client()
    folder = common.with_box_retry(common.get_folder, client, folder_id)

    if folder is None:
        common.log_action(
            "WARNING",
            "webhook_worker",
            "folder_not_found",
            work_id=work_id,
            folder_id=folder_id,
        )
        _update_work_item(queue_table, work_id, status="failed", error=f"Folder {folder_id} not found in Box")
        return

    root_shared = common.is_box_object_public(folder)

    # Correct folder sharing if needed
    if not root_shared:
        try:
            folder.create_shared_link(access="open", allow_download=True)
            root_shared = True
            folder_cache_table.put_item(
                Item={
                    "folder_id": folder_id,
                    "effective_access": "open",
                    "effective_permission": "can_download",
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            common.log_action(
                "INFO",
                "webhook_worker",
                "folder_sharing_corrected",
                folder_id=folder_id,
            )
        except Exception as e:
            common.log_action(
                "WARNING",
                "webhook_worker",
                "folder_sharing_correction_failed",
                folder_id=folder_id,
                error_type=type(e).__name__,
            )

    offset = checkpoint.get("last_offset", 0) if checkpoint else 0
    processed_count = checkpoint.get("processed_count", 0) if checkpoint else 0
    count = 0

    for file, shared in common.iterate_files(folder, shared=root_shared):
        if count < offset:
            count += 1
            continue

        # Check timeout budget
        if context.get_remaining_time_in_millis() < TIMEOUT_BUFFER_MS:
            common.log_action(
                "INFO",
                "webhook_worker",
                "checkpoint_timeout",
                work_id=work_id,
                processed_count=processed_count,
                last_offset=count,
            )
            _update_work_item(
                queue_table,
                work_id,
                status="continuing",
                checkpoint={"last_offset": count, "processed_count": processed_count},
            )
            return

        # Apply sharing logic (same pattern as _full_sync)
        if (not common.is_box_object_public(file)) and shared:
            file = common.with_box_retry(common.create_shared_link, client, file, access="open", allow_download=True)
        elif (common.is_box_object_public(file)) and (not shared):
            file = common.with_box_retry(common.remove_shared_link, client, file)

        if common.is_box_object_public(file):
            # Delete stale entry if filepath changed (e.g., folder renamed/moved)
            new_filepath = common.get_filepath(file)
            old_item = common.get_manifest_item_by_box_file_id(manifest_table, file.id)
            if old_item and old_item["filepath"] != new_filepath:
                manifest_table.delete_item(Key={"filepath": old_item["filepath"]})
            common.put_file_item(manifest_table, file)
        else:
            common.delete_file_item(manifest_table, file)

        count += 1
        processed_count += 1

    common.log_action(
        "INFO",
        "webhook_worker",
        "work_item_completed",
        work_id=work_id,
        processed_count=processed_count,
    )
    _update_work_item(queue_table, work_id, status="completed", processed_count=processed_count)


def lambda_handler(event, context):
    common.log_action(
        "INFO",
        "webhook_worker",
        "stream_event_received",
        record_count=len(event.get("Records", [])),
    )

    queue_table = common.get_webhook_queue_table()
    manifest_table = common.get_ddb_table()
    folder_cache_table = common.get_folder_cache_table()

    for work_item, checkpoint in _parse_stream_records(event):
        work_id = work_item["work_id"]

        if not _acquire_lock(queue_table, work_id):
            common.log_action(
                "INFO",
                "webhook_worker",
                "duplicate_invocation_skipped",
                work_id=work_id,
            )
            continue

        try:
            trigger = work_item.get("trigger", "")

            if trigger == "FOLDER.DELETED":
                _process_folder_deleted(work_item, manifest_table, queue_table)
            else:
                _process_folder_enumeration(
                    work_item, checkpoint, manifest_table, queue_table, folder_cache_table, context
                )
        except Exception as e:
            common.log_action(
                "ERROR",
                "webhook_worker",
                "work_item_failed",
                work_id=work_id,
                error_type=type(e).__name__,
                error=str(e),
            )
            try:
                _update_work_item(queue_table, work_id, status="failed", error=str(e))
            except Exception:
                common.log_action(
                    "ERROR",
                    "webhook_worker",
                    "status_update_failed",
                    work_id=work_id,
                )

    return {"statusCode": 200}
