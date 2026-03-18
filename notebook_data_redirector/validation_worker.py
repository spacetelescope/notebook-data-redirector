from datetime import datetime, timezone

from boto3.dynamodb.types import TypeDeserializer

import common

_deserializer = TypeDeserializer()


def _parse_stream_records(event):
    """Parse and filter DDB Streams records to INSERT events only.

    Yields dicts with 'filepath' and 'download_url'.
    """
    for record in event.get("Records", []):
        event_name = record.get("eventName")
        if event_name != "INSERT":
            common.log_action(
                "INFO",
                "validation_worker",
                "stream_record_skipped",
                reason=f"eventName={event_name}",
            )
            continue

        new_image = record.get("dynamodb", {}).get("NewImage")
        if not new_image:
            common.log_action(
                "INFO",
                "validation_worker",
                "stream_record_skipped",
                reason="no_new_image",
            )
            continue

        deserialized = {k: _deserializer.deserialize(v) for k, v in new_image.items()}
        filepath = deserialized.get("filepath")
        download_url = deserialized.get("download_url")
        if not filepath or not download_url:
            common.log_action(
                "WARNING",
                "validation_worker",
                "stream_record_skipped",
                reason="missing_fields",
            )
            continue
        yield {"filepath": filepath, "download_url": download_url}


def _process_validation(record, manifest_table, queue_table):
    """Process a single validation queue item.

    Looks up manifest, validates URL, repairs if broken, manages queue item lifecycle.
    """
    filepath = record["filepath"]
    download_url = record["download_url"]

    # Look up filepath in ManifestTable
    manifest_result = manifest_table.get_item(Key={"filepath": filepath})
    if "Item" not in manifest_result:
        queue_table.delete_item(Key={"filepath": filepath})
        common.log_action("INFO", "validation_worker", "orphaned_queue_item", filepath=filepath)
        return

    manifest_item = manifest_result["Item"]

    # Check stale threshold
    if not common.is_stale(manifest_item.get("last_validated")):
        queue_table.delete_item(Key={"filepath": filepath})
        common.log_action("INFO", "validation_worker", "fresh_entry_skipped", filepath=filepath)
        return

    # Validate URL
    result = common.validate_download_url(download_url)

    if result == "valid":
        manifest_table.update_item(
            Key={"filepath": filepath},
            UpdateExpression="SET last_validated = :ts",
            ExpressionAttributeValues={":ts": datetime.now(timezone.utc).isoformat()},
        )
        queue_table.delete_item(Key={"filepath": filepath})
        common.log_action("INFO", "validation_worker", "validate_valid", filepath=filepath)
    elif result == "broken":
        common.log_action("WARNING", "validation_worker", "validate_broken", filepath=filepath)
        common.repair_broken_url(filepath, manifest_item, manifest_table, caller="validation_worker")
        queue_table.delete_item(Key={"filepath": filepath})
    else:
        # Uncertain: leave queue item for TTL cleanup or retry
        common.log_action("WARNING", "validation_worker", "validate_uncertain", filepath=filepath)


def lambda_handler(event, context):
    common.log_action(
        "INFO",
        "validation_worker",
        "stream_event_received",
        record_count=len(event.get("Records", [])),
    )

    manifest_table = common.get_ddb_table()
    queue_table = common.get_validation_queue_table()

    for record in _parse_stream_records(event):
        try:
            _process_validation(record, manifest_table, queue_table)
        except Exception as e:
            common.log_action(
                "ERROR",
                "validation_worker",
                "validation_failed",
                filepath=record.get("filepath"),
                error_type=type(e).__name__,
                error=str(e),
            )

    return {"statusCode": 200}
