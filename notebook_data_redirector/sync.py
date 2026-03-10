from datetime import datetime, timezone

import common


def lambda_handler(event, context):
    mode = event.get("mode", "drain-queue")
    common.log_action("INFO", "sync", "sync_start", mode=mode)

    if mode == "full-sync":
        _full_sync()
        return
    elif mode == "drain-queue":
        _drain_queue(context)
        return
    elif mode == "reconciliation":
        common.log_action("INFO", "sync", "mode_not_implemented", mode=mode)
        return
    else:
        common.log_action("WARNING", "sync", "unknown_mode", mode=mode)
        return


def _drain_queue(context):
    queue_table = common.get_validation_queue_table()
    manifest_table = common.get_ddb_table()
    sync_state_table = common.get_sync_state_table()

    # Create sync state record
    sync_id = f"drain-queue-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
    sync_state_table.put_item(Item={
        "sync_id": sync_id,
        "mode": "drain-queue",
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "items_checked": 0,
        "items_repaired": 0,
    })

    items_checked = 0
    items_valid = 0

    # Scan queue (handle paging)
    scan_response = queue_table.scan()
    while True:
        for item in scan_response["Items"]:
            # Check timeout budget
            if context.get_remaining_time_in_millis() < 50000:
                common.log_action("INFO", "sync", "drain_timeout_approaching",
                                  items_checked=items_checked)
                break

            filepath = item["filepath"]
            download_url = item["download_url"]

            # Check ManifestTable for this filepath
            manifest_result = manifest_table.get_item(Key={"filepath": filepath})
            if "Item" not in manifest_result:
                queue_table.delete_item(Key={"filepath": filepath})
                common.log_action("INFO", "sync", "drain_validate_orphaned_queue_item",
                                  filepath=filepath)
                continue

            manifest_item = manifest_result["Item"]

            # Check stale threshold
            if not common.is_stale(manifest_item.get("last_validated")):
                queue_table.delete_item(Key={"filepath": filepath})
                continue

            # Validate URL
            result = common.validate_download_url(download_url)
            items_checked += 1

            if result == "valid":
                # Stamp last_validated
                manifest_table.update_item(
                    Key={"filepath": filepath},
                    UpdateExpression="SET last_validated = :ts",
                    ExpressionAttributeValues={":ts": datetime.now(timezone.utc).isoformat()},
                )
                items_valid += 1
                common.log_action("INFO", "sync", "drain_validate_valid", filepath=filepath)
            elif result == "broken":
                # Story 1.3 will add repair here
                common.log_action("WARNING", "sync", "drain_validate_broken", filepath=filepath)
            else:
                common.log_action("WARNING", "sync", "drain_validate_uncertain", filepath=filepath)

            # Always delete queue item after processing
            queue_table.delete_item(Key={"filepath": filepath})
        else:
            # for-loop completed without break — check for more pages
            if scan_response.get("LastEvaluatedKey"):
                scan_response = queue_table.scan(
                    ExclusiveStartKey=scan_response["LastEvaluatedKey"]
                )
                continue
            break
        break  # for-loop was broken (timeout) — exit while

    # Update sync state
    sync_state_table.update_item(
        Key={"sync_id": sync_id},
        UpdateExpression="SET #s = :status, completed_at = :ts, items_checked = :checked, items_valid = :valid",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":status": "completed",
            ":ts": datetime.now(timezone.utc).isoformat(),
            ":checked": items_checked,
            ":valid": items_valid,
        },
    )


def _full_sync():
    ddb_table = common.get_ddb_table()
    box_client = common.get_box_client()
    root_folder = common.get_folder(box_client, common.BOX_FOLDER_ID)
    root_shared = common.is_box_object_public(root_folder)

    common.log_action("INFO", "sync", "checking_box_files")
    shared_file_ids = set()
    shared_filepaths = set()
    count = 0
    for file, shared in common.iterate_files(root_folder, shared=root_shared):
        count += 1
        if (not common.is_box_object_public(file)) and shared:
            # this includes an API call
            file = common.create_shared_link(box_client, file, access="open", allow_download=True)
        elif (common.is_box_object_public(file)) and (not shared):
            file = common.remove_shared_link(box_client, file)

        if common.is_box_object_public(file):
            shared_file_ids.add(file.id)
            shared_filepaths.add(common.get_filepath(file))
            common.put_file_item(ddb_table, file)
        else:
            common.delete_file_item(ddb_table, file)
    common.log_action("INFO", "sync", "box_files_processed", count=count)

    common.log_action("INFO", "sync", "checking_ddb_items")
    count = 0
    scan_response = ddb_table.scan()
    delete_keys = set()
    while True:
        for item in scan_response["Items"]:
            count += 1
            if (item["box_file_id"] not in shared_file_ids) or (item["filepath"] not in shared_filepaths):
                delete_keys.add(item["filepath"])

        # If the data returned by a scan would exceed 1MB, DynamoDB will begin paging.
        # The LastEvaluatedKey field is the placeholder used to request the next page.
        if scan_response.get("LastEvaluatedKey"):
            scan_response = ddb_table.scan(ExclusiveStartKey=scan_response["LastEvaluatedKey"])
        else:
            # this clause isn't reached by testing atm
            break

    for key in delete_keys:
        ddb_table.delete_item(Key={"filepath": key})
    common.log_action("INFO", "sync", "ddb_items_processed", count=count)
