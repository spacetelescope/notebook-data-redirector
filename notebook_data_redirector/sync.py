from datetime import datetime, timedelta, timezone

import common


def lambda_handler(event, context):
    mode = event.get("mode", "drain-queue")
    common.log_action("INFO", "sync", "sync_start", mode=mode)

    if mode == "full-sync":
        _full_sync(context)
        return
    elif mode == "drain-queue":
        _drain_queue(context)
        return
    elif mode == "reconciliation":
        _reconciliation(context)
        return
    else:
        common.log_action("WARNING", "sync", "unknown_mode", mode=mode)
        return


def _create_sync_state(sync_state_table, mode):
    """Write initial sync state record. Returns sync_id."""
    now = datetime.now(timezone.utc)
    sync_id = f"{mode}-{now.strftime('%Y-%m-%dT%H:%M:%S')}"
    sync_state_table.put_item(
        Item={
            "sync_id": sync_id,
            "mode": mode,
            "status": "running",
            "started_at": now.isoformat(),
            "items_checked": 0,
            "items_repaired": 0,
            "expires_at": int((now + timedelta(days=30)).timestamp()),
        }
    )
    return sync_id


def _validate_stat_keys(stats):
    """Raise ValueError if any stat key is not a safe DynamoDB attribute name."""
    for key in stats:
        if not key.isidentifier():
            raise ValueError(f"Invalid stat key: {key!r}")


def _complete_sync_state(sync_state_table, sync_id, stats):
    """Update sync state record to completed with final stats."""
    _validate_stat_keys(stats)
    expr_parts = ["#s = :status", "completed_at = :ts"]
    names = {"#s": "status"}
    values = {
        ":status": "completed",
        ":ts": datetime.now(timezone.utc).isoformat(),
    }
    for i, (key, val) in enumerate(stats.items()):
        placeholder = f":stat{i}"
        expr_parts.append(f"{key} = {placeholder}")
        values[placeholder] = val

    sync_state_table.update_item(
        Key={"sync_id": sync_id},
        UpdateExpression="SET " + ", ".join(expr_parts),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def _partial_sync_state(sync_state_table, sync_id, last_filepath, stats):
    """Update sync state record to partial with checkpoint."""
    _validate_stat_keys(stats)
    expr_parts = ["#s = :status", "last_filepath = :lfp"]
    names = {"#s": "status"}
    values = {
        ":status": "partial",
        ":lfp": last_filepath,
    }
    for i, (key, val) in enumerate(stats.items()):
        placeholder = f":stat{i}"
        expr_parts.append(f"{key} = {placeholder}")
        values[placeholder] = val

    sync_state_table.update_item(
        Key={"sync_id": sync_id},
        UpdateExpression="SET " + ", ".join(expr_parts),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def _find_reconciliation_checkpoint(sync_state_table):
    """Find last_filepath from the most recent partial reconciliation."""
    try:
        response = sync_state_table.scan()
        partial_records = [
            item
            for item in response.get("Items", [])
            if item.get("mode") == "reconciliation" and item.get("status") == "partial"
        ]
        if not partial_records:
            return None
        latest = max(partial_records, key=lambda x: x.get("started_at", ""))
        return latest.get("last_filepath")
    except Exception as e:
        common.log_action("WARNING", "sync", "checkpoint_lookup_failed", error_type=type(e).__name__)
        return None


def _repair_broken_url(filepath, manifest_item, manifest_table):
    """Attempt to repair a broken Box download URL.

    Returns "repaired", "deleted", or "failed".
    """
    try:
        client = common.get_box_client()
        box_file_id = manifest_item["box_file_id"]
        file = common.with_box_retry(common.get_file, client, box_file_id)

        if file is None:
            manifest_table.delete_item(Key={"filepath": filepath})
            common.log_action("WARNING", "sync", "file_not_found", filepath=filepath, box_file_id=box_file_id)
            return "deleted"

        file = common.with_box_retry(common.create_shared_link, client, file, access="open", allow_download=True)

        # Ensure ancestor folders are correctly shared (best-effort)
        try:
            common.ensure_folder_shared(client, file)
        except Exception as e:
            common.log_action(
                "WARNING", "sync", "folder_sharing_check_failed", filepath=filepath, error_type=type(e).__name__
            )

        item = common.make_ddb_item(file)
        item["last_validated"] = datetime.now(timezone.utc).isoformat()
        manifest_table.put_item(Item=item)

        common.log_action("INFO", "sync", "repair_success", filepath=filepath, box_file_id=box_file_id)
        return "repaired"
    except Exception as e:
        common.log_action(
            "WARNING",
            "sync",
            "repair_failed",
            filepath=filepath,
            box_file_id=manifest_item.get("box_file_id"),
            error_type=type(e).__name__,
        )
        return "failed"


def _drain_queue(context):
    queue_table = common.get_validation_queue_table()
    manifest_table = common.get_ddb_table()
    sync_state_table = common.get_sync_state_table()

    try:
        sync_id = _create_sync_state(sync_state_table, "drain-queue")
    except Exception as e:
        common.log_action("WARNING", "sync", "sync_state_init_failed", error_type=type(e).__name__)
        sync_id = None

    items_checked = 0
    items_valid = 0
    items_repaired = 0

    # Scan queue (handle paging)
    scan_response = queue_table.scan()
    while True:
        for item in scan_response["Items"]:
            # Check timeout budget
            if context.get_remaining_time_in_millis() < 50000:
                common.log_action("INFO", "sync", "drain_timeout_approaching", items_checked=items_checked)
                break

            filepath = item["filepath"]
            download_url = item["download_url"]

            # Check ManifestTable for this filepath
            manifest_result = manifest_table.get_item(Key={"filepath": filepath})
            if "Item" not in manifest_result:
                queue_table.delete_item(Key={"filepath": filepath})
                common.log_action("INFO", "sync", "drain_validate_orphaned_queue_item", filepath=filepath)
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
                common.log_action("WARNING", "sync", "drain_validate_broken", filepath=filepath)
                repair_result = _repair_broken_url(filepath, manifest_item, manifest_table)
                if repair_result in ("repaired", "deleted"):
                    items_repaired += 1
            else:
                common.log_action("WARNING", "sync", "drain_validate_uncertain", filepath=filepath)

            # Always delete queue item after processing
            queue_table.delete_item(Key={"filepath": filepath})
        else:
            # for-loop completed without break — check for more pages
            if scan_response.get("LastEvaluatedKey"):
                scan_response = queue_table.scan(ExclusiveStartKey=scan_response["LastEvaluatedKey"])
                continue
            break
        break  # for-loop was broken (timeout) — exit while

    # Update sync state
    if sync_id is not None:
        try:
            _complete_sync_state(
                sync_state_table,
                sync_id,
                {"items_checked": items_checked, "items_valid": items_valid, "items_repaired": items_repaired},
            )
        except Exception as e:
            common.log_action("WARNING", "sync", "sync_state_complete_failed", error_type=type(e).__name__)


def _reconciliation(context):
    sync_state_table = common.get_sync_state_table()
    manifest_table = common.get_ddb_table()

    try:
        sync_id = _create_sync_state(sync_state_table, "reconciliation")
    except Exception as e:
        common.log_action("WARNING", "sync", "sync_state_init_failed", error_type=type(e).__name__)
        sync_id = None

    common.log_action("INFO", "sync", "reconciliation_start")

    # Check for partial checkpoint from a previous run
    checkpoint_filepath = _find_reconciliation_checkpoint(sync_state_table)

    items_checked = 0
    items_valid = 0
    items_repaired = 0
    last_processed_filepath = None

    scan_kwargs = {}
    if checkpoint_filepath:
        scan_kwargs["ExclusiveStartKey"] = {"filepath": checkpoint_filepath}
        common.log_action("INFO", "sync", "reconciliation_resume", last_filepath=checkpoint_filepath)

    try:
        scan_response = manifest_table.scan(**scan_kwargs)
    except Exception:
        if checkpoint_filepath:
            common.log_action("WARNING", "sync", "checkpoint_resume_failed", last_filepath=checkpoint_filepath)
            scan_response = manifest_table.scan()
        else:
            raise

    while True:
        for item in scan_response["Items"]:
            # Check timeout budget
            if context.get_remaining_time_in_millis() < 50000:
                common.log_action("INFO", "sync", "reconciliation_timeout", items_checked=items_checked)
                if sync_id is not None and last_processed_filepath is not None:
                    try:
                        _partial_sync_state(
                            sync_state_table,
                            sync_id,
                            last_processed_filepath,
                            {
                                "items_checked": items_checked,
                                "items_valid": items_valid,
                                "items_repaired": items_repaired,
                            },
                        )
                    except Exception as e:
                        common.log_action(
                            "WARNING",
                            "sync",
                            "sync_state_partial_failed",
                            error_type=type(e).__name__,
                        )
                return

            filepath = item["filepath"]

            # Skip fresh entries
            if not common.is_stale(item.get("last_validated")):
                last_processed_filepath = filepath
                continue

            # Validate URL
            result = common.validate_download_url(item["download_url"])
            items_checked += 1

            if result == "valid":
                manifest_table.update_item(
                    Key={"filepath": filepath},
                    UpdateExpression="SET last_validated = :ts",
                    ExpressionAttributeValues={":ts": datetime.now(timezone.utc).isoformat()},
                )
                items_valid += 1
                common.log_action("INFO", "sync", "reconciliation_validate_valid", filepath=filepath)
            elif result == "broken":
                common.log_action("WARNING", "sync", "reconciliation_validate_broken", filepath=filepath)
                repair_result = _repair_broken_url(filepath, item, manifest_table)
                if repair_result in ("repaired", "deleted"):
                    items_repaired += 1
            else:
                common.log_action("WARNING", "sync", "reconciliation_validate_uncertain", filepath=filepath)

            last_processed_filepath = filepath
        else:
            # for-loop completed without break — check for more pages
            if scan_response.get("LastEvaluatedKey"):
                scan_response = manifest_table.scan(ExclusiveStartKey=scan_response["LastEvaluatedKey"])
                continue
            break
        break  # for-loop was broken (timeout) — exit while

    if sync_id is not None:
        try:
            _complete_sync_state(
                sync_state_table,
                sync_id,
                {
                    "items_checked": items_checked,
                    "items_valid": items_valid,
                    "items_repaired": items_repaired,
                },
            )
        except Exception as e:
            common.log_action("WARNING", "sync", "sync_state_complete_failed", error_type=type(e).__name__)

    common.log_action("INFO", "sync", "reconciliation_complete", items_checked=items_checked)


def _full_sync(context):
    sync_state_table = common.get_sync_state_table()

    try:
        sync_id = _create_sync_state(sync_state_table, "full-sync")
    except Exception as e:
        common.log_action("WARNING", "sync", "sync_state_init_failed", error_type=type(e).__name__)
        sync_id = None

    ddb_table = common.get_ddb_table()
    box_client = common.get_box_client()
    root_folder = common.get_folder(box_client, common.BOX_FOLDER_ID)
    root_shared = common.is_box_object_public(root_folder)

    common.log_action("INFO", "sync", "checking_box_files")
    shared_file_ids = set()
    shared_filepaths = set()
    items_checked = 0
    items_valid = 0
    items_repaired = 0
    for file, shared in common.iterate_files(root_folder, shared=root_shared):
        items_checked += 1
        if (not common.is_box_object_public(file)) and shared:
            # this includes an API call
            file = common.create_shared_link(box_client, file, access="open", allow_download=True)
            items_repaired += 1
        elif (common.is_box_object_public(file)) and (not shared):
            file = common.remove_shared_link(box_client, file)
            items_repaired += 1
        else:
            items_valid += 1

        if common.is_box_object_public(file):
            shared_file_ids.add(file.id)
            shared_filepaths.add(common.get_filepath(file))
            common.put_file_item(ddb_table, file)
        else:
            common.delete_file_item(ddb_table, file)
    common.log_action("INFO", "sync", "box_files_processed", count=items_checked)

    common.log_action("INFO", "sync", "checking_ddb_items")
    scan_response = ddb_table.scan()
    delete_keys = set()
    while True:
        for item in scan_response["Items"]:
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
    items_repaired += len(delete_keys)
    common.log_action("INFO", "sync", "ddb_items_processed", count=len(delete_keys))

    if sync_id is not None:
        try:
            _complete_sync_state(
                sync_state_table,
                sync_id,
                {"items_checked": items_checked, "items_valid": items_valid, "items_repaired": items_repaired},
            )
        except Exception as e:
            common.log_action("WARNING", "sync", "sync_state_complete_failed", error_type=type(e).__name__)
