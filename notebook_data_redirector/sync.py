import json
import os
from datetime import datetime, timedelta, timezone

import boto3

import common

CHUNK_SIZE = 3000  # ~300KB per chunk, under DDB's 400KB item limit
MAX_CHAIN_DEPTH = 10  # ~2.5 hours of processing (10 x 15-min Lambda)


def lambda_handler(event, context):
    mode = event.get("mode", "drain-queue")
    common.log_action("INFO", "sync", "sync_start", mode=mode)

    if mode == "full-sync":
        _full_sync(context, event)
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
        partial_records = []
        response = sync_state_table.scan()
        while True:
            partial_records.extend(
                item
                for item in response.get("Items", [])
                if item.get("mode") == "reconciliation" and item.get("status") == "partial"
            )
            if response.get("LastEvaluatedKey"):
                response = sync_state_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            else:
                break
        if not partial_records:
            return None
        latest = max(partial_records, key=lambda x: x.get("started_at", ""))
        return latest.get("last_filepath")
    except Exception as e:
        common.log_action("WARNING", "sync", "checkpoint_lookup_failed", error_type=type(e).__name__)
        return None


def _find_full_sync_checkpoint(sync_state_table):
    """Find the most recent partial full-sync record."""
    try:
        partial_records = []
        response = sync_state_table.scan()
        while True:
            partial_records.extend(
                item
                for item in response.get("Items", [])
                if item.get("mode") == "full-sync" and item.get("status") == "partial"
            )
            if response.get("LastEvaluatedKey"):
                response = sync_state_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            else:
                break
        if not partial_records:
            return None
        return max(partial_records, key=lambda x: x.get("started_at", ""))
    except Exception as e:
        common.log_action("WARNING", "sync", "checkpoint_lookup_failed", error_type=type(e).__name__)
        return None


def _full_sync_partial_state(sync_state_table, sync_id, last_chunk_index, last_file_index, chain_depth, stats):
    """Save checkpoint for full-sync with chunk position and chain depth."""
    _validate_stat_keys(stats)
    expr_parts = [
        "#s = :status",
        "last_chunk_index = :lci",
        "last_file_index = :lfi",
        "chain_depth = :cd",
    ]
    names = {"#s": "status"}
    values = {
        ":status": "partial",
        ":lci": last_chunk_index,
        ":lfi": last_file_index,
        ":cd": chain_depth,
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


def _store_file_list_chunks(sync_state_table, sync_id, file_list):
    """Store enumerated file list as chunks in SyncStateTable. Returns chunk count."""
    now = datetime.now(timezone.utc)
    expires_at = int((now + timedelta(days=7)).timestamp())
    chunk_count = 0
    for i in range(0, len(file_list), CHUNK_SIZE):
        chunk = file_list[i : i + CHUNK_SIZE]
        serializable = [
            {"filepath": f["filepath"], "box_file_id": f["box_file_id"], "shared": f["shared"]} for f in chunk
        ]
        sync_state_table.put_item(
            Item={
                "sync_id": f"{sync_id}:chunk:{chunk_count}",
                "files": serializable,
                "expires_at": expires_at,
            }
        )
        chunk_count += 1

    sync_state_table.update_item(
        Key={"sync_id": sync_id},
        UpdateExpression="SET chunk_count = :cc",
        ExpressionAttributeValues={":cc": chunk_count},
    )
    return chunk_count


def _load_file_list_chunks(sync_state_table, sync_id, chunk_count):
    """Load file list from stored chunks. Returns list or None if chunks missing."""
    file_list = []
    for i in range(chunk_count):
        result = sync_state_table.get_item(Key={"sync_id": f"{sync_id}:chunk:{i}"})
        if "Item" not in result:
            return None
        file_list.extend(result["Item"]["files"])
    return file_list


def _cleanup_chunks(sync_state_table, sync_id, chunk_count):
    """Delete all chunk items for a sync run."""
    for i in range(chunk_count):
        try:
            sync_state_table.delete_item(Key={"sync_id": f"{sync_id}:chunk:{i}"})
        except Exception as e:
            common.log_action(
                "WARNING",
                "sync",
                "chunk_cleanup_failed",
                sync_id=sync_id,
                chunk_index=i,
                error_type=type(e).__name__,
            )


def _fail_sync_state(sync_state_table, sync_id):
    """Mark a sync state record as failed."""
    sync_state_table.update_item(
        Key={"sync_id": sync_id},
        UpdateExpression="SET #s = :status",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":status": "failed"},
    )


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


def _full_sync(context, event=None):
    if event is None:
        event = {}

    sync_state_table = common.get_sync_state_table()
    ddb_table = common.get_ddb_table()
    box_client = common.get_box_client()

    resume = event.get("resume", False)
    sync_id = event.get("sync_id")
    chain_depth = event.get("chain_depth", 0)

    file_list = None
    chunk_count = 0
    start_index = 0
    items_checked = 0
    items_valid = 0
    items_repaired = 0

    if resume and sync_id:
        # Resume: load checkpoint and chunks from DDB
        result = sync_state_table.get_item(Key={"sync_id": sync_id})
        if "Item" in result:
            checkpoint = result["Item"]
            chunk_count = checkpoint.get("chunk_count", 0)
            last_chunk = checkpoint.get("last_chunk_index", 0)
            last_file = checkpoint.get("last_file_index", -1)
            start_index = last_chunk * CHUNK_SIZE + last_file + 1
            items_checked = checkpoint.get("items_checked", 0)
            items_valid = checkpoint.get("items_valid", 0)
            items_repaired = checkpoint.get("items_repaired", 0)

            file_list = _load_file_list_chunks(sync_state_table, sync_id, chunk_count)
            if file_list is not None:
                common.log_action(
                    "INFO",
                    "sync",
                    "full_sync_resume",
                    sync_id=sync_id,
                    chain_depth=chain_depth,
                    start_index=start_index,
                )
            else:
                common.log_action("WARNING", "sync", "full_sync_chunks_missing", sync_id=sync_id)
        else:
            common.log_action("WARNING", "sync", "full_sync_checkpoint_missing", sync_id=sync_id)

    if file_list is None:
        # Fresh enumeration
        # Clean up leftover chunks from a previous failed chain
        stale = _find_full_sync_checkpoint(sync_state_table)
        if stale:
            stale_id = stale["sync_id"]
            _cleanup_chunks(sync_state_table, stale_id, stale.get("chunk_count", 0))
            try:
                _fail_sync_state(sync_state_table, stale_id)
            except Exception:
                pass

        try:
            sync_id = _create_sync_state(sync_state_table, "full-sync")
        except Exception as e:
            common.log_action("WARNING", "sync", "sync_state_init_failed", error_type=type(e).__name__)
            sync_id = None

        chain_depth = 0

        root_folder = common.get_folder(box_client, common.BOX_FOLDER_ID)
        root_shared = common.is_box_object_public(root_folder)

        common.log_action("INFO", "sync", "full_sync_enumerating")
        file_list = []
        for file, shared in common.iterate_files(root_folder, shared=root_shared):
            file_list.append(
                {
                    "filepath": common.get_filepath(file),
                    "box_file_id": file.id,
                    "shared": shared,
                    "file_ref": file,
                }
            )
        common.log_action("INFO", "sync", "full_sync_enumerated", count=len(file_list))

        # Store chunks for resume capability
        if sync_id is not None:
            try:
                chunk_count = _store_file_list_chunks(sync_state_table, sync_id, file_list)
            except Exception as e:
                common.log_action(
                    "ERROR",
                    "sync",
                    "full_sync_chunk_store_failed",
                    error_type=type(e).__name__,
                )
                # Can't resume without chunks — disable self-invocation
                sync_id = None

        start_index = 0

    # Phase 2: Process files
    common.log_action("INFO", "sync", "full_sync_processing", total=len(file_list), start=start_index)
    timed_out = False
    last_processed_index = None

    for idx in range(start_index, len(file_list)):
        if context.get_remaining_time_in_millis() < 50000:
            timed_out = True
            common.log_action("INFO", "sync", "full_sync_timeout", items_checked=items_checked)
            break

        entry = file_list[idx]
        filepath = entry["filepath"]
        box_file_id = entry["box_file_id"]
        shared = entry["shared"]
        file_ref = entry.get("file_ref")

        # Skip fresh entries (AC #9)
        manifest_result = ddb_table.get_item(Key={"filepath": filepath})
        if "Item" in manifest_result and not common.is_stale(manifest_result["Item"].get("last_validated")):
            last_processed_index = idx
            continue

        # Get file object if we don't have it (resume case)
        if file_ref is None:
            try:
                file_ref = common.with_box_retry(common.get_file, box_client, box_file_id)
            except Exception as e:
                common.log_action(
                    "WARNING",
                    "sync",
                    "full_sync_file_fetch_failed",
                    filepath=filepath,
                    error_type=type(e).__name__,
                )
                last_processed_index = idx
                continue
            if file_ref is None:
                last_processed_index = idx
                continue

        items_checked += 1

        try:
            if (not common.is_box_object_public(file_ref)) and shared:
                file_ref = common.create_shared_link(box_client, file_ref, access="open", allow_download=True)
                items_repaired += 1
            elif common.is_box_object_public(file_ref) and (not shared):
                file_ref = common.remove_shared_link(box_client, file_ref)
                items_repaired += 1
            else:
                items_valid += 1

            if common.is_box_object_public(file_ref):
                common.put_file_item(ddb_table, file_ref)
            else:
                common.delete_file_item(ddb_table, file_ref)
        except Exception as e:
            common.log_action(
                "WARNING",
                "sync",
                "full_sync_file_process_failed",
                filepath=filepath,
                error_type=type(e).__name__,
            )

        last_processed_index = idx

    if timed_out:
        # Checkpoint and self-invoke
        if sync_id is not None and last_processed_index is not None:
            if chain_depth + 1 >= MAX_CHAIN_DEPTH:
                common.log_action(
                    "ERROR",
                    "sync",
                    "full_sync_chain_depth_exceeded",
                    sync_id=sync_id,
                    chain_depth=chain_depth,
                )
                try:
                    _fail_sync_state(sync_state_table, sync_id)
                except Exception:
                    pass
                _cleanup_chunks(sync_state_table, sync_id, chunk_count)
                return

            chunk_idx = last_processed_index // CHUNK_SIZE
            file_idx = last_processed_index % CHUNK_SIZE

            try:
                _full_sync_partial_state(
                    sync_state_table,
                    sync_id,
                    chunk_idx,
                    file_idx,
                    chain_depth,
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
                    "full_sync_checkpoint_save_failed",
                    error_type=type(e).__name__,
                )
                return

            try:
                lambda_client = boto3.client("lambda")
                lambda_client.invoke(
                    FunctionName=os.environ.get("AWS_LAMBDA_FUNCTION_NAME", context.function_name),
                    InvocationType="Event",
                    Payload=json.dumps(
                        {
                            "mode": "full-sync",
                            "sync_id": sync_id,
                            "resume": True,
                            "chain_depth": chain_depth + 1,
                        }
                    ),
                )
                common.log_action(
                    "INFO",
                    "sync",
                    "full_sync_self_invoke",
                    sync_id=sync_id,
                    chain_depth=chain_depth + 1,
                )
            except Exception as e:
                common.log_action(
                    "ERROR",
                    "sync",
                    "full_sync_invoke_failed",
                    sync_id=sync_id,
                    error_type=type(e).__name__,
                )
        return

    # Processing complete — orphan detection (AC #5)
    common.log_action("INFO", "sync", "full_sync_orphan_detection")
    shared_file_ids = set()
    shared_filepaths = set()
    for entry in file_list:
        if entry["shared"]:
            shared_file_ids.add(entry["box_file_id"])
            shared_filepaths.add(entry["filepath"])

    scan_response = ddb_table.scan()
    delete_keys = set()
    while True:
        for item in scan_response["Items"]:
            if (item["box_file_id"] not in shared_file_ids) or (item["filepath"] not in shared_filepaths):
                delete_keys.add(item["filepath"])

        if scan_response.get("LastEvaluatedKey"):
            scan_response = ddb_table.scan(ExclusiveStartKey=scan_response["LastEvaluatedKey"])
        else:
            break

    for key in delete_keys:
        ddb_table.delete_item(Key={"filepath": key})
    items_repaired += len(delete_keys)
    common.log_action("INFO", "sync", "full_sync_orphans_deleted", count=len(delete_keys))

    # Cleanup chunks and complete
    if sync_id is not None:
        _cleanup_chunks(sync_state_table, sync_id, chunk_count)
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

    common.log_action("INFO", "sync", "full_sync_complete", items_checked=items_checked)
