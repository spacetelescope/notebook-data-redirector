from unittest.mock import MagicMock

import pytest

import webhook_worker
import common


def _make_stream_record(event_name, new_image_attrs, old_image_attrs=None):
    """Helper to create a DDB Streams record in wire format."""
    record = {
        "eventID": "test-event-id",
        "eventName": event_name,
        "dynamodb": {
            "NewImage": {k: _to_ddb_attr(v) for k, v in new_image_attrs.items()},
        },
    }
    if old_image_attrs:
        record["dynamodb"]["OldImage"] = {k: _to_ddb_attr(v) for k, v in old_image_attrs.items()}
    return record


def _to_ddb_attr(value):
    """Convert a Python value to DDB wire format attribute."""
    if isinstance(value, bool):
        return {"BOOL": value}
    elif isinstance(value, str):
        return {"S": value}
    elif isinstance(value, (int, float)):
        return {"N": str(value)}
    elif isinstance(value, dict):
        return {"M": {k: _to_ddb_attr(v) for k, v in value.items()}}
    elif isinstance(value, list):
        return {"L": [_to_ddb_attr(item) for item in value]}
    elif value is None:
        return {"NULL": True}
    else:
        return {"S": str(value)}


def _make_pending_work_item(folder_id="12345", trigger="FOLDER.MOVED", source_data=None):
    """Create a standard pending work item in DDB wire format."""
    attrs = {
        "work_id": f"{folder_id}:evt-001",
        "folder_id": folder_id,
        "trigger": trigger,
        "status": "pending",
        "source_data": source_data or {},
        "created_at": "2026-03-16T12:00:00+00:00",
        "expires_at": 1647532800,
    }
    return _make_stream_record("INSERT", attrs)


# =========================================================================
# Task 7: Tests for DDB Streams record parsing (AC: #1)
# =========================================================================


class TestStreamRecordParsing:
    def test_insert_pending_processed(self):
        """INSERT record with status: pending → processed."""
        record = _make_pending_work_item()
        event = {"Records": [record]}

        results = list(webhook_worker._parse_stream_records(event))
        assert len(results) == 1
        work_item, checkpoint = results[0]
        assert work_item["status"] == "pending"
        assert work_item["work_id"] == "12345:evt-001"
        assert checkpoint is None

    def test_insert_completed_skipped(self, capture_log):
        """INSERT record with status: completed → skipped."""
        record = _make_stream_record(
            "INSERT",
            {"work_id": "12345:evt-001", "status": "completed"},
        )
        event = {"Records": [record]}

        results = list(webhook_worker._parse_stream_records(event))
        assert len(results) == 0
        assert "stream_record_skipped" in capture_log.getvalue()
        assert "status=completed" in capture_log.getvalue()

    def test_modify_continuing_with_checkpoint(self):
        """MODIFY record with status: continuing → processed with checkpoint."""
        checkpoint_data = {"last_offset": 50, "processed_count": 50}
        record = _make_stream_record(
            "MODIFY",
            {
                "work_id": "12345:evt-001",
                "status": "continuing",
                "checkpoint": checkpoint_data,
                "folder_id": "12345",
                "trigger": "FOLDER.MOVED",
            },
        )
        event = {"Records": [record]}

        results = list(webhook_worker._parse_stream_records(event))
        assert len(results) == 1
        work_item, checkpoint = results[0]
        assert work_item["status"] == "continuing"
        assert checkpoint == checkpoint_data

    def test_remove_record_skipped(self, capture_log):
        """REMOVE record → skipped."""
        record = {
            "eventID": "test-event-id",
            "eventName": "REMOVE",
            "dynamodb": {
                "OldImage": {"work_id": {"S": "12345:evt-001"}},
            },
        }
        event = {"Records": [record]}

        results = list(webhook_worker._parse_stream_records(event))
        assert len(results) == 0
        assert "stream_record_skipped" in capture_log.getvalue()

    def test_multiple_records_filtered_independently(self, capture_log):
        """Multiple records in batch → each filtered independently."""
        records = [
            _make_pending_work_item(folder_id="111"),
            _make_stream_record("INSERT", {"work_id": "222:evt-001", "status": "completed"}),
            _make_stream_record(
                "MODIFY",
                {
                    "work_id": "333:evt-001",
                    "status": "continuing",
                    "checkpoint": {"last_offset": 10, "processed_count": 10},
                    "folder_id": "333",
                    "trigger": "FOLDER.MOVED",
                },
            ),
        ]
        event = {"Records": records}

        results = list(webhook_worker._parse_stream_records(event))
        assert len(results) == 2
        assert results[0][0]["work_id"] == "111:evt-001"
        assert results[0][1] is None  # pending, no checkpoint
        assert results[1][0]["work_id"] == "333:evt-001"
        assert results[1][1] is not None  # continuing, has checkpoint

    def test_no_new_image_skipped(self, capture_log):
        """Record with no NewImage → skipped."""
        record = {"eventID": "test", "eventName": "INSERT", "dynamodb": {}}
        event = {"Records": [record]}

        results = list(webhook_worker._parse_stream_records(event))
        assert len(results) == 0
        assert "no_new_image" in capture_log.getvalue()


# =========================================================================
# Task 8: Tests for processing lock (AC: #2)
# =========================================================================


class TestProcessingLock:
    def test_lock_acquired_successfully(self, mock_webhook_queue_table, webhook_queue_items):
        """Lock acquired successfully → processing proceeds."""
        webhook_queue_items.append({"work_id": "12345:evt-001", "status": "pending"})

        result = webhook_worker._acquire_lock(mock_webhook_queue_table, "12345:evt-001")
        assert result is True

    def test_lock_already_held_skipped(self, mock_webhook_queue_table, capture_log):
        """Lock already held (ConditionalCheckFailedException) → skipped."""
        mock_webhook_queue_table.conditional_check_fails = True

        result = webhook_worker._acquire_lock(mock_webhook_queue_table, "12345:evt-001")
        assert result is False

    def test_lock_non_conditional_error_propagates(self, mock_webhook_queue_table):
        """Non-ConditionalCheckFailedException errors propagate."""
        from botocore.exceptions import ClientError

        def raise_other_error(**kwargs):
            raise ClientError(
                {"Error": {"Code": "InternalServerError", "Message": "boom"}},
                "UpdateItem",
            )

        mock_webhook_queue_table.update_item = raise_other_error

        with pytest.raises(ClientError) as exc_info:
            webhook_worker._acquire_lock(mock_webhook_queue_table, "12345:evt-001")
        assert exc_info.value.response["Error"]["Code"] == "InternalServerError"


# =========================================================================
# Task 9: Tests for folder enumeration and ManifestTable updates (AC: #3, #5)
# =========================================================================


class TestFolderEnumeration:
    def test_folder_with_files_updates_manifest(
        self,
        monkeypatch,
        mock_ddb_table,
        mock_webhook_queue_table,
        webhook_queue_items,
        ddb_items,
        mock_box_client,
        create_shared_file,
        create_shared_folder,
        managed_folder,
        mock_context,
    ):
        """Folder with 3 files → all 3 ManifestTable entries created/updated."""
        # Create a shared folder with 3 files
        shared_folder = create_shared_folder(parent_folder=managed_folder, id="99001")
        create_shared_file(parent_folder=shared_folder, name="file1.txt")
        create_shared_file(parent_folder=shared_folder, name="file2.txt")
        create_shared_file(parent_folder=shared_folder, name="file3.txt")

        work_item = {
            "work_id": "99001:evt-001",
            "folder_id": "99001",
            "trigger": "FOLDER.MOVED",
            "source_data": {},
        }
        webhook_queue_items.append({"work_id": "99001:evt-001", "status": "pending"})

        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)

        folder_cache_items = []

        class MockFolderCacheTable:
            def put_item(self, Item):
                folder_cache_items.append(Item)

        webhook_worker._process_folder_enumeration(
            work_item, None, mock_ddb_table, mock_webhook_queue_table, MockFolderCacheTable(), mock_context
        )

        assert len(ddb_items) == 3
        filepaths = {item["filepath"] for item in ddb_items}
        assert "test-folder-99001/file1.txt" in filepaths
        assert "test-folder-99001/file2.txt" in filepaths
        assert "test-folder-99001/file3.txt" in filepaths

        # Work item should be completed
        completed_item = next(i for i in webhook_queue_items if i["work_id"] == "99001:evt-001")
        assert completed_item["status"] == "completed"
        assert completed_item["processed_count"] == 3

    def test_folder_with_nested_subfolder(
        self,
        monkeypatch,
        mock_ddb_table,
        mock_webhook_queue_table,
        webhook_queue_items,
        ddb_items,
        mock_box_client,
        create_shared_file,
        create_shared_folder,
        create_folder,
        managed_folder,
        mock_context,
    ):
        """Folder with nested subfolder → recursive enumeration."""
        parent = create_shared_folder(parent_folder=managed_folder, id="99002")
        child_folder = create_folder(parent_folder=parent, id="99003")
        create_shared_file(parent_folder=parent, name="top.txt")
        create_shared_file(parent_folder=child_folder, name="nested.txt")

        work_item = {
            "work_id": "99002:evt-001",
            "folder_id": "99002",
            "trigger": "FOLDER.MOVED",
            "source_data": {},
        }
        webhook_queue_items.append({"work_id": "99002:evt-001", "status": "pending"})

        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)

        webhook_worker._process_folder_enumeration(
            work_item, None, mock_ddb_table, mock_webhook_queue_table, MagicMock(), mock_context
        )

        assert len(ddb_items) == 2
        filepaths = {item["filepath"] for item in ddb_items}
        assert "test-folder-99002/top.txt" in filepaths
        assert "test-folder-99002/test-folder-99003/nested.txt" in filepaths

    def test_folder_sharing_corrected(
        self,
        monkeypatch,
        mock_ddb_table,
        mock_webhook_queue_table,
        webhook_queue_items,
        ddb_items,
        mock_box_client,
        create_shared_file,
        create_folder,
        managed_folder,
        mock_context,
        capture_log,
    ):
        """Folder with incorrect sharing → sharing corrected."""
        # Create folder WITHOUT shared link (not public)
        unshared_folder = create_folder(parent_folder=managed_folder, id="99004")
        create_shared_file(parent_folder=unshared_folder, name="data.txt")

        work_item = {
            "work_id": "99004:evt-001",
            "folder_id": "99004",
            "trigger": "FOLDER.MOVED",
            "source_data": {},
        }
        webhook_queue_items.append({"work_id": "99004:evt-001", "status": "pending"})

        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)

        folder_cache_items = []

        class MockFolderCacheTable:
            def put_item(self, Item):
                folder_cache_items.append(Item)

        webhook_worker._process_folder_enumeration(
            work_item, None, mock_ddb_table, mock_webhook_queue_table, MockFolderCacheTable(), mock_context
        )

        # Folder sharing should have been corrected
        assert "folder_sharing_corrected" in capture_log.getvalue()
        assert len(folder_cache_items) == 1
        assert folder_cache_items[0]["folder_id"] == "99004"
        assert folder_cache_items[0]["effective_access"] == "open"

    def test_folder_missing_from_box_marked_failed(
        self,
        monkeypatch,
        mock_ddb_table,
        mock_webhook_queue_table,
        webhook_queue_items,
        mock_box_client,
        mock_context,
        capture_log,
    ):
        """Folder missing from Box (not DELETED trigger) → marked failed."""
        work_item = {
            "work_id": "99999:evt-001",
            "folder_id": "99999",
            "trigger": "FOLDER.MOVED",
            "source_data": {},
        }
        webhook_queue_items.append({"work_id": "99999:evt-001", "status": "pending"})

        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)

        webhook_worker._process_folder_enumeration(
            work_item, None, mock_ddb_table, mock_webhook_queue_table, MagicMock(), mock_context
        )

        failed_item = next(i for i in webhook_queue_items if i["work_id"] == "99999:evt-001")
        assert failed_item["status"] == "failed"
        assert "not found" in failed_item["error"]
        assert "folder_not_found" in capture_log.getvalue()


# =========================================================================
# Task 10: Tests for FOLDER.DELETED handling (AC: #4)
# =========================================================================


class TestFolderDeleted:
    def test_entries_under_folder_path_deleted(
        self,
        mock_ddb_table,
        mock_webhook_queue_table,
        webhook_queue_items,
        ddb_items,
        capture_log,
    ):
        """3 ManifestTable entries under folder path → all deleted."""
        ddb_items.extend(
            [
                {"filepath": "reports/q1/file1.txt", "box_file_id": "f1", "download_url": "https://url1"},
                {"filepath": "reports/q1/file2.txt", "box_file_id": "f2", "download_url": "https://url2"},
                {"filepath": "reports/q1/sub/file3.txt", "box_file_id": "f3", "download_url": "https://url3"},
                {"filepath": "other/file4.txt", "box_file_id": "f4", "download_url": "https://url4"},
            ]
        )

        work_item = {
            "work_id": "777:evt-001",
            "folder_id": "777",
            "trigger": "FOLDER.DELETED",
            "source_data": {
                "item": {
                    "type": "folder",
                    "id": "777",
                    "name": "q1",
                    "path_collection": {
                        "total_count": 3,
                        "entries": [
                            {"id": "0", "name": "All Files"},
                            {"id": "5", "name": "DMD_Managed_Data"},
                            {"id": "100", "name": "reports"},
                        ],
                    },
                }
            },
        }
        webhook_queue_items.append({"work_id": "777:evt-001", "status": "pending"})

        webhook_worker._process_folder_deleted(work_item, mock_ddb_table, mock_webhook_queue_table)

        assert len(ddb_items) == 1
        assert ddb_items[0]["filepath"] == "other/file4.txt"

        log_output = capture_log.getvalue()
        assert "folder_deleted_cleanup" in log_output
        assert '"deleted_count": 3' in log_output

    def test_no_matching_entries_no_error(
        self,
        mock_ddb_table,
        mock_webhook_queue_table,
        webhook_queue_items,
        ddb_items,
    ):
        """No matching entries → no error, marked completed."""
        work_item = {
            "work_id": "888:evt-001",
            "folder_id": "888",
            "trigger": "FOLDER.DELETED",
            "source_data": {
                "item": {
                    "type": "folder",
                    "id": "888",
                    "name": "empty-folder",
                    "path_collection": {
                        "total_count": 2,
                        "entries": [
                            {"id": "0", "name": "All Files"},
                            {"id": "5", "name": "DMD_Managed_Data"},
                        ],
                    },
                }
            },
        }
        webhook_queue_items.append({"work_id": "888:evt-001", "status": "pending"})

        webhook_worker._process_folder_deleted(work_item, mock_ddb_table, mock_webhook_queue_table)

        completed_item = next(i for i in webhook_queue_items if i["work_id"] == "888:evt-001")
        assert completed_item["status"] == "completed"
        assert completed_item["processed_count"] == 0

    def test_entries_paginated_across_batches(
        self,
        mock_ddb_table,
        mock_webhook_queue_table,
        webhook_queue_items,
        ddb_items,
        capture_log,
    ):
        """7 matching entries across 2 scan pages → all deleted."""
        # BATCH_SIZE is 5, so 8 total items forces pagination
        for i in range(7):
            ddb_items.append(
                {"filepath": f"docs/api/file{i}.txt", "box_file_id": f"f{i}", "download_url": f"https://u{i}"}
            )
        ddb_items.append({"filepath": "unrelated/keep.txt", "box_file_id": "fkeep", "download_url": "https://ukeep"})

        work_item = {
            "work_id": "770:evt-001",
            "folder_id": "770",
            "trigger": "FOLDER.DELETED",
            "source_data": {
                "item": {
                    "type": "folder",
                    "id": "770",
                    "name": "api",
                    "path_collection": {
                        "total_count": 3,
                        "entries": [
                            {"id": "0", "name": "All Files"},
                            {"id": "5", "name": "DMD_Managed_Data"},
                            {"id": "101", "name": "docs"},
                        ],
                    },
                }
            },
        }
        webhook_queue_items.append({"work_id": "770:evt-001", "status": "pending"})

        webhook_worker._process_folder_deleted(work_item, mock_ddb_table, mock_webhook_queue_table)

        assert len(ddb_items) == 1
        assert ddb_items[0]["filepath"] == "unrelated/keep.txt"

        log_output = capture_log.getvalue()
        assert "folder_deleted_cleanup" in log_output
        assert '"deleted_count": 7' in log_output

    def test_folder_deleted_no_path_skips_cleanup(
        self,
        mock_ddb_table,
        mock_webhook_queue_table,
        webhook_queue_items,
        capture_log,
    ):
        """FOLDER.DELETED with no path info → skip cleanup, mark completed."""
        work_item = {
            "work_id": "999:evt-001",
            "folder_id": "999",
            "trigger": "FOLDER.DELETED",
            "source_data": {},
        }
        webhook_queue_items.append({"work_id": "999:evt-001", "status": "pending"})

        webhook_worker._process_folder_deleted(work_item, mock_ddb_table, mock_webhook_queue_table)

        assert "folder_deleted_no_path" in capture_log.getvalue()
        completed_item = next(i for i in webhook_queue_items if i["work_id"] == "999:evt-001")
        assert completed_item["status"] == "completed"


# =========================================================================
# Task 11: Tests for checkpoint continuation (AC: #6)
# =========================================================================


class TestCheckpointContinuation:
    def test_timeout_approaching_writes_checkpoint(
        self,
        monkeypatch,
        mock_ddb_table,
        mock_webhook_queue_table,
        webhook_queue_items,
        ddb_items,
        mock_box_client,
        create_shared_file,
        create_shared_folder,
        managed_folder,
    ):
        """Timeout approaching with remaining files → checkpoint written, status = continuing."""
        folder = create_shared_folder(parent_folder=managed_folder, id="55001")
        for i in range(5):
            create_shared_file(parent_folder=folder, name=f"file{i}.txt")

        work_item = {
            "work_id": "55001:evt-001",
            "folder_id": "55001",
            "trigger": "FOLDER.MOVED",
            "source_data": {},
        }
        webhook_queue_items.append({"work_id": "55001:evt-001", "status": "pending"})

        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)

        # Return enough time for 2 files, then signal timeout
        call_count = {"n": 0}

        class TimeoutContext:
            def get_remaining_time_in_millis(self):
                call_count["n"] += 1
                # First 2 checks: plenty of time. Then: timeout.
                if call_count["n"] <= 2:
                    return 300000
                return 60000  # Less than 120000 → checkpoint

        webhook_worker._process_folder_enumeration(
            work_item, None, mock_ddb_table, mock_webhook_queue_table, MagicMock(), TimeoutContext()
        )

        continuing_item = next(i for i in webhook_queue_items if i["work_id"] == "55001:evt-001")
        assert continuing_item["status"] == "continuing"
        assert "checkpoint" in continuing_item
        assert continuing_item["checkpoint"]["last_offset"] >= 1
        assert continuing_item["checkpoint"]["processed_count"] >= 1

    def test_continuation_resumes_from_offset(
        self,
        monkeypatch,
        mock_ddb_table,
        mock_webhook_queue_table,
        webhook_queue_items,
        ddb_items,
        mock_box_client,
        create_shared_file,
        create_shared_folder,
        managed_folder,
        mock_context,
    ):
        """Continuation record with checkpoint → resumes from offset."""
        folder = create_shared_folder(parent_folder=managed_folder, id="55002")
        for i in range(5):
            create_shared_file(parent_folder=folder, name=f"file{i}.txt")

        work_item = {
            "work_id": "55002:evt-001",
            "folder_id": "55002",
            "trigger": "FOLDER.MOVED",
            "source_data": {},
        }
        webhook_queue_items.append({"work_id": "55002:evt-001", "status": "continuing"})

        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)

        # Resume from offset 3 (already processed 3 files)
        checkpoint = {"last_offset": 3, "processed_count": 3}

        webhook_worker._process_folder_enumeration(
            work_item, checkpoint, mock_ddb_table, mock_webhook_queue_table, MagicMock(), mock_context
        )

        # Should have processed only 2 remaining files (indices 3 and 4)
        completed_item = next(i for i in webhook_queue_items if i["work_id"] == "55002:evt-001")
        assert completed_item["status"] == "completed"
        assert completed_item["processed_count"] == 5  # 3 from before + 2 new


# =========================================================================
# Task 12: Tests for completion and failure (AC: #7, #8)
# =========================================================================


class TestCompletionAndFailure:
    def test_all_files_processed_completed(
        self,
        monkeypatch,
        mock_ddb_table,
        mock_webhook_queue_table,
        webhook_queue_items,
        ddb_items,
        mock_box_client,
        create_shared_file,
        create_shared_folder,
        managed_folder,
        mock_context,
        capture_log,
    ):
        """All files processed → status = completed."""
        folder = create_shared_folder(parent_folder=managed_folder, id="66001")
        create_shared_file(parent_folder=folder, name="data.txt")

        work_item = {
            "work_id": "66001:evt-001",
            "folder_id": "66001",
            "trigger": "FOLDER.RENAMED",
            "source_data": {},
        }
        webhook_queue_items.append({"work_id": "66001:evt-001", "status": "pending"})

        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)

        webhook_worker._process_folder_enumeration(
            work_item, None, mock_ddb_table, mock_webhook_queue_table, MagicMock(), mock_context
        )

        completed_item = next(i for i in webhook_queue_items if i["work_id"] == "66001:evt-001")
        assert completed_item["status"] == "completed"
        assert completed_item["processed_count"] == 1
        assert "work_item_completed" in capture_log.getvalue()

    def test_box_api_error_marked_failed(
        self,
        monkeypatch,
        mock_ddb_table,
        mock_webhook_queue_table,
        webhook_queue_items,
        mock_context,
        capture_log,
    ):
        """Box API error → status = failed with error message."""
        from boxsdk.exception import BoxAPIException

        def exploding_client():
            raise BoxAPIException(500, message="Internal Server Error")

        webhook_queue_items.append({"work_id": "66002:evt-001", "status": "pending"})

        monkeypatch.setattr(common, "get_box_client", exploding_client)

        # Call via lambda_handler so the exception handler wraps properly
        event = {
            "Records": [
                _make_stream_record(
                    "INSERT",
                    {
                        "work_id": "66002:evt-001",
                        "folder_id": "66002",
                        "trigger": "FOLDER.MOVED",
                        "status": "pending",
                        "source_data": {},
                    },
                )
            ]
        }

        monkeypatch.setattr(common, "get_webhook_queue_table", lambda: mock_webhook_queue_table)
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_folder_cache_table", MagicMock)

        result = webhook_worker.lambda_handler(event, mock_context)
        assert result == {"statusCode": 200}

        failed_item = next(i for i in webhook_queue_items if i["work_id"] == "66002:evt-001")
        assert failed_item["status"] == "failed"
        assert "error" in failed_item
        assert "work_item_failed" in capture_log.getvalue()

    def test_ddb_write_error_marked_failed(
        self,
        monkeypatch,
        mock_webhook_queue_table,
        webhook_queue_items,
        mock_box_client,
        create_shared_file,
        create_shared_folder,
        managed_folder,
        mock_context,
        capture_log,
    ):
        """DDB write error during processing → status = failed."""
        folder = create_shared_folder(parent_folder=managed_folder, id="66003")
        create_shared_file(parent_folder=folder, name="data.txt")

        webhook_queue_items.append({"work_id": "66003:evt-001", "status": "pending"})

        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)

        # Make manifest table explode on put_item
        exploding_table = MagicMock()
        exploding_table.put_item.side_effect = Exception("DDB write failed")
        # get_item needs to return no existing item so put_file_item proceeds
        exploding_table.get_item.return_value = {}

        event = {
            "Records": [
                _make_stream_record(
                    "INSERT",
                    {
                        "work_id": "66003:evt-001",
                        "folder_id": "66003",
                        "trigger": "FOLDER.MOVED",
                        "status": "pending",
                        "source_data": {},
                    },
                )
            ]
        }

        monkeypatch.setattr(common, "get_webhook_queue_table", lambda: mock_webhook_queue_table)
        monkeypatch.setattr(common, "get_ddb_table", lambda: exploding_table)
        monkeypatch.setattr(common, "get_folder_cache_table", MagicMock)

        result = webhook_worker.lambda_handler(event, mock_context)
        assert result == {"statusCode": 200}

        failed_item = next(i for i in webhook_queue_items if i["work_id"] == "66003:evt-001")
        assert failed_item["status"] == "failed"
        assert "DDB write failed" in failed_item["error"]


# =========================================================================
# Integration: Full lambda_handler flow tests
# =========================================================================


class TestLambdaHandlerIntegration:
    def test_handler_logs_event_and_returns_200(self, monkeypatch, capture_log, mock_context):
        """Handler returns 200 and logs stream_event_received."""
        monkeypatch.setattr(common, "get_webhook_queue_table", MagicMock)
        monkeypatch.setattr(common, "get_ddb_table", MagicMock)
        monkeypatch.setattr(common, "get_folder_cache_table", MagicMock)

        event = {"Records": []}
        result = webhook_worker.lambda_handler(event, mock_context)
        assert result == {"statusCode": 200}
        assert "stream_event_received" in capture_log.getvalue()

    def test_duplicate_invocation_skipped(
        self,
        monkeypatch,
        mock_webhook_queue_table,
        mock_ddb_table,
        webhook_queue_items,
        capture_log,
        mock_context,
    ):
        """Lock already held → duplicate_invocation_skipped logged."""
        mock_webhook_queue_table.conditional_check_fails = True

        event = {"Records": [_make_pending_work_item()]}

        monkeypatch.setattr(common, "get_webhook_queue_table", lambda: mock_webhook_queue_table)
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_folder_cache_table", MagicMock)

        result = webhook_worker.lambda_handler(event, mock_context)
        assert result == {"statusCode": 200}
        assert "duplicate_invocation_skipped" in capture_log.getvalue()

    def test_full_flow_folder_moved(
        self,
        monkeypatch,
        mock_ddb_table,
        mock_webhook_queue_table,
        webhook_queue_items,
        ddb_items,
        mock_box_client,
        create_shared_file,
        create_shared_folder,
        managed_folder,
        mock_context,
    ):
        """Full flow: FOLDER.MOVED with files → all entries updated."""
        folder = create_shared_folder(parent_folder=managed_folder, id="77001")
        create_shared_file(parent_folder=folder, name="readme.md")
        create_shared_file(parent_folder=folder, name="data.csv")

        webhook_queue_items.append({"work_id": "77001:evt-001", "status": "pending"})

        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "get_webhook_queue_table", lambda: mock_webhook_queue_table)
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_folder_cache_table", MagicMock)

        event = {
            "Records": [
                _make_stream_record(
                    "INSERT",
                    {
                        "work_id": "77001:evt-001",
                        "folder_id": "77001",
                        "trigger": "FOLDER.MOVED",
                        "status": "pending",
                        "source_data": {},
                    },
                )
            ]
        }

        result = webhook_worker.lambda_handler(event, mock_context)
        assert result == {"statusCode": 200}
        assert len(ddb_items) == 2

    def test_full_flow_folder_deleted(
        self,
        monkeypatch,
        mock_ddb_table,
        mock_webhook_queue_table,
        webhook_queue_items,
        ddb_items,
        mock_context,
    ):
        """Full flow: FOLDER.DELETED → matching entries removed."""
        ddb_items.extend(
            [
                {"filepath": "data/report.csv", "box_file_id": "f1", "download_url": "https://u1"},
                {"filepath": "data/summary.txt", "box_file_id": "f2", "download_url": "https://u2"},
                {"filepath": "other/keep.txt", "box_file_id": "f3", "download_url": "https://u3"},
            ]
        )

        webhook_queue_items.append({"work_id": "88001:evt-001", "status": "pending"})

        monkeypatch.setattr(common, "get_webhook_queue_table", lambda: mock_webhook_queue_table)
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_folder_cache_table", MagicMock)

        event = {
            "Records": [
                _make_stream_record(
                    "INSERT",
                    {
                        "work_id": "88001:evt-001",
                        "folder_id": "88001",
                        "trigger": "FOLDER.DELETED",
                        "status": "pending",
                        "source_data": {
                            "item": {
                                "type": "folder",
                                "id": "88001",
                                "name": "data",
                                "path_collection": {
                                    "total_count": 2,
                                    "entries": [
                                        {"id": "0", "name": "All Files"},
                                        {"id": "5", "name": "DMD_Managed_Data"},
                                    ],
                                },
                            }
                        },
                    },
                )
            ]
        }

        result = webhook_worker.lambda_handler(event, mock_context)
        assert result == {"statusCode": 200}

        assert len(ddb_items) == 1
        assert ddb_items[0]["filepath"] == "other/keep.txt"
