import json
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest
from boxsdk.exception import BoxAPIException

import common
import sync


class TestSync:
    @pytest.fixture(autouse=True)
    def monkeypatch_clients(self, monkeypatch, mock_ddb_table, mock_box_client, mock_sync_state_table):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "get_sync_state_table", lambda: mock_sync_state_table)

    def test_sync_empty(self, ddb_items, mock_context):
        sync.lambda_handler({"mode": "full-sync"}, mock_context)
        assert len(ddb_items) == 0

    def test_sync(
        self,
        ddb_items,
        create_folder,
        create_file,
        create_shared_file,
        create_shared_folder,
        managed_folder,
        create_shared_link,
        mock_box_client,
        mock_context,
    ):
        # supposed to exist after sync
        shared_folder = create_shared_folder(parent_folder=managed_folder)
        correct_file = create_shared_file(parent_folder=shared_folder)
        ddb_items.append(common.make_ddb_item(correct_file))

        # supposed to exist after sync
        missing_file = create_shared_file(parent_folder=shared_folder)

        # not supposed to exist after sync
        no_longer_shared_file = create_file(parent_folder=managed_folder)
        ddb_items.append(
            {
                "filepath": common.get_filepath(no_longer_shared_file),
                "box_file_id": no_longer_shared_file.id,
                "download_url": "some-bogus-download-url",
            }
        )

        # not supposed to exist after sync
        ddb_items.append(
            {
                "filepath": "some/deleted/file.dat",
                "box_file_id": "123456789",
                "download_url": "some-other-bogus-download-url",
            }
        )

        # file in a shared folder that's missing from ddb
        # supposed to exist after sync
        unshared_file = create_file(parent_folder=shared_folder)

        # shared file in an unshared folder, not supposed to exist after sync
        unshared_folder = create_folder(parent=managed_folder)
        shared_file = create_shared_file(parent=unshared_folder)

        sync.lambda_handler({"mode": "full-sync"}, mock_context)

        assert len(ddb_items) == 3
        file_ids = {i["box_file_id"] for i in ddb_items}
        assert file_ids == {correct_file.id, missing_file.id, unshared_file.id}
        assert common.is_box_object_public(shared_file) is False

    def test_sync_ddb_paging(self, ddb_items, mock_context):
        for i in range(5 * 2 + 1):
            ddb_items.append(
                {
                    "filepath": f"some/defunct/file-{i}.dat",
                    "box_file_id": f"123456{i}",
                    "download_url": f"some-defunct-download-url-{i}",
                }
            )

        sync.lambda_handler({"mode": "full-sync"}, mock_context)

        assert len(ddb_items) == 0


class TestSyncModeDispatch:
    def test_reconciliation_mode_dispatches(
        self, capture_log, monkeypatch, mock_sync_state_table, mock_ddb_table, mock_context
    ):
        monkeypatch.setattr(common, "get_sync_state_table", lambda: mock_sync_state_table)
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        sync.lambda_handler({"mode": "reconciliation"}, mock_context)
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "reconciliation_start" in actions
        assert "reconciliation_complete" in actions
        assert "mode_not_implemented" not in actions

    def test_unknown_mode(self, capture_log):
        sync.lambda_handler({"mode": "bogus"}, None)
        log_lines = capture_log.getvalue().strip().split("\n")
        entries = [json.loads(line) for line in log_lines]
        assert any(e["action"] == "unknown_mode" for e in entries)

    def test_default_mode_is_drain_queue(
        self, capture_log, monkeypatch, mock_ddb_table, mock_validation_queue_table, mock_sync_state_table, mock_context
    ):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_validation_queue_table", lambda: mock_validation_queue_table)
        monkeypatch.setattr(common, "get_sync_state_table", lambda: mock_sync_state_table)
        sync.lambda_handler({}, mock_context)
        log_lines = capture_log.getvalue().strip().split("\n")
        entries = [json.loads(line) for line in log_lines]
        assert any(e.get("mode") == "drain-queue" for e in entries)


class TestDrainQueue:
    @pytest.fixture(autouse=True)
    def setup_drain(self, monkeypatch, mock_ddb_table, mock_validation_queue_table, mock_sync_state_table):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_validation_queue_table", lambda: mock_validation_queue_table)
        monkeypatch.setattr(common, "get_sync_state_table", lambda: mock_sync_state_table)

    def test_drains_queue_and_validates_valid_url(
        self, monkeypatch, ddb_items, validation_queue_items, sync_state_items, mock_context, capture_log
    ):
        ddb_items.append(
            {"filepath": "data/file1.fits", "box_file_id": "111", "download_url": "https://box.com/dl/abc"}
        )
        validation_queue_items.append({"filepath": "data/file1.fits", "download_url": "https://box.com/dl/abc"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "valid")

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        # Queue item should be deleted
        assert len(validation_queue_items) == 0
        # last_validated should be stamped
        assert "last_validated" in ddb_items[0]
        # Sync state should be completed
        assert len(sync_state_items) == 1
        assert sync_state_items[0]["status"] == "completed"
        assert sync_state_items[0]["items_checked"] == 1
        # Log should contain drain_validate_valid
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "drain_validate_valid" in actions

    def test_skips_fresh_items(self, monkeypatch, ddb_items, validation_queue_items, sync_state_items, mock_context):
        fresh_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        ddb_items.append(
            {
                "filepath": "data/fresh.fits",
                "box_file_id": "222",
                "download_url": "https://box.com/dl/fresh",
                "last_validated": fresh_ts,
            }
        )
        validation_queue_items.append({"filepath": "data/fresh.fits", "download_url": "https://box.com/dl/fresh"})
        validate_called = {"value": False}
        original = common.validate_download_url

        def spy(url):
            validate_called["value"] = True
            return original(url)

        monkeypatch.setattr(common, "validate_download_url", spy)

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert len(validation_queue_items) == 0  # queue item deleted
        assert validate_called["value"] is False  # validate was NOT called
        assert sync_state_items[0]["items_checked"] == 0

    def test_handles_orphaned_queue_items(
        self, monkeypatch, ddb_items, validation_queue_items, sync_state_items, mock_context, capture_log
    ):
        # Queue item with no matching manifest entry
        validation_queue_items.append({"filepath": "deleted/file.fits", "download_url": "https://box.com/dl/gone"})

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert len(validation_queue_items) == 0
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "drain_validate_orphaned_queue_item" in actions

    def test_broken_url_triggers_repair(
        self, monkeypatch, ddb_items, validation_queue_items, sync_state_items, mock_context, capture_log
    ):
        ddb_items.append(
            {"filepath": "data/broken.fits", "box_file_id": "333", "download_url": "https://box.com/dl/broken"}
        )
        validation_queue_items.append({"filepath": "data/broken.fits", "download_url": "https://box.com/dl/broken"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")
        # Mock Box client — repair fails so manifest stays unchanged
        monkeypatch.setattr(common, "get_box_client", lambda: MagicMock())

        def raise_error(func, *a, **kw):
            raise ConnectionError("network failure")

        monkeypatch.setattr(common, "with_box_retry", raise_error)

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        # Repair failed, so no last_validated stamp
        assert "last_validated" not in ddb_items[0]
        assert len(validation_queue_items) == 0
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "drain_validate_broken" in actions
        assert "repair_failed" in actions

    def test_does_not_stamp_uncertain_results(
        self, monkeypatch, ddb_items, validation_queue_items, sync_state_items, mock_context, capture_log
    ):
        ddb_items.append(
            {"filepath": "data/uncertain.fits", "box_file_id": "444", "download_url": "https://box.com/dl/uncertain"}
        )
        validation_queue_items.append(
            {"filepath": "data/uncertain.fits", "download_url": "https://box.com/dl/uncertain"}
        )
        monkeypatch.setattr(common, "validate_download_url", lambda url: "uncertain")

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert "last_validated" not in ddb_items[0]
        assert len(validation_queue_items) == 0
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "drain_validate_uncertain" in actions

    def test_deletes_queue_items_all_outcomes(
        self, monkeypatch, ddb_items, validation_queue_items, sync_state_items, mock_context
    ):
        outcomes = {"valid": 0, "broken": 1, "uncertain": 2}
        for outcome, i in outcomes.items():
            ddb_items.append(
                {"filepath": f"data/file{i}.fits", "box_file_id": str(i), "download_url": f"https://box.com/dl/{i}"}
            )
            validation_queue_items.append({"filepath": f"data/file{i}.fits", "download_url": f"https://box.com/dl/{i}"})

        url_outcomes = {f"https://box.com/dl/{i}": outcome for outcome, i in outcomes.items()}
        monkeypatch.setattr(common, "validate_download_url", lambda url: url_outcomes[url])
        # Broken path now triggers repair — mock Box client to let it fail gracefully
        monkeypatch.setattr(common, "get_box_client", lambda: MagicMock())

        def raise_error(func, *a, **kw):
            raise ConnectionError("network failure")

        monkeypatch.setattr(common, "with_box_retry", raise_error)

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert len(validation_queue_items) == 0

    def test_stops_on_timeout(self, monkeypatch, ddb_items, validation_queue_items, sync_state_items, capture_log):
        # Add several items
        for i in range(5):
            ddb_items.append(
                {"filepath": f"data/file{i}.fits", "box_file_id": str(i), "download_url": f"https://box.com/dl/{i}"}
            )
            validation_queue_items.append({"filepath": f"data/file{i}.fits", "download_url": f"https://box.com/dl/{i}"})

        monkeypatch.setattr(common, "validate_download_url", lambda url: "valid")

        # Context that returns low time remaining immediately
        class TimeoutContext:
            def get_remaining_time_in_millis(self):
                return 30000  # < 50000 threshold

        context = TimeoutContext()

        sync.lambda_handler({"mode": "drain-queue"}, context)

        # Should have stopped before processing any items
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "drain_timeout_approaching" in actions
        # All queue items should remain unprocessed
        assert len(validation_queue_items) == 5
        # Sync state should still be completed
        assert sync_state_items[0]["status"] == "completed"
        assert sync_state_items[0]["items_checked"] == 0

    def test_creates_and_updates_sync_state(
        self, monkeypatch, ddb_items, validation_queue_items, sync_state_items, mock_context
    ):
        ddb_items.append({"filepath": "data/file.fits", "box_file_id": "555", "download_url": "https://box.com/dl/555"})
        validation_queue_items.append({"filepath": "data/file.fits", "download_url": "https://box.com/dl/555"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "valid")

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert len(sync_state_items) == 1
        state = sync_state_items[0]
        assert state["mode"] == "drain-queue"
        assert state["status"] == "completed"
        assert "started_at" in state
        assert "completed_at" in state
        assert state["items_checked"] == 1

    def test_handles_empty_queue(self, monkeypatch, ddb_items, validation_queue_items, sync_state_items, mock_context):
        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert len(sync_state_items) == 1
        assert sync_state_items[0]["status"] == "completed"
        assert sync_state_items[0]["items_checked"] == 0

    def test_stamps_last_validated_for_valid(
        self, monkeypatch, ddb_items, validation_queue_items, sync_state_items, mock_context
    ):
        ddb_items.append(
            {"filepath": "data/valid.fits", "box_file_id": "666", "download_url": "https://box.com/dl/666"}
        )
        validation_queue_items.append({"filepath": "data/valid.fits", "download_url": "https://box.com/dl/666"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "valid")

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert "last_validated" in ddb_items[0]
        # Verify it's a valid ISO 8601 timestamp
        ts = datetime.fromisoformat(ddb_items[0]["last_validated"])
        assert ts.tzinfo is not None

    def test_pagination_across_batches(
        self, monkeypatch, ddb_items, validation_queue_items, sync_state_items, mock_context
    ):
        # BATCH_SIZE is 5 in MockTable, so 7 items forces pagination
        for i in range(7):
            ddb_items.append(
                {"filepath": f"data/page{i}.fits", "box_file_id": str(i), "download_url": f"https://box.com/dl/{i}"}
            )
            validation_queue_items.append({"filepath": f"data/page{i}.fits", "download_url": f"https://box.com/dl/{i}"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "valid")

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert len(validation_queue_items) == 0
        assert sync_state_items[0]["items_checked"] == 7
        # All 7 items should have last_validated stamped
        for item in ddb_items:
            assert "last_validated" in item


class TestRepairBrokenUrl:
    """Unit tests for sync._repair_broken_url() in isolation."""

    @pytest.fixture
    def data_folder(self, create_folder, managed_folder):
        return create_folder(parent_folder=managed_folder, name="data")

    @pytest.fixture
    def manifest_item(self):
        return {"filepath": "data/broken.fits", "box_file_id": "111", "download_url": "https://box.com/dl/old"}

    @pytest.fixture
    def mock_repaired_file(self, create_shared_file, data_folder):
        return create_shared_file(parent_folder=data_folder, id="111", name="broken.fits")

    def test_successful_repair(
        self, monkeypatch, mock_ddb_table, ddb_items, manifest_item, mock_repaired_file, mock_box_client, capture_log
    ):
        ddb_items.append(manifest_item)
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))

        result = sync._repair_broken_url("data/broken.fits", manifest_item, mock_ddb_table)

        assert result == "repaired"
        assert len(ddb_items) == 1
        assert "last_validated" in ddb_items[0]
        assert ddb_items[0]["box_file_id"] == "111"
        assert ddb_items[0]["download_url"] == mock_repaired_file.shared_link["download_url"]
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "repair_success" in actions

    def test_file_not_found_deletes_manifest(self, monkeypatch, mock_ddb_table, ddb_items, manifest_item, capture_log):
        ddb_items.append(manifest_item)
        monkeypatch.setattr(common, "get_box_client", lambda: MagicMock())
        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))
        monkeypatch.setattr(common, "get_file", lambda client, fid: None)

        result = sync._repair_broken_url("data/broken.fits", manifest_item, mock_ddb_table)

        assert result == "deleted"
        assert len(ddb_items) == 0
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "file_not_found" in actions

    def test_repair_failure_returns_failed(self, monkeypatch, mock_ddb_table, ddb_items, manifest_item, capture_log):
        ddb_items.append(manifest_item)
        monkeypatch.setattr(common, "get_box_client", lambda: MagicMock())

        def raise_error(func, *a, **kw):
            raise ConnectionError("network failure")

        monkeypatch.setattr(common, "with_box_retry", raise_error)

        result = sync._repair_broken_url("data/broken.fits", manifest_item, mock_ddb_table)

        assert result == "failed"
        assert len(ddb_items) == 1
        assert "last_validated" not in ddb_items[0]
        log_lines = capture_log.getvalue().strip().split("\n")
        entries = [json.loads(line) for line in log_lines]
        assert any(e["action"] == "repair_failed" and e["error_type"] == "ConnectionError" for e in entries)

    def test_uses_with_box_retry(
        self, monkeypatch, mock_ddb_table, ddb_items, manifest_item, mock_repaired_file, mock_box_client
    ):
        ddb_items.append(manifest_item)
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "ensure_folder_shared", lambda client, file: None)

        retry_calls = []

        def spy_retry(func, *args, **kwargs):
            retry_calls.append(func)
            return func(*args, **kwargs)

        monkeypatch.setattr(common, "with_box_retry", spy_retry)

        sync._repair_broken_url("data/broken.fits", manifest_item, mock_ddb_table)

        assert len(retry_calls) == 2
        assert retry_calls[0] is common.get_file
        assert retry_calls[1] is common.create_shared_link

    def test_create_shared_link_failure(
        self, monkeypatch, mock_ddb_table, ddb_items, manifest_item, capture_log, create_file, data_folder
    ):
        ddb_items.append(manifest_item)
        create_file(parent_folder=data_folder, id="111", name="broken.fits")
        monkeypatch.setattr(common, "get_box_client", lambda: MagicMock())
        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))

        def fail_shared_link(client, file, **kwargs):
            raise BoxAPIException(500, message="internal error")

        monkeypatch.setattr(common, "create_shared_link", fail_shared_link)

        result = sync._repair_broken_url("data/broken.fits", manifest_item, mock_ddb_table)

        assert result == "failed"
        assert len(ddb_items) == 1
        assert "last_validated" not in ddb_items[0]


class TestDrainQueueWithRepair:
    """Integration tests for drain-queue triggering repair on broken URLs."""

    @pytest.fixture(autouse=True)
    def setup_drain(self, monkeypatch, mock_ddb_table, mock_validation_queue_table, mock_sync_state_table):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_validation_queue_table", lambda: mock_validation_queue_table)
        monkeypatch.setattr(common, "get_sync_state_table", lambda: mock_sync_state_table)

    @pytest.fixture
    def data_folder(self, create_folder, managed_folder):
        return create_folder(parent_folder=managed_folder, name="data")

    def test_broken_url_triggers_repair_and_updates_manifest(
        self,
        monkeypatch,
        ddb_items,
        validation_queue_items,
        sync_state_items,
        mock_context,
        capture_log,
        create_shared_file,
        data_folder,
        mock_box_client,
    ):
        ddb_items.append(
            {"filepath": "data/repair.fits", "box_file_id": "111", "download_url": "https://box.com/dl/old"}
        )
        validation_queue_items.append({"filepath": "data/repair.fits", "download_url": "https://box.com/dl/old"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")

        repaired_file = create_shared_file(parent_folder=data_folder, id="111", name="repair.fits")
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert len(ddb_items) == 1
        assert "last_validated" in ddb_items[0]
        assert ddb_items[0]["download_url"] == repaired_file.shared_link["download_url"]
        assert len(validation_queue_items) == 0

    def test_broken_url_file_gone_deletes_manifest(
        self, monkeypatch, ddb_items, validation_queue_items, sync_state_items, mock_context, capture_log
    ):
        ddb_items.append(
            {"filepath": "data/gone.fits", "box_file_id": "999", "download_url": "https://box.com/dl/gone"}
        )
        validation_queue_items.append({"filepath": "data/gone.fits", "download_url": "https://box.com/dl/gone"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")
        monkeypatch.setattr(common, "get_box_client", lambda: MagicMock())
        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))
        monkeypatch.setattr(common, "get_file", lambda client, fid: None)

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert len(ddb_items) == 0
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "file_not_found" in actions

    def test_broken_url_repair_fails_no_stamp(
        self, monkeypatch, ddb_items, validation_queue_items, sync_state_items, mock_context, capture_log
    ):
        ddb_items.append(
            {"filepath": "data/fail.fits", "box_file_id": "888", "download_url": "https://box.com/dl/fail"}
        )
        validation_queue_items.append({"filepath": "data/fail.fits", "download_url": "https://box.com/dl/fail"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")
        monkeypatch.setattr(common, "get_box_client", lambda: MagicMock())

        def raise_error(func, *a, **kw):
            raise ConnectionError("network failure")

        monkeypatch.setattr(common, "with_box_retry", raise_error)

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert len(ddb_items) == 1
        assert "last_validated" not in ddb_items[0]
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "repair_failed" in actions

    def test_items_repaired_counter_in_sync_state(
        self,
        monkeypatch,
        ddb_items,
        validation_queue_items,
        sync_state_items,
        mock_context,
        create_shared_file,
        data_folder,
        mock_box_client,
    ):
        ddb_items.append({"filepath": "data/fix.fits", "box_file_id": "777", "download_url": "https://box.com/dl/fix"})
        validation_queue_items.append({"filepath": "data/fix.fits", "download_url": "https://box.com/dl/fix"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")

        create_shared_file(parent_folder=data_folder, id="777", name="fix.fits")
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert sync_state_items[0]["items_repaired"] == 1

    def test_successful_repair_calls_ensure_folder_shared(
        self,
        monkeypatch,
        ddb_items,
        validation_queue_items,
        sync_state_items,
        mock_context,
        capture_log,
        create_shared_file,
        data_folder,
        mock_box_client,
    ):
        ddb_items.append(
            {"filepath": "data/repair.fits", "box_file_id": "111", "download_url": "https://box.com/dl/old"}
        )
        validation_queue_items.append({"filepath": "data/repair.fits", "download_url": "https://box.com/dl/old"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")

        create_shared_file(parent_folder=data_folder, id="111", name="repair.fits")
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))

        folder_shared_calls = []
        original = common.ensure_folder_shared

        def spy(client, file):
            folder_shared_calls.append(file)
            return original(client, file)

        monkeypatch.setattr(common, "ensure_folder_shared", spy)

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert len(folder_shared_calls) == 1
        assert ddb_items[0]["download_url"] != "https://box.com/dl/old"

    def test_folder_sharing_failure_does_not_prevent_repair(
        self,
        monkeypatch,
        ddb_items,
        validation_queue_items,
        sync_state_items,
        mock_context,
        capture_log,
        create_shared_file,
        data_folder,
        mock_box_client,
    ):
        ddb_items.append(
            {"filepath": "data/repair.fits", "box_file_id": "111", "download_url": "https://box.com/dl/old"}
        )
        validation_queue_items.append({"filepath": "data/repair.fits", "download_url": "https://box.com/dl/old"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")

        repaired_file = create_shared_file(parent_folder=data_folder, id="111", name="repair.fits")
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))

        def explode(client, file):
            raise RuntimeError("Box API exploded")

        monkeypatch.setattr(common, "ensure_folder_shared", explode)

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        # Repair still succeeded despite folder sharing failure
        assert len(ddb_items) == 1
        assert "last_validated" in ddb_items[0]
        assert ddb_items[0]["download_url"] == repaired_file.shared_link["download_url"]
        assert sync_state_items[0]["items_repaired"] == 1
        # Warning logged
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "folder_sharing_check_failed" in actions

    def test_ensure_folder_shared_exception_caught_and_logged(
        self, monkeypatch, mock_ddb_table, ddb_items, create_shared_file, data_folder, mock_box_client, capture_log
    ):
        """Unit-level: _repair_broken_url catches ensure_folder_shared exception."""
        manifest_item = {"filepath": "data/broken.fits", "box_file_id": "111", "download_url": "https://box.com/dl/old"}
        ddb_items.append(manifest_item)
        create_shared_file(parent_folder=data_folder, id="111", name="broken.fits")
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))

        def explode(client, file):
            raise ValueError("something went wrong")

        monkeypatch.setattr(common, "ensure_folder_shared", explode)

        result = sync._repair_broken_url("data/broken.fits", manifest_item, mock_ddb_table)

        assert result == "repaired"
        log_lines = capture_log.getvalue().strip().split("\n")
        entries = [json.loads(line) for line in log_lines]
        assert any(e["action"] == "folder_sharing_check_failed" and e["error_type"] == "ValueError" for e in entries)

    def test_mixed_outcomes_in_drain_run(
        self,
        monkeypatch,
        ddb_items,
        validation_queue_items,
        sync_state_items,
        mock_context,
        capture_log,
        create_shared_file,
        data_folder,
        mock_box_client,
    ):
        # valid item
        ddb_items.append({"filepath": "data/ok.fits", "box_file_id": "100", "download_url": "https://box.com/dl/ok"})
        validation_queue_items.append({"filepath": "data/ok.fits", "download_url": "https://box.com/dl/ok"})
        # broken → repaired
        ddb_items.append({"filepath": "data/fix.fits", "box_file_id": "200", "download_url": "https://box.com/dl/fix"})
        validation_queue_items.append({"filepath": "data/fix.fits", "download_url": "https://box.com/dl/fix"})
        create_shared_file(parent_folder=data_folder, id="200", name="fix.fits")
        # broken → deleted (file gone from Box)
        ddb_items.append(
            {"filepath": "data/gone.fits", "box_file_id": "300", "download_url": "https://box.com/dl/gone"}
        )
        validation_queue_items.append({"filepath": "data/gone.fits", "download_url": "https://box.com/dl/gone"})
        # uncertain
        ddb_items.append(
            {"filepath": "data/maybe.fits", "box_file_id": "400", "download_url": "https://box.com/dl/maybe"}
        )
        validation_queue_items.append({"filepath": "data/maybe.fits", "download_url": "https://box.com/dl/maybe"})

        url_outcomes = {
            "https://box.com/dl/ok": "valid",
            "https://box.com/dl/fix": "broken",
            "https://box.com/dl/gone": "broken",
            "https://box.com/dl/maybe": "uncertain",
        }
        monkeypatch.setattr(common, "validate_download_url", lambda url: url_outcomes[url])
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))
        # get_file returns None for id "300" (gone), real lookup otherwise
        original_get_file = common.get_file

        def selective_get_file(client, fid):
            if fid == "300":
                return None
            return original_get_file(client, fid)

        monkeypatch.setattr(common, "get_file", selective_get_file)

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert len(validation_queue_items) == 0
        assert sync_state_items[0]["items_checked"] == 4
        assert sync_state_items[0]["items_valid"] == 1
        assert sync_state_items[0]["items_repaired"] == 2  # 1 repaired + 1 deleted
        # "gone.fits" should be deleted from manifest
        filepaths = {item["filepath"] for item in ddb_items}
        assert "data/gone.fits" not in filepaths
        # "maybe.fits" should be unchanged (no last_validated)
        maybe_item = next(i for i in ddb_items if i["filepath"] == "data/maybe.fits")
        assert "last_validated" not in maybe_item


class TestFullSyncState:
    """Tests for sync state tracking in _full_sync() (AC #3, #4, #8)."""

    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch, mock_ddb_table, mock_box_client, mock_sync_state_table):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "get_sync_state_table", lambda: mock_sync_state_table)

    def test_creates_sync_state_running(self, sync_state_items, mock_context):
        sync.lambda_handler({"mode": "full-sync"}, mock_context)

        assert len(sync_state_items) == 1
        state = sync_state_items[0]
        assert state["mode"] == "full-sync"
        assert state["status"] == "completed"
        assert state["sync_id"].startswith("full-sync-")
        assert "started_at" in state
        assert "completed_at" in state

    def test_counts_files_processed(
        self, sync_state_items, mock_context, ddb_items, create_shared_file, create_shared_folder, managed_folder
    ):
        shared_folder = create_shared_folder(parent_folder=managed_folder)
        create_shared_file(parent_folder=shared_folder)
        create_shared_file(parent_folder=shared_folder)

        sync.lambda_handler({"mode": "full-sync"}, mock_context)

        assert sync_state_items[0]["items_checked"] == 2

    def test_counts_orphans_deleted(self, sync_state_items, mock_context, ddb_items):
        ddb_items.append(
            {"filepath": "stale/file.dat", "box_file_id": "999", "download_url": "https://box.com/dl/stale"}
        )

        sync.lambda_handler({"mode": "full-sync"}, mock_context)

        assert sync_state_items[0]["items_repaired"] >= 1

    def test_includes_expires_at(self, sync_state_items, mock_context):
        sync.lambda_handler({"mode": "full-sync"}, mock_context)

        state = sync_state_items[0]
        assert "expires_at" in state
        # expires_at should be ~30 days from now (epoch seconds)
        now_epoch = datetime.now(timezone.utc).timestamp()
        thirty_days = 30 * 24 * 3600
        assert abs(state["expires_at"] - (now_epoch + thirty_days)) < 60


class TestReconciliationScaffold:
    """Tests for reconciliation handler basics (AC #7, #11)."""

    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch, mock_sync_state_table, mock_ddb_table):
        monkeypatch.setattr(common, "get_sync_state_table", lambda: mock_sync_state_table)
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)

    def test_creates_sync_state_and_completes(self, sync_state_items, mock_context):
        sync.lambda_handler({"mode": "reconciliation"}, mock_context)

        assert len(sync_state_items) == 1
        state = sync_state_items[0]
        assert state["mode"] == "reconciliation"
        assert state["status"] == "completed"
        assert state["sync_id"].startswith("reconciliation-")
        assert "started_at" in state
        assert "completed_at" in state

    def test_includes_expires_at(self, sync_state_items, mock_context):
        sync.lambda_handler({"mode": "reconciliation"}, mock_context)

        assert "expires_at" in sync_state_items[0]

    def test_no_mode_not_implemented_log(self, capture_log, mock_context):
        sync.lambda_handler({"mode": "reconciliation"}, mock_context)

        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "mode_not_implemented" not in actions


class TestSyncStateHelpers:
    """Tests for shared _create_sync_state and _complete_sync_state (AC #3, #4, #5)."""

    def test_create_sync_state_writes_initial_record(self, mock_sync_state_table, sync_state_items):
        sync_id = sync._create_sync_state(mock_sync_state_table, "drain-queue")

        assert sync_id.startswith("drain-queue-")
        assert len(sync_state_items) == 1
        state = sync_state_items[0]
        assert state["status"] == "running"
        assert state["mode"] == "drain-queue"
        assert state["items_checked"] == 0
        assert state["items_repaired"] == 0
        assert "started_at" in state

    def test_create_sync_state_includes_expires_at(self, mock_sync_state_table, sync_state_items):
        sync._create_sync_state(mock_sync_state_table, "full-sync")

        state = sync_state_items[0]
        assert "expires_at" in state
        now_epoch = datetime.now(timezone.utc).timestamp()
        thirty_days = 30 * 24 * 3600
        assert abs(state["expires_at"] - (now_epoch + thirty_days)) < 60

    def test_complete_sync_state_updates_correctly(self, mock_sync_state_table, sync_state_items):
        sync_id = sync._create_sync_state(mock_sync_state_table, "drain-queue")

        sync._complete_sync_state(mock_sync_state_table, sync_id, {"items_checked": 5, "items_repaired": 2})

        state = sync_state_items[0]
        assert state["status"] == "completed"
        assert state["items_checked"] == 5
        assert state["items_repaired"] == 2
        assert "completed_at" in state


class TestDrainQueueExpiresAt:
    """Test that drain-queue sync state includes expires_at after refactor (Task 9)."""

    @pytest.fixture(autouse=True)
    def setup_drain(self, monkeypatch, mock_ddb_table, mock_validation_queue_table, mock_sync_state_table):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_validation_queue_table", lambda: mock_validation_queue_table)
        monkeypatch.setattr(common, "get_sync_state_table", lambda: mock_sync_state_table)

    def test_drain_queue_sync_state_includes_expires_at(self, sync_state_items, mock_context):
        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert "expires_at" in sync_state_items[0]
        now_epoch = datetime.now(timezone.utc).timestamp()
        thirty_days = 30 * 24 * 3600
        assert abs(sync_state_items[0]["expires_at"] - (now_epoch + thirty_days)) < 60


class TestValidateStatKeys:
    """Tests for _validate_stat_keys helper."""

    def test_accepts_valid_keys(self):
        sync._validate_stat_keys({"items_checked": 0, "items_repaired": 0, "items_valid": 0})

    def test_rejects_key_with_spaces(self):
        with pytest.raises(ValueError, match="Invalid stat key"):
            sync._validate_stat_keys({"items checked": 0})

    def test_rejects_empty_key(self):
        with pytest.raises(ValueError, match="Invalid stat key"):
            sync._validate_stat_keys({"": 0})

    def test_rejects_key_with_special_chars(self):
        with pytest.raises(ValueError, match="Invalid stat key"):
            sync._validate_stat_keys({"items-repaired": 0})


class TestReconciliation:
    """Tests for reconciliation sweep (Story 3.2)."""

    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch, mock_ddb_table, mock_sync_state_table):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_sync_state_table", lambda: mock_sync_state_table)

    @pytest.fixture
    def data_folder(self, create_folder, managed_folder):
        return create_folder(parent_folder=managed_folder, name="data")

    # --- Task 5: Happy path (AC #1, #2, #3, #4, #10) ---

    def test_scans_and_validates_stale(self, monkeypatch, ddb_items, sync_state_items, mock_context, capture_log):
        ddb_items.append(
            {"filepath": "data/stale.fits", "box_file_id": "111", "download_url": "https://box.com/dl/stale"}
        )
        monkeypatch.setattr(common, "validate_download_url", lambda url: "valid")

        sync.lambda_handler({"mode": "reconciliation"}, mock_context)

        assert "last_validated" in ddb_items[0]
        state = sync_state_items[-1]
        assert state["status"] == "completed"
        assert state["items_checked"] == 1
        assert state["items_valid"] == 1
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "reconciliation_validate_valid" in actions

    def test_skips_fresh_entries(self, monkeypatch, ddb_items, sync_state_items, mock_context):
        fresh_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        ddb_items.append(
            {
                "filepath": "data/fresh.fits",
                "box_file_id": "222",
                "download_url": "https://box.com/dl/fresh",
                "last_validated": fresh_ts,
            }
        )
        validate_called = {"value": False}

        def spy(url):
            validate_called["value"] = True
            return "valid"

        monkeypatch.setattr(common, "validate_download_url", spy)

        sync.lambda_handler({"mode": "reconciliation"}, mock_context)

        assert validate_called["value"] is False
        assert sync_state_items[-1]["items_checked"] == 0

    def test_valid_urls_get_last_validated_stamped(self, monkeypatch, ddb_items, mock_context):
        ddb_items.append(
            {"filepath": "data/check.fits", "box_file_id": "333", "download_url": "https://box.com/dl/check"}
        )
        monkeypatch.setattr(common, "validate_download_url", lambda url: "valid")

        sync.lambda_handler({"mode": "reconciliation"}, mock_context)

        assert "last_validated" in ddb_items[0]
        ts = datetime.fromisoformat(ddb_items[0]["last_validated"])
        assert ts.tzinfo is not None

    def test_zero_box_api_for_valid_urls(self, monkeypatch, ddb_items, mock_context):
        """AC #10: valid URLs only trigger Range GETs, no Box API calls."""
        ddb_items.append(
            {"filepath": "data/valid.fits", "box_file_id": "444", "download_url": "https://box.com/dl/valid"}
        )
        monkeypatch.setattr(common, "validate_download_url", lambda url: "valid")
        box_calls = []
        monkeypatch.setattr(common, "get_box_client", lambda: box_calls.append("called") or MagicMock())

        sync.lambda_handler({"mode": "reconciliation"}, mock_context)

        assert len(box_calls) == 0

    # --- Task 6: Broken URL repair (AC #5, #6) ---

    def test_broken_url_triggers_repair(
        self,
        monkeypatch,
        ddb_items,
        sync_state_items,
        mock_context,
        capture_log,
        create_shared_file,
        data_folder,
        mock_box_client,
    ):
        ddb_items.append(
            {"filepath": "data/broken.fits", "box_file_id": "111", "download_url": "https://box.com/dl/old"}
        )
        create_shared_file(parent_folder=data_folder, id="111", name="broken.fits")
        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))

        sync.lambda_handler({"mode": "reconciliation"}, mock_context)

        assert len(ddb_items) == 1
        assert "last_validated" in ddb_items[0]
        assert ddb_items[0]["download_url"] != "https://box.com/dl/old"
        assert sync_state_items[-1]["items_repaired"] == 1
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "reconciliation_validate_broken" in actions

    def test_permanently_missing_deletes_manifest(
        self, monkeypatch, ddb_items, sync_state_items, mock_context, capture_log
    ):
        ddb_items.append(
            {"filepath": "data/gone.fits", "box_file_id": "999", "download_url": "https://box.com/dl/gone"}
        )
        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")
        monkeypatch.setattr(common, "get_box_client", lambda: MagicMock())
        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))
        monkeypatch.setattr(common, "get_file", lambda client, fid: None)

        sync.lambda_handler({"mode": "reconciliation"}, mock_context)

        assert len(ddb_items) == 0
        assert sync_state_items[-1]["items_repaired"] == 1
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "file_not_found" in actions

    def test_repair_failure_does_not_increment_repaired(
        self, monkeypatch, ddb_items, sync_state_items, mock_context, capture_log
    ):
        ddb_items.append(
            {"filepath": "data/fail.fits", "box_file_id": "999", "download_url": "https://box.com/dl/fail"}
        )
        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")

        def exploding_client():
            raise Exception("boom")

        monkeypatch.setattr(common, "get_box_client", exploding_client)

        sync.lambda_handler({"mode": "reconciliation"}, mock_context)

        assert sync_state_items[-1]["items_repaired"] == 0
        assert sync_state_items[-1]["items_checked"] == 1
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "repair_failed" in actions

    def test_items_repaired_counts_repaired_and_deleted(
        self,
        monkeypatch,
        ddb_items,
        sync_state_items,
        mock_context,
        create_shared_file,
        data_folder,
        mock_box_client,
    ):
        # One repairable, one permanently gone
        ddb_items.append({"filepath": "data/fix.fits", "box_file_id": "100", "download_url": "https://box.com/dl/fix"})
        ddb_items.append(
            {"filepath": "data/gone.fits", "box_file_id": "200", "download_url": "https://box.com/dl/gone"}
        )
        create_shared_file(parent_folder=data_folder, id="100", name="fix.fits")
        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))
        original_get_file = common.get_file

        def selective_get_file(client, fid):
            if fid == "200":
                return None
            return original_get_file(client, fid)

        monkeypatch.setattr(common, "get_file", selective_get_file)

        sync.lambda_handler({"mode": "reconciliation"}, mock_context)

        assert sync_state_items[-1]["items_repaired"] == 2

    # --- Task 7: Uncertain results (AC #7) ---

    def test_uncertain_skips_stamping_and_logs(
        self, monkeypatch, ddb_items, sync_state_items, mock_context, capture_log
    ):
        ddb_items.append(
            {"filepath": "data/uncertain.fits", "box_file_id": "555", "download_url": "https://box.com/dl/unc"}
        )
        monkeypatch.setattr(common, "validate_download_url", lambda url: "uncertain")

        sync.lambda_handler({"mode": "reconciliation"}, mock_context)

        assert "last_validated" not in ddb_items[0]
        assert sync_state_items[-1]["items_checked"] == 1
        assert sync_state_items[-1]["items_repaired"] == 0
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "reconciliation_validate_uncertain" in actions

    # --- Task 8: Timeout + checkpoint (AC #8, #9) ---

    def test_timeout_writes_partial_state(self, monkeypatch, ddb_items, sync_state_items, mock_context, capture_log):
        for i in range(5):
            ddb_items.append(
                {"filepath": f"data/file{i}.fits", "box_file_id": str(i), "download_url": f"https://box.com/dl/{i}"}
            )
        monkeypatch.setattr(common, "validate_download_url", lambda url: "valid")

        # Allow 2 items to process, then trigger timeout on the 3rd
        call_count = {"n": 0}

        class TimeoutContext:
            def get_remaining_time_in_millis(self):
                call_count["n"] += 1
                if call_count["n"] <= 2:
                    return 900000  # plenty of time
                return 30000  # below 50000 threshold

        sync.lambda_handler({"mode": "reconciliation"}, TimeoutContext())

        # Should have partial status checkpointed at last processed item
        state = sync_state_items[-1]
        assert state["status"] == "partial"
        assert "last_filepath" in state
        assert state["last_filepath"] == "data/file1.fits"  # last item completed before timeout
        assert state["items_checked"] == 2
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "reconciliation_timeout" in actions
        # reconciliation_complete should NOT be logged on timeout
        assert "reconciliation_complete" not in actions

    def test_immediate_timeout_no_partial_state(self, monkeypatch, ddb_items, sync_state_items, capture_log):
        """If timeout hits before any item is processed, no partial checkpoint is written."""
        ddb_items.append({"filepath": "data/file0.fits", "box_file_id": "0", "download_url": "https://box.com/dl/0"})

        class TimeoutContext:
            def get_remaining_time_in_millis(self):
                return 30000

        sync.lambda_handler({"mode": "reconciliation"}, TimeoutContext())

        # sync_state should exist but not be partial (no checkpoint to write)
        state = sync_state_items[-1]
        assert state["status"] == "running"  # never updated from initial state
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "reconciliation_timeout" in actions

    def test_resume_from_checkpoint(self, monkeypatch, ddb_items, sync_state_items, mock_context, capture_log):
        # Pre-insert a partial checkpoint
        sync_state_items.append(
            {
                "sync_id": "reconciliation-2026-03-16T12:00:00",
                "mode": "reconciliation",
                "status": "partial",
                "started_at": "2026-03-16T12:00:00+00:00",
                "last_filepath": "data/file2.fits",
                "items_checked": 2,
                "items_valid": 2,
                "items_repaired": 0,
                "expires_at": 9999999999,
            }
        )
        # Add items — only items after "data/file2.fits" should be processed
        for i in range(5):
            ddb_items.append(
                {"filepath": f"data/file{i}.fits", "box_file_id": str(i), "download_url": f"https://box.com/dl/{i}"}
            )
        monkeypatch.setattr(common, "validate_download_url", lambda url: "valid")

        sync.lambda_handler({"mode": "reconciliation"}, mock_context)

        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "reconciliation_resume" in actions
        # The new sync state (last item) should reflect only items after checkpoint
        new_state = sync_state_items[-1]
        assert new_state["status"] == "completed"
        assert new_state["items_checked"] == 2  # file3 and file4

    def test_invalid_checkpoint_falls_back(self, monkeypatch, ddb_items, sync_state_items, mock_context, capture_log):
        # Partial checkpoint with filepath that doesn't exist in manifest
        sync_state_items.append(
            {
                "sync_id": "reconciliation-2026-03-16T12:00:00",
                "mode": "reconciliation",
                "status": "partial",
                "started_at": "2026-03-16T12:00:00+00:00",
                "last_filepath": "nonexistent/path.fits",
                "items_checked": 0,
                "expires_at": 9999999999,
            }
        )
        ddb_items.append({"filepath": "data/file.fits", "box_file_id": "111", "download_url": "https://box.com/dl/111"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "valid")

        sync.lambda_handler({"mode": "reconciliation"}, mock_context)

        # Should still process all items (mock starts from 0 when key not found)
        new_state = sync_state_items[-1]
        assert new_state["status"] == "completed"
        assert new_state["items_checked"] == 1

    # --- Task 9: Empty manifest ---

    def test_empty_manifest_completes(self, sync_state_items, mock_context):
        sync.lambda_handler({"mode": "reconciliation"}, mock_context)

        state = sync_state_items[-1]
        assert state["status"] == "completed"
        assert state["items_checked"] == 0
        assert state["items_repaired"] == 0

    # --- Pagination ---

    def test_pagination_across_batches(self, monkeypatch, ddb_items, sync_state_items, mock_context):
        # BATCH_SIZE is 5 in MockTable, so 7 items forces pagination
        for i in range(7):
            ddb_items.append(
                {"filepath": f"data/page{i}.fits", "box_file_id": str(i), "download_url": f"https://box.com/dl/{i}"}
            )
        monkeypatch.setattr(common, "validate_download_url", lambda url: "valid")

        sync.lambda_handler({"mode": "reconciliation"}, mock_context)

        assert sync_state_items[-1]["items_checked"] == 7
        for item in ddb_items:
            assert "last_validated" in item

    # --- Mixed outcomes ---

    def test_mixed_outcomes(
        self,
        monkeypatch,
        ddb_items,
        sync_state_items,
        mock_context,
        capture_log,
        create_shared_file,
        data_folder,
        mock_box_client,
    ):
        fresh_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        # fresh (skip)
        ddb_items.append(
            {
                "filepath": "data/fresh.fits",
                "box_file_id": "10",
                "download_url": "https://box.com/dl/fresh",
                "last_validated": fresh_ts,
            }
        )
        # valid
        ddb_items.append({"filepath": "data/ok.fits", "box_file_id": "20", "download_url": "https://box.com/dl/ok"})
        # broken → repaired
        ddb_items.append({"filepath": "data/fix.fits", "box_file_id": "30", "download_url": "https://box.com/dl/fix"})
        create_shared_file(parent_folder=data_folder, id="30", name="fix.fits")
        # uncertain
        ddb_items.append(
            {"filepath": "data/maybe.fits", "box_file_id": "40", "download_url": "https://box.com/dl/maybe"}
        )

        url_outcomes = {
            "https://box.com/dl/ok": "valid",
            "https://box.com/dl/fix": "broken",
            "https://box.com/dl/maybe": "uncertain",
        }
        monkeypatch.setattr(common, "validate_download_url", lambda url: url_outcomes[url])
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))

        sync.lambda_handler({"mode": "reconciliation"}, mock_context)

        state = sync_state_items[-1]
        assert state["items_checked"] == 3  # ok, fix, maybe (fresh skipped)
        assert state["items_valid"] == 1
        assert state["items_repaired"] == 1


class TestPartialSyncState:
    """Tests for _partial_sync_state helper."""

    def test_writes_partial_with_checkpoint(self, mock_sync_state_table, sync_state_items):
        sync_id = sync._create_sync_state(mock_sync_state_table, "reconciliation")

        sync._partial_sync_state(
            mock_sync_state_table,
            sync_id,
            "data/checkpoint.fits",
            {"items_checked": 5, "items_valid": 3, "items_repaired": 1},
        )

        state = sync_state_items[0]
        assert state["status"] == "partial"
        assert state["last_filepath"] == "data/checkpoint.fits"
        assert state["items_checked"] == 5
        assert state["items_valid"] == 3
        assert state["items_repaired"] == 1


class TestFindReconciliationCheckpoint:
    """Tests for _find_reconciliation_checkpoint helper."""

    def test_returns_none_when_no_partial(self, mock_sync_state_table, sync_state_items):
        # Add a completed reconciliation — should not be returned
        sync_state_items.append(
            {
                "sync_id": "reconciliation-2026-03-16T12:00:00",
                "mode": "reconciliation",
                "status": "completed",
                "started_at": "2026-03-16T12:00:00+00:00",
            }
        )
        result = sync._find_reconciliation_checkpoint(mock_sync_state_table)
        assert result is None

    def test_returns_last_filepath_from_partial(self, mock_sync_state_table, sync_state_items):
        sync_state_items.append(
            {
                "sync_id": "reconciliation-2026-03-16T12:00:00",
                "mode": "reconciliation",
                "status": "partial",
                "started_at": "2026-03-16T12:00:00+00:00",
                "last_filepath": "data/resume.fits",
            }
        )
        result = sync._find_reconciliation_checkpoint(mock_sync_state_table)
        assert result == "data/resume.fits"

    def test_returns_most_recent_partial(self, mock_sync_state_table, sync_state_items):
        sync_state_items.append(
            {
                "sync_id": "reconciliation-2026-03-15T12:00:00",
                "mode": "reconciliation",
                "status": "partial",
                "started_at": "2026-03-15T12:00:00+00:00",
                "last_filepath": "data/old.fits",
            }
        )
        sync_state_items.append(
            {
                "sync_id": "reconciliation-2026-03-16T12:00:00",
                "mode": "reconciliation",
                "status": "partial",
                "started_at": "2026-03-16T12:00:00+00:00",
                "last_filepath": "data/new.fits",
            }
        )
        result = sync._find_reconciliation_checkpoint(mock_sync_state_table)
        assert result == "data/new.fits"

    def test_ignores_non_reconciliation_partial(self, mock_sync_state_table, sync_state_items):
        sync_state_items.append(
            {
                "sync_id": "drain-queue-2026-03-16T12:00:00",
                "mode": "drain-queue",
                "status": "partial",
                "started_at": "2026-03-16T12:00:00+00:00",
                "last_filepath": "data/drain.fits",
            }
        )
        result = sync._find_reconciliation_checkpoint(mock_sync_state_table)
        assert result is None


class TestFullSyncCheckpoint:
    """Tests for resumable full-sync (Story 3.3)."""

    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch, mock_ddb_table, mock_box_client, mock_sync_state_table):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "get_sync_state_table", lambda: mock_sync_state_table)

    # --- Task 6: Enumeration + chunking (AC #1, #6) ---

    def test_enumeration_stores_chunks(
        self, sync_state_items, mock_context, create_shared_file, create_shared_folder, managed_folder
    ):
        shared_folder = create_shared_folder(parent_folder=managed_folder)
        for _ in range(3):
            create_shared_file(parent_folder=shared_folder)

        sync.lambda_handler({"mode": "full-sync"}, mock_context)

        # Find the main sync state record
        main_state = next(
            s for s in sync_state_items if s["sync_id"].startswith("full-sync-") and ":chunk:" not in s["sync_id"]
        )
        assert "chunk_count" in main_state
        assert main_state["chunk_count"] >= 1

    def test_chunks_cleaned_up_after_completion(
        self, sync_state_items, mock_context, create_shared_file, create_shared_folder, managed_folder
    ):
        shared_folder = create_shared_folder(parent_folder=managed_folder)
        create_shared_file(parent_folder=shared_folder)

        sync.lambda_handler({"mode": "full-sync"}, mock_context)

        # After completion, chunk items should be deleted
        chunk_items = [s for s in sync_state_items if ":chunk:" in s.get("sync_id", "")]
        assert len(chunk_items) == 0

    def test_empty_enumeration_no_chunks(self, sync_state_items, mock_context):
        sync.lambda_handler({"mode": "full-sync"}, mock_context)

        # No files → no chunks stored
        chunk_items = [s for s in sync_state_items if ":chunk:" in s.get("sync_id", "")]
        assert len(chunk_items) == 0
        main_state = next(
            s for s in sync_state_items if s["sync_id"].startswith("full-sync-") and ":chunk:" not in s["sync_id"]
        )
        assert main_state["chunk_count"] == 0

    # --- Task 7: Timeout + self-invocation (AC #2, #8) ---

    def test_timeout_triggers_checkpoint_and_self_invoke(
        self,
        monkeypatch,
        sync_state_items,
        create_shared_file,
        create_shared_folder,
        managed_folder,
        capture_log,
    ):
        shared_folder = create_shared_folder(parent_folder=managed_folder)
        for _ in range(5):
            create_shared_file(parent_folder=shared_folder)

        call_count = {"n": 0}

        class TimeoutContext:
            function_name = "test-sync-function"

            def get_remaining_time_in_millis(self):
                call_count["n"] += 1
                if call_count["n"] <= 2:
                    return 900000
                return 30000

        invoke_calls = []

        class MockLambdaClient:
            def invoke(self, **kwargs):
                invoke_calls.append(kwargs)
                return {"StatusCode": 202}

        monkeypatch.setattr("boto3.client", lambda svc: MockLambdaClient())

        sync.lambda_handler({"mode": "full-sync"}, TimeoutContext())

        # Should have saved partial state
        main_state = next(
            (s for s in sync_state_items if s["sync_id"].startswith("full-sync-") and ":chunk:" not in s["sync_id"]),
            None,
        )
        assert main_state is not None
        assert main_state["status"] == "partial"
        assert "last_chunk_index" in main_state
        assert "last_file_index" in main_state

        # Should have invoked self
        assert len(invoke_calls) == 1
        payload = json.loads(invoke_calls[0]["Payload"])
        assert payload["mode"] == "full-sync"
        assert payload["resume"] is True
        assert payload["chain_depth"] == 1
        assert "sync_id" in payload

        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "full_sync_timeout" in actions
        assert "full_sync_self_invoke" in actions

    def test_chain_depth_guard_prevents_invocation(
        self,
        monkeypatch,
        sync_state_items,
        create_shared_file,
        create_shared_folder,
        managed_folder,
        mock_box_client,
        capture_log,
    ):
        shared_folder = create_shared_folder(parent_folder=managed_folder)
        created_files = []
        for _ in range(3):
            created_files.append(create_shared_file(parent_folder=shared_folder))

        # Set up a sync state for resume at max chain depth
        sync_id = "full-sync-2026-03-18T12:00:00"
        files = [{"filepath": common.get_filepath(f), "box_file_id": f.id, "shared": True} for f in created_files]
        sync_state_items.append(
            {
                "sync_id": sync_id,
                "mode": "full-sync",
                "status": "partial",
                "started_at": "2026-03-18T12:00:00+00:00",
                "chunk_count": 1,
                "last_chunk_index": 0,
                "last_file_index": -1,
                "chain_depth": 9,
                "items_checked": 0,
                "items_valid": 0,
                "items_repaired": 0,
                "expires_at": 9999999999,
            }
        )
        sync_state_items.append(
            {
                "sync_id": f"{sync_id}:chunk:0",
                "files": files,
                "expires_at": 9999999999,
            }
        )

        # Allow one file to process, then timeout
        call_count = {"n": 0}

        class TimeoutContext:
            function_name = "test-sync-function"

            def get_remaining_time_in_millis(self):
                call_count["n"] += 1
                if call_count["n"] <= 1:
                    return 900000
                return 30000

        invoke_calls = []

        class MockLambdaClient:
            def invoke(self, **kwargs):
                invoke_calls.append(kwargs)

        monkeypatch.setattr("boto3.client", lambda svc: MockLambdaClient())
        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))

        sync.lambda_handler(
            {"mode": "full-sync", "sync_id": sync_id, "resume": True, "chain_depth": 9},
            TimeoutContext(),
        )

        # Should NOT self-invoke
        assert len(invoke_calls) == 0

        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "full_sync_chain_depth_exceeded" in actions

        # Should mark failed and clean up chunks
        main_state = next(s for s in sync_state_items if s["sync_id"] == sync_id)
        assert main_state["status"] == "failed"
        chunk_items = [s for s in sync_state_items if ":chunk:" in s.get("sync_id", "")]
        assert len(chunk_items) == 0

    def test_self_invoke_payload_correct(
        self,
        monkeypatch,
        sync_state_items,
        create_shared_file,
        create_shared_folder,
        managed_folder,
    ):
        shared_folder = create_shared_folder(parent_folder=managed_folder)
        for _ in range(3):
            create_shared_file(parent_folder=shared_folder)

        call_count = {"n": 0}

        class TimeoutContext:
            function_name = "test-sync-function"

            def get_remaining_time_in_millis(self):
                call_count["n"] += 1
                if call_count["n"] <= 1:
                    return 900000
                return 30000

        invoke_calls = []

        class MockLambdaClient:
            def invoke(self, **kwargs):
                invoke_calls.append(kwargs)
                return {"StatusCode": 202}

        monkeypatch.setattr("boto3.client", lambda svc: MockLambdaClient())

        sync.lambda_handler({"mode": "full-sync"}, TimeoutContext())

        assert len(invoke_calls) == 1
        assert invoke_calls[0]["InvocationType"] == "Event"
        payload = json.loads(invoke_calls[0]["Payload"])
        assert payload["mode"] == "full-sync"
        assert payload["resume"] is True
        assert payload["chain_depth"] == 1
        assert payload["sync_id"].startswith("full-sync-")

    # --- Task 8: Resume from checkpoint (AC #3, #4, #7) ---

    def test_resume_loads_chunks_and_skips_to_checkpoint(
        self,
        monkeypatch,
        sync_state_items,
        ddb_items,
        mock_context,
        capture_log,
        mock_box_client,
        create_shared_file,
        create_shared_folder,
        managed_folder,
    ):
        shared_folder = create_shared_folder(parent_folder=managed_folder)
        files = []
        for i in range(5):
            f = create_shared_file(parent_folder=shared_folder)
            files.append({"filepath": common.get_filepath(f), "box_file_id": f.id, "shared": True})

        sync_id = "full-sync-2026-03-18T12:00:00"
        sync_state_items.append(
            {
                "sync_id": sync_id,
                "mode": "full-sync",
                "status": "partial",
                "started_at": "2026-03-18T12:00:00+00:00",
                "chunk_count": 1,
                "last_chunk_index": 0,
                "last_file_index": 1,
                "chain_depth": 1,
                "items_checked": 2,
                "items_valid": 2,
                "items_repaired": 0,
                "expires_at": 9999999999,
            }
        )
        sync_state_items.append(
            {
                "sync_id": f"{sync_id}:chunk:0",
                "files": files,
                "expires_at": 9999999999,
            }
        )

        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))

        sync.lambda_handler(
            {"mode": "full-sync", "sync_id": sync_id, "resume": True, "chain_depth": 2},
            mock_context,
        )

        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "full_sync_resume" in actions
        assert "full_sync_complete" in actions

        # Should have processed remaining 3 files (indices 2, 3, 4)
        main_state = next(s for s in sync_state_items if s["sync_id"] == sync_id)
        assert main_state["status"] == "completed"
        assert main_state["items_checked"] == 5  # 2 from before + 3 new

    def test_resume_processes_remaining_files(
        self,
        monkeypatch,
        sync_state_items,
        ddb_items,
        mock_context,
        mock_box_client,
        create_shared_file,
        create_shared_folder,
        managed_folder,
    ):
        shared_folder = create_shared_folder(parent_folder=managed_folder)
        created_files = []
        for _ in range(4):
            f = create_shared_file(parent_folder=shared_folder)
            created_files.append(f)

        files = [{"filepath": common.get_filepath(f), "box_file_id": f.id, "shared": True} for f in created_files]

        sync_id = "full-sync-2026-03-18T12:00:00"
        sync_state_items.append(
            {
                "sync_id": sync_id,
                "mode": "full-sync",
                "status": "partial",
                "started_at": "2026-03-18T12:00:00+00:00",
                "chunk_count": 1,
                "last_chunk_index": 0,
                "last_file_index": 1,
                "chain_depth": 0,
                "items_checked": 2,
                "items_valid": 2,
                "items_repaired": 0,
                "expires_at": 9999999999,
            }
        )
        sync_state_items.append(
            {
                "sync_id": f"{sync_id}:chunk:0",
                "files": files,
                "expires_at": 9999999999,
            }
        )

        # Pre-populate DDB with entries from the "previous invocation" (files 0 and 1)
        for f in created_files[:2]:
            ddb_items.append(common.make_ddb_item(f))

        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))

        sync.lambda_handler(
            {"mode": "full-sync", "sync_id": sync_id, "resume": True, "chain_depth": 1},
            mock_context,
        )

        # All 4 files should now be in DDB (2 pre-existing + 2 newly processed)
        assert len(ddb_items) == 4
        file_ids = {item["box_file_id"] for item in ddb_items}
        for f in created_files:
            assert f.id in file_ids

    def test_invalid_checkpoint_falls_back_to_fresh(
        self,
        monkeypatch,
        sync_state_items,
        ddb_items,
        mock_context,
        capture_log,
    ):
        sync_id = "full-sync-2026-03-18T12:00:00"
        # Sync state exists but chunks are missing
        sync_state_items.append(
            {
                "sync_id": sync_id,
                "mode": "full-sync",
                "status": "partial",
                "started_at": "2026-03-18T12:00:00+00:00",
                "chunk_count": 2,
                "last_chunk_index": 0,
                "last_file_index": 5,
                "chain_depth": 1,
                "items_checked": 0,
                "expires_at": 9999999999,
            }
        )
        # No chunk items → chunks missing

        sync.lambda_handler(
            {"mode": "full-sync", "sync_id": sync_id, "resume": True, "chain_depth": 2},
            mock_context,
        )

        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "full_sync_chunks_missing" in actions
        assert "full_sync_enumerating" in actions
        assert "full_sync_complete" in actions

    # --- Task 9: Orphan detection across chain (AC #5, #9, #10) ---

    def test_orphan_detection_on_final_invocation(
        self,
        monkeypatch,
        sync_state_items,
        ddb_items,
        mock_context,
        mock_box_client,
        create_shared_file,
        create_shared_folder,
        managed_folder,
        capture_log,
    ):
        shared_folder = create_shared_folder(parent_folder=managed_folder)
        f = create_shared_file(parent_folder=shared_folder)

        # Add orphan to manifest
        ddb_items.append(
            {"filepath": "orphan/gone.dat", "box_file_id": "999", "download_url": "https://box.com/dl/gone"}
        )

        files = [{"filepath": common.get_filepath(f), "box_file_id": f.id, "shared": True}]

        sync_id = "full-sync-2026-03-18T12:00:00"
        sync_state_items.append(
            {
                "sync_id": sync_id,
                "mode": "full-sync",
                "status": "partial",
                "started_at": "2026-03-18T12:00:00+00:00",
                "chunk_count": 1,
                "last_chunk_index": 0,
                "last_file_index": -1,
                "chain_depth": 0,
                "items_checked": 0,
                "items_valid": 0,
                "items_repaired": 0,
                "expires_at": 9999999999,
            }
        )
        sync_state_items.append(
            {
                "sync_id": f"{sync_id}:chunk:0",
                "files": files,
                "expires_at": 9999999999,
            }
        )

        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))

        sync.lambda_handler(
            {"mode": "full-sync", "sync_id": sync_id, "resume": True, "chain_depth": 1},
            mock_context,
        )

        # Orphan should be deleted
        filepaths = {item["filepath"] for item in ddb_items}
        assert "orphan/gone.dat" not in filepaths

        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "full_sync_orphan_detection" in actions
        assert "full_sync_orphans_deleted" in actions

    def test_fresh_entries_tracked_for_orphans_but_not_repaired(
        self,
        monkeypatch,
        sync_state_items,
        ddb_items,
        mock_context,
        mock_box_client,
        create_shared_file,
        create_shared_folder,
        managed_folder,
    ):
        shared_folder = create_shared_folder(parent_folder=managed_folder)
        f = create_shared_file(parent_folder=shared_folder)
        filepath = common.get_filepath(f)

        # File already in manifest with fresh last_validated
        fresh_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        ddb_items.append(
            {
                "filepath": filepath,
                "box_file_id": f.id,
                "download_url": f.shared_link["download_url"],
                "last_validated": fresh_ts,
            }
        )

        # Add orphan to verify it gets deleted
        ddb_items.append(
            {"filepath": "orphan/stale.dat", "box_file_id": "888", "download_url": "https://box.com/dl/stale"}
        )

        sync.lambda_handler({"mode": "full-sync"}, mock_context)

        # Fresh file should still be in manifest (not deleted by orphan detection)
        filepaths = {item["filepath"] for item in ddb_items}
        assert filepath in filepaths
        # Orphan should be deleted
        assert "orphan/stale.dat" not in filepaths

        # The fresh file's last_validated should be unchanged (wasn't re-processed)
        fresh_item = next(i for i in ddb_items if i["filepath"] == filepath)
        assert fresh_item["last_validated"] == fresh_ts

    def test_new_file_discovered_during_processing(
        self,
        monkeypatch,
        sync_state_items,
        ddb_items,
        mock_context,
        mock_box_client,
        create_shared_file,
        create_shared_folder,
        managed_folder,
    ):
        """AC #10: New files found in Box but not in ManifestTable get shared links and entries."""
        shared_folder = create_shared_folder(parent_folder=managed_folder)
        new_file = create_shared_file(parent_folder=shared_folder)
        # File exists in Box but NOT in DDB — should be discovered and added

        sync.lambda_handler({"mode": "full-sync"}, mock_context)

        assert len(ddb_items) == 1
        assert ddb_items[0]["box_file_id"] == new_file.id


class TestFullSyncChunkStoreFailure:
    """Test degraded behavior when chunk storage fails."""

    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch, mock_ddb_table, mock_box_client, mock_sync_state_table):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "get_sync_state_table", lambda: mock_sync_state_table)

    def test_chunk_store_failure_disables_self_invoke(
        self,
        monkeypatch,
        sync_state_items,
        ddb_items,
        create_shared_file,
        create_shared_folder,
        managed_folder,
        capture_log,
    ):
        shared_folder = create_shared_folder(parent_folder=managed_folder)
        for _ in range(3):
            create_shared_file(parent_folder=shared_folder)

        # Make chunk storage fail
        def fail_store(*args, **kwargs):
            raise RuntimeError("DDB write failed")

        monkeypatch.setattr(sync, "_store_file_list_chunks", fail_store)

        call_count = {"n": 0}

        class TimeoutContext:
            function_name = "test-sync-function"

            def get_remaining_time_in_millis(self):
                call_count["n"] += 1
                if call_count["n"] <= 1:
                    return 900000
                return 30000

        invoke_calls = []

        class MockLambdaClient:
            def invoke(self, **kwargs):
                invoke_calls.append(kwargs)

        monkeypatch.setattr("boto3.client", lambda svc: MockLambdaClient())

        sync.lambda_handler({"mode": "full-sync"}, TimeoutContext())

        # No self-invocation should occur
        assert len(invoke_calls) == 0

        # Files processed before timeout should still be in DDB
        assert len(ddb_items) >= 1

        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "full_sync_chunk_store_failed" in actions


class TestFullSyncHelpers:
    """Tests for full-sync helper functions."""

    def test_find_full_sync_checkpoint_returns_none_when_empty(self, mock_sync_state_table):
        result = sync._find_full_sync_checkpoint(mock_sync_state_table)
        assert result is None

    def test_find_full_sync_checkpoint_returns_partial_record(self, mock_sync_state_table, sync_state_items):
        sync_state_items.append(
            {
                "sync_id": "full-sync-2026-03-18T12:00:00",
                "mode": "full-sync",
                "status": "partial",
                "started_at": "2026-03-18T12:00:00+00:00",
                "chunk_count": 2,
                "last_chunk_index": 1,
                "last_file_index": 50,
            }
        )
        result = sync._find_full_sync_checkpoint(mock_sync_state_table)
        assert result is not None
        assert result["sync_id"] == "full-sync-2026-03-18T12:00:00"

    def test_find_full_sync_checkpoint_ignores_reconciliation(self, mock_sync_state_table, sync_state_items):
        sync_state_items.append(
            {
                "sync_id": "reconciliation-2026-03-18T12:00:00",
                "mode": "reconciliation",
                "status": "partial",
                "started_at": "2026-03-18T12:00:00+00:00",
            }
        )
        result = sync._find_full_sync_checkpoint(mock_sync_state_table)
        assert result is None

    def test_store_and_load_chunks(self, mock_sync_state_table, sync_state_items):
        sync_id = "full-sync-test"
        sync_state_items.append({"sync_id": sync_id, "mode": "full-sync", "status": "running"})

        file_list = [{"filepath": f"data/file{i}.dat", "box_file_id": str(i), "shared": True} for i in range(10)]
        chunk_count = sync._store_file_list_chunks(mock_sync_state_table, sync_id, file_list)
        assert chunk_count == 1  # 10 files < CHUNK_SIZE

        loaded = sync._load_file_list_chunks(mock_sync_state_table, sync_id, chunk_count)
        assert loaded is not None
        assert len(loaded) == 10
        assert loaded[0]["filepath"] == "data/file0.dat"

    def test_load_chunks_returns_none_when_missing(self, mock_sync_state_table):
        result = sync._load_file_list_chunks(mock_sync_state_table, "nonexistent", 2)
        assert result is None

    def test_cleanup_chunks_removes_all(self, mock_sync_state_table, sync_state_items):
        sync_id = "full-sync-test"
        sync_state_items.append({"sync_id": sync_id, "mode": "full-sync", "status": "running"})

        file_list = [{"filepath": f"data/file{i}.dat", "box_file_id": str(i), "shared": True} for i in range(10)]
        chunk_count = sync._store_file_list_chunks(mock_sync_state_table, sync_id, file_list)

        sync._cleanup_chunks(mock_sync_state_table, sync_id, chunk_count)

        chunk_items = [s for s in sync_state_items if ":chunk:" in s.get("sync_id", "")]
        assert len(chunk_items) == 0

    def test_full_sync_partial_state_writes_checkpoint(self, mock_sync_state_table, sync_state_items):
        sync_id = sync._create_sync_state(mock_sync_state_table, "full-sync")

        sync._full_sync_partial_state(
            mock_sync_state_table,
            sync_id,
            2,
            150,
            3,
            {"items_checked": 100, "items_valid": 95, "items_repaired": 5},
        )

        state = sync_state_items[0]
        assert state["status"] == "partial"
        assert state["last_chunk_index"] == 2
        assert state["last_file_index"] == 150
        assert state["chain_depth"] == 3
        assert state["items_checked"] == 100

    def test_fail_sync_state(self, mock_sync_state_table, sync_state_items):
        sync_id = sync._create_sync_state(mock_sync_state_table, "full-sync")

        sync._fail_sync_state(mock_sync_state_table, sync_id)

        assert sync_state_items[0]["status"] == "failed"
