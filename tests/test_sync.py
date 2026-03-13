import json
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest
from boxsdk.exception import BoxAPIException

import common
import sync


class TestSync:
    @pytest.fixture(autouse=True)
    def monkeypatch_clients(self, monkeypatch, mock_ddb_table, mock_box_client):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)

    def test_sync_empty(self, ddb_items):
        sync.lambda_handler({"mode": "full-sync"}, None)
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

        sync.lambda_handler({"mode": "full-sync"}, None)

        assert len(ddb_items) == 3
        file_ids = {i["box_file_id"] for i in ddb_items}
        assert file_ids == {correct_file.id, missing_file.id, unshared_file.id}
        assert common.is_box_object_public(shared_file) is False

    def test_sync_ddb_paging(self, ddb_items):
        for i in range(5 * 2 + 1):
            ddb_items.append(
                {
                    "filepath": f"some/defunct/file-{i}.dat",
                    "box_file_id": f"123456{i}",
                    "download_url": f"some-defunct-download-url-{i}",
                }
            )

        sync.lambda_handler({"mode": "full-sync"}, None)

        assert len(ddb_items) == 0


class TestSyncModeDispatch:
    def test_reconciliation_mode_stub(self, capture_log):
        sync.lambda_handler({"mode": "reconciliation"}, None)
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "mode_not_implemented" in actions

    def test_unknown_mode(self, capture_log):
        sync.lambda_handler({"mode": "bogus"}, None)
        log_lines = capture_log.getvalue().strip().split("\n")
        entries = [json.loads(line) for line in log_lines]
        assert any(e["action"] == "unknown_mode" for e in entries)

    def test_default_mode_is_drain_queue(self, capture_log, monkeypatch,
                                         mock_ddb_table, mock_validation_queue_table,
                                         mock_sync_state_table, mock_context):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_validation_queue_table", lambda: mock_validation_queue_table)
        monkeypatch.setattr(common, "get_sync_state_table", lambda: mock_sync_state_table)
        sync.lambda_handler({}, mock_context)
        log_lines = capture_log.getvalue().strip().split("\n")
        entries = [json.loads(line) for line in log_lines]
        assert any(e.get("mode") == "drain-queue" for e in entries)


class TestDrainQueue:
    @pytest.fixture(autouse=True)
    def setup_drain(self, monkeypatch, mock_ddb_table, mock_validation_queue_table,
                    mock_sync_state_table):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_validation_queue_table", lambda: mock_validation_queue_table)
        monkeypatch.setattr(common, "get_sync_state_table", lambda: mock_sync_state_table)

    def test_drains_queue_and_validates_valid_url(self, monkeypatch, ddb_items,
                                                   validation_queue_items, sync_state_items,
                                                   mock_context, capture_log):
        ddb_items.append({"filepath": "data/file1.fits", "box_file_id": "111",
                          "download_url": "https://box.com/dl/abc"})
        validation_queue_items.append({"filepath": "data/file1.fits",
                                       "download_url": "https://box.com/dl/abc"})
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

    def test_skips_fresh_items(self, monkeypatch, ddb_items, validation_queue_items,
                               sync_state_items, mock_context):
        fresh_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        ddb_items.append({"filepath": "data/fresh.fits", "box_file_id": "222",
                          "download_url": "https://box.com/dl/fresh",
                          "last_validated": fresh_ts})
        validation_queue_items.append({"filepath": "data/fresh.fits",
                                       "download_url": "https://box.com/dl/fresh"})
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

    def test_handles_orphaned_queue_items(self, monkeypatch, ddb_items,
                                          validation_queue_items, sync_state_items,
                                          mock_context, capture_log):
        # Queue item with no matching manifest entry
        validation_queue_items.append({"filepath": "deleted/file.fits",
                                       "download_url": "https://box.com/dl/gone"})

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert len(validation_queue_items) == 0
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "drain_validate_orphaned_queue_item" in actions

    def test_broken_url_triggers_repair(self, monkeypatch, ddb_items,
                                        validation_queue_items, sync_state_items,
                                        mock_context, capture_log):
        ddb_items.append({"filepath": "data/broken.fits", "box_file_id": "333",
                          "download_url": "https://box.com/dl/broken"})
        validation_queue_items.append({"filepath": "data/broken.fits",
                                       "download_url": "https://box.com/dl/broken"})
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
        assert "drain_repair_failed" in actions

    def test_does_not_stamp_uncertain_results(self, monkeypatch, ddb_items,
                                              validation_queue_items, sync_state_items,
                                              mock_context, capture_log):
        ddb_items.append({"filepath": "data/uncertain.fits", "box_file_id": "444",
                          "download_url": "https://box.com/dl/uncertain"})
        validation_queue_items.append({"filepath": "data/uncertain.fits",
                                       "download_url": "https://box.com/dl/uncertain"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "uncertain")

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert "last_validated" not in ddb_items[0]
        assert len(validation_queue_items) == 0
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "drain_validate_uncertain" in actions

    def test_deletes_queue_items_all_outcomes(self, monkeypatch, ddb_items,
                                              validation_queue_items, sync_state_items,
                                              mock_context):
        outcomes = {"valid": 0, "broken": 1, "uncertain": 2}
        for outcome, i in outcomes.items():
            ddb_items.append({"filepath": f"data/file{i}.fits", "box_file_id": str(i),
                              "download_url": f"https://box.com/dl/{i}"})
            validation_queue_items.append({"filepath": f"data/file{i}.fits",
                                           "download_url": f"https://box.com/dl/{i}"})

        url_outcomes = {f"https://box.com/dl/{i}": outcome for outcome, i in outcomes.items()}
        monkeypatch.setattr(common, "validate_download_url",
                            lambda url: url_outcomes[url])
        # Broken path now triggers repair — mock Box client to let it fail gracefully
        monkeypatch.setattr(common, "get_box_client", lambda: MagicMock())

        def raise_error(func, *a, **kw):
            raise ConnectionError("network failure")
        monkeypatch.setattr(common, "with_box_retry", raise_error)

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert len(validation_queue_items) == 0

    def test_stops_on_timeout(self, monkeypatch, ddb_items, validation_queue_items,
                              sync_state_items, capture_log):
        # Add several items
        for i in range(5):
            ddb_items.append({"filepath": f"data/file{i}.fits", "box_file_id": str(i),
                              "download_url": f"https://box.com/dl/{i}"})
            validation_queue_items.append({"filepath": f"data/file{i}.fits",
                                           "download_url": f"https://box.com/dl/{i}"})

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

    def test_creates_and_updates_sync_state(self, monkeypatch, ddb_items,
                                             validation_queue_items, sync_state_items,
                                             mock_context):
        ddb_items.append({"filepath": "data/file.fits", "box_file_id": "555",
                          "download_url": "https://box.com/dl/555"})
        validation_queue_items.append({"filepath": "data/file.fits",
                                       "download_url": "https://box.com/dl/555"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "valid")

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert len(sync_state_items) == 1
        state = sync_state_items[0]
        assert state["mode"] == "drain-queue"
        assert state["status"] == "completed"
        assert "started_at" in state
        assert "completed_at" in state
        assert state["items_checked"] == 1

    def test_handles_empty_queue(self, monkeypatch, ddb_items, validation_queue_items,
                                 sync_state_items, mock_context):
        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert len(sync_state_items) == 1
        assert sync_state_items[0]["status"] == "completed"
        assert sync_state_items[0]["items_checked"] == 0

    def test_stamps_last_validated_for_valid(self, monkeypatch, ddb_items,
                                             validation_queue_items, sync_state_items,
                                             mock_context):
        ddb_items.append({"filepath": "data/valid.fits", "box_file_id": "666",
                          "download_url": "https://box.com/dl/666"})
        validation_queue_items.append({"filepath": "data/valid.fits",
                                       "download_url": "https://box.com/dl/666"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "valid")

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert "last_validated" in ddb_items[0]
        # Verify it's a valid ISO 8601 timestamp
        ts = datetime.fromisoformat(ddb_items[0]["last_validated"])
        assert ts.tzinfo is not None

    def test_pagination_across_batches(self, monkeypatch, ddb_items,
                                       validation_queue_items, sync_state_items,
                                       mock_context):
        # BATCH_SIZE is 5 in MockTable, so 7 items forces pagination
        for i in range(7):
            ddb_items.append({"filepath": f"data/page{i}.fits", "box_file_id": str(i),
                              "download_url": f"https://box.com/dl/{i}"})
            validation_queue_items.append({"filepath": f"data/page{i}.fits",
                                           "download_url": f"https://box.com/dl/{i}"})
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
        return {"filepath": "data/broken.fits", "box_file_id": "111",
                "download_url": "https://box.com/dl/old"}

    @pytest.fixture
    def mock_repaired_file(self, create_shared_file, data_folder):
        return create_shared_file(parent_folder=data_folder, id="111",
                                  name="broken.fits")

    def test_successful_repair(self, monkeypatch, mock_ddb_table, ddb_items,
                                manifest_item, mock_repaired_file, mock_box_client,
                                capture_log):
        ddb_items.append(manifest_item)
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "with_box_retry",
                            lambda func, *a, **kw: func(*a, **kw))

        result = sync._repair_broken_url("data/broken.fits", manifest_item, mock_ddb_table)

        assert result == "repaired"
        assert len(ddb_items) == 1
        assert "last_validated" in ddb_items[0]
        assert ddb_items[0]["box_file_id"] == "111"
        assert ddb_items[0]["download_url"] == mock_repaired_file.shared_link["download_url"]
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "drain_repair_success" in actions

    def test_file_not_found_deletes_manifest(self, monkeypatch, mock_ddb_table,
                                              ddb_items, manifest_item, capture_log):
        ddb_items.append(manifest_item)
        monkeypatch.setattr(common, "get_box_client", lambda: MagicMock())
        monkeypatch.setattr(common, "with_box_retry",
                            lambda func, *a, **kw: func(*a, **kw))
        monkeypatch.setattr(common, "get_file", lambda client, fid: None)

        result = sync._repair_broken_url("data/broken.fits", manifest_item, mock_ddb_table)

        assert result == "deleted"
        assert len(ddb_items) == 0
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "file_not_found" in actions

    def test_repair_failure_returns_failed(self, monkeypatch, mock_ddb_table,
                                           ddb_items, manifest_item, capture_log):
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
        assert any(e["action"] == "drain_repair_failed" and e["error_type"] == "ConnectionError"
                    for e in entries)

    def test_uses_with_box_retry(self, monkeypatch, mock_ddb_table, ddb_items,
                                  manifest_item, mock_repaired_file, mock_box_client):
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

    def test_create_shared_link_failure(self, monkeypatch, mock_ddb_table,
                                        ddb_items, manifest_item, capture_log,
                                        create_file, data_folder):
        ddb_items.append(manifest_item)
        create_file(parent_folder=data_folder, id="111", name="broken.fits")
        monkeypatch.setattr(common, "get_box_client", lambda: MagicMock())
        monkeypatch.setattr(common, "with_box_retry",
                            lambda func, *a, **kw: func(*a, **kw))

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
    def setup_drain(self, monkeypatch, mock_ddb_table, mock_validation_queue_table,
                    mock_sync_state_table):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_validation_queue_table", lambda: mock_validation_queue_table)
        monkeypatch.setattr(common, "get_sync_state_table", lambda: mock_sync_state_table)

    @pytest.fixture
    def data_folder(self, create_folder, managed_folder):
        return create_folder(parent_folder=managed_folder, name="data")

    def test_broken_url_triggers_repair_and_updates_manifest(
            self, monkeypatch, ddb_items, validation_queue_items,
            sync_state_items, mock_context, capture_log,
            create_shared_file, data_folder, mock_box_client):
        ddb_items.append({"filepath": "data/repair.fits", "box_file_id": "111",
                          "download_url": "https://box.com/dl/old"})
        validation_queue_items.append({"filepath": "data/repair.fits",
                                       "download_url": "https://box.com/dl/old"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")

        repaired_file = create_shared_file(parent_folder=data_folder, id="111",
                                            name="repair.fits")
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "with_box_retry",
                            lambda func, *a, **kw: func(*a, **kw))

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert len(ddb_items) == 1
        assert "last_validated" in ddb_items[0]
        assert ddb_items[0]["download_url"] == repaired_file.shared_link["download_url"]
        assert len(validation_queue_items) == 0

    def test_broken_url_file_gone_deletes_manifest(
            self, monkeypatch, ddb_items, validation_queue_items,
            sync_state_items, mock_context, capture_log):
        ddb_items.append({"filepath": "data/gone.fits", "box_file_id": "999",
                          "download_url": "https://box.com/dl/gone"})
        validation_queue_items.append({"filepath": "data/gone.fits",
                                       "download_url": "https://box.com/dl/gone"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")
        monkeypatch.setattr(common, "get_box_client", lambda: MagicMock())
        monkeypatch.setattr(common, "with_box_retry",
                            lambda func, *a, **kw: func(*a, **kw))
        monkeypatch.setattr(common, "get_file", lambda client, fid: None)

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert len(ddb_items) == 0
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "file_not_found" in actions

    def test_broken_url_repair_fails_no_stamp(
            self, monkeypatch, ddb_items, validation_queue_items,
            sync_state_items, mock_context, capture_log):
        ddb_items.append({"filepath": "data/fail.fits", "box_file_id": "888",
                          "download_url": "https://box.com/dl/fail"})
        validation_queue_items.append({"filepath": "data/fail.fits",
                                       "download_url": "https://box.com/dl/fail"})
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
        assert "drain_repair_failed" in actions

    def test_items_repaired_counter_in_sync_state(
            self, monkeypatch, ddb_items, validation_queue_items,
            sync_state_items, mock_context,
            create_shared_file, data_folder, mock_box_client):
        ddb_items.append({"filepath": "data/fix.fits", "box_file_id": "777",
                          "download_url": "https://box.com/dl/fix"})
        validation_queue_items.append({"filepath": "data/fix.fits",
                                       "download_url": "https://box.com/dl/fix"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")

        create_shared_file(parent_folder=data_folder, id="777", name="fix.fits")
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "with_box_retry",
                            lambda func, *a, **kw: func(*a, **kw))

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert sync_state_items[0]["items_repaired"] == 1

    def test_successful_repair_calls_ensure_folder_shared(
            self, monkeypatch, ddb_items, validation_queue_items,
            sync_state_items, mock_context, capture_log,
            create_shared_file, data_folder, mock_box_client):
        ddb_items.append({"filepath": "data/repair.fits", "box_file_id": "111",
                          "download_url": "https://box.com/dl/old"})
        validation_queue_items.append({"filepath": "data/repair.fits",
                                       "download_url": "https://box.com/dl/old"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")

        create_shared_file(parent_folder=data_folder, id="111", name="repair.fits")
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "with_box_retry",
                            lambda func, *a, **kw: func(*a, **kw))

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
            self, monkeypatch, ddb_items, validation_queue_items,
            sync_state_items, mock_context, capture_log,
            create_shared_file, data_folder, mock_box_client):
        ddb_items.append({"filepath": "data/repair.fits", "box_file_id": "111",
                          "download_url": "https://box.com/dl/old"})
        validation_queue_items.append({"filepath": "data/repair.fits",
                                       "download_url": "https://box.com/dl/old"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")

        repaired_file = create_shared_file(parent_folder=data_folder, id="111",
                                            name="repair.fits")
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "with_box_retry",
                            lambda func, *a, **kw: func(*a, **kw))

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
            self, monkeypatch, mock_ddb_table, ddb_items,
            create_shared_file, data_folder, mock_box_client, capture_log):
        """Unit-level: _repair_broken_url catches ensure_folder_shared exception."""
        manifest_item = {"filepath": "data/broken.fits", "box_file_id": "111",
                         "download_url": "https://box.com/dl/old"}
        ddb_items.append(manifest_item)
        create_shared_file(parent_folder=data_folder, id="111", name="broken.fits")
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "with_box_retry",
                            lambda func, *a, **kw: func(*a, **kw))

        def explode(client, file):
            raise ValueError("something went wrong")
        monkeypatch.setattr(common, "ensure_folder_shared", explode)

        result = sync._repair_broken_url("data/broken.fits", manifest_item, mock_ddb_table)

        assert result == "repaired"
        log_lines = capture_log.getvalue().strip().split("\n")
        entries = [json.loads(line) for line in log_lines]
        assert any(e["action"] == "folder_sharing_check_failed" and
                   e["error_type"] == "ValueError" for e in entries)

    def test_mixed_outcomes_in_drain_run(
            self, monkeypatch, ddb_items, validation_queue_items,
            sync_state_items, mock_context, capture_log,
            create_shared_file, data_folder, mock_box_client):
        # valid item
        ddb_items.append({"filepath": "data/ok.fits", "box_file_id": "100",
                          "download_url": "https://box.com/dl/ok"})
        validation_queue_items.append({"filepath": "data/ok.fits",
                                       "download_url": "https://box.com/dl/ok"})
        # broken → repaired
        ddb_items.append({"filepath": "data/fix.fits", "box_file_id": "200",
                          "download_url": "https://box.com/dl/fix"})
        validation_queue_items.append({"filepath": "data/fix.fits",
                                       "download_url": "https://box.com/dl/fix"})
        create_shared_file(parent_folder=data_folder, id="200", name="fix.fits")
        # broken → deleted (file gone from Box)
        ddb_items.append({"filepath": "data/gone.fits", "box_file_id": "300",
                          "download_url": "https://box.com/dl/gone"})
        validation_queue_items.append({"filepath": "data/gone.fits",
                                       "download_url": "https://box.com/dl/gone"})
        # uncertain
        ddb_items.append({"filepath": "data/maybe.fits", "box_file_id": "400",
                          "download_url": "https://box.com/dl/maybe"})
        validation_queue_items.append({"filepath": "data/maybe.fits",
                                       "download_url": "https://box.com/dl/maybe"})

        url_outcomes = {
            "https://box.com/dl/ok": "valid",
            "https://box.com/dl/fix": "broken",
            "https://box.com/dl/gone": "broken",
            "https://box.com/dl/maybe": "uncertain",
        }
        monkeypatch.setattr(common, "validate_download_url",
                            lambda url: url_outcomes[url])
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "with_box_retry",
                            lambda func, *a, **kw: func(*a, **kw))
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
        assert sync_state_items[0]["items_repaired"] == 1
        # "gone.fits" should be deleted from manifest
        filepaths = {item["filepath"] for item in ddb_items}
        assert "data/gone.fits" not in filepaths
        # "maybe.fits" should be unchanged (no last_validated)
        maybe_item = next(i for i in ddb_items if i["filepath"] == "data/maybe.fits")
        assert "last_validated" not in maybe_item
