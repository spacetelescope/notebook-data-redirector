import json
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest

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

    def test_does_not_stamp_broken_urls(self, monkeypatch, ddb_items,
                                        validation_queue_items, sync_state_items,
                                        mock_context, capture_log):
        ddb_items.append({"filepath": "data/broken.fits", "box_file_id": "333",
                          "download_url": "https://box.com/dl/broken"})
        validation_queue_items.append({"filepath": "data/broken.fits",
                                       "download_url": "https://box.com/dl/broken"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")

        sync.lambda_handler({"mode": "drain-queue"}, mock_context)

        assert "last_validated" not in ddb_items[0]
        assert len(validation_queue_items) == 0
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "drain_validate_broken" in actions

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
        outcomes = ["valid", "broken", "uncertain"]
        for i, outcome in enumerate(outcomes):
            ddb_items.append({"filepath": f"data/file{i}.fits", "box_file_id": str(i),
                              "download_url": f"https://box.com/dl/{i}"})
            validation_queue_items.append({"filepath": f"data/file{i}.fits",
                                           "download_url": f"https://box.com/dl/{i}"})

        call_count = {"value": 0}
        def rotating_validate(url):
            idx = call_count["value"]
            call_count["value"] += 1
            return outcomes[idx]
        monkeypatch.setattr(common, "validate_download_url", rotating_validate)

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
