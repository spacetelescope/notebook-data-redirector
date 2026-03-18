import json
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest

import common
import validation_worker


def _make_stream_event(*records):
    """Build a DDB Streams event from a list of record dicts."""
    return {"Records": records}


def _make_insert_record(filepath, download_url):
    """Build an INSERT stream record in DDB Streams wire format."""
    return {
        "eventName": "INSERT",
        "dynamodb": {
            "NewImage": {
                "filepath": {"S": filepath},
                "download_url": {"S": download_url},
            }
        },
    }


def _make_remove_record():
    """Build a REMOVE stream record (TTL deletion)."""
    return {
        "eventName": "REMOVE",
        "dynamodb": {
            "OldImage": {
                "filepath": {"S": "data/expired.fits"},
            }
        },
    }


def _make_modify_record():
    """Build a MODIFY stream record."""
    return {
        "eventName": "MODIFY",
        "dynamodb": {
            "NewImage": {
                "filepath": {"S": "data/modified.fits"},
                "download_url": {"S": "https://box.com/dl/mod"},
            }
        },
    }


class TestParseStreamRecords:
    def test_insert_event_yields_record(self):
        event = _make_stream_event(_make_insert_record("data/file.fits", "https://box.com/dl/abc"))
        records = list(validation_worker._parse_stream_records(event))
        assert len(records) == 1
        assert records[0]["filepath"] == "data/file.fits"
        assert records[0]["download_url"] == "https://box.com/dl/abc"

    def test_remove_event_skipped(self):
        event = _make_stream_event(_make_remove_record())
        records = list(validation_worker._parse_stream_records(event))
        assert len(records) == 0

    def test_modify_event_skipped(self):
        event = _make_stream_event(_make_modify_record())
        records = list(validation_worker._parse_stream_records(event))
        assert len(records) == 0

    def test_no_new_image_skipped(self):
        event = _make_stream_event({"eventName": "INSERT", "dynamodb": {}})
        records = list(validation_worker._parse_stream_records(event))
        assert len(records) == 0

    def test_missing_download_url_skipped(self):
        event = _make_stream_event(
            {
                "eventName": "INSERT",
                "dynamodb": {
                    "NewImage": {
                        "filepath": {"S": "data/partial.fits"},
                    }
                },
            }
        )
        records = list(validation_worker._parse_stream_records(event))
        assert len(records) == 0

    def test_missing_filepath_skipped(self):
        event = _make_stream_event(
            {
                "eventName": "INSERT",
                "dynamodb": {
                    "NewImage": {
                        "download_url": {"S": "https://box.com/dl/abc"},
                    }
                },
            }
        )
        records = list(validation_worker._parse_stream_records(event))
        assert len(records) == 0

    def test_multiple_records_mixed(self):
        event = _make_stream_event(
            _make_insert_record("data/a.fits", "https://box.com/dl/a"),
            _make_remove_record(),
            _make_insert_record("data/b.fits", "https://box.com/dl/b"),
        )
        records = list(validation_worker._parse_stream_records(event))
        assert len(records) == 2
        assert records[0]["filepath"] == "data/a.fits"
        assert records[1]["filepath"] == "data/b.fits"


class TestProcessValidation:
    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch, mock_ddb_table, mock_validation_queue_table):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_validation_queue_table", lambda: mock_validation_queue_table)
        self.manifest_table = mock_ddb_table
        self.queue_table = mock_validation_queue_table

    def test_valid_url_stamps_last_validated(self, monkeypatch, ddb_items, validation_queue_items):
        ddb_items.append({"filepath": "data/file.fits", "box_file_id": "111", "download_url": "https://box.com/dl/abc"})
        validation_queue_items.append({"filepath": "data/file.fits", "download_url": "https://box.com/dl/abc"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "valid")

        record = {"filepath": "data/file.fits", "download_url": "https://box.com/dl/abc"}
        validation_worker._process_validation(record, self.manifest_table, self.queue_table)

        assert "last_validated" in ddb_items[0]
        ts = datetime.fromisoformat(ddb_items[0]["last_validated"])
        assert ts.tzinfo is not None
        assert len(validation_queue_items) == 0

    def test_broken_url_triggers_repair(self, monkeypatch, ddb_items, validation_queue_items):
        ddb_items.append(
            {"filepath": "data/broken.fits", "box_file_id": "222", "download_url": "https://box.com/dl/old"}
        )
        validation_queue_items.append({"filepath": "data/broken.fits", "download_url": "https://box.com/dl/old"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")

        repair_calls = []

        def mock_repair(filepath, item, table, caller="repair"):
            repair_calls.append(filepath)
            return "repaired"

        monkeypatch.setattr(common, "repair_broken_url", mock_repair)

        record = {"filepath": "data/broken.fits", "download_url": "https://box.com/dl/old"}
        validation_worker._process_validation(record, self.manifest_table, self.queue_table)

        assert len(repair_calls) == 1
        assert repair_calls[0] == "data/broken.fits"
        assert len(validation_queue_items) == 0

    def test_uncertain_url_leaves_queue_item(self, monkeypatch, ddb_items, validation_queue_items):
        ddb_items.append({"filepath": "data/unc.fits", "box_file_id": "333", "download_url": "https://box.com/dl/unc"})
        validation_queue_items.append({"filepath": "data/unc.fits", "download_url": "https://box.com/dl/unc"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "uncertain")

        record = {"filepath": "data/unc.fits", "download_url": "https://box.com/dl/unc"}
        validation_worker._process_validation(record, self.manifest_table, self.queue_table)

        assert "last_validated" not in ddb_items[0]
        assert len(validation_queue_items) == 1  # still there

    def test_orphaned_queue_item_deleted(self, monkeypatch, ddb_items, validation_queue_items, capture_log):
        # No manifest entry
        validation_queue_items.append({"filepath": "data/orphan.fits", "download_url": "https://box.com/dl/orphan"})

        record = {"filepath": "data/orphan.fits", "download_url": "https://box.com/dl/orphan"}
        validation_worker._process_validation(record, self.manifest_table, self.queue_table)

        assert len(validation_queue_items) == 0
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "orphaned_queue_item" in actions

    def test_fresh_entry_skipped(self, monkeypatch, ddb_items, validation_queue_items):
        fresh_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        ddb_items.append(
            {
                "filepath": "data/fresh.fits",
                "box_file_id": "444",
                "download_url": "https://box.com/dl/fresh",
                "last_validated": fresh_ts,
            }
        )
        validation_queue_items.append({"filepath": "data/fresh.fits", "download_url": "https://box.com/dl/fresh"})

        validate_called = {"value": False}

        def spy(url):
            validate_called["value"] = True
            return "valid"

        monkeypatch.setattr(common, "validate_download_url", spy)

        record = {"filepath": "data/fresh.fits", "download_url": "https://box.com/dl/fresh"}
        validation_worker._process_validation(record, self.manifest_table, self.queue_table)

        assert validate_called["value"] is False
        assert len(validation_queue_items) == 0


class TestLambdaHandler:
    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch, mock_ddb_table, mock_validation_queue_table):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_validation_queue_table", lambda: mock_validation_queue_table)

    def test_processes_batch_of_records(self, monkeypatch, ddb_items, validation_queue_items):
        for i in range(3):
            ddb_items.append(
                {"filepath": f"data/file{i}.fits", "box_file_id": str(i), "download_url": f"https://box.com/dl/{i}"}
            )
            validation_queue_items.append({"filepath": f"data/file{i}.fits", "download_url": f"https://box.com/dl/{i}"})
        monkeypatch.setattr(common, "validate_download_url", lambda url: "valid")

        event = _make_stream_event(
            *[_make_insert_record(f"data/file{i}.fits", f"https://box.com/dl/{i}") for i in range(3)]
        )
        result = validation_worker.lambda_handler(event, None)

        assert result == {"statusCode": 200}
        assert len(validation_queue_items) == 0
        for item in ddb_items:
            assert "last_validated" in item

    def test_per_item_error_isolation(self, monkeypatch, ddb_items, validation_queue_items, capture_log):
        # First item will fail, second should still process
        ddb_items.append({"filepath": "data/ok.fits", "box_file_id": "2", "download_url": "https://box.com/dl/ok"})
        validation_queue_items.append({"filepath": "data/ok.fits", "download_url": "https://box.com/dl/ok"})

        call_count = {"n": 0}

        def failing_then_valid(url):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("boom")
            return "valid"

        monkeypatch.setattr(common, "validate_download_url", failing_then_valid)

        event = _make_stream_event(
            _make_insert_record("data/bad.fits", "https://box.com/dl/bad"),
            _make_insert_record("data/ok.fits", "https://box.com/dl/ok"),
        )

        # Need manifest entry for the bad one too so it gets past the lookup
        ddb_items.insert(0, {"filepath": "data/bad.fits", "box_file_id": "1", "download_url": "https://box.com/dl/bad"})

        result = validation_worker.lambda_handler(event, None)

        assert result == {"statusCode": 200}
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "validation_failed" in actions
        assert "validate_valid" in actions

    def test_always_returns_200(self, monkeypatch, capture_log):
        # Even with empty event
        result = validation_worker.lambda_handler({"Records": []}, None)
        assert result == {"statusCode": 200}


class TestRepairIntegration:
    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch, mock_ddb_table, mock_validation_queue_table):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_validation_queue_table", lambda: mock_validation_queue_table)
        self.manifest_table = mock_ddb_table
        self.queue_table = mock_validation_queue_table

    def test_broken_url_repaired_successfully(
        self,
        monkeypatch,
        ddb_items,
        validation_queue_items,
        create_shared_file,
        create_folder,
        managed_folder,
        mock_box_client,
    ):
        data_folder = create_folder(parent_folder=managed_folder, name="data")
        repaired_file = create_shared_file(parent_folder=data_folder, id="111", name="broken.fits")

        ddb_items.append(
            {"filepath": "data/broken.fits", "box_file_id": "111", "download_url": "https://box.com/dl/old"}
        )
        validation_queue_items.append({"filepath": "data/broken.fits", "download_url": "https://box.com/dl/old"})

        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))

        event = _make_stream_event(_make_insert_record("data/broken.fits", "https://box.com/dl/old"))
        validation_worker.lambda_handler(event, None)

        assert len(ddb_items) == 1
        assert "last_validated" in ddb_items[0]
        assert ddb_items[0]["download_url"] == repaired_file.shared_link["download_url"]
        assert len(validation_queue_items) == 0

    def test_broken_url_file_deleted_from_box(self, monkeypatch, ddb_items, validation_queue_items, capture_log):
        ddb_items.append(
            {"filepath": "data/gone.fits", "box_file_id": "999", "download_url": "https://box.com/dl/gone"}
        )
        validation_queue_items.append({"filepath": "data/gone.fits", "download_url": "https://box.com/dl/gone"})

        monkeypatch.setattr(common, "validate_download_url", lambda url: "broken")
        monkeypatch.setattr(common, "get_box_client", lambda: MagicMock())
        monkeypatch.setattr(common, "with_box_retry", lambda func, *a, **kw: func(*a, **kw))
        monkeypatch.setattr(common, "get_file", lambda client, fid: None)

        event = _make_stream_event(_make_insert_record("data/gone.fits", "https://box.com/dl/gone"))
        validation_worker.lambda_handler(event, None)

        assert len(ddb_items) == 0
        assert len(validation_queue_items) == 0
        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "file_not_found" in actions
