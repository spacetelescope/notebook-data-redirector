import json
import base64
import time
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

import common
import webhook_receiver

SHARED_LINK_TRIGGERS = {"SHARED_LINK.CREATED", "SHARED_LINK.UPDATED", "SHARED_LINK.DELETED"}


def handle_event(event):
    assert webhook_receiver.lambda_handler(event, {})["statusCode"] == 200


class TestWebhookReceiver:
    @pytest.fixture(autouse=True)
    def monkeypatch_clients(
        self,
        monkeypatch,
        mock_ddb_table,
        mock_box_client,
        box_webhook_signature_key,
        compute_webhook_signature,
        mock_webhook_queue_table,
    ):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "get_webhook_signature_key", lambda: box_webhook_signature_key)
        monkeypatch.setattr(common, "get_webhook_queue_table", lambda: mock_webhook_queue_table)
        mock_dedup = MagicMock()
        mock_dedup.get_item.return_value = {}
        monkeypatch.setattr(common, "get_event_dedup_table", lambda: mock_dedup)

        def mock_validate(body, headers, signature_key):
            return compute_webhook_signature(body) == headers["box-signature-primary"]

        monkeypatch.setattr(common, "validate_webhook_message", mock_validate)

    @pytest.fixture
    def create_webhook_event(self, box_webhook_signature_key, box_webhook_id, compute_webhook_signature):
        def _create_webhook_event(trigger, box_object, signature=None):
            source = {"item": {"id": box_object.id, "type": box_object.type}}

            body = {"trigger": trigger, "source": source, "webhook": {"id": box_webhook_id}}
            json_body = json.dumps(body)
            if not signature:
                signature = compute_webhook_signature(bytes(json_body, "utf-8"))

            return {"body": json_body, "headers": {"box-signature-primary": signature}}

        return _create_webhook_event

    def test_invalid_signature(self, create_webhook_event, create_shared_file, ddb_items):
        file = create_shared_file()
        event = create_webhook_event("SHARED_LINK.CREATED", file, signature=base64.b64encode(b"nope").decode("utf-8"))
        handle_event(event)
        assert len(ddb_items) == 0

    def test_unhandled_webhook(self, create_webhook_event, create_shared_file, ddb_items):
        file = create_shared_file()
        event = create_webhook_event("FILE.BLORPED", file)
        handle_event(event)
        assert len(ddb_items) == 0

    def test_shared_link_created(
        self,
        create_webhook_event,
        create_shared_file,
        ddb_items,
        create_shared_folder,
        create_file,
        managed_folder,
        webhook_queue_items,
    ):
        # due to changes in the BOX API, sharing a file directly will result in the shared link being deleted in the webhook receiver
        file = create_shared_file()
        event = create_webhook_event("SHARED_LINK.CREATED", file)
        handle_event(event)
        assert len(ddb_items) == 0

        # folder SHARED_LINK.CREATED now queues a work item (and checks/fixes sharing)
        folder = create_shared_folder(parent_folder=managed_folder)
        event = create_webhook_event("SHARED_LINK.CREATED", folder)
        handle_event(event)
        assert len(webhook_queue_items) == 1
        assert webhook_queue_items[0]["folder_id"] == folder.id
        assert webhook_queue_items[0]["trigger"] == "SHARED_LINK.CREATED"

    def test_shared_link_updated(
        self,
        create_webhook_event,
        create_shared_file,
        ddb_items,
        box_files,
        box_folders,
        create_shared_link,
        managed_folder,
        create_shared_folder,
        webhook_queue_items,
    ):
        # due to changes in the BOX API, sharing a file directly will result in the shared link being deleted in the webhook receiver
        file = create_shared_file()
        event = create_webhook_event("SHARED_LINK.UPDATED", file)
        handle_event(event)
        assert len(ddb_items) == 0

        # so we must create the file in a shared folder first
        box_files.remove(file)
        folder = create_shared_folder(parent_folder=managed_folder)
        file = create_shared_file(id=file.id, parent_folder=folder)
        event = create_webhook_event("SHARED_LINK.UPDATED", file)
        handle_event(event)
        assert len(ddb_items) == 1
        assert ddb_items[0]["box_file_id"] == file.id

        # folder SHARED_LINK.UPDATED now queues a work item (and checks/fixes sharing)
        box_files.remove(file)
        box_folders.remove(folder)
        folder = create_shared_folder(
            id=folder.id, parent_folder=managed_folder, shared_link=create_shared_link(effective_access="company")
        )
        event = create_webhook_event("SHARED_LINK.UPDATED", folder)
        handle_event(event)
        assert len(webhook_queue_items) == 1
        assert webhook_queue_items[0]["folder_id"] == folder.id

    def test_shared_link_deleted(self, create_webhook_event, create_file, ddb_items, managed_folder):
        file = create_file(parent_folder=managed_folder)
        ddb_items.append({"filepath": common.get_filepath(file)})
        event = create_webhook_event("SHARED_LINK.DELETED", file)
        handle_event(event)
        assert len(ddb_items) == 0

    def test_file_trashed(self, create_webhook_event, create_shared_file, ddb_items, box_files):
        file = create_shared_file()
        ddb_items.append(common.make_ddb_item(file))
        box_files.remove(file)
        event = create_webhook_event("FILE.TRASHED", file)
        handle_event(event)
        assert len(ddb_items) == 1

    def test_file_restored(
        self, create_webhook_event, create_file, ddb_items, box_files, create_shared_folder, managed_folder
    ):
        folder = create_shared_folder(parent_folder=managed_folder)
        file = create_file(parent_folder=folder)
        event = create_webhook_event("FILE.RESTORED", file)
        handle_event(event)
        assert len(ddb_items) == 1
        assert ddb_items[0]["box_file_id"] == file.id

    def test_file_moved(
        self,
        create_webhook_event,
        create_file,
        ddb_items,
        managed_folder,
        create_shared_folder,
    ):
        folder = create_shared_folder(parent_folder=managed_folder)
        file = create_file(parent_folder=folder)
        ddb_items.append({"filepath": "some/old/path.dat", "box_file_id": file.id, "download_url": "some-download-url"})
        event = create_webhook_event("FILE.MOVED", file)
        handle_event(event)
        # Old entry deleted via GSI lookup, new entry created
        assert len(ddb_items) == 1
        assert ddb_items[0]["filepath"] == common.get_filepath(file)

    def test_folder_restored(
        self, create_shared_folder, managed_folder, create_file, create_webhook_event, webhook_queue_items
    ):
        folder = create_shared_folder(parent_folder=managed_folder)
        event = create_webhook_event("FOLDER.RESTORED", folder)
        handle_event(event)
        assert len(webhook_queue_items) == 1
        assert webhook_queue_items[0]["folder_id"] == folder.id
        assert webhook_queue_items[0]["trigger"] == "FOLDER.RESTORED"

    def test_folder_trashed(self, create_folder, managed_folder, create_webhook_event, webhook_queue_items):
        folder = create_folder(parent_folder=managed_folder)
        event = create_webhook_event("FOLDER.TRASHED", folder)
        handle_event(event)
        assert len(webhook_queue_items) == 1
        assert webhook_queue_items[0]["folder_id"] == folder.id
        assert webhook_queue_items[0]["trigger"] == "FOLDER.TRASHED"

    def test_file_uploaded_with_dedup(
        self, create_shared_folder, create_file, managed_folder, ddb_items, compute_webhook_signature
    ):
        """Full flow: cascade pass → dedup write → signature → processing."""
        folder = create_shared_folder(parent_folder=managed_folder)
        file = create_file(parent_folder=folder)
        source = {"item": {"id": file.id, "type": "file"}}
        body = {
            "id": "evt_integration_1",
            "trigger": "FILE.UPLOADED",
            "source": source,
            "created_by": {"type": "user", "id": "99999999", "name": "Other"},
        }
        json_body = json.dumps(body)
        signature = compute_webhook_signature(bytes(json_body, "utf-8"))
        event = {"body": json_body, "headers": {"box-signature-primary": signature}}
        handle_event(event)
        assert len(ddb_items) == 1
        assert ddb_items[0]["box_file_id"] == file.id

    def test_folder_moved(self, create_shared_folder, managed_folder, create_webhook_event, webhook_queue_items):
        folder = create_shared_folder(parent_folder=managed_folder)
        event = create_webhook_event("FOLDER.MOVED", folder)
        handle_event(event)
        assert len(webhook_queue_items) == 1
        assert webhook_queue_items[0]["folder_id"] == folder.id
        assert webhook_queue_items[0]["trigger"] == "FOLDER.MOVED"


class TestCascadePrevention:
    """Task 4: Tests for cascade prevention (AC #1)."""

    @pytest.fixture(autouse=True)
    def monkeypatch_clients(self, monkeypatch, mock_ddb_table, mock_box_client, box_webhook_signature_key):
        self.mock_ddb_fn = MagicMock(return_value=mock_ddb_table)
        monkeypatch.setattr(common, "get_ddb_table", self.mock_ddb_fn)
        self.mock_box_client_fn = MagicMock(return_value=mock_box_client)
        monkeypatch.setattr(common, "get_box_client", self.mock_box_client_fn)
        monkeypatch.setattr(common, "get_webhook_signature_key", lambda: box_webhook_signature_key)
        self.mock_dedup_table = MagicMock()
        monkeypatch.setattr(common, "get_event_dedup_table", lambda: self.mock_dedup_table)
        self.mock_validate_fn = MagicMock(return_value=True)
        monkeypatch.setattr(common, "validate_webhook_message", self.mock_validate_fn)

    def _make_event(self, trigger="FILE.UPLOADED", created_by=None, box_id="99999"):
        body = {
            "id": "evt_unique_1",
            "trigger": trigger,
            "source": {"item": {"id": box_id, "type": "file"}},
        }
        if created_by is not None:
            body["created_by"] = created_by
        return {"body": json.dumps(body), "headers": {"box-signature-primary": "sig"}}

    def test_service_account_event_skipped(self, capture_log):
        """Event with created_by.id matching SERVICE_ACCOUNT_USER_ID → 200, no DDB writes, no Box API calls."""
        event = self._make_event(created_by={"type": "user", "id": "12345678", "name": "Service"})
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        self.mock_dedup_table.get_item.assert_not_called()
        self.mock_dedup_table.put_item.assert_not_called()
        self.mock_box_client_fn.assert_not_called()
        self.mock_ddb_fn.assert_not_called()
        self.mock_validate_fn.assert_not_called()
        log_output = capture_log.getvalue()
        assert '"action": "cascade_event_skipped"' in log_output

    def test_different_user_continues(self):
        """Event with different created_by.id → processing continues (not filtered)."""
        self.mock_dedup_table.get_item.return_value = {}
        event = self._make_event(created_by={"type": "user", "id": "99999999", "name": "Other"})
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        # Dedup check should have been called (processing continued past cascade check)
        self.mock_dedup_table.get_item.assert_called_once()

    def test_no_created_by_continues(self):
        """Event with no created_by field → processing continues (not filtered)."""
        self.mock_dedup_table.get_item.return_value = {}
        event = self._make_event(created_by=None)
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        self.mock_dedup_table.get_item.assert_called_once()

    def test_empty_service_account_id_skips_check(self, monkeypatch):
        """SERVICE_ACCOUNT_USER_ID is empty → cascade check skipped, processing continues."""
        monkeypatch.setattr(common, "SERVICE_ACCOUNT_USER_ID", "")
        self.mock_dedup_table.get_item.return_value = {}
        event = self._make_event(created_by={"type": "user", "id": "12345678", "name": "Service"})
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        # Should have continued past cascade check to dedup
        self.mock_dedup_table.get_item.assert_called_once()


class TestEventDeduplication:
    """Task 5: Tests for event deduplication (AC #2, #3, #4)."""

    @pytest.fixture(autouse=True)
    def monkeypatch_clients(self, monkeypatch, mock_ddb_table, mock_box_client, box_webhook_signature_key):
        self.mock_ddb_fn = MagicMock(return_value=mock_ddb_table)
        monkeypatch.setattr(common, "get_ddb_table", self.mock_ddb_fn)
        self.mock_box_client_fn = MagicMock(return_value=mock_box_client)
        monkeypatch.setattr(common, "get_box_client", self.mock_box_client_fn)
        monkeypatch.setattr(common, "get_webhook_signature_key", lambda: box_webhook_signature_key)
        self.mock_dedup_table = MagicMock()
        monkeypatch.setattr(common, "get_event_dedup_table", lambda: self.mock_dedup_table)
        self.mock_validate_fn = MagicMock(return_value=True)
        monkeypatch.setattr(common, "validate_webhook_message", self.mock_validate_fn)

    def _make_event(self, event_id="evt_abc123", trigger="FILE.UPLOADED", box_id="99999"):
        body = {
            "id": event_id,
            "trigger": trigger,
            "source": {"item": {"id": box_id, "type": "file"}},
            "created_by": {"type": "user", "id": "99999999", "name": "Other"},
        }
        return {"body": json.dumps(body), "headers": {"box-signature-primary": "sig"}}

    def test_new_event_dedup_write_after_processing(self):
        """First event with unique event_id → dedup record written after processing."""
        self.mock_dedup_table.get_item.return_value = {}  # not a duplicate
        event = self._make_event(event_id="evt_new_1")
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        self.mock_dedup_table.get_item.assert_called_once()
        self.mock_dedup_table.put_item.assert_called_once()

    def test_dedup_item_schema(self):
        """Dedup item has correct schema: event_id, processed_at, expires_at with 48h TTL."""
        self.mock_dedup_table.get_item.return_value = {}
        event = self._make_event(event_id="evt_schema_check")
        webhook_receiver.lambda_handler(event, {})
        call_kwargs = self.mock_dedup_table.put_item.call_args[1]
        item = call_kwargs["Item"]
        assert item["event_id"] == "evt_schema_check"
        assert "processed_at" in item
        assert isinstance(item["expires_at"], int)
        # expires_at should be ~48 hours from now
        expected_min = int(time.time()) + common.EVENT_DEDUP_TTL_SECONDS - 10
        expected_max = int(time.time()) + common.EVENT_DEDUP_TTL_SECONDS + 10
        assert expected_min <= item["expires_at"] <= expected_max

    def test_duplicate_event_returns_200(self, capture_log):
        """Duplicate event → get_item finds existing record, 200 returned, no further processing."""
        self.mock_dedup_table.get_item.return_value = {
            "Item": {"event_id": "evt_dup_1", "processed_at": "2026-03-16T00:00:00+00:00", "expires_at": 9999999999}
        }
        event = self._make_event(event_id="evt_dup_1")
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        log_output = capture_log.getvalue()
        assert '"action": "duplicate_event_skipped"' in log_output
        self.mock_box_client_fn.assert_not_called()
        self.mock_ddb_fn.assert_not_called()
        self.mock_validate_fn.assert_not_called()
        self.mock_dedup_table.put_item.assert_not_called()

    def test_missing_event_id_skips_dedup(self):
        """Event with no id field → dedup skipped, processing continues."""
        body = {
            "trigger": "FILE.UPLOADED",
            "source": {"item": {"id": "99999", "type": "file"}},
            "created_by": {"type": "user", "id": "99999999", "name": "Other"},
        }
        event = {"body": json.dumps(body), "headers": {"box-signature-primary": "sig"}}
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        self.mock_dedup_table.get_item.assert_not_called()
        self.mock_dedup_table.put_item.assert_not_called()

    def test_dedup_check_error_continues_processing(self, capture_log):
        """DDB error on dedup check → processing continues (best-effort dedup)."""
        self.mock_dedup_table.get_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "service error"}},
            "PutItem",
        )
        event = self._make_event(event_id="evt_err_1")
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        log_output = capture_log.getvalue()
        assert '"action": "dedup_check_failed"' in log_output
        # Post-processing dedup write should still be attempted
        self.mock_dedup_table.put_item.assert_called_once()

    def test_dedup_write_error_swallowed(self, capture_log):
        """DDB error on post-processing dedup write → logged but doesn't fail the handler."""
        self.mock_dedup_table.get_item.return_value = {}
        self.mock_dedup_table.put_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "service error"}},
            "PutItem",
        )
        event = self._make_event(event_id="evt_err_2")
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        log_output = capture_log.getvalue()
        assert '"action": "dedup_write_failed"' in log_output


class TestErrorPropagation:
    """Verify real errors propagate (trigger Box retries) while intentional skips return 200."""

    @pytest.fixture(autouse=True)
    def monkeypatch_clients(self, monkeypatch):
        monkeypatch.setattr(common, "get_event_dedup_table", lambda: MagicMock())

    def test_malformed_body_raises(self):
        """Completely malformed body → exception propagates."""
        event = {"body": "not valid json", "headers": {}}
        with pytest.raises(json.JSONDecodeError):
            webhook_receiver.lambda_handler(event, {})

    def test_missing_trigger_raises(self):
        """Body missing trigger field → KeyError propagates."""
        event = {"body": json.dumps({"source": {}}), "headers": {}}
        with pytest.raises(KeyError):
            webhook_receiver.lambda_handler(event, {})

    def test_missing_source_id_raises(self, monkeypatch):
        """Source with no id field → RuntimeError propagates."""
        monkeypatch.setattr(common, "get_webhook_signature_key", lambda: "key")
        monkeypatch.setattr(common, "validate_webhook_message", lambda body, headers, key: True)
        body = {
            "id": "evt_1",
            "trigger": "FILE.UPLOADED",
            "source": {},
            "created_by": {"type": "user", "id": "99999999"},
        }
        event = {"body": json.dumps(body), "headers": {"box-signature-primary": "sig"}}
        with pytest.raises(RuntimeError, match="Missing id field"):
            webhook_receiver.lambda_handler(event, {})


class TestGSIQueryHelper:
    """Task 5: Tests for get_manifest_item_by_box_file_id (AC: #4)."""

    def test_item_exists(self):
        mock_table = MagicMock()
        mock_table.query.return_value = {
            "Items": [{"filepath": "data/test.fits", "box_file_id": "111", "download_url": "https://example.com"}]
        }
        result = common.get_manifest_item_by_box_file_id(mock_table, "111")
        assert result is not None
        assert result["box_file_id"] == "111"
        mock_table.query.assert_called_once()

    def test_no_item(self):
        mock_table = MagicMock()
        mock_table.query.return_value = {"Items": []}
        result = common.get_manifest_item_by_box_file_id(mock_table, "999")
        assert result is None


class TestFileLevelTriage:
    """Task 6: Tests for file-level triage (AC: #2, #3, #4)."""

    @pytest.fixture(autouse=True)
    def monkeypatch_clients(
        self, monkeypatch, mock_ddb_table, mock_box_client, box_webhook_signature_key, mock_webhook_queue_table
    ):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "get_webhook_signature_key", lambda: box_webhook_signature_key)
        monkeypatch.setattr(common, "get_webhook_queue_table", lambda: mock_webhook_queue_table)
        mock_dedup = MagicMock()
        mock_dedup.get_item.return_value = {}
        monkeypatch.setattr(common, "get_event_dedup_table", lambda: mock_dedup)
        self.mock_validate_fn = MagicMock(return_value=True)
        monkeypatch.setattr(common, "validate_webhook_message", self.mock_validate_fn)

    def _make_event(self, trigger, box_id, box_type="file"):
        body = {
            "id": f"evt_{trigger}_{box_id}",
            "trigger": trigger,
            "source": {"item": {"id": box_id, "type": box_type}},
            "created_by": {"type": "user", "id": "99999999", "name": "Other"},
        }
        return {"body": json.dumps(body), "headers": {"box-signature-primary": "sig"}}

    def test_file_deleted_found_in_manifest(self, ddb_items, create_shared_file, managed_folder):
        file = create_shared_file(parent_folder=managed_folder)
        ddb_items.append(common.make_ddb_item(file))
        assert len(ddb_items) == 1
        event = self._make_event("FILE.DELETED", file.id)
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        assert len(ddb_items) == 0

    def test_file_deleted_not_in_manifest(self, ddb_items):
        event = self._make_event("FILE.DELETED", "nonexistent_id")
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        assert len(ddb_items) == 0

    def test_file_renamed(self, ddb_items, create_file, managed_folder, create_shared_folder):
        folder = create_shared_folder(parent_folder=managed_folder)
        file = create_file(parent_folder=folder)
        ddb_items.append({"filepath": "old/path.dat", "box_file_id": file.id, "download_url": "https://old"})
        event = self._make_event("FILE.RENAMED", file.id)
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        # Old entry deleted, new entry created
        assert len(ddb_items) == 1
        assert ddb_items[0]["filepath"] == common.get_filepath(file)

    def test_file_uploaded_shared_folder(self, ddb_items, create_file, managed_folder, create_shared_folder):
        folder = create_shared_folder(parent_folder=managed_folder)
        file = create_file(parent_folder=folder)
        event = self._make_event("FILE.UPLOADED", file.id)
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        assert len(ddb_items) == 1
        assert ddb_items[0]["box_file_id"] == file.id

    def test_file_copied(self, ddb_items, create_file, managed_folder, create_shared_folder):
        folder = create_shared_folder(parent_folder=managed_folder)
        file = create_file(parent_folder=folder)
        event = self._make_event("FILE.COPIED", file.id)
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        assert len(ddb_items) == 1
        assert ddb_items[0]["box_file_id"] == file.id

    def test_existing_shared_link_deleted_still_works(self, create_file, ddb_items, managed_folder):
        """Regression: SHARED_LINK.DELETED still removes manifest entry."""
        file = create_file(parent_folder=managed_folder)
        ddb_items.append({"filepath": common.get_filepath(file), "box_file_id": file.id, "download_url": "https://x"})
        event = self._make_event("SHARED_LINK.DELETED", file.id)
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        assert len(ddb_items) == 0


class TestFolderQueueWrite:
    """Task 7: Tests for folder-level queue write (AC: #5, #6, #8)."""

    @pytest.fixture(autouse=True)
    def monkeypatch_clients(self, monkeypatch, mock_ddb_table, box_webhook_signature_key, mock_webhook_queue_table):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_webhook_signature_key", lambda: box_webhook_signature_key)
        monkeypatch.setattr(common, "get_webhook_queue_table", lambda: mock_webhook_queue_table)
        mock_dedup = MagicMock()
        mock_dedup.get_item.return_value = {}
        monkeypatch.setattr(common, "get_event_dedup_table", lambda: mock_dedup)
        self.mock_validate_fn = MagicMock(return_value=True)
        monkeypatch.setattr(common, "validate_webhook_message", self.mock_validate_fn)
        self.mock_box_client_fn = MagicMock()
        monkeypatch.setattr(common, "get_box_client", self.mock_box_client_fn)

    def _make_folder_event(self, trigger, folder_id, event_id="evt_folder_1"):
        source = {"item": {"id": folder_id, "type": "folder"}}
        body = {
            "id": event_id,
            "trigger": trigger,
            "source": source,
            "created_by": {"type": "user", "id": "99999999", "name": "Other"},
        }
        return {"body": json.dumps(body), "headers": {"box-signature-primary": "sig"}}

    def test_folder_renamed_queued(self, webhook_queue_items):
        event = self._make_folder_event("FOLDER.RENAMED", "12345", event_id="evt_r1")
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        assert len(webhook_queue_items) == 1
        item = webhook_queue_items[0]
        assert item["work_id"] == "12345:evt_r1"
        assert item["folder_id"] == "12345"
        assert item["trigger"] == "FOLDER.RENAMED"
        assert item["status"] == "pending"
        assert "created_at" in item
        assert isinstance(item["expires_at"], int)

    def test_folder_deleted_queued_no_box_api(self, webhook_queue_items):
        event = self._make_folder_event("FOLDER.DELETED", "67890")
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        assert len(webhook_queue_items) == 1
        # No Box client initialized for non-SHARED_LINK folder events
        self.mock_box_client_fn.assert_not_called()

    def test_folder_moved_work_id_format(self, webhook_queue_items):
        event = self._make_folder_event("FOLDER.MOVED", "111", event_id="evt_m1")
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        assert webhook_queue_items[0]["work_id"] == "111:evt_m1"

    def test_no_box_client_for_folder_events(self, webhook_queue_items):
        """get_box_client() NOT called for non-SHARED_LINK folder events."""
        for trigger in (
            "FOLDER.RENAMED",
            "FOLDER.MOVED",
            "FOLDER.DELETED",
            "FOLDER.TRASHED",
            "FOLDER.RESTORED",
            "FOLDER.COPIED",
        ):
            event = self._make_folder_event(trigger, "999")
            webhook_receiver.lambda_handler(event, {})
        self.mock_box_client_fn.assert_not_called()

    def test_queue_write_failure_propagates(self, monkeypatch):
        mock_queue = MagicMock()
        mock_queue.put_item.side_effect = Exception("DDB write failed")
        monkeypatch.setattr(common, "get_webhook_queue_table", lambda: mock_queue)
        event = self._make_folder_event("FOLDER.RENAMED", "12345")
        with pytest.raises(Exception, match="DDB write failed"):
            webhook_receiver.lambda_handler(event, {})


class TestFolderSharingCorrection:
    """Task 8: Tests for folder sharing immediate correction (AC: #7)."""

    @pytest.fixture(autouse=True)
    def monkeypatch_clients(
        self, monkeypatch, mock_ddb_table, mock_box_client, box_webhook_signature_key, mock_webhook_queue_table
    ):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "get_webhook_signature_key", lambda: box_webhook_signature_key)
        monkeypatch.setattr(common, "get_webhook_queue_table", lambda: mock_webhook_queue_table)
        mock_dedup = MagicMock()
        mock_dedup.get_item.return_value = {}
        monkeypatch.setattr(common, "get_event_dedup_table", lambda: mock_dedup)
        self.mock_validate_fn = MagicMock(return_value=True)
        monkeypatch.setattr(common, "validate_webhook_message", self.mock_validate_fn)

    def _make_folder_event(self, trigger, folder_id, event_id="evt_share_1"):
        source = {"item": {"id": folder_id, "type": "folder"}}
        body = {
            "id": event_id,
            "trigger": trigger,
            "source": source,
            "created_by": {"type": "user", "id": "99999999", "name": "Other"},
        }
        return {"body": json.dumps(body), "headers": {"box-signature-primary": "sig"}}

    def test_shared_link_created_incorrect_sharing_corrected(
        self, create_folder, managed_folder, webhook_queue_items, capture_log
    ):
        """Folder with incorrect sharing → sharing corrected, then queued."""
        folder = create_folder(parent_folder=managed_folder)
        event = self._make_folder_event("SHARED_LINK.CREATED", folder.id)
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        # Sharing should have been corrected (folder had no shared_link)
        log_output = capture_log.getvalue()
        assert '"action": "fix_folder_sharing"' in log_output
        # Work item still queued
        assert len(webhook_queue_items) == 1

    def test_shared_link_updated_correct_sharing_no_correction(
        self, create_shared_folder, managed_folder, webhook_queue_items, capture_log
    ):
        """Folder with correct sharing → no correction, work item queued."""
        folder = create_shared_folder(parent_folder=managed_folder)
        event = self._make_folder_event("SHARED_LINK.UPDATED", folder.id)
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        log_output = capture_log.getvalue()
        assert '"action": "fix_folder_sharing"' not in log_output
        assert len(webhook_queue_items) == 1

    def test_shared_link_created_folder_missing(self, webhook_queue_items, box_folders, capture_log):
        """Folder missing from Box → log warning, still queue work item."""
        box_folders.clear()
        event = self._make_folder_event("SHARED_LINK.CREATED", "nonexistent")
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        log_output = capture_log.getvalue()
        assert '"action": "folder_missing"' in log_output
        assert len(webhook_queue_items) == 1

    def test_sharing_correction_failure_propagates(self, monkeypatch, create_folder, managed_folder):
        """Box API failure during sharing correction → exception propagates."""
        folder = create_folder(parent_folder=managed_folder)
        monkeypatch.setattr(folder, "create_shared_link", MagicMock(side_effect=Exception("Box API error")))
        event = self._make_folder_event("SHARED_LINK.CREATED", folder.id)
        with pytest.raises(Exception, match="Box API error"):
            webhook_receiver.lambda_handler(event, {})
