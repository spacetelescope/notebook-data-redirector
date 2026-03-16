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
        self, monkeypatch, mock_ddb_table, mock_box_client, box_webhook_signature_key, compute_webhook_signature
    ):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_box_client", lambda: mock_box_client)
        monkeypatch.setattr(common, "get_webhook_signature_key", lambda: box_webhook_signature_key)
        monkeypatch.setattr(common, "get_event_dedup_table", lambda: MagicMock())

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
        self, create_webhook_event, create_shared_file, ddb_items, create_shared_folder, create_file, managed_folder
    ):
        # due to changes in the BOX API, sharing a file directly will result in the shared link being deleted in the webhook receiver
        file = create_shared_file()
        event = create_webhook_event("SHARED_LINK.CREATED", file)
        handle_event(event)
        assert len(ddb_items) == 0

        # but a shared folder is supported
        folder = create_shared_folder(parent_folder=managed_folder)
        file = create_file(parent_folder=folder)
        event = create_webhook_event("SHARED_LINK.CREATED", folder)
        handle_event(event)
        assert len(ddb_items) == 1
        assert ddb_items[0]["box_file_id"] == file.id

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

        # change the file's shared_link by changing the folder and see that it's removed
        box_files.remove(file)
        box_folders.remove(folder)
        folder = create_shared_folder(
            id=folder.id, parent_folder=managed_folder, shared_link=create_shared_link(effective_access="company")
        )
        file = create_shared_file(id=file.id, parent_folder=folder)
        event = create_webhook_event("SHARED_LINK.UPDATED", folder)
        handle_event(event)
        assert len(ddb_items) == 0

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
        create_folder,
        ddb_items,
        box_files,
        managed_folder,
        create_shared_folder,
    ):
        folder = create_shared_folder(parent_folder=managed_folder)
        file = create_file(parent_folder=folder)
        ddb_items.append({"filepath": "some/old/path.dat", "box_file_id": file.id, "download_url": "some-download-url"})
        event = create_webhook_event("FILE.MOVED", file)
        handle_event(event)
        assert len(ddb_items) == 2
        assert {i["filepath"] for i in ddb_items} == {"some/old/path.dat", common.get_filepath(file)}

    def test_folder_restored(self, create_shared_folder, managed_folder, create_file, create_webhook_event, ddb_items):
        folder = create_shared_folder(parent_folder=managed_folder)
        file1 = create_file(parent_folder=folder)
        file2 = create_file(parent_folder=folder)
        event = create_webhook_event("FOLDER.RESTORED", folder)
        handle_event(event)
        assert len(ddb_items) == 2
        assert {i["box_file_id"] for i in ddb_items} == {file1.id, file2.id}

    def test_folder_trashed(
        self, create_folder, managed_folder, create_shared_file, create_webhook_event, ddb_items, box_folders
    ):
        folder = create_folder(parent_folder=managed_folder)
        file1 = create_shared_file(parent_folder=folder)
        file2 = create_shared_file(parent_folder=folder)
        ddb_items.append(common.make_ddb_item(file1))
        ddb_items.append(common.make_ddb_item(file2))
        box_folders.clear()
        event = create_webhook_event("FOLDER.TRASHED", folder)
        handle_event(event)
        assert len(ddb_items) == 2

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

    def test_folder_moved(self, create_shared_folder, managed_folder, create_file, create_webhook_event, ddb_items):
        folder = create_shared_folder(parent_folder=managed_folder)
        file1 = create_file(parent_folder=folder)
        file2 = create_file(parent_folder=folder)
        ddb_items.append(
            {"filepath": "some/old/path1.dat", "box_file_id": file1.id, "download_url": "some-download-url"}
        )
        ddb_items.append(
            {"filepath": "some/old/path2.dat", "box_file_id": file2.id, "download_url": "some-download-url"}
        )
        event = create_webhook_event("FOLDER.MOVED", folder)
        handle_event(event)
        assert len(ddb_items) == 4
        assert {i["filepath"] for i in ddb_items} == {
            "some/old/path1.dat",
            "some/old/path2.dat",
            common.get_filepath(file1),
            common.get_filepath(file2),
        }


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
        self.mock_dedup_table.put_item.assert_not_called()
        self.mock_box_client_fn.assert_not_called()
        self.mock_ddb_fn.assert_not_called()
        self.mock_validate_fn.assert_not_called()
        log_output = capture_log.getvalue()
        assert '"action": "cascade_event_skipped"' in log_output

    def test_different_user_continues(self):
        """Event with different created_by.id → processing continues (not filtered)."""
        event = self._make_event(created_by={"type": "user", "id": "99999999", "name": "Other"})
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        # Dedup table should have been called (processing continued past cascade check)
        self.mock_dedup_table.put_item.assert_called_once()

    def test_no_created_by_continues(self):
        """Event with no created_by field → processing continues (not filtered)."""
        event = self._make_event(created_by=None)
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        self.mock_dedup_table.put_item.assert_called_once()

    def test_empty_service_account_id_skips_check(self, monkeypatch):
        """SERVICE_ACCOUNT_USER_ID is empty → cascade check skipped, processing continues."""
        monkeypatch.setattr(common, "SERVICE_ACCOUNT_USER_ID", "")
        event = self._make_event(created_by={"type": "user", "id": "12345678", "name": "Service"})
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        # Should have continued past cascade check to dedup
        self.mock_dedup_table.put_item.assert_called_once()


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

    def test_new_event_dedup_write_succeeds(self):
        """First event with unique event_id → dedup write succeeds, processing continues."""
        event = self._make_event(event_id="evt_new_1")
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        self.mock_dedup_table.put_item.assert_called_once()

    def test_dedup_item_schema(self):
        """Dedup item has correct schema: event_id, processed_at, expires_at with 48h TTL."""
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
        assert call_kwargs["ConditionExpression"] == "attribute_not_exists(event_id)"

    def test_duplicate_event_returns_200(self, capture_log):
        """Duplicate event → ConditionalCheckFailedException, 200 returned, no further processing."""
        self.mock_dedup_table.put_item.side_effect = ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException", "Message": "condition not met"}},
            "PutItem",
        )
        event = self._make_event(event_id="evt_dup_1")
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        log_output = capture_log.getvalue()
        assert '"action": "duplicate_event_skipped"' in log_output
        self.mock_box_client_fn.assert_not_called()
        self.mock_ddb_fn.assert_not_called()
        self.mock_validate_fn.assert_not_called()

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
        self.mock_dedup_table.put_item.assert_not_called()

    def test_ddb_error_continues_processing(self, capture_log):
        """DDB error on dedup write → processing continues (best-effort dedup)."""
        self.mock_dedup_table.put_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "service error"}},
            "PutItem",
        )
        event = self._make_event(event_id="evt_err_1")
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        log_output = capture_log.getvalue()
        assert '"action": "dedup_write_failed"' in log_output


class TestAlwaysReturn200:
    """Task 3: Ensure all code paths return 200 (AC #5)."""

    @pytest.fixture(autouse=True)
    def monkeypatch_clients(self, monkeypatch):
        monkeypatch.setattr(common, "get_event_dedup_table", lambda: MagicMock())

    def test_malformed_body_returns_200(self):
        """Completely malformed body → 200 (caught by outer try/except)."""
        event = {"body": "not valid json", "headers": {}}
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200

    def test_missing_trigger_returns_200(self):
        """Body missing trigger field → 200."""
        event = {"body": json.dumps({"source": {}}), "headers": {}}
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200

    def test_missing_source_id_returns_200(self, monkeypatch, capture_log):
        """Source with no id field → 200 (RuntimeError caught)."""
        monkeypatch.setattr(common, "get_webhook_signature_key", lambda: "key")
        monkeypatch.setattr(common, "validate_webhook_message", lambda body, headers, key: True)
        body = {
            "id": "evt_1",
            "trigger": "FILE.UPLOADED",
            "source": {},
            "created_by": {"type": "user", "id": "99999999"},
        }
        event = {"body": json.dumps(body), "headers": {"box-signature-primary": "sig"}}
        result = webhook_receiver.lambda_handler(event, {})
        assert result["statusCode"] == 200
        log_output = capture_log.getvalue()
        assert '"action": "webhook_handler_error"' in log_output
        assert '"error"' in log_output
