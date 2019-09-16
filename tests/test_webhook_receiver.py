import json
import base64

import pytest

import common
import webhook_receiver


SHARED_LINK_TRIGGERS = {"SHARED_LINK.CREATED", "SHARED_LINK.UPDATED", "SHARED_LINK.DELETED"}


def handle_event(event):
    assert webhook_receiver.lambda_handler(event, {})["statusCode"] == 200


class TestWebhookReceiver:
    @pytest.fixture(autouse=True)
    def monkeypatch_clients(self, monkeypatch, mock_ddb_table, mock_box_client, box_webhook_signature_key):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)
        monkeypatch.setattr(common, "get_box_client", lambda: (mock_box_client, box_webhook_signature_key))

    @pytest.fixture
    def create_webhook_event(self, box_webhook_signature_key, box_webhook_id, compute_webhook_signature):
        def _create_webhook_event(trigger, object_id, signature=None):
            if trigger in SHARED_LINK_TRIGGERS:
                source = {"item": {"id": object_id, "type": "file"}}
            elif trigger in common.HANDLED_FOLDER_TRIGGERS:
                source = {"id": object_id, "type": "folder"}
            else:
                source = {"id": object_id, "type": "file"}

            body = {"trigger": trigger, "source": source, "webhook": {"id": box_webhook_id}}
            json_body = json.dumps(body)
            if not signature:
                signature = compute_webhook_signature(bytes(json_body, "utf-8"))

            return {"body": json_body, "headers": {"box-signature-primary": signature}}

        return _create_webhook_event

    def test_invalid_signature(self, create_webhook_event, create_shared_file, ddb_items):
        file = create_shared_file()
        event = create_webhook_event(
            "SHARED_LINK.CREATED", file.id, signature=base64.b64encode(b"nope").decode("utf-8")
        )
        handle_event(event)
        assert len(ddb_items) == 0

    def test_unhandled_webhook(self, create_webhook_event, create_shared_file, ddb_items):
        file = create_shared_file()
        event = create_webhook_event("FILE.BLORPED", file.id)
        handle_event(event)
        assert len(ddb_items) == 0

    def test_shared_link_created(self, create_webhook_event, create_shared_file, ddb_items):
        file = create_shared_file()
        event = create_webhook_event("SHARED_LINK.CREATED", file.id)
        handle_event(event)
        assert len(ddb_items) == 1
        assert ddb_items[0]["box_file_id"] == file.id

    def test_shared_link_updated(
        self, create_webhook_event, create_shared_file, ddb_items, box_files, create_shared_link
    ):
        file = create_shared_file()
        event = create_webhook_event("SHARED_LINK.UPDATED", file.id)
        handle_event(event)
        assert len(ddb_items) == 1
        assert ddb_items[0]["box_file_id"] == file.id

        box_files.remove(file)
        create_shared_file(id=file.id, shared_link=create_shared_link(effective_access="company"))
        handle_event(event)
        assert len(ddb_items) == 0

    def test_shared_link_deleted(self, create_webhook_event, create_file, ddb_items, shared_folder):
        file = create_file(parent_folder=shared_folder)
        ddb_items.append({"filepath": common.get_filepath(file)})
        event = create_webhook_event("SHARED_LINK.DELETED", file.id)
        handle_event(event)
        assert len(ddb_items) == 0

    def test_file_trashed(self, create_webhook_event, create_shared_file, ddb_items, box_files):
        file = create_shared_file()
        ddb_items.append(common.make_ddb_item(file))
        box_files.remove(file)
        event = create_webhook_event("FILE.TRASHED", file.id)
        handle_event(event)
        assert len(ddb_items) == 1

    def test_file_restored(self, create_webhook_event, create_shared_file, ddb_items, box_files):
        file = create_shared_file()
        event = create_webhook_event("FILE.RESTORED", file.id)
        handle_event(event)
        assert len(ddb_items) == 1
        assert ddb_items[0]["box_file_id"] == file.id

    def test_file_moved(self, create_webhook_event, create_shared_file, create_folder, ddb_items, box_files):
        file = create_shared_file()
        ddb_items.append({"filepath": "some/old/path.dat", "box_file_id": file.id, "download_url": "some-download-url"})
        event = create_webhook_event("FILE.MOVED", file.id)
        handle_event(event)
        assert len(ddb_items) == 2
        assert {i["filepath"] for i in ddb_items} == {"some/old/path.dat", file.name}

    def test_folder_restored(self, create_folder, shared_folder, create_shared_file, create_webhook_event, ddb_items):
        folder = create_folder(parent_folder=shared_folder)
        file1 = create_shared_file(parent_folder=folder)
        file2 = create_shared_file(parent_folder=folder)
        event = create_webhook_event("FOLDER.RESTORED", folder.id)
        handle_event(event)
        assert len(ddb_items) == 2
        assert {i["box_file_id"] for i in ddb_items} == {file1.id, file2.id}

    def test_folder_trashed(
        self, create_folder, shared_folder, create_shared_file, create_webhook_event, ddb_items, box_folders
    ):
        folder = create_folder(parent_folder=shared_folder)
        file1 = create_shared_file(parent_folder=folder)
        file2 = create_shared_file(parent_folder=folder)
        ddb_items.append(common.make_ddb_item(file1))
        ddb_items.append(common.make_ddb_item(file2))
        box_folders.clear()
        event = create_webhook_event("FOLDER.TRASHED", folder.id)
        handle_event(event)
        assert len(ddb_items) == 2

    def test_folder_moved(self, create_folder, shared_folder, create_shared_file, create_webhook_event, ddb_items):
        folder = create_folder(parent_folder=shared_folder)
        file1 = create_shared_file(parent_folder=folder)
        file2 = create_shared_file(parent_folder=folder)
        ddb_items.append(
            {"filepath": "some/old/path1.dat", "box_file_id": file1.id, "download_url": "some-download-url"}
        )
        ddb_items.append(
            {"filepath": "some/old/path2.dat", "box_file_id": file2.id, "download_url": "some-download-url"}
        )
        event = create_webhook_event("FOLDER.MOVED", folder.id)
        handle_event(event)
        assert len(ddb_items) == 4
        assert {i["filepath"] for i in ddb_items} == {
            "some/old/path1.dat",
            "some/old/path2.dat",
            common.get_filepath(file1),
            common.get_filepath(file2),
        }
