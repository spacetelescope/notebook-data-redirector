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
