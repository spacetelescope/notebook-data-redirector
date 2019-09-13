import os
import random
import string

import boxsdk
import pytest


ROOT_FOLDER = boxsdk.folder.Folder(
    None,
    "0",
    {
        "type": "folder",
        "id": "0",
        "sequence_id": None,
        "etag": None,
        "name": "All Files",
        "path_collection": {"total_count": 0, "entries": []},
    },
)

SECRET_ARN = "arn:aws:secretsmanager:test-region:000000000000:secret:test-secret-Az1Qw2"
SHARED_BOX_FOLDER_ID = "5"
MANIFEST_TABLE_NAME = "test-manifest-table"


os.environ["SECRET_ARN"] = SECRET_ARN
os.environ["BOX_FOLDER_ID"] = SHARED_BOX_FOLDER_ID
os.environ["MANIFEST_TABLE_NAME"] = MANIFEST_TABLE_NAME
os.environ["AWS_DEFAULT_REGION"] = "gl-north-14"


@pytest.fixture
def shared_folder(create_folder):
    return create_folder(id=SHARED_BOX_FOLDER_ID)


@pytest.fixture
def box_files():
    return []


@pytest.fixture
def box_folders():
    return [ROOT_FOLDER]


@pytest.fixture
def ddb_items():
    return []


@pytest.fixture
def create_file(box_files, monkeypatch):
    def _create_file(parent_folder=ROOT_FOLDER, **kwargs):
        object_id = kwargs.pop("id", None)
        if object_id is None:
            object_id = _next_box_object_id()

        response_object = {
            "type": "file",
            "id": object_id,
            "sequence_id": "0",
            "etag": "0",
            "path_collection": _get_path_collection(parent_folder),
            "shared_link": None,
        }
        response_object.update(kwargs)

        if "name" not in response_object:
            response_object["name"] = f"test-folder-{object_id}"

        file = boxsdk.file.File(None, object_id, response_object)
        monkeypatch.setattr(file, "get", lambda: file)

        box_files.append(file)
        return file

    return _create_file


@pytest.fixture
def create_folder(box_folders, box_files, monkeypatch):
    def _create_folder(parent_folder=ROOT_FOLDER, **kwargs):
        object_id = kwargs.pop("id", None)
        if object_id is None:
            object_id = _next_box_object_id()

        response_object = {
            "type": "file",
            "id": object_id,
            "sequence_id": "0",
            "etag": "0",
            "path_collection": _get_path_collection(parent_folder),
        }
        response_object.update(kwargs)

        if "name" not in response_object:
            response_object["name"] = f"test-file-{object_id}"

        folder = boxsdk.folder.Folder(None, object_id, response_object)

        def get_items(limit=100, offset=0, fields=None):
            folder_items = [
                f
                for f in box_files + box_folders
                if f.path_collection["total_count"] > 0 and f.path_collection["entries"][-1]["id"] == folder.id
            ]
            return folder_items[offset : offset + limit]

        monkeypatch.setattr(folder, "get_items", get_items)

        box_folders.append(folder)
        return folder

    return _create_folder


@pytest.fixture
def create_shared_link():
    def _create_shared_link(suffix="dat", **kwargs):
        shared_id = "".join(random.choices(string.ascii_lowercase + string.digits, k=32))
        result = {
            "effective_access": "open",
            "effective_permission": "can_download",
            "download_url": f"https://company.box.com/shared/static/{shared_id}.{suffix}",
        }
        result.update(kwargs)
        return result

    return _create_shared_link


@pytest.fixture
def create_shared_file(create_file, create_shared_link, shared_folder):
    def _create_shared_file(parent_folder=shared_folder, **kwargs):
        if "shared_link" not in kwargs:
            kwargs["shared_link"] = create_shared_link()
        return create_file(parent_folder=parent_folder, **kwargs)

    return _create_shared_file


@pytest.fixture
def mock_ddb_table(ddb_items):
    class MockTable:
        def put_item(self, Item):
            ddb_items.append(Item)

        def delete_item(self, Key):
            item = next((i for i in ddb_items if {i[k] for k in Key.keys()} == set(Key.values())), None)
            if item:
                ddb_items.remove(item)

        def get_item(self, Key):
            result = {}
            item = next((i for i in ddb_items if {i[k] for k in Key.keys()} == set(Key.values())), None)
            if item:
                result["Item"] = item
            return result

    return MockTable()


@pytest.fixture
def mock_box_client(box_folders, box_files):
    class MockBoxClient:
        def file(self, file_id):
            try:
                return next(f for f in box_files if f.object_id == file_id)
            except StopIteration:
                raise boxsdk.exception.BoxAPIException(404)

        def folder(self, folder_id):
            try:
                return next(f for f in box_folders if f.object_id == folder_id)
            except StopIteration:
                raise boxsdk.exception.BoxAPIException(404)

    return MockBoxClient()


def _next_box_object_id():
    result = str(_next_box_object_id._next_id)
    _next_box_object_id._next_id += 1
    return result


# Here we're storing the id sequence variable as an attribute
# of the function object itself.  Just a sneaky way to achieve
# a stateful function.
_next_box_object_id._next_id = 500000000000


def _get_path_collection(parent_folder):
    total_count = parent_folder.response_object["path_collection"]["total_count"] + 1
    entries = parent_folder.response_object["path_collection"]["entries"] + [
        {
            "id": parent_folder.id,
            "name": parent_folder.name,
            "type": parent_folder.type,
            "sequence_id": parent_folder.sequence_id,
            "etag": parent_folder.etag,
        }
    ]
    return {"total_count": total_count, "entries": entries}
