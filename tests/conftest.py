import boxsdk
import pytest
import os


ROOT_FOLDER = boxsdk.folder.Folder(
    None, "0", {"type": "folder", "id": "0", "name": "All Files", "path_collection": {"total_count": 0, "entries": []}}
)

SHARED_FOLDER = boxsdk.folder.Folder(
    None,
    "1",
    {
        "type": "folder",
        "id": "1",
        "name": "TestSharedFolder",
        "path_collection": {
            "total_count": 1,
            "entries": [{"id": ROOT_FOLDER.id, "name": ROOT_FOLDER.name, "type": ROOT_FOLDER.type}],
        },
    },
)

SECRET_ARN = "arn:aws:secretsmanager:test-region:000000000000:secret:test-secret-Az1Qw2"
SHARED_BOX_FOLDER_ID = SHARED_FOLDER.id
MANIFEST_TABLE_NAME = "test-manifest-table"


os.environ["SECRET_ARN"] = SECRET_ARN
os.environ["BOX_FOLDER_ID"] = SHARED_BOX_FOLDER_ID
os.environ["MANIFEST_TABLE_NAME"] = MANIFEST_TABLE_NAME


@pytest.fixture
def create_file():
    def _create_file(parent_folder=ROOT_FOLDER, **kwargs):
        object_id = kwargs.pop("id", None)
        if object_id is None:
            object_id = _next_box_object_id()

        response_object = {
            "type": "file",
            "id": object_id,
            "path_collection": _get_path_collection(parent_folder),
            "shared_link": None,
        }
        response_object.update(kwargs)

        if "name" not in response_object:
            response_object["name"] = f"test-folder-{object_id}"

        return boxsdk.file.File(None, object_id, response_object)

    return _create_file


@pytest.fixture
def create_folder():
    def _create_folder(parent_folder=ROOT_FOLDER, **kwargs):
        object_id = kwargs.pop("id", None)
        if object_id is None:
            object_id = _next_box_object_id()

        response_object = {"type": "file", "id": "0", "path_collection": _get_path_collection(parent_folder)}
        response_object.update(kwargs)

        if "name" not in response_object:
            response_object["name"] = f"test-file-{object_id}"

        return boxsdk.folder.Folder(None, object_id, response_object)

    return _create_folder


def _next_box_object_id():
    result = str(_next_box_object_id._next_id)
    _next_box_object_id._next_id += 1
    return result


_next_box_object_id._next_id = 500000000000


def _get_path_collection(parent_folder):
    total_count = parent_folder.response_object["path_collection"]["total_count"] + 1
    entries = [
        {"id": parent_folder.id, "name": parent_folder.name, "type": parent_folder.type}
    ] + parent_folder.response_object["path_collection"]["entries"]
    return {"total_count": total_count, "entries": entries}
