import json

import pytest
from botocore.exceptions import ClientError
import boxsdk

from . import conftest
import common


def test_get_box_client(monkeypatch):
    client_id = "client_id"
    client_secret = "client_secret"
    enterprise_id = "enterprise_id"
    jwt_key_id = "jwt_key_id"
    rsa_private_key_data = "rsa_private_key_data"
    rsa_private_key_passphrase = "rsa_private_key_passphrase"
    webhook_signature_key = "webhook_signature_key"

    class MockSecretsClient:
        def get_secret_value(self, SecretId):
            if SecretId == conftest.SECRET_ARN:
                secret = {
                    "box_client_id": client_id,
                    "box_client_secret": client_secret,
                    "box_enterprise_id": enterprise_id,
                    "box_jwt_key_id": jwt_key_id,
                    "box_rsa_private_key_data": rsa_private_key_data,
                    "box_rsa_private_key_passphrase": rsa_private_key_passphrase,
                    "box_webhook_signature_key": webhook_signature_key,
                }
                secret_string = json.dumps(secret)
                return {"SecretString": secret_string}
            else:
                raise ClientError({}, "GetSecretValue")

    mock_secrets_client = MockSecretsClient()

    def mock_client(service_name):
        if service_name == "secretsmanager":
            return mock_secrets_client
        else:
            raise NotImplementedError()

    monkeypatch.setattr("boto3.client", mock_client)

    class MockJWTAuth:
        def __init__(
            self, client_id, client_secret, enterprise_id, jwt_key_id, rsa_private_key_data, rsa_private_key_passphrase
        ):
            self._client_id = client_id
            self._client_secret = client_secret
            self._enterprise_id = enterprise_id
            self._jwt_key_id = jwt_key_id
            self._rsa_private_key_data = rsa_private_key_data
            self._rsa_private_key_passphrase = rsa_private_key_passphrase
            self._authenticated = False

        def authenticate_instance(self):
            assert self._client_id == client_id
            assert self._client_secret == client_secret
            assert self._enterprise_id == enterprise_id
            assert self._jwt_key_id == jwt_key_id
            assert self._rsa_private_key_data == rsa_private_key_data
            assert self._rsa_private_key_passphrase == rsa_private_key_passphrase
            self._authenticated = True

    monkeypatch.setattr(common, "JWTAuth", MockJWTAuth)

    class MockClient:
        USERS = []

        def __init__(self, auth):
            self._auth = auth
            self._as_user = None

        def users(self):
            return (u for u in MockClient.USERS)

        def as_user(self, user):
            self._as_user = user
            return self

    monkeypatch.setattr(common, "Client", MockClient)

    client, key = common.get_box_client()
    assert client._auth._authenticated is True
    assert client._as_user is None
    assert key == webhook_signature_key

    user = object()
    MockClient.USERS.append(user)
    client, key = common.get_box_client()
    assert client._auth._authenticated is True
    assert client._as_user is user
    assert key == webhook_signature_key

    def get_secret_value_binary(SecretId):
        return {"SecretBinary": b"super-secret-bytes"}

    monkeypatch.setattr(mock_secrets_client, "get_secret_value", get_secret_value_binary)
    with pytest.raises(NotImplementedError):
        common.get_box_client()


def test_is_box_object_public(create_file, create_shared_link, monkeypatch):
    bad_file = create_file()
    monkeypatch.delattr(bad_file, "shared_link")
    with pytest.raises(RuntimeError):
        common.is_box_object_public(bad_file)

    unshared_file = create_file()
    assert common.is_box_object_public(unshared_file) is False

    shared_incorrect_access_file = create_file(shared_link=create_shared_link(effective_access="company"))
    assert common.is_box_object_public(shared_incorrect_access_file) is False

    shared_incorrect_permission_file = create_file(shared_link=create_shared_link(effective_permission="can_preview"))
    assert common.is_box_object_public(shared_incorrect_permission_file) is False

    shared_file = create_file(shared_link=create_shared_link())
    assert common.is_box_object_public(shared_file) is True


def test_is_any_parent_public(create_file, create_folder, create_shared_folder, mock_box_client):
    client = mock_box_client

    unshared_folder = create_folder(id=common.BOX_FOLDER_ID)
    unshared_child_folder = create_folder(parent_folder=unshared_folder, id=f"{conftest._next_box_object_id()}")
    shared_child_folder = create_shared_folder(parent_folder=unshared_folder, id=f"{conftest._next_box_object_id()}")

    unshared_file = create_file(parent_folder=unshared_child_folder)
    assert common.is_any_parent_public(client, unshared_file) is False

    shared_file = create_file(parent_folder=shared_child_folder)
    assert common.is_any_parent_public(client, shared_file) is True

    # making sure the caching in common is working
    assert common.is_any_parent_public(client, unshared_file) is False
    assert common.is_any_parent_public(client, shared_file) is True


def test_create_shared_link(create_folder, create_file, create_shared_link, mock_box_client, monkeypatch):
    client = mock_box_client

    bad_folder = create_folder()
    monkeypatch.delattr(bad_folder, "shared_link")
    with pytest.raises(RuntimeError):
        common.create_shared_link(mock_box_client, bad_folder)

    folder = create_folder()
    assert folder.shared_link is None

    folder = common.create_shared_link(client, folder)
    assert folder.shared_link["effective_access"] == "open"
    assert folder.shared_link["effective_permission"] == "can_download"

    file = create_file()
    assert file.shared_link is None

    file = common.create_shared_link(client, file)
    assert folder.shared_link["effective_access"] == "open"
    assert folder.shared_link["effective_permission"] == "can_download"


def test_remove_shared_link(
    create_shared_folder, create_shared_file, create_file, managed_folder, mock_box_client, monkeypatch
):
    client = mock_box_client

    bad_folder = create_shared_folder()
    monkeypatch.delattr(bad_folder, "shared_link")
    with pytest.raises(RuntimeError):
        common.remove_shared_link(mock_box_client, bad_folder)

    shared_file = create_shared_file()
    shared_file = common.remove_shared_link(client, shared_file)
    assert common.is_box_object_public(shared_file) is False

    shared_folder = create_shared_folder(parent_folder=managed_folder)
    unshared_file = create_file(parent_folder=shared_folder)
    assert common.is_any_parent_public(client, unshared_file) is True

    unshared_folder = common.remove_shared_link(client, shared_folder)
    assert common.is_any_parent_public(client, unshared_file) is False
    assert common.is_box_object_public(unshared_folder) is False


def test_get_ddb_table():
    table = common.get_ddb_table()
    assert table.name == conftest.MANIFEST_TABLE_NAME


def test_get_filepath(create_folder, create_shared_file, managed_folder):
    shared_file = create_shared_file()
    assert common.get_filepath(shared_file) == shared_file.name

    nested_folder_one = create_folder(parent_folder=managed_folder)
    nested_folder_two = create_folder(parent_folder=nested_folder_one)
    nested_file = create_shared_file(parent_folder=nested_folder_two)
    expected_path = f"{nested_folder_one.name}/{nested_folder_two.name}/{nested_file.name}"
    assert common.get_filepath(nested_file) == expected_path

    root_file = create_shared_file(parent_folder=conftest.ROOT_FOLDER)
    with pytest.raises(ValueError):
        common.get_filepath(root_file)


def test_make_ddb_item(create_folder, create_shared_file, managed_folder):
    folder = create_folder(parent_folder=managed_folder)
    file = create_shared_file(parent_folder=folder)

    item = common.make_ddb_item(file)
    assert item["filepath"] == f"{folder.name}/{file.name}"
    assert item["box_file_id"] == file.id
    assert item["download_url"] == file.shared_link["download_url"]


def test_put_file_item(create_file, create_shared_file, mock_ddb_table, ddb_items, managed_folder):
    shared_file = create_shared_file()
    common.put_file_item(mock_ddb_table, shared_file)

    assert len(ddb_items) == 1
    assert ddb_items[0]["box_file_id"] == shared_file.id

    private_file = create_file(parent_folder=managed_folder)
    with pytest.raises(ValueError):
        common.put_file_item(mock_ddb_table, private_file)


def test_delete_file_item(create_shared_file, mock_ddb_table, ddb_items):
    file = create_shared_file()
    ddb_items.append(common.make_ddb_item(file))

    common.delete_file_item(mock_ddb_table, file)

    assert len(ddb_items) == 0


def test_get_download_url(create_shared_file, mock_ddb_table, ddb_items):
    file = create_shared_file()
    ddb_items.append(common.make_ddb_item(file))

    assert common.get_download_url(mock_ddb_table, common.get_filepath(file)) == file.shared_link["download_url"]

    assert common.get_download_url(mock_ddb_table, "non/existant/file.dat") is None


def test_get_file(create_file, mock_box_client, monkeypatch):
    file = create_file()
    assert common.get_file(mock_box_client, file.id) is file

    assert common.get_file(mock_box_client, "1234") is None

    def file_raising(file_id):
        raise boxsdk.exception.BoxAPIException(400)

    monkeypatch.setattr(mock_box_client, "file", file_raising)

    with pytest.raises(boxsdk.exception.BoxAPIException):
        common.get_file(mock_box_client, "1234")


def test_get_folder(create_folder, mock_box_client, monkeypatch):
    folder = create_folder()
    assert common.get_folder(mock_box_client, folder.id) is folder

    assert common.get_folder(mock_box_client, "5678") is None

    def folder_raising(folder_id):
        raise boxsdk.exception.BoxAPIException(400)

    monkeypatch.setattr(mock_box_client, "folder", folder_raising)

    with pytest.raises(boxsdk.exception.BoxAPIException):
        common.get_folder(mock_box_client, "1234")


def test_iterate_files(create_folder, create_file, managed_folder):
    # this test never hits the else: break in iterate files
    # I assume that what this means is that folder.get_items works.
    # so indirectly, the fact that else: break isn't covered means that get_items works
    folders = [managed_folder]
    folders.append(create_folder(parent_folder=managed_folder))
    folders.append(create_folder(parent_folder=managed_folder))
    folders.append(create_folder(parent_folder=folders[-1]))

    files = set()
    for folder in folders:
        for _ in range(5):
            files.add(create_file(parent_folder=folder))

    results_files, results_shared = [], []
    for item, shared in common.iterate_files(managed_folder):
        results_files.append(item)
        results_shared.append(shared)

    assert len(results_files) == len(files)
    assert set(results_files) == files

    # Test behavior when we are forced to page through a large number of files
    # in a single folder:
    for _ in range(common.GET_ITEMS_LIMIT * 2 + 1):
        files.add(create_file(parent_folder=managed_folder))

    results_files, results_shared = [], []
    for item, shared in common.iterate_files(managed_folder):
        results_files.append(item)
        results_shared.append(shared)

    assert len(results_files) == len(files)
    assert set(results_files) == files

    # TODO: Test a mix of shared folders
