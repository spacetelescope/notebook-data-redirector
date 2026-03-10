import json
import logging
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
import boxsdk
from boxsdk.exception import BoxAPIException
import urllib3.exceptions

from . import conftest
import common

# --- Shared helpers for Box client tests ---

MOCK_SECRET = {
    "box_client_id": "client_id",
    "box_client_secret": "client_secret",
    "box_enterprise_id": "enterprise_id",
    "box_jwt_key_id": "jwt_key_id",
    "box_rsa_private_key_data": "rsa_private_key_data",
    "box_rsa_private_key_passphrase": "rsa_private_key_passphrase",
    "box_webhook_signature_key": "webhook_signature_key",
}


@pytest.fixture
def mock_box_infra(monkeypatch):
    """Set up mock Secrets Manager, JWTAuth, and Client for Box client tests."""
    create_count = {"value": 0}

    class MockSecretsClient:
        def get_secret_value(self, SecretId):
            if SecretId == conftest.SECRET_ARN:
                return {"SecretString": json.dumps(MOCK_SECRET)}
            raise ClientError({}, "GetSecretValue")

    def mock_boto_client(service_name):
        if service_name == "secretsmanager":
            return MockSecretsClient()
        raise NotImplementedError()

    monkeypatch.setattr("boto3.client", mock_boto_client)

    class MockJWTAuth:
        def __init__(self, **kwargs):
            self._authenticated = False

        def authenticate_instance(self):
            self._authenticated = True

    monkeypatch.setattr(common, "JWTAuth", MockJWTAuth)

    class MockClient:
        USERS = []

        def __init__(self, auth):
            self._auth = auth
            self._as_user = None
            create_count["value"] += 1

        def users(self):
            return (u for u in MockClient.USERS)

        def as_user(self, user):
            self._as_user = user
            return self

    monkeypatch.setattr(common, "Client", MockClient)

    return {"MockClient": MockClient, "create_count": create_count}


def test_get_box_client_fresh(mock_box_infra):
    client = common.get_box_client()
    assert client._auth._authenticated is True
    assert client._as_user is None
    assert mock_box_infra["create_count"]["value"] == 1


def test_get_box_client_with_app_user(mock_box_infra):
    user = object()
    mock_box_infra["MockClient"].USERS.append(user)
    client = common.get_box_client()
    assert client._auth._authenticated is True
    assert client._as_user is user


def test_get_box_client_cached(mock_box_infra):
    client1 = common.get_box_client()
    client2 = common.get_box_client()
    assert client1 is client2
    assert mock_box_infra["create_count"]["value"] == 1


def test_get_box_client_expiry(mock_box_infra, monkeypatch):
    client1 = common.get_box_client()
    assert mock_box_infra["create_count"]["value"] == 1

    # Force expiry by setting expires_at to the past
    common._box_client_expires_at = 0

    client2 = common.get_box_client()
    assert client2 is not client1
    assert mock_box_infra["create_count"]["value"] == 2


def test_get_box_client_binary_secret(monkeypatch):
    class MockSecretsClient:
        def get_secret_value(self, SecretId):
            return {"SecretBinary": b"super-secret-bytes"}

    def mock_boto_client(service_name):
        if service_name == "secretsmanager":
            return MockSecretsClient()
        raise NotImplementedError()

    monkeypatch.setattr("boto3.client", mock_boto_client)

    with pytest.raises(NotImplementedError):
        common._get_cached_secret()


def test_get_webhook_signature_key_no_client_init(mock_box_infra):
    key = common.get_webhook_signature_key()
    assert key == "webhook_signature_key"
    # Box client should NOT have been created
    assert mock_box_infra["create_count"]["value"] == 0


def test_with_box_retry_success():
    result = common.with_box_retry(lambda: "ok")
    assert result == "ok"


def test_with_box_retry_401(mock_box_infra):
    call_count = {"value": 0}

    def failing_then_ok():
        call_count["value"] += 1
        if call_count["value"] == 1:
            raise BoxAPIException(401)
        return "recovered"

    result = common.with_box_retry(failing_then_ok)
    assert result == "recovered"
    assert call_count["value"] == 2


def test_with_box_retry_non_401():
    def always_fails():
        raise BoxAPIException(403)

    with pytest.raises(BoxAPIException):
        common.with_box_retry(always_fails)


def test_with_box_retry_401_twice(mock_box_infra):
    def always_401():
        raise BoxAPIException(401)

    with pytest.raises(BoxAPIException):
        common.with_box_retry(always_401)


def test_log_action_structured_output(capture_log):
    common.log_action("INFO", "redirector", "request_received", filepath="test/path.dat")
    output = capture_log.getvalue().strip()
    log_entry = json.loads(output)
    assert log_entry["level"] == "INFO"
    assert log_entry["function"] == "redirector"
    assert log_entry["action"] == "request_received"
    assert log_entry["filepath"] == "test/path.dat"


def test_log_action_omits_none_fields(capture_log):
    common.log_action("INFO", "sync", "checking_files")
    output = capture_log.getvalue().strip()
    log_entry = json.loads(output)
    assert "filepath" not in log_entry
    assert "box_file_id" not in log_entry
    assert "duration_ms" not in log_entry
    assert "error_type" not in log_entry
    assert "remediation" not in log_entry


def test_log_action_no_credential_leak(capture_log):
    common.log_action("ERROR", "common", "test_action", error_type="auth_failure", remediation="retry")
    output = capture_log.getvalue().strip()
    log_entry = json.loads(output)
    assert log_entry["error_type"] == "auth_failure"
    assert log_entry["remediation"] == "retry"
    # Verify no secret fields leak
    for secret_field in (
        "box_client_id",
        "box_client_secret",
        "rsa_private_key_data",
        "box_rsa_private_key_passphrase",
    ):
        assert secret_field not in output


def test_third_party_loggers_suppressed():
    """boxsdk/urllib3 log at INFO during auth, leaking JWT secrets. Must be WARNING+."""
    for name in ("boxsdk", "urllib3", "boto3", "botocore"):
        assert (
            logging.getLogger(name).level >= logging.WARNING
        ), f"{name} logger not suppressed — credentials may leak to CloudWatch"


def test_webhook_handler_signature_key_without_client(mock_box_infra):
    """Verify get_webhook_signature_key does not trigger _create_box_client."""
    key = common.get_webhook_signature_key()
    assert key == "webhook_signature_key"
    # Verify no client was created (create_count stays at 0)
    assert mock_box_infra["create_count"]["value"] == 0
    # Now get client — should bump count
    common.get_box_client()
    assert mock_box_infra["create_count"]["value"] == 1


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


class TestValidateDownloadUrl:
    def _mock_response(self, status):
        resp = MagicMock()
        resp.status = status
        return resp

    def test_valid_on_206(self, monkeypatch):
        resp = self._mock_response(206)
        monkeypatch.setattr(common, "_http", MagicMock(**{"request.return_value": resp}))
        assert common.validate_download_url("https://example.com/file") == "valid"
        resp.release_conn.assert_called_once()

    def test_valid_on_200(self, monkeypatch):
        resp = self._mock_response(200)
        monkeypatch.setattr(common, "_http", MagicMock(**{"request.return_value": resp}))
        assert common.validate_download_url("https://example.com/file") == "valid"
        resp.release_conn.assert_called_once()

    def test_broken_on_403(self, monkeypatch):
        resp = self._mock_response(403)
        monkeypatch.setattr(common, "_http", MagicMock(**{"request.return_value": resp}))
        assert common.validate_download_url("https://example.com/file") == "broken"

    def test_broken_on_404(self, monkeypatch):
        resp = self._mock_response(404)
        monkeypatch.setattr(common, "_http", MagicMock(**{"request.return_value": resp}))
        assert common.validate_download_url("https://example.com/file") == "broken"

    def test_uncertain_on_429(self, monkeypatch):
        resp = self._mock_response(429)
        monkeypatch.setattr(common, "_http", MagicMock(**{"request.return_value": resp}))
        assert common.validate_download_url("https://example.com/file") == "uncertain"

    def test_uncertain_on_500(self, monkeypatch):
        resp = self._mock_response(500)
        monkeypatch.setattr(common, "_http", MagicMock(**{"request.return_value": resp}))
        assert common.validate_download_url("https://example.com/file") == "uncertain"

    def test_uncertain_on_timeout(self, monkeypatch):
        mock_pool = MagicMock()
        mock_pool.request.side_effect = urllib3.exceptions.TimeoutError()
        monkeypatch.setattr(common, "_http", mock_pool)
        assert common.validate_download_url("https://example.com/file") == "uncertain"

    def test_uncertain_on_connection_error(self, monkeypatch):
        mock_pool = MagicMock()
        mock_pool.request.side_effect = urllib3.exceptions.HTTPError("connection failed")
        monkeypatch.setattr(common, "_http", mock_pool)
        assert common.validate_download_url("https://example.com/file") == "uncertain"

    def test_release_conn_called(self, monkeypatch):
        resp = self._mock_response(206)
        monkeypatch.setattr(common, "_http", MagicMock(**{"request.return_value": resp}))
        common.validate_download_url("https://example.com/file")
        resp.release_conn.assert_called_once()

    def test_request_parameters(self, monkeypatch):
        resp = self._mock_response(206)
        mock_pool = MagicMock(**{"request.return_value": resp})
        monkeypatch.setattr(common, "_http", mock_pool)
        common.validate_download_url("https://example.com/file")
        mock_pool.request.assert_called_once()
        args, kwargs = mock_pool.request.call_args
        assert args == ("GET", "https://example.com/file")
        assert kwargs["headers"] == {"Range": "bytes=0-0"}
        assert kwargs["preload_content"] is False
        assert kwargs["timeout"].connect_timeout == 5.0
        assert kwargs["timeout"].read_timeout == 5.0
        assert kwargs["retries"].redirect == 3


class TestIsStale:
    def test_none_is_stale(self):
        assert common.is_stale(None) is True

    def test_old_timestamp_is_stale(self):
        old = (datetime.now(timezone.utc) - timedelta(hours=21)).isoformat()
        assert common.is_stale(old) is True

    def test_fresh_timestamp_is_not_stale(self):
        fresh = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        assert common.is_stale(fresh) is False
