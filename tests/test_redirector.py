import json
import urllib.parse
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

import common
import redirector


class TestRedirector:
    @pytest.fixture(autouse=True)
    def monkeypatch_table(self, monkeypatch, mock_ddb_table):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)

    @pytest.fixture
    def create_redirector_event(self):
        def _create_redirector_event(filepath):
            return {"pathParameters": {"filepath": urllib.parse.quote(filepath)}}

        return _create_redirector_event

    @pytest.fixture
    def mock_queue_table(self, monkeypatch):
        table = MagicMock()
        monkeypatch.setattr(common, "get_validation_queue_table", lambda: table)
        return table

    def test_missing_path(self, create_redirector_event):
        event = create_redirector_event("some/bogus/path.dat")
        result = redirector.lambda_handler(event, None)
        assert result["statusCode"] == 404
        assert result["headers"]["Content-Type"] == "application/json"
        body = json.loads(result["body"])
        assert body == {"error": "file_not_found", "filepath": "some/bogus/path.dat"}

    def test_missing_path_no_box_calls(self, create_redirector_event):
        event = create_redirector_event("some/bogus/path.dat")
        with patch.object(common, "get_box_client") as mock_box:
            result = redirector.lambda_handler(event, None)
        assert result["statusCode"] == 404
        mock_box.assert_not_called()

    def test_missing_path_no_queue_writes(self, mock_queue_table, create_redirector_event):
        event = create_redirector_event("some/bogus/path.dat")
        result = redirector.lambda_handler(event, None)
        assert result["statusCode"] == 404
        mock_queue_table.put_item.assert_not_called()

    @pytest.mark.parametrize("filename", ["normal-file.dat", "file with spaces.dat"])
    def test_redirect_path(
        self, create_redirector_event, mock_queue_table, create_folder, create_shared_file, managed_folder, ddb_items, filename
    ):
        subfolder = create_folder(parent_folder=managed_folder)
        file = create_shared_file(parent_folder=subfolder, name=filename)
        ddb_items.append(common.make_ddb_item(file))

        filepath = subfolder.name + "/" + file.name
        expected_location = file.shared_link["download_url"]

        event = create_redirector_event(filepath)
        result = redirector.lambda_handler(event, None)
        assert result["statusCode"] == 302
        assert result["headers"]["Location"] == expected_location


class TestAsyncQueueWrite:
    @pytest.fixture(autouse=True)
    def monkeypatch_table(self, monkeypatch, mock_ddb_table):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)

    @pytest.fixture
    def create_redirector_event(self):
        def _create_redirector_event(filepath):
            return {"pathParameters": {"filepath": urllib.parse.quote(filepath)}}

        return _create_redirector_event

    @pytest.fixture
    def mock_queue_table(self, monkeypatch):
        table = MagicMock()
        monkeypatch.setattr(common, "get_validation_queue_table", lambda: table)
        return table

    @pytest.fixture
    def setup_redirect(self, create_folder, create_shared_file, managed_folder, ddb_items):
        subfolder = create_folder(parent_folder=managed_folder)
        file = create_shared_file(parent_folder=subfolder, name="data.dat")
        ddb_items.append(common.make_ddb_item(file))
        filepath = subfolder.name + "/" + file.name
        download_url = file.shared_link["download_url"]
        return filepath, download_url

    def test_successful_queue_write(self, monkeypatch, create_redirector_event, mock_queue_table, setup_redirect):
        monkeypatch.setattr(redirector, "ENABLE_ASYNC_VALIDATION", "true")
        filepath, download_url = setup_redirect
        event = create_redirector_event(filepath)

        result = redirector.lambda_handler(event, None)

        assert result["statusCode"] == 302
        mock_queue_table.put_item.assert_called_once()
        call_kwargs = mock_queue_table.put_item.call_args[1]
        item = call_kwargs["Item"]
        assert item["filepath"] == filepath
        assert item["download_url"] == download_url
        assert "queued_at" in item
        assert isinstance(item["expires_at"], int)
        assert call_kwargs["ConditionExpression"] == "attribute_not_exists(filepath)"

    def test_dedup_conditional_check_failed(
        self, monkeypatch, create_redirector_event, mock_queue_table, setup_redirect, capture_log
    ):
        monkeypatch.setattr(redirector, "ENABLE_ASYNC_VALIDATION", "true")
        filepath, _ = setup_redirect
        mock_queue_table.put_item.side_effect = ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException", "Message": "condition not met"}},
            "PutItem",
        )
        event = create_redirector_event(filepath)

        result = redirector.lambda_handler(event, None)

        assert result["statusCode"] == 302
        log_output = capture_log.getvalue()
        assert '"action": "queue_write_dedup"' in log_output

    def test_queue_write_failure_swallowed(
        self, monkeypatch, create_redirector_event, mock_queue_table, setup_redirect, capture_log
    ):
        monkeypatch.setattr(redirector, "ENABLE_ASYNC_VALIDATION", "true")
        filepath, _ = setup_redirect
        mock_queue_table.put_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "service error"}},
            "PutItem",
        )
        event = create_redirector_event(filepath)

        result = redirector.lambda_handler(event, None)

        assert result["statusCode"] == 302
        log_output = capture_log.getvalue()
        assert '"action": "queue_write_failed"' in log_output
        assert '"error_type": "ClientError"' in log_output

    def test_feature_toggle_off(self, monkeypatch, create_redirector_event, mock_queue_table, setup_redirect):
        monkeypatch.setattr(redirector, "ENABLE_ASYNC_VALIDATION", "false")
        filepath, _ = setup_redirect
        event = create_redirector_event(filepath)

        result = redirector.lambda_handler(event, None)

        assert result["statusCode"] == 302
        mock_queue_table.put_item.assert_not_called()

    def test_queue_write_non_client_error_swallowed(
        self, monkeypatch, create_redirector_event, mock_queue_table, setup_redirect, capture_log
    ):
        monkeypatch.setattr(redirector, "ENABLE_ASYNC_VALIDATION", "true")
        filepath, _ = setup_redirect
        mock_queue_table.put_item.side_effect = RuntimeError("unexpected failure")
        event = create_redirector_event(filepath)

        result = redirector.lambda_handler(event, None)

        assert result["statusCode"] == 302
        log_output = capture_log.getvalue()
        assert '"action": "queue_write_failed"' in log_output
        assert '"error_type": "RuntimeError"' in log_output

    def test_no_queue_write_on_404(self, monkeypatch, create_redirector_event, mock_queue_table):
        monkeypatch.setattr(redirector, "ENABLE_ASYNC_VALIDATION", "true")
        event = create_redirector_event("some/bogus/path.dat")

        result = redirector.lambda_handler(event, None)

        assert result["statusCode"] == 404
        mock_queue_table.put_item.assert_not_called()


class TestDdbErrorHandling:
    @pytest.fixture
    def create_redirector_event(self):
        def _create_redirector_event(filepath):
            return {"pathParameters": {"filepath": urllib.parse.quote(filepath)}}

        return _create_redirector_event

    def test_get_download_url_client_error(self, monkeypatch, create_redirector_event, capture_log):
        monkeypatch.setattr(common, "get_ddb_table", lambda: MagicMock())
        monkeypatch.setattr(
            common,
            "get_download_url",
            lambda *a, **kw: (_ for _ in ()).throw(
                ClientError({"Error": {"Code": "InternalServerError", "Message": "err"}}, "GetItem")
            ),
        )
        event = create_redirector_event("some/path.dat")

        result = redirector.lambda_handler(event, None)

        assert result["statusCode"] == 502
        assert result["headers"]["Content-Type"] == "application/json"
        body = json.loads(result["body"])
        assert body == {"error": "service_error"}
        log_output = capture_log.getvalue()
        assert '"action": "ddb_error"' in log_output

    def test_get_ddb_table_raises(self, monkeypatch, create_redirector_event):
        monkeypatch.setattr(common, "get_ddb_table", lambda: (_ for _ in ()).throw(RuntimeError("connection failed")))
        event = create_redirector_event("some/path.dat")

        result = redirector.lambda_handler(event, None)

        assert result["statusCode"] == 502
        assert result["headers"]["Content-Type"] == "application/json"
        body = json.loads(result["body"])
        assert body == {"error": "service_error"}


class TestConcurrency:
    @pytest.fixture(autouse=True)
    def monkeypatch_table(self, monkeypatch, mock_ddb_table):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)

    @pytest.fixture
    def create_redirector_event(self):
        def _create_redirector_event(filepath):
            return {"pathParameters": {"filepath": urllib.parse.quote(filepath)}}

        return _create_redirector_event

    @pytest.fixture
    def mock_queue_table(self, monkeypatch):
        table = MagicMock()
        monkeypatch.setattr(common, "get_validation_queue_table", lambda: table)
        return table

    def test_multiple_calls_same_filepath_dedup(
        self, monkeypatch, create_redirector_event, mock_queue_table,
        create_folder, create_shared_file, managed_folder, ddb_items,
    ):
        monkeypatch.setattr(redirector, "ENABLE_ASYNC_VALIDATION", "true")
        subfolder = create_folder(parent_folder=managed_folder)
        file = create_shared_file(parent_folder=subfolder, name="data.dat")
        ddb_items.append(common.make_ddb_item(file))
        filepath = subfolder.name + "/" + file.name
        event = create_redirector_event(filepath)

        # First call succeeds, subsequent raise ConditionalCheckFailedException
        call_count = 0

        def put_item_side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                raise ClientError(
                    {"Error": {"Code": "ConditionalCheckFailedException", "Message": "dup"}},
                    "PutItem",
                )

        mock_queue_table.put_item.side_effect = put_item_side_effect

        results = [redirector.lambda_handler(event, None) for _ in range(3)]
        assert all(r["statusCode"] == 302 for r in results)

    def test_handler_stateless_independent_calls(
        self, monkeypatch, create_redirector_event, mock_queue_table,
        create_folder, create_shared_file, managed_folder, ddb_items,
    ):
        monkeypatch.setattr(redirector, "ENABLE_ASYNC_VALIDATION", "true")
        subfolder = create_folder(parent_folder=managed_folder)
        files = []
        for i in range(5):
            f = create_shared_file(parent_folder=subfolder, name=f"file-{i}.dat")
            ddb_items.append(common.make_ddb_item(f))
            files.append(f)

        results = []
        for f in files:
            filepath = subfolder.name + "/" + f.name
            event = create_redirector_event(filepath)
            results.append(redirector.lambda_handler(event, None))

        assert all(r["statusCode"] == 302 for r in results)
        for r, f in zip(results, files):
            assert r["headers"]["Location"] == f.shared_link["download_url"]
