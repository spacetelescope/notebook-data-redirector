import json
import urllib.parse

import pytest

import common
import redirector


class TestRedirector:
    @pytest.fixture(autouse=True)
    def monkeypatch_table(self, monkeypatch, mock_ddb_table):
        monkeypatch.setattr(common, "get_ddb_table", lambda: mock_ddb_table)

    @pytest.fixture(autouse=True)
    def stub_validation(self, monkeypatch):
        monkeypatch.setattr(common, "validate_download_url", lambda url: True)

    @pytest.fixture
    def create_redirector_event(self):
        def _create_redirector_event(filepath):
            return {"pathParameters": {"filepath": urllib.parse.quote(filepath)}}

        return _create_redirector_event

    def test_missing_path(self, create_redirector_event):
        event = create_redirector_event("some/bogus/path.dat")
        result = redirector.lambda_handler(event, None)
        assert result["statusCode"] == 404

    @pytest.mark.parametrize("filename", ["normal-file.dat", "file with spaces.dat"])
    def test_redirect_path(
        self, create_redirector_event, create_folder, create_shared_file, managed_folder, ddb_items, filename
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

    def test_calls_validate_when_enabled(
        self, monkeypatch, create_redirector_event, create_folder, create_shared_file, managed_folder, ddb_items
    ):
        subfolder = create_folder(parent_folder=managed_folder)
        file = create_shared_file(parent_folder=subfolder)
        ddb_items.append(common.make_ddb_item(file))

        called = {"value": False}

        def tracking_validate(url):
            called["value"] = True
            return True

        monkeypatch.setattr(common, "validate_download_url", tracking_validate)

        filepath = subfolder.name + "/" + file.name
        event = create_redirector_event(filepath)
        result = redirector.lambda_handler(event, None)
        assert result["statusCode"] == 302
        assert called["value"] is True

    def test_skips_validation_when_disabled(
        self, monkeypatch, create_redirector_event, create_folder, create_shared_file, managed_folder, ddb_items
    ):
        monkeypatch.setenv("ENABLE_URL_VALIDATION", "false")

        subfolder = create_folder(parent_folder=managed_folder)
        file = create_shared_file(parent_folder=subfolder)
        ddb_items.append(common.make_ddb_item(file))

        called = {"value": False}

        def tracking_validate(url):
            called["value"] = True
            return True

        monkeypatch.setattr(common, "validate_download_url", tracking_validate)

        filepath = subfolder.name + "/" + file.name
        event = create_redirector_event(filepath)
        result = redirector.lambda_handler(event, None)
        assert result["statusCode"] == 302
        assert called["value"] is False

    def test_returns_302_when_validation_fails(
        self, monkeypatch, create_redirector_event, create_folder, create_shared_file, managed_folder, ddb_items
    ):
        subfolder = create_folder(parent_folder=managed_folder)
        file = create_shared_file(parent_folder=subfolder)
        ddb_items.append(common.make_ddb_item(file))

        monkeypatch.setattr(common, "validate_download_url", lambda url: False)

        filepath = subfolder.name + "/" + file.name
        expected_location = file.shared_link["download_url"]

        event = create_redirector_event(filepath)
        result = redirector.lambda_handler(event, None)
        assert result["statusCode"] == 302
        assert result["headers"]["Location"] == expected_location

    def test_returns_302_on_unexpected_validation_exception(
        self, monkeypatch, create_redirector_event, create_folder, create_shared_file, managed_folder, ddb_items
    ):
        subfolder = create_folder(parent_folder=managed_folder)
        file = create_shared_file(parent_folder=subfolder)
        ddb_items.append(common.make_ddb_item(file))

        def exploding_validate(url):
            raise RuntimeError("unexpected boom")

        monkeypatch.setattr(common, "validate_download_url", exploding_validate)

        filepath = subfolder.name + "/" + file.name
        expected_location = file.shared_link["download_url"]

        event = create_redirector_event(filepath)
        result = redirector.lambda_handler(event, None)
        assert result["statusCode"] == 302
        assert result["headers"]["Location"] == expected_location

    def test_structured_log_on_validation_failure(
        self,
        monkeypatch,
        create_redirector_event,
        create_folder,
        create_shared_file,
        managed_folder,
        ddb_items,
        capture_log,
    ):
        subfolder = create_folder(parent_folder=managed_folder)
        file = create_shared_file(parent_folder=subfolder)
        ddb_items.append(common.make_ddb_item(file))

        monkeypatch.setattr(common, "validate_download_url", lambda url: False)

        filepath = subfolder.name + "/" + file.name
        event = create_redirector_event(filepath)
        redirector.lambda_handler(event, None)

        log_lines = capture_log.getvalue().strip().split("\n")
        actions = [json.loads(line)["action"] for line in log_lines]
        assert "validate_url_failed" in actions

    def test_structured_log_contains_duration_ms(
        self,
        monkeypatch,
        create_redirector_event,
        create_folder,
        create_shared_file,
        managed_folder,
        ddb_items,
        capture_log,
    ):
        subfolder = create_folder(parent_folder=managed_folder)
        file = create_shared_file(parent_folder=subfolder)
        ddb_items.append(common.make_ddb_item(file))

        monkeypatch.setattr(common, "validate_download_url", lambda url: True)

        filepath = subfolder.name + "/" + file.name
        event = create_redirector_event(filepath)
        redirector.lambda_handler(event, None)

        log_lines = capture_log.getvalue().strip().split("\n")
        redirect_logs = [json.loads(line) for line in log_lines if "duration_ms" in line]
        assert len(redirect_logs) >= 1
        assert isinstance(redirect_logs[0]["duration_ms"], int)
