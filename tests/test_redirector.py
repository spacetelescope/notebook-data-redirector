import urllib.parse
import pytest

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

    def test_missing_path(self, create_redirector_event):
        event = create_redirector_event("some/bogus/path.dat")
        result = redirector.lambda_handler(event, None)
        assert result["statusCode"] == 404

    @pytest.mark.parametrize("filename", ["normal-file.dat", "file with spaces.dat"])
    def test_redirect_path(
        self, create_redirector_event, create_folder, create_shared_file, shared_folder, ddb_items, filename
    ):
        subfolder = create_folder(parent_folder=shared_folder)
        file = create_shared_file(parent_folder=subfolder, name=filename)
        ddb_items.append(common.make_ddb_item(file))

        filepath = subfolder.name + "/" + file.name
        expected_location = file.shared_link["download_url"]

        event = create_redirector_event(filepath)
        result = redirector.lambda_handler(event, None)
        assert result["statusCode"] == 302
        assert result["headers"]["Location"] == expected_location
