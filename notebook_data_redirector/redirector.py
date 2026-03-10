import os
import time
import urllib.parse

import common


def lambda_handler(event, context):
    filepath = urllib.parse.unquote(event["pathParameters"]["filepath"])

    common.log_action("INFO", "redirector", "request_received", filepath=filepath)

    ddb_table = common.get_ddb_table()
    download_url = common.get_download_url(ddb_table, filepath)

    if download_url is None:
        common.log_action("INFO", "redirector", "not_found", filepath=filepath)
        return {"statusCode": 404}

    if os.environ.get("ENABLE_URL_VALIDATION", "true") != "false":
        try:
            start = time.time()
            is_valid = common.validate_download_url(download_url)
            duration_ms = int((time.time() - start) * 1000)

            if is_valid:
                common.log_action(
                    "INFO",
                    "redirector",
                    "redirect",
                    filepath=filepath,
                    duration_ms=duration_ms,
                )
            else:
                common.log_action(
                    "WARNING",
                    "redirector",
                    "validate_url_failed",
                    filepath=filepath,
                    duration_ms=duration_ms,
                )
        except Exception:
            common.log_action(
                "WARNING",
                "redirector",
                "validate_url_degraded",
                filepath=filepath,
                error_type="unexpected_exception",
            )
    else:
        common.log_action("INFO", "redirector", "validation_skipped", filepath=filepath)

    return {"statusCode": 302, "headers": {"Location": download_url}}
