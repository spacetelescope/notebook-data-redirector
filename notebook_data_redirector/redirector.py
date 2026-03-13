import json
import os
import urllib.parse
from datetime import datetime, timezone

from botocore.exceptions import ClientError

import common

ENABLE_ASYNC_VALIDATION = os.environ.get("ENABLE_ASYNC_VALIDATION", "true")


def _maybe_queue_validation(filepath, download_url):
    if ENABLE_ASYNC_VALIDATION != "true":
        return
    try:
        queue_table = common.get_validation_queue_table()
        now = datetime.now(timezone.utc)
        queue_table.put_item(
            Item={
                "filepath": filepath,
                "download_url": download_url,
                "queued_at": now.isoformat(),
                "expires_at": int(now.timestamp()) + 90000,  # 25 hours
            },
            ConditionExpression="attribute_not_exists(filepath)",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            common.log_action("INFO", "redirector", "queue_write_dedup", filepath=filepath)
        else:
            common.log_action("WARNING", "redirector", "queue_write_failed",
                              filepath=filepath, error_type=type(e).__name__)
    except Exception as e:
        common.log_action("WARNING", "redirector", "queue_write_failed",
                          filepath=filepath, error_type=type(e).__name__)


def lambda_handler(event, context):
    filepath = urllib.parse.unquote(event["pathParameters"]["filepath"])

    common.log_action("INFO", "redirector", "request_received", filepath=filepath)

    try:
        ddb_table = common.get_ddb_table()
        download_url = common.get_download_url(ddb_table, filepath)
    except Exception as e:
        common.log_action("ERROR", "redirector", "ddb_error",
                          filepath=filepath, error_type=type(e).__name__)
        return {
            "statusCode": 502,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "service_error"}),
        }

    if download_url is None:
        common.log_action("INFO", "redirector", "ddb_miss", filepath=filepath)
        return {
            "statusCode": 404,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "file_not_found", "filepath": filepath}),
        }

    common.log_action("INFO", "redirector", "redirect", filepath=filepath)
    response = {"statusCode": 302, "headers": {"Location": download_url}}
    _maybe_queue_validation(filepath, download_url)
    return response
