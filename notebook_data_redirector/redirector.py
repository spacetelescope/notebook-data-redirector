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

    common.log_action("INFO", "redirector", "redirect", filepath=filepath)
    return {"statusCode": 302, "headers": {"Location": download_url}}
