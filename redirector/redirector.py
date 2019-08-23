import logging

import common


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def lambda_handler(event, context):
    filename = event["pathParameters"]["filename"]

    LOGGER.info("Received request for %s", filename)

    ddb_table = common.get_ddb_table()
    download_url = common.get_download_url(ddb_table, filename)

    if download_url:
        LOGGER.info("Redirecting to %s", download_url)
        return {"statusCode": 302, "headers": {"Location": download_url}}
    else:
        LOGGER.info("Not found, returning 404")
        return {"statusCode": 404}
