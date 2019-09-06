import logging

import common


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def lambda_handler(event, context):
    filepath = event["pathParameters"]["filepath"]

    LOGGER.info("Received request for %s", filepath)

    ddb_table = common.get_ddb_table()
    download_url = common.get_download_url(ddb_table, filepath)

    if download_url is None:
        LOGGER.info("Not found, returning 404")
        return {"statusCode": 404}
    else:
        LOGGER.info("Redirecting to %s", download_url)
        return {"statusCode": 302, "headers": {"Location": download_url}}
