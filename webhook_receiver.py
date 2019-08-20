import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

validate_webhook = False


def get_secret():
    import base64
    from botocore.exceptions import ClientError

    secret_name = "BoxApiCredentials"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "DecryptionFailureException":
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InternalServiceErrorException":
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InvalidParameterException":
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InvalidRequestException":
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "ResourceNotFoundException":
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
            return eval(secret)
        else:
            logger.error("binary secret not implemented")
            # raise NotImplementedError
            # decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            # return decoded_binary_secret


def get_box_client(app_user=True):
    from boxsdk import Client, JWTAuth

    secret = get_secret()

    CLIENT_ID = secret["clientID"]
    CLIENT_SECRET = secret["clientSecret"]
    PUBLIC_KEY_ID = secret["publicKeyID"]
    PRIVATE_KEY = secret["privateKey"].replace("\\n", "\n")
    PASSPHRASE = secret["passphrase"]
    ENTERPRISE_ID = secret["enterpriseID"]
    WEBHOOK_KEY = secret["webhook_key"]

    auth = JWTAuth(
        CLIENT_ID,
        CLIENT_SECRET,
        ENTERPRISE_ID,
        PUBLIC_KEY_ID,
        rsa_private_key_data=PRIVATE_KEY,
        rsa_private_key_passphrase=PASSPHRASE,
    )
    auth.authenticate_instance()

    client = Client(auth)

    if app_user:
        users = client.users()
        try:
            appuser = users.next()
        except StopIteration:
            logger.info(
                "no app user exists, so the service account will be used as the box api client"
            )
            return client

        appClient = client.as_user(appuser)

        return appClient, WEBHOOK_KEY


def get_file_id_and_hash(webhook_event, box_client):
    file_id = webhook_event["body"]["source"]["item"]["id"]
    box_file = box_client.file(file_id=file_id)
    filename = box_file.get().name

    hashed_url = webhook_event["body"]["source"]["url"]
    try:
        box_hash = hashed_url.split("/")[-1]
    except AttributeError:
        box_hash = None
        logger.info(
            f"no hash available for {filename}, likely because the link is lost before the SHARED_LINK.DELETED webhook is sent"
        )

    return filename, box_hash


def lambda_handler(event, context):

    # only get a box client if we're actually going to need one
    handled_triggers = [
        "SHARED_LINK.CREATED",
        "SHARED_LINK.UPDATED",
        "SHARED_LINK.DELETED",
    ]
    if event["body"]["trigger"] in handled_triggers:
        client, webhook_key = get_box_client(app_user=True)
        ddb = boto3.resource("dynamodb", region_name="us-east-1").Table(
            "BoxWebhookTest"
        )
        logger.info(event)
    else:
        logger.info(f"{event['body']['trigger']} is not supported by this endpoint")
        return {"statusCode": 200}

    if validate_webhook:
        logger.info(event["body"])
        logger.info(event["headers"])
        webhook = client.webhook(event["body"]["webhook"]["id"])
        is_valid = webhook.validate_message(
            bytes(f"{event['body']}", "utf-8"), event["headers"], webhook_key
        )
        if not is_valid:
            logger.info(f"received invalid webhook")
            logger.critical(event)
            return {"statusCode": 200}

    if event["body"]["trigger"] == "SHARED_LINK.CREATED":
        filename, box_hash = get_file_id_and_hash(event, client)

        put_item = {"Filename": filename, "hash": box_hash}
        ddb.put_item(Item=put_item)

    elif event["body"]["trigger"] == "SHARED_LINK.DELETED":
        from boto3.dynamodb.conditions import Key

        filename, box_hash = get_file_id_and_hash(event, client)

        all_items = ddb.query(KeyConditionExpression=Key("Filename").eq(filename))
        for item in all_items["Items"]:
            ddb.delete_item(Key=item)

    elif event["body"]["trigger"] == "SHARED_LINK.UPDATED":
        logger.info(
            "SHARED_LINK.UPDATED was not implemented because I cannot figure out what triggers them and what the event looks like"
        )

    return {"statusCode": 200}
