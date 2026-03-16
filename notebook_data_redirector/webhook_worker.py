import common


def lambda_handler(event, context):
    common.log_action("INFO", "webhook_worker", "stream_event_received", record_count=len(event.get("Records", [])))
    return {"statusCode": 200}
