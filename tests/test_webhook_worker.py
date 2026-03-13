import json

import webhook_worker


class TestWebhookWorkerHandler:
    def test_handler_returns_200(self, capture_log):
        event = {
            "Records": [
                {
                    "eventID": "1",
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": {
                            "work_id": {"S": "test-work-id"},
                        }
                    },
                }
            ]
        }
        result = webhook_worker.lambda_handler(event, None)
        assert result == {"statusCode": 200}

        log_output = capture_log.getvalue()
        assert "stream_event_received" in log_output
        entry = json.loads(log_output.strip().split("\n")[-1])
        assert entry["record_count"] == 1

    def test_handler_handles_empty_records(self, capture_log):
        event = {"Records": []}
        result = webhook_worker.lambda_handler(event, None)
        assert result == {"statusCode": 200}

        log_output = capture_log.getvalue()
        entry = json.loads(log_output.strip().split("\n")[-1])
        assert entry["record_count"] == 0

    def test_handler_handles_missing_records_key(self, capture_log):
        event = {}
        result = webhook_worker.lambda_handler(event, None)
        assert result == {"statusCode": 200}

        log_output = capture_log.getvalue()
        entry = json.loads(log_output.strip().split("\n")[-1])
        assert entry["record_count"] == 0
