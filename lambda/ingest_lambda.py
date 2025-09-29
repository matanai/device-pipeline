import json
import os
import boto3
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
sqs = boto3.client("sqs")

BUCKET_NAME = os.environ["BUCKET_NAME"]
QUEUE_URL = os.environ["QUEUE_URL"]

JSON_ROOT = "processed_devices"


def _bad_request(msg: str) -> Dict[str, Any]:
    return {
        "statusCode": 400,
        "headers": { "Content-Type": "application/json" },
        "body": json.dumps({ "error": msg })
    }

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Store JSON data as-is to S3 and send each JSON item to SQS"""
    try:
        body = event.get("body")
        if body is None:
            return _bad_request("Missing request body")

        data = json.loads(body)
        if JSON_ROOT not in data or not isinstance(data[JSON_ROOT], list):
            return _bad_request(f"Body must include root attribute '{JSON_ROOT}'")

        # Store raw JSON as-is to S3
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        key = f"raw/{now}-{uuid.uuid4()}.json"
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=body,
            ContentType="application/json",
        )

        # Fan out each item to SQS for async processing
        count = 0
        for item in data[JSON_ROOT]:
            if not all(k in item for k in ("type", "state", "timestamp")):
                logger.warning(f"Skipping invalid item: {item}")
                continue

            sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(item)
            )
            count += 1

        logger.debug(f"Enqueued {count} messages to SQS")

        return {
            "statusCode": 200,
            "headers": { "Content-Type": "application/json" },
            "body": json.dumps({
                "status": "accepted",
                "raw_key": key,
                "enqueued": count,
            })
        }
    except Exception as ex:
        logger.error(f"Unexpected error: {ex}")
        return {
            "statusCode": 500,
            "headers": { "Content-Type": "application/json" },
            "body": json.dumps({"error": str(ex)})
        }




