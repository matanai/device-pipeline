import json
import os
import boto3
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict

BUCKET_NAME = os.environ["BUCKET_NAME"]
QUEUE_URL   = os.environ["QUEUE_URL"]
LOG_LEVEL   = os.getenv("LOG_LEVEL", "INFO").upper()
JSON_ROOT   = "processed_devices"

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

s3 = boto3.client("s3")
sqs = boto3.client("sqs")

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Store JSON data as-is to S3 and send each JSON item to SQS"""

    req_id = getattr(context, "aws_request_id", None)
    logger.info(f"start path={event.get('path')}, method={event.get('httpMethod')}, req_id={req_id}")

    try:
        body = event.get("body")
        if body is None:
            logger.warning(f"bad_request: missing body, req_id={req_id}")

            return {
                "statusCode": 400,
                "headers": { "Content-Type": "application/json" },
                "body": json.dumps({ "error": "Missing request body" })
            }

        data = json.loads(body)
        if JSON_ROOT not in data or not isinstance(data[JSON_ROOT], list):
            logger.warning(f"bad_request: missing root '{JSON_ROOT}', req_id={req_id}")

            return {
                "statusCode": 400,
                "headers": { "Content-Type": "application/json" },
                "body": json.dumps({ "error": f"Body must include root attribute '{JSON_ROOT}'" })
            }

        # Store raw JSON as-is to S3
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        key = f"raw/{now}-{uuid.uuid4()}.json"
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=body,
            ContentType="application/json",
        )

        logger.info(f"raw_saved_bucket={BUCKET_NAME}, key={key}, items={len(data[JSON_ROOT])}, req_id={req_id}")

        # Fan out each item to SQS for async processing
        enqueued = 0
        for item in data[JSON_ROOT]:
            if not all(k in item for k in ("type", "state", "timestamp")):
                logger.warning(f"skip_item: missing fields preview={str(item)[:200]}, req_id={req_id}")
                continue

            sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(item),
                MessageAttributes={
                    "corr_id": { "DataType": "String", "StringValue": req_id or "" }
                }
            )
            enqueued += 1

        logger.info(f"enqueued queue={QUEUE_URL} count={enqueued}, req_id={req_id}")

        return {
            "statusCode": 200,
            "headers": { "Content-Type": "application/json" },
            "body": json.dumps({
                "status": "accepted",
                "raw_key": key,
                "enqueued": enqueued
            })
        }
    except Exception:
        logger.exception(f"unhandled_error, req_id={req_id}")
        return {
            "statusCode": 500,
            "headers": { "Content-Type": "application/json" },
            "body": json.dumps({"error": "Internal server error"})
        }




