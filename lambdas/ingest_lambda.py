import json
import uuid
from common import json_response, get_logger, require_env, get_s3_client, get_sqs_client
from datetime import datetime, timezone
from typing import Any, Dict, Optional

JSON_ROOT = "processed_devices"

logger = get_logger(__name__)

def handler(
        event: Dict[str, Any],
        context: Any,
        *,
        s3_client: Optional[Any] = None,
        sqs_client: Optional[Any] = None,
) -> Dict[str, Any]:
    """Store JSON data as-is to S3 and send each JSON item to SQS"""

    req_id = getattr(context, "aws_request_id", None)
    logger.info(f"start path={event.get('path')}, method={event.get('httpMethod')}, req_id={req_id}")

    try:
        bucket_name, queue_url = require_env("BUCKET_NAME", "QUEUE_URL")
        s3c, sqsc = get_s3_client(s3_client), get_sqs_client(sqs_client)

        body = event.get("body")
        if body is None:
            logger.warning(f"bad_request: missing body, req_id={req_id}")
            return json_response(400, { "error": "Missing request body" })

        try:
            data = json.loads(body)
        except Exception:
            logger.warning(f"bad_request: invalid JSON, req_id={req_id}")
            return json_response(400, { "error": "Body must be valid JSON" })

        if JSON_ROOT not in data or not isinstance(data[JSON_ROOT], list):
            logger.warning(f"bad_request: missing root '{JSON_ROOT}', req_id={req_id}")
            return json_response(400, { "error": f"Body must include root attribute '{JSON_ROOT}'" })

        # Store raw JSON as-is to S3
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        key = f"raw/{now}-{uuid.uuid4()}.json"

        s3c.put_object(
            Bucket = bucket_name,
            Key = key,
            Body = body,
            ContentType = "application/json",
        )

        logger.info(f"raw_saved_bucket={bucket_name}, key={key}, items={len(data[JSON_ROOT])}, req_id={req_id}")

        # Fan out each item to SQS for async processing
        enqueued = 0
        for item in data[JSON_ROOT]:
            if not all(k in item for k in ("type", "state", "timestamp")):
                logger.warning(f"skip_item: missing fields preview={str(item)[:200]}, req_id={req_id}")
                continue

            sqsc.send_message(
                QueueUrl = queue_url,
                MessageBody = json.dumps(item),
                MessageAttributes = {
                    "corr_id": { "DataType": "String", "StringValue": req_id or "" }
                }
            )
            enqueued += 1

        logger.info(f"enqueued queue={queue_url} count={enqueued}, req_id={req_id}")
        return json_response(200, { "status": "accepted", "raw_key": key, "enqueued": enqueued })

    except Exception:
        logger.exception(f"unhandled_error, req_id={req_id}")
        return json_response(500, { "error": "Internal server error" })
