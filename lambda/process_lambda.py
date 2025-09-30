import json
import os
import boto3
import logging
from typing import Any, Dict, List

TABLE_NAME = os.environ['TABLE_NAME']
LOG_LEVEL  = os.getenv("LOG_LEVEL", "INFO").upper()

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

ddb = boto3.client('dynamodb')

def handler(event: Dict[str, Any], context: Any) -> Dict[str, List[Dict[str, str]]]:
    """Pull SQS messages and update DynamoDB aggregates"""

    req_id = getattr(context, "aws_request_id", None)
    records = event.get("Records", []) or []
    logger.info(f"start batch_size={len(records)}, req_id={req_id}")

    failures: List[Dict[str, str]] = []

    for record in records:
        msg_id = record.get('messageId')
        attrs = (record.get('messageAttributes') or {})
        corr_id = (attrs.get("corr_id") or {}).get("stringValue")
        body = record.get('body')

        try:
            payload = json.loads(body or "{}")

            _type = str(payload['type'])
            _state = str(payload['state'])
            _timestamp = str(payload['timestamp'])
            date = _timestamp[:10] # aggregate per day
            type_state = f"{_type}#{_state}"

            ddb.update_item(
                TableName=TABLE_NAME,
                Key={
                    'date': {'S': date},
                    'type_state': {'S': type_state},
                },
                UpdateExpression='SET #t = :t, #s = :s ADD #c :one',
                ExpressionAttributeNames={
                    '#t': 'type',
                    '#s': 'state',
                    '#c': 'count'
                },
                ExpressionAttributeValues={
                    ':t': {'S': _type},
                    ':s': {'S': _state},
                    ':one': {'N': '1'}
                },
            )

            logger.info(f"updated msg_id={msg_id}, corr_id={corr_id}, date={date}, type_state={type_state}, req_id={req_id}")

        except Exception:
            logger.exception(f"process_error preview={str(body)[:200]}, req_id={req_id}")
            if msg_id:
                failures.append({"itemIdentifier": msg_id})

    # SQS partial batch response format
    logger.info(f"done failures={len(failures)}, req_id={req_id}")
    return {"batchItemFailures": failures}
