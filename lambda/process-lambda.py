import json
import os
import logging
import boto3
from typing import Any, Dict, List

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.environ['TABLE_NAME']
ddb = boto3.client('dynamodb')


def handler(event: Dict[str, Any], context: Any) -> Dict[str, List[Dict[str, str]]]:
    """Pull SQS messages and update DynamoDB aggregates"""
    failures = []

    for record in event.get('Records', []):
        msg_id = record.get('messageId')

        try:
            payload = json.loads(record['body'])

            _type = str(payload['type'])
            _state = str(payload['state'])
            _timestamp = str(payload['timestamp'])

            ddb.update_item(
                TableName=TABLE_NAME,
                Key={
                    'date': {'S': _timestamp[:10]},  # take date part only
                    'type_state': {'S': f"{_type}#{_state}"},
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
        except Exception as ex:
            logger.warning(f"Failed to process message {msg_id}: {ex}")
            if msg_id:
                failures.append({"itemIdentifier": msg_id})

    # SQS partial batch response format
    return {"batchItemFailures": failures}
