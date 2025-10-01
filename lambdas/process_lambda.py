import json
from json import JSONDecodeError
from typing import Any, Dict, List, Optional, TypedDict

from common import get_logger, require_env, get_ddb_client

logger = get_logger(__name__)

# DTO
class Payload(TypedDict):
    type: str
    state: str
    timestamp: str

def _update_ddb(client, table_name: str, payload: Payload) -> None:
    client.update_item(
        TableName = table_name,
        Key = {
            'date': {'S': payload['timestamp'][:10]},
            'type_state': {'S': f"{payload['type']}#{payload['state']}"},
        },
        UpdateExpression = 'SET #t = :t, #s = :s ADD #c :one',
        ExpressionAttributeNames = {
            '#t': 'type',
            '#s': 'state',
            '#c': 'count'
        },
        ExpressionAttributeValues = {
            ':t': {'S': payload["type"]},
            ':s': {'S': payload["state"]},
            ':one': {'N': '1'}
        },
    )

def _parse_payload(body: Optional[str]) -> Payload:
    """Parse and validate the message body into a Payload"""
    if not body:
        raise ValueError("Payload body is empty")

    try:
        raw = json.loads(body)
    except JSONDecodeError as ex:
        raise ValueError(f"invalid JSON: {ex}") from ex

    if not isinstance(raw, dict):
        raise ValueError("Payload must be JSON object")

    try:
        return {
            "type": str(raw["type"]),
            "state": str(raw["state"]),
            "timestamp": str(raw["timestamp"]),
        }
    except KeyError as ex:
        raise ValueError(f"missing required field: {ex.args[0]}") from ex

def handler(
        event: Dict[str, Any],
        context: Any,
        *,
        ddb_client: Optional[Any] = None
) -> Dict[str, List[Dict[str, str]]]:
    """Process an SQS batch: parse messages and update per-day aggregates in DynamoDB."""

    req_id = getattr(context, "aws_request_id", None)
    records = event.get("Records", [])
    logger.info(f"start batch_size={len(records)}, req_id={req_id}")

    failures: List[Dict[str, str]] = []

    ddbc = get_ddb_client(ddb_client)
    (table_name, ) = require_env("TABLE_NAME")

    for record in records:
        msg_id = record.get('messageId')
        attrs = (record.get('messageAttributes') or {})
        corr_id = (attrs.get("corr_id") or {}).get("stringValue")
        body = record.get('body')

        try:
            payload = _parse_payload(body)
            _update_ddb(ddbc, table_name, payload)
            logger.info(f"updated msg_id={msg_id}, corr_id={corr_id}, payload={payload}, req_id={req_id}")

        except Exception:
            logger.exception(f"process_error msg_id={msg_id} preview={str(body)[:200]}, req_id={req_id}")
            if msg_id:
                failures.append({"itemIdentifier": msg_id})

    # SQS partial batch response format
    logger.info(f"done failures={len(failures)}, req_id={req_id}")
    return { "batchItemFailures": failures }
