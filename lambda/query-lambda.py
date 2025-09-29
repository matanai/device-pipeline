import json
import os
import logging
import boto3
import datetime
from typing import Any, Dict, List

from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.environ.get("TABLE_NAME")
db = boto3.resource("dynamodb")
table = db.Table(TABLE_NAME)

def _resp(status: int, body: Any, content_type: str="application/json") -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": { "Content-Type": content_type },
        "body": body if content_type == "text/html" else json.dumps(body),
    }

def _validate_date(date_str: str) -> bool:
    try:
        datetime.datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Query aggregates from DynamoDB"""
    params = event.get("queryStringParameters") or {}
    date = params.get("date")

    if not date:
        return _resp(400, {"error": "Missing required query param: date=YYYY-MM-DD"})

    if not _validate_date(date):
        return _resp(400, {"error": "Invalid date format; expected YYYY-MM-DD"})

    # Query the partition
    try:
        resp = table.query(KeyConditionExpression=Key("date").eq(date))
    except Exception as ex:
        logger.warning(f"Failed to query DynamoDB table: {ex}")
        return _resp(500, {"error": "Internal server error"})

    items: List[Dict[str, Any]] = resp.get("Items", [])

    # Normalize
    result = [
        {
            'date': i['date'],
            'type': i.get('type', i['type_state'].split('#', 1)[0]),
            'state': i.get('state', i['type_state'].split('#', 1)[1] if '#' in i['type_state'] else None),
            'count': int(i.get('count', 0)),
        }
        for i in items
    ]

    # HTML view if path ends with /stats-html
    path = event.get('path', '')
    if path.endswith('/stats-html'):
        rows = ''.join(
            f"<tr><td>{r['date']}</td><td>{r['type']}</td><td>{r['state']}</td><td>{r['count']}</td></tr>"
            for r in result
        )
        html = f"""
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="UTF-8"/>
                <title>Aggregates {date}</title>
            </head>
            <body>
                <h2>Aggregates for {date}</h2>
                <table border="1" cellpadding="6" cellspacing="0">
                    <thead><tr><th>Date</th><th>Type</th><th>State</th><th>Count</th></tr></thead>
                    <tbody>{rows or '<tr><td colspan=4>No data</td></tr>'}</tbody>
                </table>
            </body>
        </html>
        """
        return _resp(200, html, content_type="text/html")

    return _resp(200, result)






