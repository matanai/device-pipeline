import datetime
from common import json_response, html_response, get_logger, get_ddb_table
from boto3.dynamodb.conditions import Key
from typing import Any, Dict, List

logger = get_logger(__name__)

def handler(
        event: Dict[str, Any],
        context: Any,
        *,
        table_resource = None
) -> Dict[str, Any]:
    """Query aggregates from DynamoDB"""
    req_id = getattr(context, "aws_request_id", None)
    params = event.get("queryStringParameters") or {}
    date = params.get("date")

    logger.info(f"start path={event.get('path')}, query={params}, req_id={req_id}")

    table_ref = get_ddb_table(table_resource)

    if not date:
        logger.warning(f"bad_request: missing date, req_id={req_id}")
        return json_response(400, { "error": "Missing required query param: date=YYYY-MM-DD" })

    if not _validate_date(date):
        logger.warning(f"bad_request: invalid date format, req_id={req_id}")
        return json_response(400, { "error": "Invalid date format; expected YYYY-MM-DD" })

    # Query the partition
    try:
        resp = table_ref.query(KeyConditionExpression = Key("date").eq(date))
    except Exception:
        logger.exception(f"ddb_error, req_id={req_id}")
        return json_response(500, { "error": "Internal server error" })

    items: List[Dict[str, Any]] = resp.get("Items", [])

    # Normalize
    result = _normalize_items(items)

    logger.debug(f"queried items={len(result)}, req_id={req_id}")

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
        logger.debug(f"html_rendered rows={len(result)}, req_id={req_id}")
        return html_response(200, html)

    return json_response(200, result)

def _validate_date(date_str: str) -> bool:
    try:
        datetime.datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def _normalize_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "date": i["date"],
            "type": i.get("type", i["type_state"].split("#", 1)[0]),
            "state": i.get("state", i["type_state"].split("#", 1)[1] if "#" in i["type_state"] else None),
            "count": int(i.get("count", 0)),
        }
        for i in items
    ]






