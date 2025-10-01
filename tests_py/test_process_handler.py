import json
from moto import mock_aws
import boto3
from lambdas.process_lambda import handler

def _sqs_event(records):
    return { "Records": records }

def _record(body):
    return {
        "messageId": "m1",
        "messageAttributes": { "corr_id": {"stringValue": "cid" }},
        "body": json.dumps(body),
    }

@mock_aws
def test_process_updates_counts(monkeypatch):
    ddb = boto3.client("dynamodb", region_name="us-east-1")
    ddb.create_table(
        TableName = "table-test",
        KeySchema = [{ "AttributeName":"date","KeyType":"HASH" },
                   { "AttributeName":"type_state","KeyType":"RANGE" }],
        AttributeDefinitions = [
            { "AttributeName":"date","AttributeType":"S" },
            { "AttributeName":"type_state","AttributeType":"S" },
        ],
        BillingMode = "PAY_PER_REQUEST"
    )
    monkeypatch.setenv("TABLE_NAME", "table-test")

    records = [
        _record({ "type":"phone","state":"erased","timestamp":"2024-01-01T10:00:00Z" }),
        _record({ "type":"phone","state":"erased","timestamp":"2024-01-01T11:00:00Z" }),
        _record({ "type":"laptop","state":"pending","timestamp":"2024-01-01T12:00:00Z" }),
        { "messageId":"bad","body":"not json" },
    ]
    resp = handler(_sqs_event(records), type("Ctx", (), {"aws_request_id":"rid"})(), ddb_client = ddb)
    assert "batchItemFailures" in resp
    # one bad message should fail
    assert len(resp["batchItemFailures"]) == 1

    # verify counts
    item = ddb.get_item(TableName = "table-test", Key={ "date":{"S":"2024-01-01" }, "type_state": { "S":"phone#erased" }}).get("Item")
    assert item and item["count"]["N"] == "2"
