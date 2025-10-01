import json
from moto import mock_aws
import boto3
from lambdas.query_lambda import handler

def _event(path = "/stats", date = "2024-01-01"):
    return { "path": path, "queryStringParameters": {"date": date} }

@mock_aws
def test_query_json(monkeypatch):
    r = boto3.resource("dynamodb", region_name = "us-east-1")
    table = r.create_table(
        TableName = "table-test",
        KeySchema = [{ "AttributeName":"date","KeyType":"HASH" },
                   { "AttributeName":"type_state","KeyType":"RANGE" }],
        AttributeDefinitions = [
            { "AttributeName":"date","AttributeType":"S" },
            { "AttributeName":"type_state","AttributeType":"S" },
        ],
        BillingMode = "PAY_PER_REQUEST"
    )
    table.put_item(Item = { "date":"2024-01-01","type_state":"phone#erased","count":2 })
    monkeypatch.setenv("TABLE_NAME", "table-test")

    resp = handler(_event(), type("Ctx", (), {})(), table_resource = table)
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body == [{ "date":"2024-01-01","type":"phone","state":"erased","count":2 }]

@mock_aws
def test_query_html(monkeypatch):
    r = boto3.resource("dynamodb", region_name = "us-east-1")
    table = r.create_table(
        TableName = "table-test",
        KeySchema = [{ "AttributeName":"date","KeyType":"HASH" },
                   { "AttributeName":"type_state","KeyType":"RANGE" }],
        AttributeDefinitions = [
            { "AttributeName":"date","AttributeType":"S" },
            { "AttributeName":"type_state","AttributeType":"S" },
        ],
        BillingMode = "PAY_PER_REQUEST"
    )
    table.put_item(Item = { "date":"2024-01-02","type_state":"laptop#pending","count":1 })
    monkeypatch.setenv("TABLE_NAME", "table-test")

    resp = handler(_event(path = "/stats-html", date = "2024-01-02"), type("Ctx", (), {})(), table_resource = table)
    assert resp["statusCode"] == 200
    assert "<table" in resp["body"]
    assert "laptop" in resp["body"]
