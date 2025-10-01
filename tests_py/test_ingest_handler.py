import json
import uuid
from datetime import datetime, timezone
import boto3
from moto import mock_aws
from lambdas.ingest_lambda import handler

def _event(payload):
    return {"httpMethod": "POST", "path": "/ingest", "body": json.dumps(payload)}

def _iso_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

@mock_aws
def test_ingest_happy_path(monkeypatch):
    s3 = boto3.client("s3", region_name = "us-east-1")
    sqs = boto3.client("sqs", region_name = "us-east-1")

    s3.create_bucket(Bucket = "bucket-test")
    q = sqs.create_queue(QueueName = "q1")
    monkeypatch.setenv("BUCKET_NAME", "bucket-test")
    monkeypatch.setenv("QUEUE_URL", q["QueueUrl"])

    payload = {
        "processed_devices": [
            { "type":"phone","state":"erased","timestamp": _iso_now() },
            { "type":"laptop","state":"pending","timestamp": _iso_now() },
            { "type":"server","state":"erasure failed","timestamp": _iso_now() },
        ]
    }

    resp = handler(_event(payload), type("Ctx", (), { "aws_request_id": str(uuid.uuid4()) }), s3_client = s3, sqs_client = sqs)

    body = json.loads(resp["body"])
    assert resp["statusCode"] == 200
    assert body["status"] == "accepted"
    assert body["enqueued"] == 3

    # Verify object landed in S3
    objs = s3.list_objects_v2(Bucket = "bucket-test", Prefix = "raw/")["KeyCount"]
    assert objs == 1

    # Verify messages are in SQS
    msgs = sqs.receive_message(QueueUrl = q["QueueUrl"], MaxNumberOfMessages = 10).get("Messages", [])
    assert len(msgs) == 3

@mock_aws
def test_ingest_rejects_bad_json(monkeypatch):
    s3 = boto3.client("s3", region_name = "us-east-1")
    sqs = boto3.client("sqs", region_name = "us-east-1")
    s3.create_bucket(Bucket = "bucket-test")
    q = sqs.create_queue(QueueName = "q1")
    monkeypatch.setenv("BUCKET_NAME", "bucket-test")
    monkeypatch.setenv("QUEUE_URL", q["QueueUrl"])

    event = {"httpMethod":"POST","path":"/ingest","body":"{not json]"}
    resp = handler(event, type("Ctx", (), {})(), s3_client = s3, sqs_client = sqs)
    assert resp["statusCode"] == 400
