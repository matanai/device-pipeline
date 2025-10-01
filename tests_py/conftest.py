import os
import pytest

@pytest.fixture(autouse=True)
def env():
    os.environ.setdefault("BUCKET_NAME", "bucket-test")
    os.environ.setdefault("QUEUE_URL", "https://sqs.example/queue")
    os.environ.setdefault("TABLE_NAME", "table-test")
