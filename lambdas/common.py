import json
import os
import boto3
import logging
from typing import Any, Dict, Tuple, Optional

def get_logger(name: str) -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level, logging.INFO))
    return logger


def require_env(*keys: str) -> Tuple[str, ...]:
    """Fetch required env vars or raise KeyError if missing"""

    missing = [k for k in keys if k not in os.environ]
    if missing:
        raise KeyError(f"Missing required env var(s): {', '.join(missing)}")

    return tuple(os.environ[k] for k in keys)

# Lazy client/resource creation with DI

def get_s3_client(client: Optional[Any] = None) -> Any:
    return client or boto3.client("s3")

def get_sqs_client(client: Optional[Any] = None) -> Any:
    return client or boto3.client("sqs")

def get_ddb_client(client: Optional[Any] = None) -> Any:
    return client or boto3.client("dynamodb")

def get_ddb_table(table: Optional[Any] = None) -> Any:
    if table is not None:
        return table

    (table_name, ) = require_env("TABLE_NAME")
    return boto3.resource("dynamodb").Table(table_name)

#  Helpers for building responses

def json_response(status: int, body: Any) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": { "Content-Type": "application/json" },
        "body": json.dumps(body),
    }

def html_response(status: int, html: str) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "text/html"},
        "body": html,
    }
