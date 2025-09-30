import random
import sys
import requests
from datetime import datetime, timezone, timedelta
from typing import Any, Dict

API = sys.argv[1] if len(sys.argv) > 1 else "https://12345.execute-api.eu-north-1.amazonaws.com/prod"

TYPES = [ "laptop", "server", "phone", "tablet" ]
STATES = [ "erased", "erasure failed", "pending" ]

def iso_utc(days_ago: int = 0, hour: int = 12) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return dt.replace(
        hour=hour,
        minute=0,
        second=0,
        microsecond=0
    ).isoformat().replace("+00:00", "Z")


def generate_batch(n: int = 10) -> Dict[str, Any]:
    items = [
        {
            "type": random.choice(TYPES),
            "state": random.choice(STATES),
            "timestamp": iso_utc(
                days_ago=random.randint(0, 3),
                hour=random.randint(0, 23)
            )
        }
        for _ in range(n)
    ]
    return { "processed_devices": items }

if __name__ == "__main__":
    payload = generate_batch(25)
    r = requests.post(f"{API}/ingest", json=payload)
    print(r.status_code, r.text)
