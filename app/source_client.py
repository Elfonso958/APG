import requests
from datetime import datetime
from flask import current_app

def fetch_flights(from_dt: datetime, to_dt: datetime):
    base = current_app.config["SOURCE_API_BASE"].rstrip("/")
    token = current_app.config["SOURCE_API_TOKEN"]
    url = f"{base}/v1/flights?from={from_dt.isoformat()}&to={to_dt.isoformat()}"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.json()
