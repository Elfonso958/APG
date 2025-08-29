import requests
from flask import current_app

def post_flights_to_apg(flights_payload: dict) -> dict:
    url = current_app.config["APG_POST_URL"]
    token = current_app.config["APG_BEARER_TOKEN"]
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    r = requests.post(url, json=flights_payload, headers=headers, timeout=120)
    try:
        data = r.json()
    except Exception:
        data = {"text": r.text[:1000]}
    return {"status_code": r.status_code, "response": data}
