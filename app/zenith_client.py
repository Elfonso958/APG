# app/zenith_client.py
from __future__ import annotations

from datetime import date as _date, datetime as _dt, timezone
from typing import Any, Dict, Optional, Union
from zoneinfo import ZoneInfo
from flask import current_app
import requests

NZ = ZoneInfo("Pacific/Auckland")

# -------- config helpers --------
def _require_cfg(keys: list[str]) -> None:
    missing = [k for k in keys if not current_app.config.get(k)]
    if missing:
        raise RuntimeError(f"Missing Zenith config keys: {', '.join(missing)}")

def _to_midnight_utc_z(day_in) -> str:
    """
    Accepts str | date | datetime and returns 'YYYY-MM-DDT00:00:00Z' (UTC midnight).
    If given an NZ-local date, we clamp that calendar day to midnight UTC for the request.
    """
    if isinstance(day_in, _dt):
        dt = day_in if day_in.tzinfo else day_in.replace(tzinfo=timezone.utc)
        dt_utc = dt.astimezone(timezone.utc)
        d = dt_utc.date()
    elif isinstance(day_in, _date):
        d = day_in
    elif isinstance(day_in, str):
        # Accept 'YYYY-MM-DD' or ISO datetime; normalize to date first
        if "T" in day_in:
            dt = _dt.fromisoformat(day_in.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            d = dt.astimezone(timezone.utc).date()
        else:
            d = _date.fromisoformat(day_in)
    else:
        raise TypeError("flight_date must be str|date|datetime")

    return _dt(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


# -------- date normalization --------
def _normalize_flight_date_to_iso(day_in: Union[str, _date, _dt]) -> str:
    """
    Accepts str | date | datetime and returns ISO 'YYYY-MM-DD'
    using the NZ calendar day.
    """
    if isinstance(day_in, _dt):
        dt = day_in if day_in.tzinfo else day_in.replace(tzinfo=timezone.utc)
        return dt.astimezone(NZ).date().isoformat()
    if isinstance(day_in, _date):
        return day_in.isoformat()
    if isinstance(day_in, str):
        try:
            if "T" in day_in:
                dt = _dt.fromisoformat(day_in.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(NZ).date().isoformat()
            return _date.fromisoformat(day_in).isoformat()
        except Exception as e:
            raise ValueError(f"Bad flight_date string: {day_in}") from e
    raise TypeError("flight_date must be str|date|datetime")

# -------- public API --------
def fetch_dcs_for_flight(
    dep_airport: str,
    flight_date,                   # str|date|datetime (we convert to midnight Z)
    airline_designator: str,       # e.g. '3C'
    flight_number: str,            # e.g. '701'
    only_status: bool = True,
) -> dict:
    """
    POSTs the exact payload required by the DCS:
    {
      "DepartureAirport": "...",
      "DepartureDate": "YYYY-MM-DDT00:00:00Z",
      "OperatingAirline": { "AirlineDesignator": "...", "FlightNumber": "..." },
      "OnlyDCSStatus": true,
      "ApiKey": "..."
    }
    """
    _require_cfg(["PROD_DCS_API_BASE", "DCS_API_FLIGHTS_PATH", "PROD_DCS_API_KEY"])

    base = current_app.config["PROD_DCS_API_BASE"].rstrip("/")
    path = current_app.config["DCS_API_FLIGHTS_PATH"]
    url  = f"{base}{path}"

    payload = {
        "DepartureAirport": (dep_airport or "").upper(),
        "DepartureDate": _to_midnight_utc_z(flight_date),
        "OperatingAirline": {
            "AirlineDesignator": (airline_designator or "").upper(),
            "FlightNumber": str(flight_number or "").strip(),
        },
        "OnlyDCSStatus": bool(only_status),
        "ApiKey": current_app.config["PROD_DCS_API_KEY"],
    }

    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    #current_app.logger.info("[DCS] POST %s payload=%s", url, {
    #    "DepartureAirport": payload["DepartureAirport"],
    #   "DepartureDate": payload["DepartureDate"],
    #    "OperatingAirline": payload["OperatingAirline"],
    #    "OnlyDCSStatus": payload["OnlyDCSStatus"],
    #    "ApiKey": f"{payload['ApiKey'][:4]}…{payload['ApiKey'][-4:]}" if payload["ApiKey"] else ""
    #})
    r = requests.post(url, json=payload, headers=headers, timeout=60)
    #current_app.logger.info("[DCS] status=%s elapsed=%.3fs",
    #                        r.status_code,
    #                        getattr(r, "elapsed", 0.0).total_seconds() if hasattr(r, "elapsed") else -1)
    r.raise_for_status()
    return r.json()

# -------- optional: debug helper used by /debug/dcs-ping --------
def _debug_call(
    day: Union[str, _date, _dt],
    dep: Optional[str] = None,
    arr: Optional[str] = None,
    airline_designator: Optional[str] = None,
    only_dcs_status: bool = True,
) -> Dict[str, Any]:
    """
    Not a real API caller—just shows what we'd send for a given day+airline
    using the single-flight endpoint shape.
    """
    dstr = _normalize_flight_date_to_iso(day)
    return {
        "would_post_to": f"{(current_app.config.get('PROD_DCS_API_BASE') or '').rstrip('/')}{current_app.config.get('DCS_API_FLIGHTS_PATH') or ''}",
        "headers": {"X-Api-Key": (current_app.config.get('PROD_DCS_API_KEY') or '')[:4] + "…"},
        "payload_example": {
            "AirlineDesignator": (airline_designator or current_app.config.get("DCS_DEFAULT_AIRLINE") or "").upper(),
            "FlightNumber": "<REQUIRED>",
            "FlightDate": dstr,
            **({"Origin": dep} if dep else {}),
        },
    }
