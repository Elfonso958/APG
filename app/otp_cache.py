import json
from datetime import date, datetime
from typing import Any

from . import db
from .models import EnvisionOtpFlightCache


def parse_otp_cache_date(value: str | None, fallback: str) -> date:
    raw = str(value or fallback).strip()
    if "T" in raw:
        raw = raw.split("T", 1)[0]
    return date.fromisoformat(raw)


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def infer_otp_flight_date(row: dict[str, Any]) -> date | None:
    for key in ("departureScheduled", "departureActual", "arrivalScheduled", "arrivalActual", "flightDate", "date"):
        dt = _parse_dt(row.get(key))
        if dt:
            return dt.date()
    return None


def infer_otp_departure_scheduled(row: dict[str, Any]) -> datetime | None:
    return _parse_dt(row.get("departureScheduled"))


def infer_otp_reg(row: dict[str, Any]) -> str | None:
    for key in ("flightRegistrationDescription", "aircraftRegistration", "registration", "reg"):
        value = str(row.get(key) or "").strip().upper()
        if value:
            return value
    return None


def upsert_otp_cache_rows(rows: list[dict[str, Any]]) -> dict[str, int]:
    now = datetime.utcnow()
    created = 0
    updated = 0
    skipped = 0

    for row in rows:
        flight_id = str(row.get("id") or "").strip()
        if not flight_id:
            skipped += 1
            continue

        cached = EnvisionOtpFlightCache.query.filter_by(envision_flight_id=flight_id).first()
        if cached is None:
            cached = EnvisionOtpFlightCache(
                envision_flight_id=flight_id,
                created_at=now,
            )
            db.session.add(cached)
            created += 1
        else:
            updated += 1

        cached.flight_date = infer_otp_flight_date(row)
        cached.departure_scheduled = infer_otp_departure_scheduled(row)
        cached.reg = infer_otp_reg(row)
        cached.row_json = json.dumps(row, ensure_ascii=False, default=str)
        cached.updated_at = now

    db.session.commit()
    return {"created": created, "updated": updated, "skipped": skipped}


def get_cached_otp_rows(date_from: date, date_to: date) -> list[dict[str, Any]]:
    query = (
        EnvisionOtpFlightCache.query
        .filter(EnvisionOtpFlightCache.flight_date >= date_from)
        .filter(EnvisionOtpFlightCache.flight_date <= date_to)
        .order_by(EnvisionOtpFlightCache.departure_scheduled.asc(), EnvisionOtpFlightCache.id.asc())
    )

    rows: list[dict[str, Any]] = []
    for cached in query.all():
        try:
            rows.append(json.loads(cached.row_json or "{}"))
        except json.JSONDecodeError:
            continue
    return rows

