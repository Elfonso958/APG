import threading
from datetime import date, datetime, timedelta
from typing import Any

from . import db
from .envision_otp_client import build_envision_otp_client
from .otp_cache import upsert_otp_cache_rows


_JOB_LOCK = threading.Lock()
_JOB_STATUS: dict[str, Any] = {
    "running": False,
    "startedAt": None,
    "finishedAt": None,
    "ok": None,
    "error": None,
    "dateFrom": None,
    "dateTo": None,
    "currentWindow": None,
    "rows": 0,
    "created": 0,
    "updated": 0,
    "skipped": 0,
    "windowsDone": 0,
    "windowsTotal": 0,
}


def _parse_date(value: str) -> date:
    return date.fromisoformat(str(value).strip())


def _windows(start: date, end: date, window_days: int) -> list[tuple[str, str]]:
    windows: list[tuple[str, str]] = []
    cursor = start
    while cursor <= end:
        window_end = min(cursor + timedelta(days=window_days - 1), end)
        windows.append((cursor.isoformat(), window_end.isoformat()))
        cursor = window_end + timedelta(days=1)
    return windows


def get_otp_cache_job_status() -> dict[str, Any]:
    with _JOB_LOCK:
        return dict(_JOB_STATUS)


def _update_status(**updates: Any) -> None:
    with _JOB_LOCK:
        _JOB_STATUS.update(updates)


def _run_job(
    app,
    *,
    date_from: str,
    date_to: str,
    window_days: int,
    chunk_days: int,
    page_size: int,
    include_details: bool,
) -> None:
    windows = _windows(_parse_date(date_from), _parse_date(date_to), window_days)
    _update_status(windowsTotal=len(windows))

    try:
        with app.app_context():
            client = build_envision_otp_client(app.config, app.logger)
            for window_from, window_to in windows:
                _update_status(currentWindow=f"{window_from} to {window_to}")
                rows = client.fetch_otp_flights(
                    window_from,
                    window_to,
                    page_size=page_size,
                    include_details=include_details,
                    chunk_days=chunk_days,
                )
                result = upsert_otp_cache_rows(rows)
                with _JOB_LOCK:
                    _JOB_STATUS["rows"] += len(rows)
                    _JOB_STATUS["created"] += result["created"]
                    _JOB_STATUS["updated"] += result["updated"]
                    _JOB_STATUS["skipped"] += result["skipped"]
                    _JOB_STATUS["windowsDone"] += 1

            _update_status(ok=True, error=None)
    except Exception as exc:
        try:
            db.session.rollback()
        except Exception:
            pass
        _update_status(ok=False, error=str(exc))
    finally:
        try:
            db.session.remove()
        except Exception:
            pass
        _update_status(
            running=False,
            finishedAt=datetime.utcnow().isoformat(timespec="seconds") + "Z",
        )


def start_otp_cache_job(
    app,
    *,
    date_from: str,
    date_to: str,
    window_days: int,
    chunk_days: int,
    page_size: int,
    include_details: bool = False,
) -> tuple[bool, dict[str, Any]]:
    with _JOB_LOCK:
        if _JOB_STATUS.get("running"):
            return False, dict(_JOB_STATUS)

        _JOB_STATUS.update(
            {
                "running": True,
                "startedAt": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "finishedAt": None,
                "ok": None,
                "error": None,
                "dateFrom": date_from,
                "dateTo": date_to,
                "currentWindow": None,
                "rows": 0,
                "created": 0,
                "updated": 0,
                "skipped": 0,
                "windowsDone": 0,
                "windowsTotal": 0,
            }
        )

    thread = threading.Thread(
        target=_run_job,
        kwargs={
            "app": app,
            "date_from": date_from,
            "date_to": date_to,
            "window_days": window_days,
            "chunk_days": chunk_days,
            "page_size": page_size,
            "include_details": include_details,
        },
        daemon=True,
    )
    thread.start()
    return True, get_otp_cache_job_status()

