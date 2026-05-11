from __future__ import annotations

import json
import os
import threading
import time
import uuid
from typing import Any
from pathlib import Path

_KMH_SESSION_TTL_SECONDS = 8 * 60 * 60
_KMH_LOCK = threading.Lock()
_KMH_STORE_FILE = Path(os.getenv("KMH_SESSION_STORE_FILE") or (Path(__file__).resolve().parents[1] / "instance" / "kmh_sessions.json"))


def _load_sessions() -> dict[str, dict[str, Any]]:
    try:
        raw = _KMH_STORE_FILE.read_text(encoding="utf-8")
        data = json.loads(raw or "{}")
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def _save_sessions(sessions: dict[str, dict[str, Any]]) -> None:
    _KMH_STORE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp_file = _KMH_STORE_FILE.with_suffix(".json.tmp")
    tmp_file.write_text(json.dumps(sessions, separators=(",", ":"), ensure_ascii=True), encoding="utf-8")
    os.replace(tmp_file, _KMH_STORE_FILE)


def create_kmh_session(token: str, username: str, refresh_token: str | None = None, expires_at: float | None = None) -> str:
    session_id = uuid.uuid4().hex
    now = time.time()
    record = {
        "token": token,
        "username": username,
        "refresh_token": refresh_token,
        "token_expires_at": float(expires_at or 0),
        "session_expires_at": now + _KMH_SESSION_TTL_SECONDS,
        "created_at": now,
    }
    with _KMH_LOCK:
        sessions = _load_sessions()
        sessions[session_id] = record
        _save_sessions(sessions)
    return session_id


def get_kmh_session(session_id: str | None) -> dict[str, Any] | None:
    sid = str(session_id or "").strip()
    if not sid:
        return None
    now = time.time()
    with _KMH_LOCK:
        sessions = _load_sessions()
        record = sessions.get(sid)
        if not record:
            return None
        if float(record.get("session_expires_at") or 0) <= now:
            sessions.pop(sid, None)
            _save_sessions(sessions)
            return None
        return dict(record)


def clear_kmh_session(session_id: str | None) -> None:
    sid = str(session_id or "").strip()
    if not sid:
        return
    with _KMH_LOCK:
        sessions = _load_sessions()
        if sid in sessions:
            sessions.pop(sid, None)
            _save_sessions(sessions)
