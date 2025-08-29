import os
import sys
import json
import logging
from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except Exception:
    ZoneInfo = None
    ZoneInfoNotFoundError = Exception  # type: ignore
from typing import Any, Dict, List, Optional, Union
from dotenv import load_dotenv
from zoneinfo import ZoneInfo  # Python 3.9+
import time
import signal
import argparse

import hashlib
CACHE_FILE = os.getenv("SYNC_CACHE_FILE", ".apg_sync_cache.json")

# Optional lightweight popup UI
try:
    import tkinter as _tk
    from tkinter import messagebox as _mb
    _HAS_TK = True
except Exception:
    _HAS_TK = False

LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "Pacific/Auckland"))

SYNC_EVENTS: list[dict] = []

load_dotenv()

import requests

# ===========================
# Configuration (use env vars)
# ===========================

# Envision
ENVISION_BASE = os.getenv("ENVISION_BASE", "https://<envision-host>/v1")
ENVISION_USER = os.getenv("ENVISION_USER", "")   # e.g. "OJB"
ENVISION_PASS = os.getenv("ENVISION_PASS", "")   # e.g. "********"

# APG (RocketRoute / FlightPlan API)
APG_BASE = os.getenv("APG_BASE", "https://fly.rocketroute.com/api")
APG_APP_KEY = os.getenv("APG_APP_KEY", "")             # Provisioned by APG
APG_API_VERSION = os.getenv("APG_API_VERSION", "1.18") # Must be sent on each call
APG_EMAIL = os.getenv("APG_EMAIL", "")                 # API user email (from APG)
APG_PASSWORD = os.getenv("APG_PASSWORD", "")           # API user password (from APG)

# Sync window
WINDOW_PAST_HOURS = int(os.getenv("WINDOW_PAST_HOURS", "24"))
WINDOW_FUTURE_HOURS = int(os.getenv("WINDOW_FUTURE_HOURS", "72"))

# Envision pagination
ENVISION_PAGE_LIMIT = int(os.getenv("ENVISION_PAGE_LIMIT", "100"))

# Registration → APG aircraft_id mapping (populate for your fleet)
REG_TO_APG_AIRCRAFT_ID = {
    # "ZK-CIZ": 137997,
    # "G-TUIA": 251490,
}

# Optional overrides from .env (comma-separated IDs). If unset we auto-discover from /Crews/Positions.
ENV_PIC_POS_OVRD = os.getenv("ENVISION_PIC_POSITION_IDS", "").strip()
ENV_PILOT_POS_OVRD = os.getenv("ENVISION_PILOT_POSITION_IDS", "").strip()

# Defaults if not provided by Envision (tune to your ops)
DEFAULT_RULES = os.getenv("DEFAULT_RULES", "I")      # I (IFR) / V (VFR)
DEFAULT_FTYPE = os.getenv("DEFAULT_FTYPE", "S")      # S=Scheduled, G=General, etc.
DEFAULT_ROUTE = os.getenv("DEFAULT_ROUTE", "DCT")    # Put real routing if available
DEFAULT_FL    = os.getenv("DEFAULT_FL", "300")       # FL as string, e.g. "300"

# IATA -> ICAO mapping (extend as needed)
IATA_TO_ICAO = {
    "AKL": "NZAA",  # Auckland
    "WLG": "NZWN",  # Wellington
    "CHC": "NZCH",  # Christchurch
    "PPQ": "NZPP",  # Paraparaumu
    "WHK": "NZWK",  # Whakatane
    "WAG": "NZWU",  # Whanganui
    "CHT": "NZCI",  # Chatham Islands
    "HLZ": "NZHN",  # Hamilton
    "ROT": "NZRO",  # Rotorua  
    "NSN": "NZNS",  # Nelson
    "ZQN": "NZQN",  # Queenstown    
    "DUD": "NZDN",  # Dunedin
    "IVC": "NZNV",  # Invercargill
    "GIS": "NZGS",  # Gisborne
    "NPE": "NZNR",  # Napier
    "TRG": "NZTG",  # Tauranga
    "BHE": "NZWB",  # Woodbourne

}

# --- APG aircraft caches/indexes ---
_APG_AIRCRAFT_RAW: list[dict] = []
_APG_BY_REG: dict[str, list[dict]] = {}   # REG (no/with dash variants) -> list of APG aircraft rows

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)


# ===========================
# Helpers
# ===========================
def _get_local_tz():
    tzname = os.getenv("LOCAL_TZ", "Pacific/Auckland")
    # Map common Windows name if someone sets LOCAL_TZ to it
    if tzname == "New Zealand Standard Time":
        tzname = "Pacific/Auckland"
    if ZoneInfo:
        try:
            return ZoneInfo(tzname)
        except ZoneInfoNotFoundError:
            pass
    # Optional: try dateutil if available
    try:
        from dateutil.tz import gettz  # python-dateutil is usually installed
        tz = gettz(tzname)
        if tz:
            return tz
    except Exception:
        pass
    logging.warning("Time zone '%s' not found; falling back to UTC. Install 'tzdata' to fix.", tzname)
    return timezone.utc

LOCAL_TZ = _get_local_tz()
def parse_iso(dt: Optional[str]) -> Optional[datetime]:
    if not dt:
        return None
    if dt.endswith("Z"):
        dt = dt.replace("Z", "+00:00")
    return datetime.fromisoformat(dt)

def to_rfc3339(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()

def minutes_to_eet_str(mins: Optional[int]) -> Optional[str]:
    if mins is None:
        return None
    try:
        mins = int(mins)
        if mins < 0:
            return None
        hh = mins // 60
        mm = mins % 60
        return f"{hh:02d}{mm:02d}"
    except Exception:
        return None

def normalize_flight_no(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.replace(" ", "").upper()
    if s.startswith("3C"):
        return "CVA" + s[2:]   # Replace prefix
    return s
def _first(d: dict, *keys: str) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None

def _within_window(dt: Optional[datetime], start_utc: datetime, end_utc: datetime) -> bool:
    if not dt:
        return False
    return start_utc <= dt <= end_utc

def aircraft_id_for_reg(reg: str) -> Optional[int]:
    if not reg:
        return None
    key = (reg or "").strip().upper().replace(" ", "")
    # try exact, no-dash, and with-dash
    if key in REG_TO_APG_AIRCRAFT_ID:
        return REG_TO_APG_AIRCRAFT_ID[key]
    no_dash = key.replace("-", "")
    if no_dash in REG_TO_APG_AIRCRAFT_ID:
        return REG_TO_APG_AIRCRAFT_ID[no_dash]
    if "-" not in key and len(key) >= 3:
        with_dash = key[:2] + "-" + key[2:]
        if with_dash in REG_TO_APG_AIRCRAFT_ID:
            return REG_TO_APG_AIRCRAFT_ID[with_dash]
    return None

def _emit_flight_event(**kw):
    rec = dict(
        envision_flight_id=str(kw.get("envision_flight_id") or ""),
        flight_no=kw.get("flight_no"),
        adep=kw.get("adep"),
        ades=kw.get("ades"),
        eobt=kw.get("eobt"),  # a datetime is fine; we store it to DB later
        reg=kw.get("reg"),
        aircraft_id=kw.get("aircraft_id"),
        pic_name=kw.get("pic_name"),
        pic_empno=kw.get("pic_empno"),
        apg_pic_id=kw.get("apg_pic_id"),
        result=kw.get("result"),   # created|updated|skipped|failed
        reason=kw.get("reason"),
        warnings=kw.get("warnings"),
    )
    SYNC_EVENTS.append(rec)

def ask_refresh_popup() -> bool:
    """
    Return True if the user clicked 'Yes' to refresh again.
    Uses Tkinter messagebox if available; otherwise falls back to CLI prompt.
    """
    if _HAS_TK and os.getenv("USE_POPUP", "1") not in ("0", "false", "False"):
        root = _tk.Tk()
        root.withdraw()  # hide main window
        try:
            return _mb.askyesno("Envision → APG", "Refresh flights now?")
        finally:
            root.destroy()
    # CLI fallback
    ans = input("Refresh flights now? [Y/n]: ").strip().lower()
    return ans in ("", "y", "yes")

def _norm_reg(reg: str) -> str:
    return (reg or "").strip().upper().replace(" ", "")

def build_aircraft_index_from_apg(aircraft: list[dict]) -> dict[str, list[dict]]:
    """
    Build an index of APG aircraft by registration. Keep all variants for the same reg
    so we can choose FRGHTR vs non-FRGHTR later per flight.
    """
    by_reg: dict[str, list[dict]] = {}
    for a in aircraft or []:
        reg = a.get("registration") or a.get("reg") or a.get("tail") or a.get("callsign")
        if not reg:
            continue
        n = _norm_reg(reg)              # ZKCIY or ZK-CIY
        no_dash = n.replace("-", "")    # ZKCIY
        with_dash = no_dash if "-" in n else (no_dash[:2] + "-" + no_dash[2:] if len(no_dash) >= 3 else no_dash)
        for key in {no_dash, with_dash}:
            by_reg.setdefault(key, []).append(a)
    return by_reg

FREIGHT_KEYWORDS = {"freight", "freight charter", "intl freight"}

def is_freight_flight(envision_flight: dict) -> bool:
    desc = (envision_flight.get("flightTypeDescription") or "").strip().lower()
    # match any keyword; keep it simple
    return any(k in desc for k in FREIGHT_KEYWORDS)

def choose_apg_aircraft_id_for_flight(envision_flight: dict) -> Optional[int]:
    """
    For a given Envision flight, pick the APG aircraft_id:
      - If freight: prefer APG aircraft with reference == 'FRGHTR'
      - Else: prefer non-FRGHTR entry
      - Fallback: any entry for this registration
    """
    reg_raw = (envision_flight.get("flightRegistrationDescription") or "")
    if not reg_raw:
        return None
    reg_keys = {_norm_reg(reg_raw).replace("-", "")}
    n = next(iter(reg_keys))
    if len(n) >= 3:
        reg_keys.add(n[:2] + "-" + n[2:])

    # gather all APG rows that match this reg (either key form)
    rows: list[dict] = []
    for k in reg_keys:
        rows.extend(_APG_BY_REG.get(k, []))

    if not rows:
        return None

    wants_freight = is_freight_flight(envision_flight)

    def ref_is_frghtr(r: dict) -> bool:
        return (r.get("reference") or "").strip().upper() == "FRGHTR"

    # 1) exact preference
    prefer = [r for r in rows if ref_is_frghtr(r)] if wants_freight else [r for r in rows if not ref_is_frghtr(r)]
    if prefer:
        try:
            return int(prefer[0].get("id"))
        except Exception:
            pass

    # 2) fallback: any
    try:
        return int(rows[0].get("id"))
    except Exception:
        return None

# Change signature:
def apg_get_plan_list(bearer: str, status: Optional[str] = None, page_size: int = 50, after: Optional[str] = None) -> list[dict]:
    """
    Fetch list of flight plans from APG, normalizing shape to List[Dict].
    Accepts:
      - {"status":{"success":true}, "data":[ ... ]}
      - {"status":{"success":true}, "data":{"items":[ ... ]}}
      - [ ... ]
    If 'status' is None/empty, it is omitted (server returns all statuses).
    """
    url = f"{APG_BASE}/plan/list"
    headers = {
        "Authorization": bearer,
        "X-API-Version": APG_API_VERSION,
        "Content-Type": "application/json",
        "User-Agent": "AirChathams-Bridge/1.0",
    }
    payload = {"page": 1, "page_size": page_size, "is_template": 0}
    if status:
        payload["status"] = status
    if after:
        payload["after"] = after

    all_plans: list[dict] = []
    while True:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()

        if isinstance(data, list):
            plans = data
        elif isinstance(data, dict):
            d = data.get("data")
            if isinstance(d, list):
                plans = d
            elif isinstance(d, dict):
                for k in ("items", "plans", "list", "rows", "results"):
                    v = d.get(k)
                    if isinstance(v, list):
                        plans = v
                        break
                else:
                    plans = next((v for v in d.values() if isinstance(v, list)), [])
            else:
                plans = []
        else:
            plans = []

        plans = [p for p in plans if isinstance(p, dict)]
        all_plans.extend(plans)

        if len(plans) < page_size:
            break
        payload["page"] += 1

    return all_plans

# ===========================
# Envision API
# ===========================

def envision_authenticate() -> Dict[str, str]:
    """
    POST /v1/Authenticate
    Body: {"username": "...", "password": "..."}
    Optional header: X-Tenant-Id (from ENVISION_TENANT) if your deployment needs it.
    """
    base = ENVISION_BASE.rstrip("/")
    # If ENVISION_BASE already ends with /v1, use /Authenticate; otherwise add /v1/Authenticate
    if base.endswith("/v1"):
        auth_url = f"{base}/Authenticate"
    else:
        auth_url = f"{base}/v1/Authenticate"

    payload = {
        "username": (ENVISION_USER or "").strip(),
        "password": (ENVISION_PASS or "").strip(),
    }
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    tenant = os.getenv("ENVISION_TENANT")
    if tenant:
        headers["X-Tenant-Id"] = tenant  # only if your API expects it

    try:
        r = requests.post(auth_url, json=payload, headers=headers, timeout=30)
        if r.status_code == 401:
            # Show server message to pinpoint cause (bad user, bad pass, user disabled, missing tenant, etc.)
            raise RuntimeError(f"Envision auth 401. URL={auth_url} Body={r.text}")
        r.raise_for_status()
    except requests.RequestException as e:
        # Surface full context for quick debugging
        raise RuntimeError(f"Envision auth failed. URL={auth_url} Error={e}")

    data = r.json()
    token = data.get("token")
    if not token:
        raise RuntimeError(f"Envision auth response missing token. Body={r.text}")
    return {"token": token, "refreshToken": data.get("refreshToken")}


def envision_get_flights(token: str, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
    """
    GET /v1/Flights?dateFrom=...&dateTo=...&offset=...&limit=...
    Returns: list of flights
    """
    headers = {"Authorization": f"Bearer {token}"}
    offset = 0
    results: List[Dict[str, Any]] = []

    while True:
        params = {
            "dateFrom": to_rfc3339(date_from),
            "dateTo": to_rfc3339(date_to),
            "offset": offset,
            "limit": ENVISION_PAGE_LIMIT,
        }
        url = f"{ENVISION_BASE}/Flights"
        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        page = r.json() or []
        if not isinstance(page, list):
            raise RuntimeError(f"Unexpected /Flights response: {page}")
        results.extend(page)
        logging.info(f"Envision: fetched {len(page)} (offset={offset}), total={len(results)}")
        if len(page) < ENVISION_PAGE_LIMIT:
            break
        offset += ENVISION_PAGE_LIMIT

    return results

# ===========================
# APG (RocketRoute) API
# ===========================

def apg_login(email: str, password: str) -> Dict[str, str]:
    """
    Logs into APG using AppKey + email/password and returns {"authorization": "Bearer ...", "refresh_token": "..."}.
    Tries (host,version) combos in this order:
      1) APG_BASE (as provided) + APG_API_VERSION
      2) APG_BASE (as provided) + 1.14
      3) toggled host (fly<->flydev) + APG_API_VERSION
      4) toggled host (fly<->flydev) + 1.14
    Produces detailed diagnostics on failure (HTTP code, Content-Type, body preview).
    """

    # --- sanitize & validate ---
    base = (APG_BASE or "").strip().rstrip("/")
    app_key = (APG_APP_KEY or "").strip()
    ver_primary = (APG_API_VERSION or "1.18").strip()
    ver_fallback = "1.14"
    email = (email or "").strip()
    password = (password or "").strip()

    if not base:
        raise RuntimeError("APG_BASE is empty. Set it in your .env (e.g., https://fly.rocketroute.com/api or https://flydev.rocketroute.com/api)")
    if not app_key:
        raise RuntimeError("APG_APP_KEY is empty. Paste the AppKey APG issued (NOT an access token) into your .env")
    if not email or not password:
        raise RuntimeError("APG_EMAIL or APG_PASSWORD is empty. Set both in your .env")

    def host_toggle(u: str) -> str:
        # swap prod/dev hosts if user set the other one by mistake
        if "flydev.rocketroute.com" in u:
            return u.replace("flydev.rocketroute.com", "fly.rocketroute.com")
        if "fly.rocketroute.com" in u:
            return u.replace("fly.rocketroute.com", "flydev.rocketroute.com")
        return u  # unknown custom host; leave as-is

    candidates = [
        (base, ver_primary),
        (base, ver_fallback) if ver_primary != ver_fallback else None,
        (host_toggle(base), ver_primary),
        (host_toggle(base), ver_fallback) if ver_primary != ver_fallback else None,
    ]
    candidates = [c for c in candidates if c is not None]

    last_error = None
    for host, ver in candidates:
        url = f"{host}/login"
        headers = {
            "Authorization": f"AppKey {app_key}",
            "X-API-Version": ver,
            "Content-Type": "application/json",
            "User-Agent": "AirChathams-Bridge/1.0",
        }
        payload = {"email": email, "password": password}

        # quick visibility (no secrets)
        print(f"[APG] Trying login → host={host} ver={ver} appkey_len={len(app_key)} email_set={bool(email)}")

        try:
            r = requests.post(url, headers=headers, json=payload, timeout=30)
        except requests.RequestException as re:
            last_error = f"Network error to {url}: {re}"
            print("[APG] ", last_error)
            continue

        ct = r.headers.get("Content-Type", "")
        if not r.ok:
            body_preview = (r.text or "")[:600]
            last_error = f"HTTP {r.status_code} (ver {ver}) CT={ct} Body={body_preview}"
            print("[APG] Login failed:", last_error)
            # Common: 403 text/plain 'Not logged in' => bad/missing AppKey, wrong host/env, or user not API-activated
            continue

        # Must be JSON per spec
        try:
            data = r.json()
        except Exception:
            body_preview = (r.text or "")[:600]
            last_error = f"Non-JSON response (ver {ver}) CT={ct} Body={body_preview}"
            print("[APG] Login failed:", last_error)
            continue

        status = data.get("status", {})
        if not status.get("success"):
            last_error = f"JSON error (ver {ver}): {status.get('message')} Body={data}"
            print("[APG] Login failed:", last_error)
            continue

        # Bearer can be in header; if not, use data.access_token
        bearer = r.headers.get("Authorization")
        if not bearer or not bearer.startswith("Bearer "):
            access_token = data.get("data", {}).get("access_token")
            if not access_token:
                last_error = f"Success without token (ver {ver}). Body={data}"
                print("[APG] Login failed:", last_error)
                continue
            bearer = f"Bearer {access_token}"

        refresh = data.get("data", {}).get("refresh_token", "")
        print(f"[APG] Login OK on host={host} ver={ver}")
        return {"authorization": bearer, "refresh_token": refresh}

    # if we exhausted all combos:
    raise RuntimeError(
        "APG login failed across all hosts/versions. "
        "Most likely causes: invalid/missing AppKey, wrong environment (prod vs dev), user not API-activated, or proxy stripping Authorization. "
        f"Last error: {last_error}"
    )



def apg_refresh(refresh_token: str) -> Dict[str, str]:
    """
    POST /api/login with AppKey + refresh_token to get a new Bearer.
    """
    url = f"{APG_BASE}/login"
    headers = {
        "Authorization": f"AppKey {APG_APP_KEY}",
        "X-API-Version": APG_API_VERSION,
        "Content-Type": "application/json",
    }
    payload = {"refresh_token": refresh_token}
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    status = data.get("status", {})
    if not status.get("success"):
        raise RuntimeError(f"APG refresh failed: {status.get('message')}")
    auth_header = r.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        access_token = data.get("data", {}).get("access_token")
        if not access_token:
            raise RuntimeError("APG refresh succeeded but no access token found")
        auth_header = f"Bearer {access_token}"
    new_refresh = data.get("data", {}).get("refresh_token", "")
    return {"authorization": auth_header, "refresh_token": new_refresh}

def apg_headers(bearer: str) -> Dict[str, str]:
    return {
        "Authorization": bearer,
        "X-API-Version": APG_API_VERSION,
        "Content-Type": "application/json",
    }

def apg_plan_edit(bearer: str, plan_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    POST /api/plan/edit — create or update a plan.
    Response always HTTP 200; check status.success and status.warnings.
    """
    url = f"{APG_BASE}/plan/edit"
    r = requests.post(url, headers=apg_headers(bearer), json=plan_payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    status = data.get("status", {})
    if not status.get("success", False):
        raise RuntimeError(f"APG plan/edit error: {status.get('message', 'unknown')}")
    return data


# ===========================
# Transform: Envision → APG
# ===========================

def envision_to_apg_plan(f: Dict[str, Any], aircraft_id: Optional[int], pic_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
    adep = to_icao(f.get("departurePlaceDescription"))
    ades = to_icao(f.get("arrivalPlaceDescription"))
    eobt_raw = f.get("departureScheduled")
    eobt = parse_iso(eobt_raw) if eobt_raw else None
    flight_no = normalize_flight_no(f.get("flightNumberDescription"))
    eet = minutes_to_eet_str(f.get("plannedFlightTime"))

    missing = []
    if not adep: missing.append("ADEP")
    if not ades: missing.append("ADES")
    if not eobt: missing.append("EOBT")
    if not aircraft_id: missing.append("aircraft_id")
    if missing:
        logging.warning(f"Skipping flight {f.get('id')} — missing: {', '.join(missing)}")
        return None

    plan = {
        "adep": adep,
        "ades": ades,
        "rules": DEFAULT_RULES,
        "flight_type": DEFAULT_FTYPE,
        "flight_no": flight_no,
        "route": DEFAULT_ROUTE,
        "fl": DEFAULT_FL,
        "eobt": to_rfc3339(eobt),
        "aircraft_id": aircraft_id,
    }
    if eet:
        plan["eet"] = eet
    if pic_name:
        plan["pic"] = pic_name
    return plan



# ===========================
# Main
# ===========================

def main():
    """
    One sync pass: Envision → APG.
    Robust idempotency:
      - Dedup against APG across all statuses (omit 'status' on /plan/list)
      - Short 'recently created' TTL to avoid immediate double-creates if APG list lags
    """
    import json as _json
    import time as _time
    from datetime import datetime, timedelta, timezone

    # ---------- small helpers (scoped) ----------
    def _fingerprint(payload: dict, pic_name: Optional[str], apg_pic_id: Optional[int]) -> str:
        core = {
            "adep": payload.get("adep"),
            "ades": payload.get("ades"),
            "eobt": payload.get("eobt"),
            "aircraft_id": payload.get("aircraft_id"),
            "flight_no": payload.get("flight_no"),
            "route": payload.get("route"),
            "fl": payload.get("fl"),
            "eet": payload.get("eet"),
            "pic_id": apg_pic_id or None,
            "pic_name": (pic_name or "").strip() or None,
        }
        s = _json.dumps(core, sort_keys=True, separators=(",", ":"))
        return hashlib.sha1(s.encode("utf-8")).hexdigest()

    def _window_now():
        local_tz = _get_local_tz()
        now_local = datetime.now(local_tz)
        date_from_local = now_local - timedelta(hours=WINDOW_PAST_HOURS)
        date_to_local   = now_local + timedelta(hours=WINDOW_FUTURE_HOURS)
        return (now_local, date_from_local, date_to_local,
                date_from_local.astimezone(timezone.utc),
                date_to_local.astimezone(timezone.utc))

    def _canon_eobt_to_utc_min_str(dt_or_str: Optional[Union[str, datetime]]) -> Optional[str]:
        """Return canonical UTC minute string 'YYYY-MM-DDTHH:MMZ' from ISO str or datetime."""
        if not dt_or_str:
            return None
        try:
            dt = dt_or_str
            if isinstance(dt_or_str, str):
                s = dt_or_str.strip()
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"
                dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
            return dt.strftime("%Y-%m-%dT%H:%MZ")
        except Exception:
            return None

    def _key_tuple_from_payload(payload: dict) -> tuple[str, str, str, Optional[str]]:
        return (
            normalize_flight_no(payload.get("flight_no")),
            (payload.get("adep") or "").strip().upper(),
            (payload.get("ades") or "").strip().upper(),
            _canon_eobt_to_utc_min_str(payload.get("eobt")),
        )

    def _key_str(key_tuple: tuple[str, str, str, Optional[str]]) -> str:
        return "|".join([key_tuple[0], key_tuple[1], key_tuple[2], key_tuple[3] or ""])

    # robust field getter + window test
    def _first(d: dict, *keys: str):
        for k in keys:
            if k in d and d[k] not in (None, ""):
                return d[k]
        return None

    def _within_window(dt: Optional[datetime], start_utc: datetime, end_utc: datetime) -> bool:
        if not dt:
            return False
        return start_utc <= dt <= end_utc

    # ---------- sanity checks ----------
    missing = []
    if not ENVISION_USER or not ENVISION_PASS or "<envision-host>" in (ENVISION_BASE or ""):
        missing.append("Envision (ENVISION_BASE/ENVISION_USER/ENVISION_PASS)")
    if not APG_APP_KEY or not APG_EMAIL or not APG_PASSWORD:
        missing.append("APG (APG_APP_KEY/APG_EMAIL/APG_PASSWORD)")
    if missing:
        logging.error(f"Missing config: {', '.join(missing)}")
        raise RuntimeError("Configuration incomplete")

    # ---------- window ----------
    now_local, date_from_local, date_to_local, date_from_utc, date_to_utc = _window_now()
    logging.info(
        "Fetching Envision flights (local NZ) %s → %s | (UTC) %s → %s",
        date_from_local.isoformat(), date_to_local.isoformat(),
        date_from_utc.isoformat(), date_to_utc.isoformat()
    )

    # ---------- idempotency cache ----------
    cache = _load_cache()  # structure: { "<envisionId>": "<sha1>", "_recent_keys": { "<key_str>": <epoch> } }
    recent_map: dict[str, float] = {}
    try:
        recent_map = dict(cache.get("_recent_keys", {}))
    except Exception:
        recent_map = {}

    # scrub old recents on load
    RECENT_TTL_SEC = int(os.getenv("RECENT_TTL_SEC", "600"))  # 10 minutes default
    now_epoch = _time.time()
    recent_map = {k: t for k, t in recent_map.items() if (now_epoch - float(t)) < RECENT_TTL_SEC}
    cache["_recent_keys"] = recent_map  # persist cleaned map

    # ---------- Envision fetch ----------
    logging.info("Authenticating to Envision…")
    env_auth = envision_authenticate()
    env_token = env_auth["token"]

    flights = envision_get_flights(env_token, date_from_utc, date_to_utc)
    logging.info(f"Envision flights fetched: {len(flights)}")

    # keep only departures >= now (UTC), with optional leeway
    PAST_LEEWAY_MIN = int(os.getenv("PAST_LEEWAY_MIN", "0"))

    def dep_utc(f):
        for key in ("departureEstimate", "departureScheduled"):
            dt = f.get(key)
            if dt:
                return parse_iso(dt).astimezone(timezone.utc)
        return None

    pre = len(flights)
    utc_now = now_local.astimezone(timezone.utc)
    flights = [f for f in flights if (d := dep_utc(f)) and d >= (utc_now - timedelta(minutes=PAST_LEEWAY_MIN))]
    logging.info(
        "Filtered past flights: kept %d of %d (NZ now=%s | UTC now=%s)",
        len(flights), pre, now_local.isoformat(), utc_now.isoformat()
    )

    # Optional testing cap
    test_limit_env = os.getenv("SYNC_TEST_LIMIT", "").strip()
    if test_limit_env:
        try:
            tlim = int(test_limit_env)
        except Exception:
            tlim = 0
    else:
        tlim = int(os.getenv("LEGACY_TEST_LIMIT", "5"))
    if tlim > 0:
        flights = flights[:tlim]
        logging.info(f"Limiting to first {len(flights)} flights for testing")

    if not flights:
        logging.info("No flights to process. Exiting.")
        return {
            "created": 0,
            "skipped": 0,
            "warnings_total": 0,
            "window_from_local": date_from_local,
            "window_to_local": date_to_local,
            "window_from_utc": date_from_utc,
            "window_to_utc": date_to_utc,
        }

    # ---------- APG auth & lookups ----------
    logging.info("Authenticating to APG…")
    apg_auth = apg_login(APG_EMAIL, APG_PASSWORD)
    apg_bearer = apg_auth["authorization"]
    apg_refresh_token = apg_auth.get("refresh_token", "")

    # Aircraft index
    try:
        aircraft = apg_get_aircraft_list(apg_bearer)
        logging.info(f"Loaded {len(aircraft)} aircraft from APG.")
        global _APG_AIRCRAFT_RAW, _APG_BY_REG
        _APG_AIRCRAFT_RAW = aircraft
        _APG_BY_REG = build_aircraft_index_from_apg(aircraft)
    except Exception as e:
        logging.warning(f"Could not fetch aircraft list from APG: {e}")
        _APG_AIRCRAFT_RAW, _APG_BY_REG = [], {}

    # Crew mapping
    try:
        crewcode_to_id = build_crewcode_to_id(apg_bearer)
        logging.info(f"Loaded {len(crewcode_to_id)} crew from APG.")
    except Exception as e:
        logging.warning(f"Could not fetch APG crew list: {e}")
        crewcode_to_id = {}

    # Presence set from APG — fetch ALL (omit 'status'), then filter to our window
    try:
        apg_plans = apg_get_plan_list(apg_bearer, status=None, page_size=200, after=None)
        logging.info(f"APG plan list fetched: {len(apg_plans)} rows (all statuses)")
    except Exception as e:
        logging.warning(f"Could not fetch APG plan list: {e}")
        apg_plans = []

    existing_keys: set[tuple[str, str, str, Optional[str]]] = set()
    for p in apg_plans:
        flight_no = normalize_flight_no(_first(p, "flight_no", "flightNo", "callsign") or "")
        adep      = (_first(p, "adep", "dep", "from", "origin") or "").strip().upper()
        ades      = (_first(p, "ades", "dest", "to", "destination") or "").strip().upper()
        eobt_raw  = _first(p, "eobt", "off_block_time", "etd", "std")

        # parse EOBT and filter to our window
        eobt_dt = parse_iso(eobt_raw) if isinstance(eobt_raw, str) else (eobt_raw if isinstance(eobt_raw, datetime) else None)
        if eobt_dt:
            if eobt_dt.tzinfo is None:
                eobt_dt = eobt_dt.replace(tzinfo=timezone.utc)
            eobt_dt_utc = eobt_dt.astimezone(timezone.utc)
        else:
            eobt_dt_utc = None

        if _within_window(eobt_dt_utc, date_from_utc, date_to_utc):
            eobt_key = _canon_eobt_to_utc_min_str(eobt_dt_utc)
            existing_keys.add((flight_no, adep, ades, eobt_key))

    REQUIRE_PIC_IN_APG = os.getenv("REQUIRE_PIC_IN_APG", "false").lower() in ("1", "true", "yes")
    created, skipped, warnings_total = 0, 0, 0

    # ---------- process each flight ----------
    for f in flights:
        # Resolve PIC
        pic_name, pic_empno = resolve_pic_for_flight(env_token, f)

        aircraft_id = choose_apg_aircraft_id_for_flight(f)
        if not aircraft_id:
            logging.warning(f"Skip flight {f.get('id')} — no APG aircraft match for reg {f.get('flightRegistrationDescription')}")
            skipped += 1
            continue

        # Build payload (also validates fields)
        payload = envision_to_apg_plan(f, aircraft_id=aircraft_id, pic_name=pic_name)
        if not payload:
            skipped += 1
            continue

        # Base record for UI logging
        base_evt = dict(
            envision_flight_id=f.get("id"),
            flight_no=normalize_flight_no(f.get("flightNumberDescription")),
            adep=to_icao(f.get("departurePlaceDescription")),
            ades=to_icao(f.get("arrivalPlaceDescription")),
            eobt=parse_iso(f.get("departureScheduled") or f.get("departureEstimate") or ""),
            reg=(f.get("flightRegistrationDescription") or "").strip().upper(),
            aircraft_id=(payload or {}).get("aircraft_id") or aircraft_id_for_reg((f.get("flightRegistrationDescription") or "").strip().upper()),
            pic_name=pic_name,
            pic_empno=pic_empno,
            apg_pic_id=None,
        )

        # Attach APG crew IDs if we can
        apg_pic_id = None
        if pic_empno:
            apg_pic_id = crewcode_to_id.get(pic_empno.upper())
        if apg_pic_id:
            payload["crew"] = {"pic_id": apg_pic_id, "fo_id": 0, "tic_id": 0}
            base_evt["apg_pic_id"] = apg_pic_id
        else:
            if REQUIRE_PIC_IN_APG and pic_empno:
                logging.warning(f"Skipping flight {f.get('id')} — PIC employeeNo {pic_empno} not found in APG crew.")
                skipped += 1
                _emit_flight_event(**base_evt, result="skipped",
                                   reason="PIC not found in APG and REQUIRE_PIC_IN_APG=true",
                                   warnings=None)
                continue
            if pic_name and pic_empno:
                logging.warning(f"No APG crew match for PIC employeeNo={pic_empno} (flight {f.get('id')}). Proceeding without crew linkage.")

        # Idempotency: skip only if (cache fp matches) AND (APG has key OR it's very recent success)
        fid = str(f.get("id") or "")
        fp = _fingerprint(payload, pic_name, apg_pic_id)
        key = _key_tuple_from_payload(payload)
        key_s = _key_str(key)

        recent_hit = (key_s in recent_map) and ((now_epoch - float(recent_map[key_s])) < RECENT_TTL_SEC)
        if fid and cache.get(fid) == fp and (key in existing_keys or recent_hit):
            why = "no changes since last sync" if key in existing_keys else "recent success (TTL)"
            logging.info(f"Skip flight {fid} — {why}")
            skipped += 1
            _emit_flight_event(**base_evt, result="skipped", reason="no changes since last sync", warnings=None)
            continue

        # Push to APG
        try:
            res = apg_plan_edit(apg_bearer, payload)
            status = res.get("status", {})
            warns = status.get("warnings")
            if warns:
                warnings_total += len(warns)
                logging.warning(f"APG warnings for Envision flight {f.get('id')}: {warns}")
            created += 1
            cache[fid] = fp  # success → remember fp
            _emit_flight_event(**base_evt, result="created", reason=None,
                               warnings=_json.dumps(warns) if warns else None)
            logging.info(
                f"APG plan created/updated for flight {f.get('id')} ({payload['flight_no']})"
                + (f" PIC={pic_name}" if pic_name else "")
                + (f" (APG pic_id={apg_pic_id})" if apg_pic_id else "")
            )
            # Update presence & recent-keys to be robust on immediate subsequent runs
            existing_keys.add(key)
            recent_map[key_s] = _time.time()
            cache["_recent_keys"] = recent_map

        except requests.HTTPError as http_err:
            if http_err.response is not None and http_err.response.status_code == 401 and apg_refresh_token:
                logging.info("APG 401 — refreshing token and retrying once…")
                new_tokens = apg_refresh(apg_refresh_token)
                apg_bearer = new_tokens["authorization"]
                apg_refresh_token = new_tokens.get("refresh_token", apg_refresh_token)

                res = apg_plan_edit(apg_bearer, payload)
                status = res.get("status", {})
                warns = status.get("warnings")
                if warns:
                    warnings_total += len(warns)
                    logging.warning(f"APG warnings for Envision flight {f.get('id')}: {warns}")
                created += 1
                cache[fid] = fp
                _emit_flight_event(**base_evt, result="created", reason=None,
                                   warnings=_json.dumps(warns) if warns else None)
                logging.info(
                    f"APG plan created/updated (after refresh) for flight {f.get('id')} ({payload['flight_no']})"
                    + (f" PIC={pic_name}" if pic_name else "")
                    + (f" (APG pic_id={apg_pic_id})" if apg_pic_id else "")
                )
                existing_keys.add(key)
                recent_map[key_s] = _time.time()
                cache["_recent_keys"] = recent_map
            else:
                skipped += 1
                body = http_err.response.text[:500] if (http_err.response and http_err.response.text) else ""
                _emit_flight_event(**base_evt, result="failed",
                                   reason=f"HTTP {http_err.response.status_code if http_err.response else ''}",
                                   warnings=body or None)
                logging.exception(f"HTTP error pushing flight {f.get('id')} to APG")

        except Exception:
            skipped += 1
            _emit_flight_event(**base_evt, result="failed",
                               reason="Unhandled exception", warnings=None)
            logging.exception(f"Error pushing flight {f.get('id')} to APG")

    # Persist cache (includes cleaned _recent_keys)
    _save_cache(cache)

    logging.info(f"Done. Created/updated: {created}, skipped: {skipped}, APG warnings: {warnings_total}")

    return {
        "created": created,
        "skipped": skipped,
        "warnings_total": warnings_total,
        "window_from_local": date_from_local,
        "window_to_local": date_to_local,
        "window_from_utc": date_from_utc,
        "window_to_utc": date_to_utc,
    }


def apg_get_aircraft_list(bearer: str) -> list[dict]:
    """
    Fetch aircraft from APG and return a list of dicts.
    NOTE: Endpoint name can vary by tenant:
      - /api/aircraft/list
      - /api/data/aircraft/list
    We'll try both and use the first that works.
    """
    headers = {
        "Authorization": bearer,
        "X-API-Version": APG_API_VERSION,
        "Content-Type": "application/json",
        "User-Agent": "AirChathams-Bridge/1.0",
    }
    bases = [APG_BASE.rstrip("/")]
    paths = ["/aircraft/list", "/data/aircraft/list"]  # try both
    
    last_err = None
    for base in bases:
        for path in paths:
            url = f"{base}{path}"
            try:
                r = requests.get(url, headers=headers, timeout=30)
                if not r.ok:
                    last_err = f"{url} -> HTTP {r.status_code} {r.text[:300]}"
                    continue
                data = r.json()
                # Common shapes:
                # {"status":{"success":true}, "data":[{...}, ...]}
                if isinstance(data, dict) and "data" in data:
                    return data["data"] or []
                if isinstance(data, list):
                    return data
            except Exception as e:
                last_err = f"{url} -> {e}"
                continue
    raise RuntimeError(f"Could not fetch aircraft list from APG. Last error: {last_err}")


def build_reg_to_id_from_apg(bearer: str) -> dict[str, int]:
    aircraft = apg_get_aircraft_list(bearer)
    mapping: dict[str, int] = {}

    def norm(reg: str) -> str:
        return (reg or "").strip().upper().replace(" ", "")

    for a in aircraft:
        reg = a.get("registration") or a.get("reg") or a.get("tail") or a.get("callsign")
        aid = a.get("id") or a.get("aircraft_id")
        if not reg or not aid:
            continue
        aid = int(aid)
        n = norm(reg)                 # e.g. ZKMCU or ZK-CIT
        no_dash = n.replace("-", "")  # ZKMCU
        with_dash = no_dash
        if "-" not in n and len(n) >= 3:
            with_dash = n[:2] + "-" + n[2:]  # ZK-MCU

        # store both keys
        mapping[no_dash] = aid
        mapping[with_dash] = aid

    return mapping

def resolve_pic_name_for_flight(token: str, flight: dict) -> Optional[str]:
    """
    Find PIC name for this flight.
    Priority:
      1) Any crew with crewPositionId in PIC positions
      2) If not found: isPilotFlying among pilot positions
      3) If not found: first-by-displayOrder among pilot positions
    """
    fid = flight.get("id")
    if not fid:
        return None

    try:
        pic_pos, pilot_pos = build_pic_pilot_position_sets(token)
        crew = envision_get_flight_crew(token, int(fid))
    except Exception as e:
        logging.warning(f"Could not resolve PIC for flight {fid}: {e}")
        return None

    # 1) Explicit PIC positions
    for c in crew:
        if c.get("crewPositionId") in pic_pos:
            emp_id = c.get("employeeId")
            if emp_id:
                emp = envision_get_employee(token, int(emp_id))
                return format_employee_name(emp)

    # 2) Pilot Flying among pilots
    for c in crew:
        if c.get("isPilotFlying") and c.get("crewPositionId") in pilot_pos:
            emp_id = c.get("employeeId")
            if emp_id:
                emp = envision_get_employee(token, int(emp_id))
                return format_employee_name(emp)

    # 3) First pilot by displayOrder
    pilots = [c for c in crew if c.get("crewPositionId") in pilot_pos]
    if pilots:
        pilots.sort(key=lambda x: (x.get("displayOrder") or 0))
        emp_id = pilots[0].get("employeeId")
        if emp_id:
            emp = envision_get_employee(token, int(emp_id))
            return format_employee_name(emp)

    return None


def guess_code(raw: Optional[str]) -> Optional[str]:
    """Pick a likely aerodrome code from the Envision field (handles 'AKL', 'NZAA', 'Auckland (AKL)' etc.)."""
    if not raw:
        return None
    s = raw.strip().upper()
    # If it looks like "Auckland (AKL)" pull the token in parentheses
    if "(" in s and ")" in s:
        inside = s[s.find("(")+1:s.find(")")]
        if inside:
            return inside.strip().upper()
    # Otherwise return the last contiguous A–Z block
    tokens = [t for t in ''.join([c if c.isalpha() else ' ' for c in s]).split() if t.isalpha()]
    return tokens[-1] if tokens else s

def to_icao(code: Optional[str]) -> Optional[str]:
    """Return ICAO code for a supplied aerodrome code (IATA or ICAO)."""
    if not code:
        return None
    c = code.strip().upper()
    if len(c) == 4:
        return c  # already ICAO
    if len(c) == 3:
        if c in IATA_TO_ICAO:
            return IATA_TO_ICAO[c]
        logging.warning(f"Unknown IATA code '{c}' — add to IATA_TO_ICAO to proceed.")
        return None
    # Fallback: sometimes Envision passes names; try guess_code then recurse once
    g = guess_code(c)
    if g and g != c:
        return to_icao(g)
    return None

# Caches
_EMP_CACHE: dict[int, dict] = {}
_CREW_POS_CACHE: list[dict] = []
_PIC_POS_IDS: set[int] = set()
_PILOT_POS_IDS: set[int] = set()

def envision_get_crew_positions(token: str) -> list[dict]:
    """GET /v1/Crews/Positions"""
    global _CREW_POS_CACHE
    if _CREW_POS_CACHE:
        return _CREW_POS_CACHE
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{ENVISION_BASE}/Crews/Positions"
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    data = r.json() or []
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected /Crews/Positions response: {data}")
    _CREW_POS_CACHE = data
    return data

def build_pic_pilot_position_sets(token: str) -> tuple[set[int], set[int]]:
    """
    Build sets of position IDs:
      - PIC positions (isCaptain == True)
      - Pilot positions (isCaptain or isFirstOfficer)
    You can override via ENVISION_PIC_POSITION_IDS / ENVISION_PILOT_POSITION_IDS.
    """
    global _PIC_POS_IDS, _PILOT_POS_IDS

    if ENV_PIC_POS_OVRD:
        _PIC_POS_IDS = {int(x) for x in ENV_PIC_POS_OVRD.split(",") if x.strip().isdigit()}
    if ENV_PILOT_POS_OVRD:
        _PILOT_POS_IDS = {int(x) for x in ENV_PILOT_POS_OVRD.split(",") if x.strip().isdigit()}

    if _PIC_POS_IDS and _PILOT_POS_IDS:
        return _PIC_POS_IDS, _PILOT_POS_IDS

    positions = envision_get_crew_positions(token)

    if not _PIC_POS_IDS:
        _PIC_POS_IDS = {p["id"] for p in positions if p.get("isCaptain")}
    if not _PILOT_POS_IDS:
        _PILOT_POS_IDS = {p["id"] for p in positions if p.get("isCaptain") or p.get("isFirstOfficer")}

    # Safety: if API ever empty, fall back to your provided list
    if not _PIC_POS_IDS:
        _PIC_POS_IDS = {71, 1072, 1074, 1075}  # CPT, C/T CPT, LT CPT, S CPT
    if not _PILOT_POS_IDS:
        _PILOT_POS_IDS = _PIC_POS_IDS | {75, 1073}  # + FO, LT FO

    return _PIC_POS_IDS, _PILOT_POS_IDS

def envision_get_flight_crew(token: str, flight_id: int) -> list[dict]:
    """GET /v1/Flights/{flightId}/Crew"""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{ENVISION_BASE}/Flights/{flight_id}/Crew"
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    data = r.json() or []
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected /Crew response for flight {flight_id}: {data}")
    return data

def envision_get_employee(token: str, employee_id: int) -> dict:
    """GET /v1/Employees/{employeeId} (cached)"""
    if employee_id in _EMP_CACHE:
        return _EMP_CACHE[employee_id]
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{ENVISION_BASE}/Employees/{employee_id}"
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    emp = r.json() or {}
    _EMP_CACHE[employee_id] = emp
    return emp

def format_employee_name(emp: dict) -> Optional[str]:
    first = (emp.get("firstName") or "").strip()
    last  = (emp.get("surname") or "").strip()
    if first or last:
        return (first + " " + last).strip()
    return (emp.get("shortDisplayName") or emp.get("employeeUsername") or "").strip() or None

def apg_get_crew_list(bearer: str) -> list[dict]:
    """
    POST /api/crew/list  (some tenants use GET; try POST first per spec)
    Returns a list of crew objects with fields incl. id, crew_code, fname, lname.
    """
    url = f"{APG_BASE.rstrip('/')}/crew/list"
    headers = {
        "Authorization": bearer,
        "X-API-Version": APG_API_VERSION,
        "Content-Type": "application/json",
        "User-Agent": "AirChathams-Bridge/1.0",
    }
    # Most installs expect POST with empty body
    r = requests.post(url, headers=headers, json={}, timeout=30)
    if not r.ok:
        # Some tenants expose GET instead
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
    else:
        r.raise_for_status()

    data = r.json()
    # Common shapes: {"status":{"success":true},"data":[...]} or just [...]
    if isinstance(data, dict) and "data" in data:
        return data["data"] or []
    if isinstance(data, list):
        return data
    raise RuntimeError(f"Unexpected /crew/list response: {data}")


def build_crewcode_to_id(bearer: str) -> dict[str, int]:
    crew = apg_get_crew_list(bearer)
    mapping: dict[str, int] = {}
    for c in crew:
        code = (c.get("crew_code") or "").strip().upper()
        cid  = c.get("id")
        if code and cid:
            try:
                mapping[code] = int(cid)
            except Exception:
                continue
    return mapping

def resolve_pic_for_flight(token: str, flight: dict) -> tuple[Optional[str], Optional[str]]:
    """
    Returns (pic_name, pic_employee_no).
    We pick the Captain (isCaptain positions). If missing, fall back as before.
    """
    fid = flight.get("id")
    if not fid:
        return None, None

    try:
        pic_pos, pilot_pos = build_pic_pilot_position_sets(token)
        crew = envision_get_flight_crew(token, int(fid))
    except Exception as e:
        logging.warning(f"Could not resolve PIC for flight {fid}: {e}")
        return None, None

    def emp_name_and_no(emp_id: int) -> tuple[Optional[str], Optional[str]]:
        emp = envision_get_employee(token, int(emp_id))
        return format_employee_name(emp), (emp.get("employeeNo") or "").strip().upper() or None

    # 1) Any Captain role
    for c in crew:
        if c.get("crewPositionId") in pic_pos and c.get("employeeId"):
            return emp_name_and_no(c["employeeId"])

    # 2) Pilot flying among pilots
    for c in crew:
        if c.get("isPilotFlying") and c.get("crewPositionId") in pilot_pos and c.get("employeeId"):
            return emp_name_and_no(c["employeeId"])

    # 3) First pilot by displayOrder
    pilots = [c for c in crew if c.get("crewPositionId") in pilot_pos and c.get("employeeId")]
    if pilots:
        pilots.sort(key=lambda x: (x.get("displayOrder") or 0))
        return emp_name_and_no(pilots[0]["employeeId"])

    return None, None

def _load_cache() -> dict:
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_cache(cache: dict) -> None:
    tmp = CACHE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, CACHE_FILE)

"Deprecated"
def watch_loop():
    """
    Repeatedly run one sync pass, then sleep.
    - Respects SYNC_INTERVAL_SEC (default 300s).
    - Uses a simple backoff on errors.
    - Recomputes time window every pass.
    - Reuses on-disk idempotency cache so only changes get pushed.
    """
    interval = int(os.getenv("SYNC_INTERVAL_SEC", "300"))  # 5 min default
    max_backoff = int(os.getenv("SYNC_MAX_BACKOFF_SEC", "1800"))  # 30 min
    jitter = int(os.getenv("SYNC_JITTER_SEC", "10"))  # +/- 10s to avoid thundering herd

    # graceful shutdown with Ctrl+C or service stop
    _stop = {"flag": False}
    def _sig_handler(signum, frame):
        logging.info(f"Received signal {signum}. Stopping after current pass…")
        _stop["flag"] = True
    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    backoff = 0
    while not _stop["flag"]:
        started = datetime.now()
        try:
            main()   # your existing single-pass sync
            backoff = 0  # reset backoff on success
        except Exception:
            logging.exception("Uncaught error in sync pass")
            backoff = min(max(30, int(backoff * 2) or 60), max_backoff)

        if _stop["flag"]:
            break

        # sleep until next run
        base = interval if backoff == 0 else backoff
        # add a tiny random jitter so multiple instances don’t align (optional)
        try:
            import random
            sleep_for = max(1, base + random.randint(-jitter, jitter))
        except Exception:
            sleep_for = base

        elapsed = (datetime.now() - started).total_seconds()
        # never sleep negative
        sleep_for = max(1, int(sleep_for - elapsed)) if sleep_for > elapsed else 1

        logging.info(f"Next sync in ~{sleep_for}s (interval={interval}s, backoff={backoff}s)")
        time.sleep(sleep_for)
# --- Adapter for Flask GUI/API ---
def run_sync_once_return_summary() -> dict:
    """
    Calls main() and returns a simple summary dict for the GUI/history.
    We lightly parse the final log line; if parsing fails we still return a best-effort result.
    """
    global SYNC_EVENTS
    SYNC_EVENTS = []

    # compute the exact window main() uses
    try:
        # use the same helper + env vars
        local_tz = _get_local_tz()
        now_local = datetime.now(local_tz)
        date_from_local = now_local - timedelta(hours=WINDOW_PAST_HOURS)
        date_to_local   = now_local + timedelta(hours=WINDOW_FUTURE_HOURS)
        date_from_utc = date_from_local.astimezone(timezone.utc)
        date_to_utc   = date_to_local.astimezone(timezone.utc)
    except Exception:
        # safe fallback
        date_from_local = date_to_local = date_from_utc = date_to_utc = None
    created = skipped = warnings_total = None
    try:
        # monkey-patch logging to capture the final line
        import io, contextlib, re
        buf = io.StringIO()
        handler = logging.StreamHandler(buf)
        root = logging.getLogger()
        root.addHandler(handler)
        try:
            main()
        finally:
            root.removeHandler(handler)
        text = buf.getvalue()
        # Look for "Done. Created/updated: X, skipped: Y, APG warnings: Z"
        m = re.search(r"Created/updated:\s*(\d+),\s*skipped:\s*(\d+),\s*APG warnings:\s*(\d+)", text)
        if m:
            created = int(m.group(1))
            skipped = int(m.group(2))
            warnings_total = int(m.group(3))
        return {
            "ok": True,
            "created": created,
            "skipped": skipped,
            "warnings": warnings_total,
            "log_tail": text[-4000:],
            "flights": SYNC_EVENTS,
            "window_from_local": date_from_local,
            "window_to_local": date_to_local,
            "window_from_utc": date_from_utc,
            "window_to_utc": date_to_utc,
        }

    except Exception as e:
        logging.exception("run_sync_once_return_summary failed")
        return {
            "ok": False,
            "error": str(e),
            "flights": SYNC_EVENTS,
            "window_from_local": date_from_local,
            "window_to_local": date_to_local,
            "window_from_utc": date_from_utc,
            "window_to_utc": date_to_utc,
        }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Envision → APG sync")
    parser.add_argument("--once", action="store_true", help="Run a single sync pass and exit")
    parser.add_argument("--watch", action="store_true", help="Run forever, syncing on a schedule")
    args = parser.parse_args()

    # default to --once if neither flag is provided
    if args.watch:
        watch_loop()
    else:
        main()

