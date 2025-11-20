import os
import sys
import json
import logging
import re
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
}

# Optional overrides from .env (comma-separated IDs). If unset we auto-discover from /Crews/Positions.
ENV_PIC_POS_OVRD = os.getenv("ENVISION_PIC_POSITION_IDS", "").strip()
ENV_PILOT_POS_OVRD = os.getenv("ENVISION_PILOT_POSITION_IDS", "").strip()

# Defaults if not provided by Envision (tune to your ops)
DEFAULT_RULES = os.getenv("DEFAULT_RULES", "I")      # I (IFR) / V (VFR)
DEFAULT_FTYPE = os.getenv("DEFAULT_FTYPE", "S")      # S=Scheduled, G=General, etc.
DEFAULT_ROUTE = os.getenv("DEFAULT_ROUTE", "DCT")    # Put real routing if available
DEFAULT_FL    = os.getenv("DEFAULT_FL", "300")       # FL as string, e.g. "300"

# --- PIC resolution caches (add near other globals) ---
_EMP_CACHE: dict[int, dict] = {}
_CREW_POS_CACHE: list[dict] = []
_PIC_POS_IDS: set[int] = set()
_PILOT_POS_IDS: set[int] = set()

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
    "VAV": "NFTV",  # Woodbourne
    "TBU": "NFTF",  # Woodbourne
    "HAP": "NFTL",  # Woodbourne
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

def _fmt_local(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(_get_local_tz()).strftime("%d-%m-%y %H:%M")

def _get_local_tz():
    tzname = os.getenv("LOCAL_TZ", "Pacific/Auckland")
    if tzname == "New Zealand Standard Time":
        tzname = "Pacific/Auckland"
    if ZoneInfo:
        try:
            return ZoneInfo(tzname)
        except ZoneInfoNotFoundError:
            pass
    try:
        from dateutil.tz import gettz
        tz = gettz(tzname)
        if tz:
            return tz
    except Exception:
        pass
    logging.warning("Time zone '%s' not found; falling back to UTC. Install 'tzdata' to fix.", tzname)
    return timezone.utc

LOCAL_TZ = _get_local_tz()

def _load_cache() -> dict:
    """Load idempotency cache from disk. Returns an empty dict on any error."""
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        logging.warning("Cache load failed; starting with empty cache.", exc_info=True)
        return {}

def _save_cache(cache: dict) -> None:
    """Atomically persist the cache to disk."""
    try:
        tmp = CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, CACHE_FILE)
    except Exception:
        logging.warning("Cache save failed; cache not updated on disk.", exc_info=True)

def envision_get_crew_positions(token: str) -> list[dict]:
    """GET /v1/Crews/Positions (cached)."""
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

def _core_from(payload: dict, pic_name: str|None, apg_pic_id: int|None) -> dict:
    crew = payload.get("crew") or {}
    return {
        "eobt_key": _canon_eobt_to_utc_min_str(payload.get("eobt")),
        "pic_name": (pic_name or None),
        "pic_id": apg_pic_id or None,
        # NEW: make FO/TIC part of the fingerprint
        "fo_id": crew.get("fo_id") or None,
        "tic_id": crew.get("tic_id") or None,
    }




def _read_cached_core(cache_entry) -> tuple[Optional[dict], Optional[str]]:
    """
    Back-compat reader:
      - old cache: str fingerprint
      - new cache: {"fp": <sha1>, "core": {...}}
    Returns (old_core_dict_or_None, old_fp_or_None).
    """
    if isinstance(cache_entry, dict):
        return cache_entry.get("core"), cache_entry.get("fp")
    if isinstance(cache_entry, str):
        return None, cache_entry
    return None, None


def _describe_changes(old_core: Optional[dict], new_core: dict) -> Optional[str]:
    if old_core is None:
        return None

    changes: list[str] = []

    # EOBT diff (show local HH:MM)
    old_k, new_k = old_core.get("eobt_key"), new_core.get("eobt_key")
    if old_k != new_k:
        def _fmt(k: Optional[str]) -> str:
            if not k:
                return "—"
            dt = datetime.strptime(k, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc).astimezone(_get_local_tz())
            return dt.strftime("%H:%M")
        changes.append(f"EOBT {_fmt(old_k)}→{_fmt(new_k)}")

    # PIC name
    if (old_core.get("pic_name") or "") != (new_core.get("pic_name") or ""):
        changes.append(f"PIC {(old_core.get('pic_name') or '—')}→{(new_core.get('pic_name') or '—')}")

    # NEW: FO (prefer names; fall back to ids)
    if (old_core.get("fo_name") or old_core.get("fo_id")) != (new_core.get("fo_name") or new_core.get("fo_id")):
        old_fo = (old_core.get("fo_name") or old_core.get("fo_id") or "—")
        new_fo = (new_core.get("fo_name") or new_core.get("fo_id") or "—")
        changes.append(f"FO {old_fo}→{new_fo}")

    # NEW: TIC (prefer names; fall back to ids)
    if (old_core.get("tic_name") or old_core.get("tic_id")) != (new_core.get("tic_name") or new_core.get("tic_id")):
        old_tic = (old_core.get("tic_name") or old_core.get("tic_id") or "—")
        new_tic = (new_core.get("tic_name") or new_core.get("tic_id") or "—")
        changes.append(f"TIC {old_tic}→{new_tic}")

    return "; ".join(changes) if changes else None



def build_pic_pilot_position_sets(token: str) -> tuple[set[int], set[int]]:
    """
    Build sets of position IDs:
      - PIC positions (isCaptain == True)
      - Pilot positions (isCaptain or isFirstOfficer)
    Overridable via ENVISION_PIC_POSITION_IDS / ENVISION_PILOT_POSITION_IDS.
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

    # Safety fallback if API empty
    if not _PIC_POS_IDS:
        _PIC_POS_IDS = {71, 1072, 1074, 1075}  # CPT, C/T CPT, LT CPT, S CPT
    if not _PILOT_POS_IDS:
        _PILOT_POS_IDS = _PIC_POS_IDS | {75, 1073}  # + FO, LT FO

    return _PIC_POS_IDS, _PILOT_POS_IDS

def envision_get_flight_crew(token: str, flight_id: int) -> list[dict]:
    """GET /v1/Flights/{flightId}/Crew."""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{ENVISION_BASE}/Flights/{flight_id}/Crew"
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    data = r.json() or []
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected /Crew response for flight {flight_id}: {data}")
    return data

def envision_get_employee(token: str, employee_id: int) -> dict:
    """GET /v1/Employees/{employeeId} (cached)."""
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

def resolve_pic_for_flight(token: str, flight: dict) -> tuple[Optional[str], Optional[str]]:
    """
    Returns (pic_name, pic_employee_no).
    Priority:
      1) Any crew in a Captain position
      2) Pilot Flying among pilot positions
      3) First pilot by displayOrder
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

    # 1) Explicit Captain role
    for c in crew:
        if c.get("crewPositionId") in pic_pos and c.get("employeeId"):
            return emp_name_and_no(c["employeeId"])

    # 2) Pilot Flying among pilots
    for c in crew:
        if c.get("isPilotFlying") and c.get("crewPositionId") in pilot_pos and c.get("employeeId"):
            return emp_name_and_no(c["employeeId"])

    # 3) First pilot by displayOrder
    pilots = [c for c in crew if c.get("crewPositionId") in pilot_pos and c.get("employeeId")]
    if pilots:
        pilots.sort(key=lambda x: (x.get("displayOrder") or 0))
        return emp_name_and_no(pilots[0]["employeeId"])

    return None, None

def resolve_fo_for_flight(token: str, flight: dict) -> tuple[Optional[str], Optional[str]]:
    """
    Returns (fo_name, fo_employee_no).
    Priority:
      1) Explicit First Officer role
      2) Among pilot positions, the first non-captain with lowest displayOrder
    """
    fid = flight.get("id")
    if not fid:
        return None, None

    try:
        pic_pos, pilot_pos = build_pic_pilot_position_sets(token)
        crew = envision_get_flight_crew(token, int(fid))
    except Exception as e:
        logging.warning(f"Could not resolve FO for flight {fid}: {e}")
        return None, None

    def emp_name_and_no(emp_id: int) -> tuple[Optional[str], Optional[str]]:
        emp = envision_get_employee(token, int(emp_id))
        return format_employee_name(emp), (emp.get("employeeNo") or "").strip().upper() or None

    # 1) Explicit FO roles: positions that are pilot but not captain
    for c in crew:
        pid = c.get("crewPositionId")
        if pid in pilot_pos and pid not in (pic_pos or set()) and c.get("employeeId"):
            return emp_name_and_no(c["employeeId"])

    # 2) Fallback: any pilot not in captain set, lowest displayOrder
    fo_candidates = [c for c in crew if c.get("crewPositionId") in pilot_pos and c.get("crewPositionId") not in (pic_pos or set()) and c.get("employeeId")]
    if fo_candidates:
        fo_candidates.sort(key=lambda x: (x.get("displayOrder") or 0))
        return emp_name_and_no(fo_candidates[0]["employeeId"])

    return None, None


def resolve_cabincrew_for_flight(token: str, flight: dict) -> list[tuple[Optional[str], Optional[str]]]:
    """
    Returns list of (name, employee_no) for all cabin crew on the flight.
    We treat any crew not in pilot_pos as 'cabin' (adjust if you have explicit flags).
    """
    fid = flight.get("id")
    if not fid:
        return []

    try:
        _, pilot_pos = build_pic_pilot_position_sets(token)
        crew = envision_get_flight_crew(token, int(fid))
    except Exception as e:
        logging.warning(f"Could not resolve Cabin Crew for flight {fid}: {e}")
        return []

    out: list[tuple[Optional[str], Optional[str]]] = []

    for c in crew:
        pid = c.get("crewPositionId")
        emp_id = c.get("employeeId")
        if not emp_id:
            continue
        # Non-pilot positions -> cabin crew
        if pid not in (pilot_pos or set()):
            emp = envision_get_employee(token, int(emp_id))
            name = format_employee_name(emp)
            eno = (emp.get("employeeNo") or "").strip().upper() or None
            out.append((name, eno))

    # Stable order for UI
    return sorted(out, key=lambda t: (t[0] or "", t[1] or ""))


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
        return "CVA" + s[2:]
    return s

def aircraft_id_for_reg(reg: str) -> Optional[int]:
    if not reg:
        return None
    key = (reg or "").strip().upper().replace(" ", "")
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
        eobt=kw.get("eobt"),
        eobt_str=kw.get("eobt_str"),
        std=kw.get("std"),
        std_str=kw.get("std_str"),
        etd=kw.get("etd"),
        etd_str=kw.get("etd_str"),
        reg=kw.get("reg"),
        aircraft_id=kw.get("aircraft_id"),
        # PIC
        pic_name=kw.get("pic_name"),
        pic_empno=kw.get("pic_empno"),
        apg_pic_id=kw.get("apg_pic_id"),
        # FO
        fo_name=kw.get("fo_name"),
        fo_empno=kw.get("fo_empno"),
        apg_fo_id=kw.get("apg_fo_id"),
        # Cabin Crew (comma-separated strings)
        cc_names=kw.get("cc_names"),
        cc_empnos=kw.get("cc_empnos"),
        apg_cc_ids=kw.get("apg_cc_ids"),
        # Outcome
        result=kw.get("result"),
        reason=kw.get("reason"),
        warnings=kw.get("warnings"),
    )
    SYNC_EVENTS.append(rec)



def ask_refresh_popup() -> bool:
    if _HAS_TK and os.getenv("USE_POPUP", "1") not in ("0", "false", "False"):
        root = _tk.Tk()
        root.withdraw()
        try:
            return _mb.askyesno("Envision → APG", "Refresh flights now?")
        finally:
            root.destroy()
    ans = input("Refresh flights now? [Y/n]: ").strip().lower()
    return ans in ("", "y", "yes")

def _norm_reg(reg: str) -> str:
    return (reg or "").strip().upper().replace(" ", "")

def build_aircraft_index_from_apg(aircraft: list[dict]) -> dict[str, list[dict]]:
    by_reg: dict[str, list[dict]] = {}
    for a in aircraft or []:
        reg = a.get("registration") or a.get("reg") or a.get("tail") or a.get("callsign")
        if not reg:
            continue
        n = _norm_reg(reg)
        no_dash = n.replace("-", "")
        with_dash = no_dash if "-" in n else (no_dash[:2] + "-" + no_dash[2:] if len(no_dash) >= 3 else no_dash)
        for key in {no_dash, with_dash}:
            by_reg.setdefault(key, []).append(a)
    return by_reg

FREIGHT_KEYWORDS = {"freight", "freight charter", "intl freight"}

def is_freight_flight(envision_flight: dict) -> bool:
    desc = (envision_flight.get("flightTypeDescription") or "").strip().lower()
    return any(k in desc for k in FREIGHT_KEYWORDS)

def _plan_id_from_row(row: dict) -> Optional[int]:
    for k in ("id", "plan_id", "planId"):
        if k in row and row[k] is not None:
            try:
                return int(row[k])
            except Exception:
                pass
    return None

def _plan_key_from_apg_row(row: dict) -> Optional[tuple[str, str, str, Optional[str]]]:
    flight_no = normalize_flight_no((row.get("flight_no") or row.get("flightNo") or row.get("callsign") or "").strip())
    adep = (row.get("adep") or row.get("dep") or row.get("from") or row.get("origin") or "").strip().upper()
    ades = (row.get("ades") or row.get("dest") or row.get("to") or row.get("destination") or "").strip().upper()
    # Try multiple EOBT-ish fields
    eobt_raw = (row.get("eobt") or row.get("off_block_time") or row.get("off_block_time_utc")
                or row.get("etd") or row.get("std"))
    eobt_key = _canon_eobt_to_utc_min_str(eobt_raw)
    if not (flight_no and adep and ades and eobt_key):
        return None
    return (flight_no, adep, ades, eobt_key)

def build_existing_plan_index(
    bearer: str,
    window_from_utc: datetime,
    window_to_utc: datetime
) -> dict[tuple[str, str, str, Optional[str]], Optional[int]]:
    statuses = [s.strip() for s in os.getenv("APG_EXIST_STATUSES", "draft,planned,active,filed").split(",") if s.strip()]
    index: dict[tuple[str, str, str, Optional[str]], Optional[int]] = {}
    seen = kept = 0

    for st in statuses:
        try:
            plans = apg_get_plan_list(bearer, status=st, page_size=200, after=None)
        except Exception as e:
            logging.warning(f"APG plan list fetch failed for status='{st}': {e}")
            continue
        for p in plans:
            seen += 1
            key = _plan_key_from_apg_row(p)
            if not key:
                continue
            # decode eobt back to dt for window filter
            try:
                dt = datetime.strptime(key[3], "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc) if key[3] else None
            except Exception:
                dt = None
            if not dt or not (window_from_utc <= dt <= window_to_utc):
                continue
            pid = _plan_id_from_row(p)
            index[key] = pid
            kept += 1

    logging.info(f"APG presence across statuses {','.join(statuses)} → scanned {seen}, kept {kept} within window")
    return index


def choose_apg_aircraft_id_for_flight(envision_flight: dict) -> Optional[int]:
    reg_raw = (envision_flight.get("flightRegistrationDescription") or "")
    if not reg_raw:
        return None
    reg_keys = {_norm_reg(reg_raw).replace("-", "")}
    n = next(iter(reg_keys))
    if len(n) >= 3:
        reg_keys.add(n[:2] + "-" + n[2:])
    rows: list[dict] = []
    for k in reg_keys:
        rows.extend(_APG_BY_REG.get(k, []))
    if not rows:
        return None
    wants_freight = is_freight_flight(envision_flight)
    def ref_is_frghtr(r: dict) -> bool:
        return (r.get("reference") or "").strip().upper() == "FRGHTR"
    prefer = [r for r in rows if ref_is_frghtr(r)] if wants_freight else [r for r in rows if not ref_is_frghtr(r)]
    if prefer:
        try:
            return int(prefer[0].get("id"))
        except Exception:
            pass
    try:
        return int(rows[0].get("id"))
    except Exception:
        return None

def _coerce_utc(dtish: Optional[Union[str, datetime]]) -> Optional[datetime]:
    """Parse an ISO string or datetime into an aware UTC datetime."""
    if not dtish:
        return None
    if isinstance(dtish, datetime):
        dt = dtish
    else:
        s = str(dtish).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# ---------------------------
# APG: plan list (simple)
# ---------------------------
def apg_get_plan_list(bearer: str, status: Optional[str] = None, page_size: int = 200, after: Optional[str] = None) -> list[dict]:
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

    out: list[dict] = []
    while True:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            rows = [p for p in data if isinstance(p, dict)]
        else:
            d = (data or {}).get("data")
            if isinstance(d, list):
                rows = d
            elif isinstance(d, dict):
                rows = next((v for v in d.values() if isinstance(v, list)), [])
            else:
                rows = []
            rows = [p for p in rows if isinstance(p, dict)]
        out.extend(rows)
        if len(rows) < page_size:
            break
        payload["page"] += 1
    return out

# ===========================
# Envision API
# ===========================
def envision_authenticate() -> Dict[str, str]:
    base = ENVISION_BASE.rstrip("/")
    if base.endswith("/v1"):
        auth_url = f"{base}/Authenticate"
    else:
        auth_url = f"{base}/v1/Authenticate"

    # 👇 add this line
    logging.info("Authenticating to Envision… base=%s auth_url=%s", base, auth_url)

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
        headers["X-Tenant-Id"] = tenant
        # (optional) show tenant without duplicating the whole header
        logging.info("Envision tenant header set: %s", tenant)

    try:
        r = requests.post(auth_url, json=payload, headers=headers, timeout=30)
        if r.status_code == 401:
            raise RuntimeError(f"Envision auth 401. URL={auth_url} Body={r.text}")
        r.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Envision auth failed. URL={auth_url} Error={e}")

    data = r.json()
    token = data.get("token")
    if not token:
        raise RuntimeError(f"Envision auth response missing token. Body={r.text}")
    return {"token": token, "refreshToken": data.get("refreshToken")}


def envision_get_flights(token: str, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
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
        if "flydev.rocketroute.com" in u:
            return u.replace("flydev.rocketroute.com", "fly.rocketroute.com")
        if "fly.rocketroute.com" in u:
            return u.replace("fly.rocketroute.com", "flydev.rocketroute.com")
        return u

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
            continue

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

    raise RuntimeError(
        "APG login failed across all hosts/versions. "
        "Most likely causes: invalid/missing AppKey, wrong environment (prod vs dev), user not API-activated, or proxy stripping Authorization. "
        f"Last error: {last_error}"
    )

def apg_refresh(refresh_token: str) -> Dict[str, str]:
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
    url = f"{APG_BASE}/plan/edit"
    headers = apg_headers(bearer)

    # 🔹 Debug payload when APG_DEBUG_PAYLOAD is enabled
    if os.getenv("APG_DEBUG_PAYLOAD", "0").lower() in ("1", "true", "yes"):
        try:
            logging.info(
                "[APG] plan/edit payload:\n%s",
                json.dumps(plan_payload, ensure_ascii=False, default=str, indent=2)[:4000],
            )
        except Exception:
            logging.info("[APG] plan/edit payload (repr): %r", plan_payload)

    r = requests.post(url, headers=headers, json=plan_payload, timeout=60)
    r.raise_for_status()
    data = r.json() or {}
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

    # Prefer latest estimate if present; otherwise use scheduled
    eobt_raw = f.get("departureEstimate") or f.get("departureScheduled")
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
def main(
    date_from_utc: Optional[Union[str, datetime]] = None,
    date_to_utc: Optional[Union[str, datetime]] = None,
):
    """
    One sync pass: Envision → APG.

    If date_from_utc/date_to_utc are given (UTC ISO or datetime), use that exact window.
    Otherwise use env-driven rolling window and drop past flights.
    """
    import json as _json
    import hashlib, requests, os, logging
    from datetime import datetime, timedelta, timezone
    from typing import Optional, Union

    # ---------- tiny helpers (local to this function) ----------
    def _build_core(payload: dict, pic_name: str|None, apg_pic_id: int|None, pic_code: str|None,
                    fo_name: str|None = None, tic_name: str|None = None) -> dict:
        crew = payload.get("crew") or {}
        return {
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
            "pic_code": (pic_code or "").strip().upper() or None,
            # NEW
            "fo_id": crew.get("fo_id") or None,
            "tic_id": crew.get("tic_id") or None,
            "fo_name": (fo_name or "").strip() or None,
            "tic_name": (tic_name or "").strip() or None,
        }


    def _fingerprint(core: dict) -> str:
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

    def _apg_plan_delete(bearer: str, plan_id: int) -> dict:
        """
        Delete a plan in APG by id.
        """
        url = f"{APG_BASE.rstrip('/')}/plan/delete"
        headers = {
            "Authorization": bearer,
            "X-API-Version": APG_API_VERSION,
            "Content-Type": "application/json",
            "User-Agent": "AirChathams-Bridge/1.0",
        }
        r = requests.post(url, headers=headers, json={"id": int(plan_id)}, timeout=30)
        r.raise_for_status()
        data = r.json() or {}
        status = data.get("status", {})
        if not status.get("success", False):
            raise RuntimeError(f"APG delete failed: {status.get('message', 'unknown')}")
        return data

    # ---------- sanity checks ----------
    missing = []
    if not ENVISION_USER or not ENVISION_PASS or "<envision-host>" in (ENVISION_BASE or ""):
        missing.append("Envision (ENVISION_BASE/ENVISION_USER/ENVISION_PASS)")
    if not APG_APP_KEY or not APG_EMAIL or not APG_PASSWORD:
        missing.append("APG (APG_APP_KEY/APG_EMAIL/APG_PASSWORD)")
    if missing:
        logging.error(f"Missing config: {', '.join(missing)}")
        raise RuntimeError("Configuration incomplete")

    # ---------- window (supports overrides) ----------
    dfu = _coerce_utc(date_from_utc)
    dtu = _coerce_utc(date_to_utc)

    if dfu and dtu:
        window_from_utc = dfu
        window_to_utc = dtu
        window_from_local = window_from_utc.astimezone(_get_local_tz())
        window_to_local = window_to_utc.astimezone(_get_local_tz())
        apply_past_filter = False
    else:
        _, window_from_local, window_to_local, window_from_utc, window_to_utc = _window_now()
        apply_past_filter = True

    logging.info(
        "Fetching Envision flights (local NZ) %s → %s | (UTC) %s → %s",
        window_from_local.isoformat(), window_to_local.isoformat(),
        window_from_utc.isoformat(), window_to_utc.isoformat()
    )

    # ---------- cache ----------
    cache = _load_cache()  # fid -> {"fp": "...", "core": {...}, "apg_id": int, "key": (...) } OR legacy "fp" string

    # ---------- Envision ----------
    logging.info("Authenticating to Envision…")
    env_auth = envision_authenticate()
    env_token = env_auth["token"]

    flights = envision_get_flights(env_token, window_from_utc, window_to_utc)
    logging.info(f"Envision flights fetched: {len(flights)}")

    # Exact in-window filter by EOBT (UTC)
    def _eobt_utc(f):
        for k in ("departureScheduled", "departureEstimate"):
            s = f.get(k)
            if s:
                dt = parse_iso(s)
                if dt:
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.astimezone(timezone.utc)
        return None

    pre_count = len(flights)
    flights = [f for f in flights if (e := _eobt_utc(f)) and (window_from_utc <= e <= window_to_utc)]
    logging.info("After in-window filter: kept %d of %d", len(flights), pre_count)

    if apply_past_filter:
        leeway = int(os.getenv("PAST_LEEWAY_MIN", "0"))
        now_utc = datetime.now(timezone.utc)
        cutoff = now_utc - timedelta(minutes=leeway)

        pre = len(flights)
        # use your existing helper that returns a tz-aware UTC datetime
        flights = [f for f in flights if (_dt := _eobt_utc(f)) and _dt >= cutoff]

        logging.info(
            "After past-filter (cutoff %s): kept %d of %d",
            cutoff.isoformat(), len(flights), pre
        )

    # Optional testing cap
    test_limit_env = os.getenv("SYNC_TEST_LIMIT", "").strip()
    if test_limit_env:
        try:
            tlim = int(test_limit_env)
        except Exception:
            tlim = 0
    else:
        tlim = int(os.getenv("LEGACY_TEST_LIMIT", "20"))
    if tlim > 0:
        flights = flights[:tlim]
        logging.info(f"Limiting to first {len(flights)} flights for testing")

    # ---------- APG auth & lookups (always do this, even if Envision has 0 flights -> for reconciliation) ----------
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

    # Presence index (key → plan_id or None) within window
    try:
        existing_index = build_existing_plan_index(apg_bearer, window_from_utc, window_to_utc)
    except Exception as e:
        logging.warning(f"Could not build presence index from APG: {e}")
        existing_index = {}

    # --- Fallback: widen presence index if some /plan/list status calls failed ---
    try:
        if os.getenv("APG_RECON_WIDEN", "1").lower() in ("1", "true", "yes"):
            plans_all = apg_get_plan_list(apg_bearer, page_size=200, after=None)  # no status filter
            for p in plans_all:
                k = _plan_key_from_apg_row(p)
                pid = _plan_id_from_row(p)
                if not (k and pid):
                    continue
                # window filter again
                try:
                    ekey = k[3]
                    dt = datetime.strptime(ekey, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc) if ekey else None
                except Exception:
                    dt = None
                if not dt or not (window_from_utc <= dt <= window_to_utc):
                    continue
                if k not in existing_index or existing_index.get(k) is None:
                    existing_index[k] = pid
    except Exception as e:
        logging.warning("Presence index widen failed: %s", e)

    # also index by (flight_no, ADEP, ADES) to ignore EOBT when matching
    existing_index_by3: dict[tuple[str, str, str], Optional[int]] = {}
    for k, v in existing_index.items():
        if k:
            existing_index_by3[(k[0], k[1], k[2])] = v

    # --- Ignore APG plans we previously failed to delete with 'Forbidden' ---
    ignore_ids = {
        e.get("apg_id")
        for e in cache.values()
        if isinstance(e, dict) and e.get("apg_delete_forbidden") and e.get("apg_id")
    }
    if ignore_ids:
        removed = 0
        for k, pid in list(existing_index.items()):
            if pid in ignore_ids:
                existing_index.pop(k, None)
                if k:
                    existing_index_by3.pop((k[0], k[1], k[2]), None)
                removed += 1
        if removed:
            logging.info("Presence index: ignoring %d APG plans flagged delete-forbidden to allow recreate.", removed)

    # --- Reconcile deletes: Envision -> APG (runs regardless of how many flights we fetched) ---
    env_ids: set[str] = {str(f.get("id") or "") for f in flights}

    recon_scanned = recon_cand = recon_inwin = recon_noid = recon_deleted = 0

    for fid, entry in list(cache.items()):
        recon_scanned += 1

        # only consider rows our sync created (dict form with metadata)
        if not isinstance(entry, dict):
            continue

        # if Envision still returned this flight id this pass, keep it
        if fid in env_ids:
            continue

        apg_id = entry.get("apg_id")
        key    = entry.get("key")  # tuple: (flight_no, ADEP, ADES, eobt_key 'YYYY-MM-DDTHH:MMZ')

        # need at least a key (to check window) or a plan id to attempt delete
        if not key and not apg_id:
            continue

        recon_cand += 1

        # keep deletes inside the current run window
        in_window = False
        dt_for_event = None
        if key and len(key) == 4 and key[3]:
            try:
                dt_for_event = datetime.strptime(key[3], "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)
                in_window = (window_from_utc <= dt_for_event <= window_to_utc)
            except Exception:
                in_window = False

        if not in_window:
            continue
        recon_inwin += 1

        # find a live APG plan id to delete: presence index first, then cached apg_id
        del_id = None
        if key:
            del_id = existing_index.get(tuple(key))
            if del_id is None:
                del_id = existing_index_by3.get((key[0], key[1], key[2]))
        if del_id is None and apg_id:
            del_id = apg_id

        if not del_id:
            recon_noid += 1
            logging.debug("Reconcile skip (no id): fid=%s key=%s apg_id=%s", fid, key, apg_id)
            continue  # nothing visible to delete

        try:
            logging.info("Reconcile deleting APG plan id=%s (fid=%s key=%s)", del_id, fid, key)
            _apg_plan_delete(apg_bearer, int(del_id))
            recon_deleted += 1

            # emit a 'deleted' event for the UI/history
            _emit_flight_event(
                envision_flight_id=fid,
                flight_no=key[0] if key else None,
                adep=key[1] if key else None,
                ades=key[2] if key else None,
                eobt=dt_for_event,
                eobt_str=_fmt_local(dt_for_event),
                std=None, std_str=None,
                etd=None, etd_str=None,
                reg=None,
                aircraft_id=None,
                pic_name=None, pic_empno=None, apg_pic_id=None,
                result="deleted",
                reason="Missing from Envision → removed in APG",
                warnings=None,
            )

            # tidy indexes/cache so we don’t try to act on it again
            if key:
                existing_index.pop(tuple(key), None)
                existing_index_by3.pop((key[0], key[1], key[2]), None)
            entry["apg_id"] = None
            del cache[fid]

        except Exception as ex:
            msg = str(ex)
            if "forbidden" in msg.lower():
                # Mark as undeletable so we stop retrying and won’t let it block creation
                entry["apg_delete_forbidden"] = True
                # Also drop from in-memory presence indices for THIS run
                if key:
                    existing_index.pop(tuple(key), None)
                    existing_index_by3.pop((key[0], key[1], key[2]), None)
                logging.warning(
                    "APG refused delete for plan id=%s (Forbidden). Marked undeletable and ignoring for presence.",
                    del_id,
                )
                _emit_flight_event(
                    envision_flight_id=fid,
                    flight_no=(key[0] if key else None),
                    adep=(key[1] if key else None),
                    ades=(key[2] if key else None),
                    eobt=dt_for_event,
                    eobt_str=_fmt_local(dt_for_event),
                    std=None, std_str=None,
                    etd=None, etd_str=None,
                    reg=None,
                    aircraft_id=None,
                    pic_name=None, pic_empno=None, apg_pic_id=None,
                    result="skipped",
                    reason="APG refused delete (Forbidden); will ignore and allow recreate",
                    warnings=None,
                )
            else:
                logging.exception("Failed to delete APG plan id=%s for missing Envision fid=%s", del_id, fid)
                _emit_flight_event(
                    envision_flight_id=fid,
                    flight_no=(key[0] if key else None),
                    adep=(key[1] if key else None),
                    ades=(key[2] if key else None),
                    eobt=dt_for_event,
                    eobt_str=_fmt_local(dt_for_event),
                    std=None, std_str=None,
                    etd=None, etd_str=None,
                    reg=None,
                    aircraft_id=None,
                    pic_name=None, pic_empno=None, apg_pic_id=None,
                    result="failed",
                    reason="Tried to delete orphaned APG plan but failed",
                    warnings=msg[:400],
                )

    logging.info("Reconcile summary: scanned=%d candidates=%d in_window=%d no_del_id=%d deleted=%d",
                 recon_scanned, recon_cand, recon_inwin, recon_noid, recon_deleted)

    REQUIRE_PIC_IN_APG = os.getenv("REQUIRE_PIC_IN_APG", "false").lower() in ("1", "true", "yes")
    created, skipped, warnings_total = 0, 0, 0

    # ---------- If no flights to process, finish after reconciliation ----------
    if not flights:
        _save_cache(cache)
        logging.info(f"Done. Created/updated: {created}, skipped: {skipped}, APG warnings: {warnings_total}")
        return {
            "created": created,
            "skipped": skipped,
            "warnings_total": warnings_total,
            "warnings": warnings_total,
            "window_from_local": window_from_local,
            "window_to_local": window_to_local,
            "window_from_utc": window_from_utc,
            "window_to_utc": window_to_utc,
        }

    # ---------- process each (remaining) flight ----------
    for f in flights:
        # Resolve PIC
        pic_name, pic_empno = resolve_pic_for_flight(env_token, f)
        fo_name, fo_empno   = resolve_fo_for_flight(env_token, f)
        cc_list             = resolve_cabincrew_for_flight(env_token, f)  # returns [(name, empno), ...]

        # Compute timings up-front
        std_dt = parse_iso(f.get("departureScheduled") or "")
        etd_dt = parse_iso(f.get("departureEstimate") or "")
        effective_eobt = etd_dt or std_dt

        # Choose APG aircraft
        aircraft_id = choose_apg_aircraft_id_for_flight(f)
        if not aircraft_id:
            reg_raw = (f.get("flightRegistrationDescription") or "").strip() or None
            logging.warning(f"Skip flight {f.get('id')} — no APG aircraft match for reg {reg_raw}")

            # Try to locate the corresponding APG plan and DELETE it
            fid = str(f.get("id") or "")
            key = (
                normalize_flight_no(f.get("flightNumberDescription")),
                (to_icao(f.get("departurePlaceDescription")) or "").strip().upper(),
                (to_icao(f.get("arrivalPlaceDescription")) or "").strip().upper(),
                _canon_eobt_to_utc_min_str((effective_eobt.isoformat() if effective_eobt else None)),
            )

            del_id = None
            if key and existing_index.get(key) is not None:
                del_id = existing_index[key]
            if del_id is None and key and existing_index_by3.get((key[0], key[1], key[2])) is not None:
                del_id = existing_index_by3[(key[0], key[1], key[2])]
            if del_id is None:
                prev_entry = cache.get(fid)
                if isinstance(prev_entry, dict):
                    del_id = prev_entry.get("apg_id")

            if del_id:
                try:
                    _apg_plan_delete(apg_bearer, int(del_id))
                    _emit_flight_event(
                        envision_flight_id=f.get("id"),
                        flight_no=normalize_flight_no(f.get("flightNumberDescription")),
                        adep=to_icao(f.get("departurePlaceDescription")),
                        ades=to_icao(f.get("arrivalPlaceDescription")),
                        eobt=effective_eobt,
                        eobt_str=_fmt_local(effective_eobt),
                        std=std_dt,  std_str=_fmt_local(std_dt),
                        etd=etd_dt,  etd_str=_fmt_local(etd_dt),
                        reg=reg_raw,
                        aircraft_id=None,
                        pic_name=None, pic_empno=None, apg_pic_id=None,
                        result="deleted",
                        reason="No registration in Envision → removed from APG",
                        warnings=None,
                    )
                    if key:
                        existing_index.pop(key, None)
                        existing_index_by3.pop((key[0], key[1], key[2]), None)
                    if isinstance(cache.get(fid), dict):
                        cache[fid]["apg_id"] = None
                except Exception as ex:
                    logging.exception("Failed to delete APG plan id=%s for flight %s", del_id, fid)
                    _emit_flight_event(
                        envision_flight_id=f.get("id"),
                        flight_no=normalize_flight_no(f.get("flightNumberDescription")),
                        adep=to_icao(f.get("departurePlaceDescription")),
                        ades=to_icao(f.get("arrivalPlaceDescription")),
                        eobt=effective_eobt,
                        eobt_str=_fmt_local(effective_eobt),
                        std=std_dt,  std_str=_fmt_local(std_dt),
                        etd=etd_dt,  etd_str=_fmt_local(etd_dt),
                        reg=reg_raw,
                        aircraft_id=None,
                        pic_name=None, pic_empno=None, apg_pic_id=None,
                        result="failed",
                        reason="Tried to delete APG plan but failed",
                        warnings=str(ex)[:400],
                    )

            skipped += 1
            continue

        payload = envision_to_apg_plan(f, aircraft_id=aircraft_id, pic_name=pic_name)
        if not payload:
            skipped += 1
            continue

        base_evt = dict(
            envision_flight_id=f.get("id"),
            flight_no=normalize_flight_no(f.get("flightNumberDescription")),
            adep=to_icao(f.get("departurePlaceDescription")),
            ades=to_icao(f.get("arrivalPlaceDescription")),
            eobt=effective_eobt,
            eobt_str=_fmt_local(effective_eobt),
            std=std_dt,  std_str=_fmt_local(std_dt),
            etd=etd_dt,  etd_str=_fmt_local(etd_dt),
            reg=(f.get("flightRegistrationDescription") or "").strip().upper(),
            aircraft_id=(payload or {}).get("aircraft_id"),
            pic_name=pic_name,
            pic_empno=pic_empno,
            apg_pic_id=None,
        )

        # Attach APG crew IDs if available
        apg_pic_id = None
        if pic_empno:
            pic_empno = pic_empno.strip().upper()
            apg_pic_id = crewcode_to_id.get(pic_empno)

        # Start/merge crew block
        crew_block = payload.get("crew", {"pic_id": 0, "fo_id": 0, "tic_id": 0})

        if apg_pic_id:
            crew_block["pic_id"] = apg_pic_id
            base_evt["apg_pic_id"] = apg_pic_id
        else:
            if REQUIRE_PIC_IN_APG and pic_empno:
                logging.warning(f"Skipping flight {f.get('id')} — PIC employeeNo {pic_empno} not found in APG crew.")
                skipped += 1
                _emit_flight_event(
                    **base_evt,
                    result="skipped",
                    reason="PIC not found in APG and REQUIRE_PIC_IN_APG=true",
                    warnings=None,
                )
                continue
            if pic_name and pic_empno:
                logging.warning(
                    f"No APG crew match for PIC employeeNo={pic_empno} (flight {f.get('id')}). Proceeding without crew linkage."
                )

        # --- FIRST OFFICER (FO) ---
        apg_fo_id = None
        if fo_empno:
            fo_empno = fo_empno.strip().upper()
            apg_fo_id = crewcode_to_id.get(fo_empno)
        if apg_fo_id:
            crew_block["fo_id"] = apg_fo_id
            base_evt["apg_fo_id"] = apg_fo_id
        else:
            if fo_name and fo_empno:
                logging.warning(
                    f"No APG crew match for FO employeeNo={fo_empno} (flight {f.get('id')}). Proceeding without FO linkage."
                )

        # --- CABIN CREW (CC) ---
        apg_cc_ids = []
        if cc_list:
            for cc_name, cc_empno in cc_list:
                cc_empno_norm = (cc_empno or "").strip().upper()
                cid = crewcode_to_id.get(cc_empno_norm) if cc_empno_norm else None
                apg_cc_ids.append(cid)

        # Choose TIC (APG supports one) = first mapped CC, else 0
        tic_id = next((cid for cid in apg_cc_ids if cid), 0)
        if tic_id:
            crew_block["tic_id"] = tic_id

        # Persist FO context into event/logs (for readable diffs)
        if fo_name is not None:
            base_evt["fo_name"] = fo_name
        if fo_empno is not None:
            base_evt["fo_empno"] = fo_empno

        # Choose a human-friendly TIC name for diffs
        tic_name = None
        if cc_list:
            if tic_id:
                for (ccn, cce) in cc_list:
                    if crewcode_to_id.get((cce or "").strip().upper()) == tic_id:
                        tic_name = ccn
                        break
            if not tic_name:
                tic_name = cc_list[0][0]

        # Persist for the UI/logs
        if tic_name is not None:
            base_evt["tic_name"] = tic_name

        # Finalize merged crew block
        payload["crew"] = crew_block


        # --- prior run state / cache ---
        fid = str(f.get("id") or "")
        prev_core = None
        prev_fp = None
        prev_apg_id = None
        prev_entry = cache.get(fid)
        if isinstance(prev_entry, dict):
            prev_core = prev_entry.get("core")
            prev_fp = prev_entry.get("fp")
            prev_apg_id = prev_entry.get("apg_id")
        elif isinstance(prev_entry, str):
            prev_fp = prev_entry  # legacy: just fp

        # current state
        new_core = _core_from(payload, pic_name, apg_pic_id)
        # Ensure FO/TIC are present in new_core even if _core_from() predates FO/TIC
        new_core["fo_id"] = crew_block.get("fo_id") or None
        new_core["tic_id"] = crew_block.get("tic_id") or None
        # (optional nicety for diffs)
        new_core["fo_name"] = base_evt.get("fo_name")
        new_core["tic_name"] = base_evt.get("tic_name")

        core = _build_core(payload, pic_name, apg_pic_id, None)  # PIC code unknown -> None
        # Ensure FO/TIC affect fingerprint & extra diff
        core["fo_id"] = crew_block.get("fo_id") or None
        core["tic_id"] = crew_block.get("tic_id") or None
        core["fo_name"] = base_evt.get("fo_name")
        core["tic_name"] = base_evt.get("tic_name")

        fp = _fingerprint(core)

        # human-readable diff (EOBT + PIC + FO/TIC)
        change_reason = _describe_changes(prev_core, new_core)

        # small formatter for “extra” diffs (PIC code / AC / route / FL / FO / TIC)
        def _format_changes(old, new):
            if not old:
                return None
            def _v(x): return x if (x is not None and x != "") else "—"
            changes = []
            if old.get("pic_name") != new.get("pic_name"):
                changes.append(f"PIC: {_v(old.get('pic_name'))} → {_v(new.get('pic_name'))}")
            if old.get("pic_code") != new.get("pic_code"):
                changes.append(f"PIC Code: {_v(old.get('pic_code'))} → {_v(new.get('pic_code'))}")
            if old.get("aircraft_id") != new.get("aircraft_id"):
                changes.append(f"AC ID: {_v(old.get('aircraft_id'))} → {_v(new.get('aircraft_id'))}")
            if old.get("route") != new.get("route"):
                changes.append(f"Route: {_v(old.get('route'))} → {_v(new.get('route'))}")
            if old.get("fl") != new.get("fl"):
                changes.append(f"FL: {_v(old.get('fl'))} → {_v(new.get('fl'))}")
            # NEW: FO/TIC diffs (prefer names; fall back to ids)
            if (old.get("fo_name") or old.get("fo_id")) != (new.get("fo_name") or new.get("fo_id")):
                changes.append(f"FO: {_v(old.get('fo_name') or old.get('fo_id'))} → {_v(new.get('fo_name') or new.get('fo_id'))}")
            if (old.get("tic_name") or old.get("tic_id")) != (new.get("tic_name") or new.get("tic_id")):
                changes.append(f"TIC: {_v(old.get('tic_name') or old.get('tic_id'))} → {_v(new.get('tic_name') or new.get('tic_id'))}")
            return "; ".join(changes) if changes else None

        # inner push with 401-refresh and special error handling
        def _push(_payload: dict, event_result: str, reason_text: Optional[str]) -> Optional[dict]:
            nonlocal apg_bearer, apg_refresh_token, warnings_total, created, skipped, cache, core
            try:
                res = apg_plan_edit(apg_bearer, _payload)

            except RuntimeError as e:
                msg = str(e).lower()

                # Access denied on update → drop id and retry as create
                if "access denied" in msg:
                    if _payload.get("id") is not None:
                        bad_id = _payload.get("id")
                        logging.error("APG plan/edit denied on update id=%s — retrying as CREATE (drop id)…", bad_id)
                        create_payload = dict(_payload); create_payload.pop("id", None)
                        try:
                            res = apg_plan_edit(apg_bearer, create_payload)
                            reason_text = (reason_text + "; " if reason_text else "") + "APG access denied on update → recreated"
                            _payload = create_payload
                        except Exception:
                            skipped += 1
                            _emit_flight_event(**base_evt, result="failed",
                                               reason="Access denied on update; retry as create failed",
                                               warnings=None)
                            logging.exception("Retry as create failed after access denied")
                            return None
                    else:
                        skipped += 1
                        _emit_flight_event(**base_evt, result="failed", reason="Access denied", warnings=None)
                        logging.error("APG plan/edit denied on create — access denied")
                        return None

                # Invalid PIC id → retry once without crew
                elif "invalid pic_id" in msg or "pic id" in msg:
                    bad_id = None
                    try:
                        bad_id = (_payload.get("crew") or {}).get("pic_id")
                    except Exception:
                        pass
                    logging.warning("APG rejected PIC id=%s; retrying without crew linkage…", bad_id)
                    clean_payload = dict(_payload); clean_payload.pop("crew", None)
                    core = dict(core); core["pic_id"] = None
                    try:
                        res = apg_plan_edit(apg_bearer, clean_payload)
                        reason_text = (reason_text + "; " if reason_text else "") + "APG rejected pic_id → retried without crew link"
                        _payload = clean_payload
                    except Exception:
                        skipped += 1
                        _emit_flight_event(**base_evt, result="failed",
                                           reason="Invalid pic_id and retry without crew failed",
                                           warnings=None)
                        logging.exception("Retry without crew failed")
                        return None
                else:
                    skipped += 1
                    _emit_flight_event(**base_evt, result="failed", reason=str(e), warnings=None)
                    logging.exception("Error pushing flight %s to APG", f.get("id"))
                    return None

            except requests.HTTPError as http_err:
                code = http_err.response.status_code if http_err.response is not None else None
                body = http_err.response.text[:500] if (http_err.response and http_err.response.text) else ""
                if code == 401 and apg_refresh_token:
                    logging.info("APG 401 — refreshing token and retrying once…")
                    tokens = apg_refresh(apg_refresh_token)
                    apg_bearer = tokens["authorization"]
                    apg_refresh_token = tokens.get("refresh_token", apg_refresh_token)
                    res = apg_plan_edit(apg_bearer, _payload)
                elif code == 403 and _payload.get("id") is not None:
                    bad_id = _payload.get("id")
                    logging.error("APG HTTP 403 on update id=%s — retrying as CREATE (drop id)…", bad_id)
                    create_payload = dict(_payload); create_payload.pop("id", None)
                    try:
                        res = apg_plan_edit(apg_bearer, create_payload)
                        reason_text = (reason_text + "; " if reason_text else "") + "APG 403 on update → recreated"
                        _payload = create_payload
                    except Exception:
                        skipped += 1
                        _emit_flight_event(**base_evt, result="failed",
                                           reason="HTTP 403 on update; retry as create failed",
                                           warnings=body or None)
                        logging.exception("HTTP 403 retry as create failed")
                        return None
                else:
                    skipped += 1
                    _emit_flight_event(**base_evt, result="failed",
                                       reason=f"HTTP {code if code is not None else ''}",
                                       warnings=body or None)
                    logging.exception("HTTP error pushing flight %s to APG", f.get("id"))
                    return None

            # success path
            status = res.get("status", {}) if isinstance(res, dict) else {}
            warns = status.get("warnings")
            if warns:
                warnings_total += len(warns)
                logging.warning("APG warnings for Envision flight %s: %s", f.get("id"), warns)

            created += 1  # count updates together with creates

            try:
                ret_id = _plan_id_from_row(res.get("data", {}) if isinstance(res, dict) else {})
            except Exception:
                ret_id = None

            new_key = _plan_key_from_payload(_payload)
            cache[str(fid)] = {
                "fp": _fingerprint(core),
                "core": core,
                "apg_id": ret_id if ret_id is not None else (_payload.get("id") if isinstance(_payload, dict) else None),
                "key": new_key,
            }
            if new_key:
                existing_index[new_key] = cache[str(fid)]["apg_id"]
                existing_index_by3[(new_key[0], new_key[1], new_key[2])] = cache[str(fid)]["apg_id"]

            _emit_flight_event(**base_evt,
                               result=event_result,
                               reason="; ".join([t for t in [change_reason, _format_changes(prev_core, core)] if t]) or "changed",
                               warnings=_json.dumps(warns) if warns else None)
            return res

        # ---------- identity & existence ----------
        key = (
            normalize_flight_no(payload["flight_no"]),
            (payload["adep"] or "").strip().upper(),
            (payload["ades"] or "").strip().upper(),
            _canon_eobt_to_utc_min_str(payload.get("eobt")),
        )

        # Presence (what APG tells us is there *now*), ignoring cache
        plan_id_presence = None
        if key in existing_index and existing_index[key] is not None:
            plan_id_presence = existing_index[key]
        elif (key[0], key[1], key[2]) in existing_index_by3 and existing_index_by3[(key[0], key[1], key[2])] is not None:
            plan_id_presence = existing_index_by3[(key[0], key[1], key[2])]

        # Cached mapping from last successful push (may be stale if plan was deleted manually)
        plan_id_cache = prev_apg_id

        # Visible *now* means only what presence index says (not cache)
        visible_now = plan_id_presence is not None

        # Pick an id to try updating with:
        #  - prefer cache id (it's the original plan we created) so we keep continuity if it still exists
        #  - else use presence id
        plan_id_to_update = plan_id_cache if plan_id_cache is not None else plan_id_presence

        # ---------- decide: skip / update / create ----------
        # Only skip if nothing changed *and* APG confirms the plan is visible now.
        if visible_now and prev_fp and prev_fp == fp:
            skipped += 1
            logging.info(f"Skip flight {fid} — plan confirmed visible in APG and no changes since last sync")
            _emit_flight_event(**base_evt, result="skipped", reason="no changes since last sync", warnings=None)
            continue

        if plan_id_to_update is not None:
            payload["id"] = int(plan_id_to_update)
            reason_text = "; ".join([t for t in [change_reason, _format_changes(prev_core, core)] if t]) or "changed"
            res = _push(payload, event_result="updated", reason_text=reason_text)
            if res is not None:
                log_add = f" Changes: {reason_text}" if reason_text else ""
                logging.info(f"APG plan updated for flight {f.get('id')} ({payload['flight_no']}).{log_add}")
        else:
            payload.pop("id", None)
            res = _push(payload, event_result="created", reason_text=None)
            if res is not None:
                logging.info(f"APG plan created for flight {f.get('id')} ({payload['flight_no']})")

    _save_cache(cache)
    logging.info(f"Done. Created/updated: {created}, skipped: {skipped}, APG warnings: {warnings_total}")

    return {
        "created": created,
        "skipped": skipped,
        "warnings_total": warnings_total,
        "warnings": warnings_total,  # alias for UI
        "window_from_local": window_from_local,
        "window_to_local": window_to_local,
        "window_from_utc": window_from_utc,
        "window_to_utc": window_to_utc,
    }

# ---------------------------
# APG aircraft & crew helpers
# ---------------------------
def apg_get_aircraft_list(bearer: str) -> list[dict]:
    """
    Fetch aircraft from APG and return a list of dicts.
    Tries both /aircraft/list and /data/aircraft/list.
    """
    headers = {
        "Authorization": bearer,
        "X-API-Version": APG_API_VERSION,
        "Content-Type": "application/json",
        "User-Agent": "AirChathams-Bridge/1.0",
    }
    bases = [APG_BASE.rstrip("/")]
    paths = ["/aircraft/list", "/data/aircraft/list"]

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
                # {"status":{"success":true},"data":[...]}  OR  [...]
                if isinstance(data, dict) and "data" in data:
                    return data.get("data") or []
                if isinstance(data, list):
                    return data
            except Exception as e:
                last_err = f"{url} -> {e}"
                continue
    raise RuntimeError(f"Could not fetch aircraft list from APG. Last error: {last_err}")


def apg_get_crew_list(bearer: str) -> list[dict]:
    """
    /crew/list (usually POST, some tenants allow GET).
    Returns list of crew rows (id, crew_code, ...).
    """
    url = f"{APG_BASE.rstrip('/')}/crew/list"
    headers = {
        "Authorization": bearer,
        "X-API-Version": APG_API_VERSION,
        "Content-Type": "application/json",
        "User-Agent": "AirChathams-Bridge/1.0",
    }
    r = requests.post(url, headers=headers, json={}, timeout=30)
    if not r.ok:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
    else:
        r.raise_for_status()

    data = r.json()
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
        cid = c.get("id")
        if code and cid:
            try:
                mapping[code] = int(cid)
            except Exception:
                pass
    return mapping

def _first(d: dict, *keys: str):
    """Return first present/non-empty value from possible keys."""
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def _canon_eobt_to_utc_min_str(dt_or_str: Optional[Union[str, int, float, datetime]]) -> Optional[str]:
    """
    Canonical UTC minute key 'YYYY-MM-DDTHH:MMZ'.
    - Accepts ISO strings, epoch seconds, or datetime.
    - If naive, assume LOCAL_TZ (APG list often returns local times).
    """
    if dt_or_str is None:
        return None
    try:
        if isinstance(dt_or_str, (int, float)):
            dt = datetime.fromtimestamp(float(dt_or_str), tz=timezone.utc)
        elif isinstance(dt_or_str, str):
            s = dt_or_str.strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(s)
            except ValueError:
                return None
        else:
            dt = dt_or_str

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_get_local_tz())  # <-- assume local NZ for APG list rows
        dt = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
        return dt.strftime("%Y-%m-%dT%H:%MZ")
    except Exception:
        return None



def _plan_key_from_apg_row(row: dict) -> Optional[tuple[str, str, str, Optional[str]]]:
    """
    Extracts the matching key tuple from an APG plan row:
      (flight_no_norm, ADEP, ADES, EOBT_key)
    """
    flight_no = normalize_flight_no(_first(row, "flight_no", "flightNo", "callsign") or "")
    adep = (_first(row, "adep", "dep", "from", "origin") or "").strip().upper()
    ades = (_first(row, "ades", "dest", "to", "destination") or "").strip().upper()
    eobt_raw = _first(row, "eobt", "off_block_time", "etd", "std")
    eobt_key = _canon_eobt_to_utc_min_str(eobt_raw if isinstance(eobt_raw, (str, datetime)) else None)

    if not flight_no or not adep or not ades or not eobt_key:
        return None
    return (flight_no, adep, ades, eobt_key)


def _plan_key_from_payload(payload: dict) -> Optional[tuple[str, str, str, Optional[str]]]:
    return (
        normalize_flight_no(payload.get("flight_no")),
        (payload.get("adep") or "").strip().upper(),
        (payload.get("ades") or "").strip().upper(),
        _canon_eobt_to_utc_min_str(payload.get("eobt")),
    )


def build_existing_plan_keyset(bearer: str,
                               window_from_utc: datetime,
                               window_to_utc: datetime) -> set[tuple[str, str, str, Optional[str]]]:
    """
    Get a set of plan keys currently in APG and within our window.
    We do NOT care about status; we only dedupe by identity.
    """
    plans = apg_get_plan_list(bearer, page_size=200, after=None)
    existing: set[tuple[str, str, str, Optional[str]]] = set()
    kept = 0

    for p in plans:
        key = _plan_key_from_apg_row(p)
        if not key:
            continue
        # Filter to our time window by decoding EOBT key back to dt
        eobt_key = key[3]
        try:
            dt = datetime.strptime(eobt_key, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc) if eobt_key else None
        except Exception:
            dt = None
        if dt and window_from_utc <= dt <= window_to_utc:
            existing.add(key)
            kept += 1

    logging.info(f"APG plan list fetched: {len(plans)} rows (kept {kept} within window)")
    return existing

# --- Adapter for Flask GUI/API (unchanged, but keep for completeness) ---
def run_sync_once_return_summary(
    date_from_utc: Optional[Union[str, datetime]] = None,
    date_to_utc: Optional[Union[str, datetime]] = None,
) -> dict:
    """
    Calls main() and returns a simple summary dict for the GUI/history.
    Accepts optional UTC overrides (ISO string or datetime) for the window.
    """
    global SYNC_EVENTS
    SYNC_EVENTS = []
    logging.info(f"Manual override received: from={date_from_utc} to={date_to_utc}")

    # Compute the same window main() will use, for metadata in error cases
    try:
        dfu = _coerce_utc(date_from_utc)
        dtu = _coerce_utc(date_to_utc)
        if dfu and dtu:
            window_from_utc = dfu
            window_to_utc = dtu
            window_from_local = window_from_utc.astimezone(_get_local_tz())
            window_to_local = window_to_utc.astimezone(_get_local_tz())
        else:
            local_tz = _get_local_tz()
            now_local = datetime.now(local_tz)
            window_from_local = now_local - timedelta(hours=WINDOW_PAST_HOURS)
            window_to_local   = now_local + timedelta(hours=WINDOW_FUTURE_HOURS)
            window_from_utc = window_from_local.astimezone(timezone.utc)
            window_to_utc   = window_to_local.astimezone(timezone.utc)
    except Exception:
        window_from_local = window_to_local = window_from_utc = window_to_utc = None

    created = skipped = warnings_total = None
    try:
        import io, re
        buf = io.StringIO()
        handler = logging.StreamHandler(buf)
        root = logging.getLogger()
        root.addHandler(handler)
        try:
            summary = main(date_from_utc=date_from_utc, date_to_utc=date_to_utc)
        finally:
            root.removeHandler(handler)

        # If main returned a proper summary, just return it augmented with log tail
        text = buf.getvalue()
        if isinstance(summary, dict):
            summary["ok"] = True
            summary["log_tail"] = text[-4000:]
            summary["flights"] = SYNC_EVENTS
            return summary

        # Fallback: parse counts from log
        m = re.search(r"Created/updated:\s*(\d+),\s*skipped:\s*(\d+),\s*APG warnings:\s*(\d+)", text)
        if m:
            created = int(m.group(1)); skipped = int(m.group(2)); warnings_total = int(m.group(3))
        return {
            "ok": True,
            "created": created,
            "skipped": skipped,
            "warnings": warnings_total,
            "log_tail": text[-4000:],
            "flights": SYNC_EVENTS,
            "window_from_local": window_from_local,
            "window_to_local": window_to_local,
            "window_from_utc": window_from_utc,
            "window_to_utc": window_to_utc,
        }

    except Exception as e:
        logging.exception("run_sync_once_return_summary failed")
        return {
            "ok": False,
            "error": str(e),
            "flights": SYNC_EVENTS,
            "window_from_local": window_from_local,
            "window_to_local": window_to_local,
            "window_from_utc": window_from_utc,
            "window_to_utc": window_to_utc,
        }

# ---------------------------
# APG: plan get + pax update
# ---------------------------

def apg_plan_get(bearer: str, plan_id: int) -> dict:
    """
    Fetch a single APG plan by id.
    Docs show GET with JSON body, but POST works consistently
    (same pattern as /plan/list and /plan/edit).
    """
    url = f"{APG_BASE.rstrip('/')}/plan/get"
    headers = {
        "Authorization": bearer,
        "X-API-Version": APG_API_VERSION,
        "Content-Type": "application/json",
        "User-Agent": "AirChathams-Bridge/1.0",
    }
    payload = {"id": int(plan_id)}
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json() or {}
    status = data.get("status", {})
    if not status.get("success", False):
        raise RuntimeError(f"APG plan/get error: {status.get('message', 'unknown')}")
    return data.get("data") or {}

def build_pax_payload_for_plan(
    plan: dict,
    pax_ad: int,
    pax_chd: int,
    pax_inf: int,
    bags_kg: float,
) -> dict:
    """
    Build a /plan/edit payload that:
      * preserves existing plan structure
      * updates baggage mass in massAndBalance

    NOTE:
    We NO LONGER add custom keys like PAX_ADT / PAX_CHD / PAX_INF in field19,
    because APG rejects unknown item names.
    """

    pax_ad   = int(pax_ad or 0)
    pax_chd  = int(pax_chd or 0)
    pax_inf  = int(pax_inf or 0)
    bags_kg  = float(bags_kg or 0.0)
    # pax_total = pax_ad + pax_chd + pax_inf  # not used anymore

    payload: dict = {
        "id": int(plan.get("id") or 0),
    }

    # --- field19: keep EXACTLY what APG already has ---
    orig_f19 = plan.get("field19") or {}
    if orig_f19:
        payload["field19"] = dict(orig_f19)

    # --- massAndBalance: only touch the baggage station ---
    mb = plan.get("massAndBalance") or {}
    loading = mb.get("loading") or []
    new_loading = []

    for st in loading:
        st = dict(st)  # shallow copy
        label = (st.get("label") or "").strip().lower()
        if label == "baggage":
            cl = dict(st.get("customLoad") or {})
            cl["mass"] = bags_kg  # total checked baggage in kg
            st["customLoad"] = cl
        new_loading.append(st)

    if new_loading:
        payload["massAndBalance"] = {
            "loading": new_loading,
            "fuelMass": mb.get("fuelMass", 0),
        }

    return payload


def apg_plan_get_details(bearer: str, plan_id: int) -> dict:
    return apg_plan_get(bearer, plan_id)

def update_apg_plan_from_dcs_row(
    bearer: str,
    plan_id: int,
    dcs_flight: dict,
    preview_only: bool = False,
) -> dict:
    """
    Update an APG plan's massAndBalance.loading[] from a single Zenith DCS flight.

    - Pull the current plan via apg_plan_get()
    - Apply DCS passengers -> 'Passenger {Seat}' rows (mass + pob_count)
    - Optionally update 'Baggage' station mass from Passenger.BaggageWeight
    - Log a detailed DEBUG block BEFORE calling plan/edit so we can see exactly
      what is being sent into APG.

    If preview_only=True:
      -> returns {"payload": <edit_payload>, "debug": <summary>} and DOES NOT
         call apg_plan_edit.

    If preview_only=False:
      -> logs the same debug and then calls apg_plan_edit(), returning the
         raw APG response.
    """
    logger = logging.getLogger(__name__)

    # --- 1) Get current plan from APG ---
    plan = apg_plan_get(bearer, plan_id)

    mb = plan.get("massAndBalance") or {}
    loading = mb.get("loading") or []

    # shallow copies so we don't mutate the original APG response object
    loading = [dict(st) for st in loading]

    # --- 2) Apply DCS passengers onto APG passenger rows ---
    apply_dcs_passengers_to_apg_rows(loading, dcs_flight)

    # --- 3) Optional baggage update from DCS ---
    total_bags_kg = 0.0
    for p in (dcs_flight or {}).get("Passengers") or []:
        try:
            total_bags_kg += float(p.get("BaggageWeight") or 0)
        except (TypeError, ValueError):
            pass

    if total_bags_kg:
        for st in loading:
            label = (st.get("label") or "").strip().lower()
            if label == "baggage":
                cl = st.setdefault("customLoad", {})
                cl["mass"] = total_bags_kg
                cl.setdefault("volume", 0)
                cl.setdefault("pob_count", 0)
                break

    # --- 4) Build the minimal plan/edit payload ---
    edit_payload: Dict[str, Any] = {
        "id": int(plan_id),
        "massAndBalance": {
            "loading": loading,
            "fuelMass": mb.get("fuelMass", 0),
        },
    }

    # --- 5) Build a debug summary so we can see what happened ---

    # 5a) DCS pax summary (with proper names)
    dcs_pax_summary: list[dict] = []
    raw_pax = (dcs_flight or {}).get("Passengers") or []

    for p in raw_pax:
        seat = (p.get("Seat") or "").strip().upper()

        full_name = " ".join(
            x
            for x in [
                (p.get("NamePrefix") or "").strip(),
                (p.get("GivenName") or "").strip(),
                (p.get("Surname") or "").strip(),
            ]
            if x
        ) or None

        dcs_pax_summary.append(
            {
                "Seat": seat or None,
                "PassengerType": p.get("PassengerType"),
                "BaggageWeight": p.get("BaggageWeight"),
                "Name": full_name,
            }
        )

    # 5b) Build seat->name map for APG pax debug view
    seat_to_name: dict[str, str] = {}
    for p in dcs_pax_summary:
        seat_code = (p.get("Seat") or "").strip().upper()
        pax_name = (p.get("Name") or "").strip()
        if seat_code and pax_name and seat_code not in seat_to_name:
            seat_to_name[seat_code] = pax_name

    # 5c) APG pax rows (after we’ve applied DCS load)
    apg_pax_rows: list[dict] = []
    for st in loading:
        label = (st.get("label") or "").strip()
        if not label.startswith("Passenger "):
            continue

        cl = st.get("customLoad") or {}

        try:
            seat_code = label.split(" ", 1)[1].strip().upper()
        except IndexError:
            seat_code = ""

        apg_pax_rows.append(
            {
                "label": label,
                "mass": cl.get("mass"),
                "pob_count": cl.get("pob_count"),
                "name": seat_to_name.get(seat_code),
            }
        )

    # 5d) TEMP: log a sample raw DCS pax so we can inspect fields
    if raw_pax:
        try:
            logger.info(
                "[APG] Sample DCS passenger raw: %s",
                json.dumps(raw_pax[0], ensure_ascii=False, default=str, indent=2),
            )
        except Exception:
            logger.info(
                "[APG] Sample DCS passenger keys: %s",
                sorted(raw_pax[0].keys()),
            )

    debug_summary = {
        "plan_id": int(plan_id),
        "fuelMass": mb.get("fuelMass"),
        "apg_pax_rows": apg_pax_rows,
        "dcs_pax": dcs_pax_summary,
    }

    # --- 6) Log payload + summary BEFORE we call plan/edit ---
    try:
        logger.info(
            "[APG] About to plan/edit (preview_only=%s) plan_id=%s\n"
            "Payload (truncated): %s\n"
            "Summary (truncated): %s",
            preview_only,
            plan_id,
            json.dumps(edit_payload, ensure_ascii=False, default=str, indent=2)[:4000],
            json.dumps(debug_summary, ensure_ascii=False, default=str, indent=2)[:4000],
        )
    except Exception as e:
        logger.warning(
            "[APG] Failed to serialise debug payload for logging: %r", e
        )

    # --- 7) Preview-only mode: return payload + summary, no API call ---
    if preview_only:
        return {
            "payload": edit_payload,
            "debug": debug_summary,
        }

    # --- 8) Real call to APG plan/edit ---
    resp = apg_plan_edit(bearer, edit_payload)
    return resp

def apg_get_plan_list(bearer: str, status: Optional[str] = None, page_size: int = 200, after: Optional[str] = None) -> list[dict]:
    """
    POST /api/plan/list

    Handles response shapes:
      - {"status": {...}, "data": {"dataset": "...", "plans": [...]} }
      - {"status": {...}, "data": [...]}
      - [ ... ]  # bare list
    """
    url = f"{APG_BASE.rstrip('/')}/plan/list"
    headers = {
        "Authorization": bearer,
        "X-API-Version": APG_API_VERSION,
        "Content-Type": "application/json",
        "User-Agent": "AirChathams-Bridge/1.0",
    }
    payload: dict[str, Any] = {"page": 1, "page_size": page_size, "is_template": 0}
    if status:
        payload["status"] = status
    if after:
        payload["after"] = after

    out: list[dict] = []

    while True:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()

        # Normalise to a list of plan dicts
        rows: list[dict] = []
        if isinstance(data, list):
            rows = [p for p in data if isinstance(p, dict)]
        else:
            d = (data or {}).get("data")
            if isinstance(d, list):
                rows = [p for p in d if isinstance(p, dict)]
            elif isinstance(d, dict):
                # New format: {"dataset": "...", "plans": [...]}
                plans = d.get("plans")
                if isinstance(plans, list):
                    rows = [p for p in plans if isinstance(p, dict)]
                else:
                    # Fallback: first list-valued field
                    rows = next((v for v in d.values() if isinstance(v, list)), [])
                    rows = [p for p in rows if isinstance(p, dict)]
            else:
                rows = []

        out.extend(rows)
        if len(rows) < page_size:
            break
        payload["page"] += 1

    return out

def _find_apg_plan_id_for_row(
    row: dict,
    existing_index: dict[tuple[str, str, str, Optional[str]], Optional[int]],
    existing_index_by3: dict[tuple[str, str, str], Optional[int]],
) -> Optional[int]:
    """
    Given one DCS row and the APG presence indexes, return the matching plan_id (or None).

    Key is (flight_no_norm, ADEP, ADES, EOBT_key).
    """
    # Flight number -> APG-normalised
    raw_flight = (row.get("flight") or row.get("Flight") or "").strip()
    flight_no = normalize_flight_no(raw_flight)
    if not flight_no:
        return None

    # ADEP (ICAO)
    adep_icao = to_icao(row.get("dep") or row.get("Dep"))
    if not adep_icao:
        return None

    # ADES (ICAO)
    dest_raw = (
        row.get("dest") or row.get("Dest") or
        row.get("ades") or row.get("arr") or row.get("Arr")
    )
    ades_icao = to_icao(dest_raw)
    if not ades_icao:
        return None

    # EOBT key from STD in UTC
    std_utc = _std_to_utc_from_row(row)
    if not std_utc:
        return None
    eobt_key = _canon_eobt_to_utc_min_str(std_utc)
    if not eobt_key:
        return None

    key = (flight_no, adep_icao, ades_icao, eobt_key)

    # Exact match
    pid = existing_index.get(key)
    if pid is not None:
        return pid

    # Fallback: ignore EOBT, match only on (flight_no, ADEP, ADES)
    pid = existing_index_by3.get((key[0], key[1], key[2]))
    return pid

def attach_apg_presence_to_rows(
    rows: list[dict],
    window_from_utc: datetime,
    window_to_utc: datetime,
) -> None:
    """
    For each row in 'rows', add:
      - row["apg_plan_id"]  -> int | None
      - row["apg_has_plan"] -> bool

    Uses the same APG presence logic as the Envision→APG sync.
    """
    # 1) Login to APG
    apg_auth = apg_login(APG_EMAIL, APG_PASSWORD)
    apg_bearer = apg_auth["authorization"]

    # 2) Build presence index in the same window you show on the page
    existing_index = build_existing_plan_index(
        apg_bearer,
        window_from_utc=window_from_utc,
        window_to_utc=window_to_utc,
    )

    # Extra index ignoring EOBT for slight timing mismatches
    existing_index_by3: dict[tuple[str, str, str], Optional[int]] = {}
    for k, v in existing_index.items():
        if k:
            existing_index_by3[(k[0], k[1], k[2])] = v

    # 3) Attach APG plan ids
    for r in rows:
        plan_id = _find_apg_plan_id_for_row(r, existing_index, existing_index_by3)
        r["apg_plan_id"] = plan_id
        r["apg_has_plan"] = bool(plan_id)

def _std_to_utc_from_row(row: dict) -> Optional[datetime]:
    """
    Extract STD from the row and convert to UTC.

    Looks for keys in this order: std_dt, std_utc, std.
    Accepts datetime or ISO string "YYYY-MM-DD HH:MM[:SS]".
    """
    raw = row.get("std_dt") or row.get("std_utc") or row.get("std")
    if not raw:
        return None

    if isinstance(raw, datetime):
        dt = raw
    else:
        try:
            # You can tweak if your format is different
            dt = datetime.fromisoformat(str(raw))
        except ValueError:
            return None

    if dt.tzinfo is None:
        # Treat naive as NZ local
        dt = dt.replace(tzinfo=_get_local_tz())

    return dt.astimezone(timezone.utc)

# === Standard passenger weights (kg) =========================================
PAX_STD_WEIGHTS_KG = {
    # Adults
    "AD":     86.0,
    "ADT":    86.0,
    "ADULT":  86.0,
    "A":      86.0,

    # Children
    "CHD":    46.0,
    "CHILD":  46.0,
    "C":      46.0,

    # Infants
    "INF":    15.0,
    "INFANT": 15.0,
}


def _lookup_pax_weight_kg(pax_type: str) -> float:
    """
    Map DCS PassengerType -> standard weight.
    If the type is unknown, fall back to adult weight.
    """
    key = (pax_type or "").strip().upper()
    if key in PAX_STD_WEIGHTS_KG:
        return float(PAX_STD_WEIGHTS_KG[key])
    # Fallback = adult
    return float(PAX_STD_WEIGHTS_KG["AD"])


def _normalise_seat_code(raw_seat: str | None) -> str | None:
    """
    Normalise a DCS seat string into something like '2B', '10A', etc.

    Examples:
      '2B'      -> '2B'
      '02b '    -> '2B'
      ' 10A'    -> '10A'
      '10-A'    -> '10A'
      '3/ C'    -> '3C'

    If the value can't be parsed into <row><letters>, return None.
    """
    if not raw_seat:
        return None

    # Clean up common separators/spaces
    s = str(raw_seat).strip().upper()
    s = s.replace("-", "").replace("/", "").replace(" ", "")
    if not s:
        return None

    # Expect digits + letters, e.g. 10A or 3C
    m = re.match(r"^(\d+)([A-Z]+)$", s)
    if not m:
        return None

    row = str(int(m.group(1)))  # drop leading zeros ('02' -> '2')
    col = m.group(2)
    return row + col


def _get_pax_seat_from_dcs(p: dict) -> str | None:
    """
    Try the various DCS seat fields and normalise:
    - Seat
    - SeatNumber
    - SeatNo
    """
    raw = (
        p.get("Seat")
        or p.get("SeatNumber")
        or p.get("SeatNo")
        or ""
    )
    return _normalise_seat_code(raw)


ADULT_MASS_KG = 86.0
CHILD_MASS_KG = 46.0
INFANT_MASS_KG = 0.0   # on lap → 0 in a seat row

def apply_dcs_passengers_to_apg_rows(
    loading: list[dict],
    dcs_flight: dict,
) -> None:
    """
    Take a Zenith DCS flight (with .Passengers list) and overwrite the APG
    'Passenger {Seat}' rows in `loading` so that they exactly match the DCS
    seat map.

    - Every seat that has a passenger in DCS gets mass + pob_count=1
    - Every seat with no passenger in DCS is zeroed (mass=0, pob_count=0)
    """

    pax_list = (dcs_flight or {}).get("Passengers") or []

    # --- Build a simple seat -> (mass, pob_count) map from Zenith ----
    seat_to_load: dict[str, dict[str, float]] = {}

    for p in pax_list:
        seat = (p.get("Seat") or "").strip().upper()
        if not seat:
            # INF w/out seat or unseated pax – we don't touch APG seat rows
            continue

        ptype = (p.get("PassengerType") or "AD").upper()

        if ptype == "CHD":
            mass = CHILD_MASS_KG
        elif ptype == "INF":
            mass = INFANT_MASS_KG
        else:
            mass = ADULT_MASS_KG

        # if multiple records somehow share a seat, last one wins (that’s
        # also what Zenith is effectively showing in the seat map)
        seat_to_load[seat] = {"mass": float(mass), "pob_count": 1}

    # --- Now push that onto the APG passenger stations ----
    for st in loading:
        label = (st.get("label") or "").strip()
        if not label.startswith("Passenger "):
            continue

        cl = st.setdefault("customLoad", {})

        try:
            seat_code = label.split(" ", 1)[1].strip().upper()
        except IndexError:
            seat_code = ""

        info = seat_to_load.get(seat_code)

        if info:
            cl["mass"] = info["mass"]
            cl["pob_count"] = info["pob_count"]
        else:
            # no DCS pax in that seat
            cl["mass"] = 0.0
            cl["pob_count"] = 0

        # make sure volume is at least present
        cl.setdefault("volume", 0)



def update_apg_plan_from_dcs_flight(
    bearer: str,
    plan_id: int,
    dcs_flight: dict,
) -> dict:
    """
    Update an APG plan's weightAndBalance from a single DCS flight:

      * Per-seat passenger loads:
          - For each APG 'Passenger XX' row, set mass + pob_count based on
            DCS Passengers[] and PAX_STD_WEIGHTS_KG.

      * Baggage:
          - Optionally update the 'Baggage' station mass by summing
            Passenger.BaggageWeight from DCS.

      * We do NOT touch any other massAndBalance fields (e.g. field19) to avoid
        APG rejecting unknown custom keys.
    """
    # 1) Get current plan details from APG
    plan = apg_plan_get(bearer, plan_id)

    mb = plan.get("massAndBalance") or {}
    loading = mb.get("loading") or []

    # Make a shallow copy of each station dict so we don't accidentally mutate
    # some shared object. For nested dicts, we mutate in-place (APG is fine).
    loading = [dict(st) for st in loading]

    # 2) Apply DCS passengers → passenger seat rows
    apply_dcs_passengers_to_apg_rows(loading, dcs_flight)

    # 3) Optional: update baggage mass from DCS
    total_bags_kg = 0.0
    for p in (dcs_flight or {}).get("Passengers") or []:
        try:
            total_bags_kg += float(p.get("BaggageWeight") or 0)
        except (TypeError, ValueError):
            # If any weird values come through, just skip them
            pass

    if total_bags_kg:
        for st in loading:
            label = (st.get("label") or "").strip().lower()
            if label == "baggage":
                cl = st.setdefault("customLoad", {})
                cl["mass"] = total_bags_kg
                # keep volume/pob if present, or default them
                cl.setdefault("volume", 0)
                cl.setdefault("pob_count", 0)
                break

    # 4) Build minimal edit payload (only the bits APG needs)
    edit_payload = {
        "id": int(plan_id),
        "massAndBalance": {
            "loading": loading,
            "fuelMass": mb.get("fuelMass", 0),
        },
    }

    # Optional debug logging – controlled via env var
    if os.getenv("APG_DEBUG_PAYLOAD", "0").lower() in ("1", "true", "yes"):
        try:
            logging.info(
                "[APG] per-seat plan/edit payload:\n%s",
                json.dumps(edit_payload, ensure_ascii=False, default=str, indent=2)[:4000],
            )
        except Exception:
            logging.info("[APG] per-seat plan/edit payload (repr): %r", edit_payload)

    # 5) Push to APG
    return apg_plan_edit(bearer, edit_payload)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Envision → APG sync")
    parser.add_argument("--once", action="store_true", help="Run a single sync pass and exit")
    parser.add_argument("--watch", action="store_true", help="Run forever, syncing on a schedule")
    args = parser.parse_args()

    if args.watch:
        #watch_loop()
    #else:
        main()
