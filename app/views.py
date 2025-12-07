from flask import Blueprint, render_template, request, redirect, jsonify, flash, current_app,send_file, abort, url_for, make_response
from datetime import date, datetime, time, timezone, timedelta
from .models import SyncRun, SyncFlightLog, AppConfig
from . import db
from .zenith_client import fetch_dcs_for_flight
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime as _dt
from datetime import date as _date
import io
from io import BytesIO

import json, requests
import re
from zoneinfo import ZoneInfo
NZ = ZoneInfo("Pacific/Auckland")
# ✅ use your existing Envision helpers

from .sync.envision_apg_sync import (
    envision_authenticate,
    envision_get_flights,
    ENVISION_BASE,               # diagnostics
    attach_apg_presence_to_rows, # APG plan presence
    apg_login,                   #add
    apg_get_plan_list,           #add
    APG_EMAIL,                   #add
    APG_PASSWORD,                #add
    envision_get_flight_times,
    fetch_flights_for_day,       #add
    envision_get_delays
)

# ✅ use your DCS single-flight call
from .zenith_client import fetch_dcs_for_flight

ui_bp = Blueprint("ui", __name__)

@ui_bp.route("/")
@ui_bp.route("/sync/runs")
def sync_runs_page():
    runs = SyncRun.query.order_by(SyncRun.id.desc()).limit(50).all()
    return render_template("sync_runs.html", runs=runs)

def _agg_passengers(passengers: list[dict]) -> dict:
    """Return counts + baggage kg from a DCS Passengers list."""
    if not isinstance(passengers, list):
        return {"pax": 0, "adults": 0, "children": 0, "bags_kg": 0.0}

    def ptype(p): 
        return (p.get("PassengerType") or "").strip().upper()

    def to_num(x):
        try:
            return float(x or 0)
        except (TypeError, ValueError):
            return 0.0

    adults   = sum(1 for p in passengers if ptype(p) in {"ADT", "ADULT", "A"})
    children = sum(1 for p in passengers if ptype(p) in {"CHD", "CHILD", "C", "INF", "INFANT"})
    bags_kg  = sum(to_num(p.get("BaggageWeight")) for p in passengers)

    return {"pax": len(passengers), "adults": adults, "children": children, "bags_kg": bags_kg}


@lru_cache(maxsize=1024)
def _fetch_dcs_cached(flt_no: str, yyyymmdd: str) -> dict | None:
    """
    Cached wrapper around your single-flight DCS client.
    `yyyymmdd` is the NZ-local calendar date string (e.g. '2025-11-03').
    Adjust inside if your DCS client expects a `date` object or ISO string.
    """
    try:
        # If your fetcher wants a date object: day = date.fromisoformat(yyyymmdd)
        # Example below uses string pass-through.
        resp = fetch_dcs_for_flight(flt_no, yyyymmdd)
        return resp or None
    except Exception as e:
        current_app.logger.warning(f"[DCS] fetch failed for {flt_no} {yyyymmdd}: {e}")
        return None

def split_designator_and_number(full_no: str) -> tuple[str | None, str | None]:
    """
    'L8 16'  -> ('L8','16')
    '3C701'  -> ('3C','701')
    'or 123' -> ('OR','123')
    """
    if not full_no:
        return None, None
    s = re.sub(r"\s+", "", str(full_no).upper())
    if len(s) < 3:
        return None, None
    desig = s[:2]
    m = re.search(r"(\d+)$", s[2:])
    if not m:
        return None, None
    return desig, m.group(1)

def _count_pax_types(passengers: list[dict]) -> dict:
    """
    Return {'ad': int, 'chd': int, 'inf': int, 'total': int}
    Robust to variant labels (AD/ADT/ADULT, CHD/CHILD, INF/INFANT).
    """
    if not isinstance(passengers, list):
        return {"ad": 0, "chd": 0, "inf": 0, "total": 0}

    def norm(x: str) -> str:
        return (x or "").strip().upper()

    ADULT_TAGS = {"AD", "ADT", "ADULT", "A"}
    CHILD_TAGS = {"CHD", "CHILD", "C"}
    INFANT_TAGS = {"INF", "INFANT"}

    ad = chd = inf = 0
    for p in passengers:
        t = norm(p.get("PassengerType"))
        if t in ADULT_TAGS:
            ad += 1
        elif t in CHILD_TAGS:
            chd += 1
        elif t in INFANT_TAGS:
            inf += 1
        else:
            # If your DCS reliably uses AD/CHD/INF only, you can ignore this.
            # Optionally treat unknowns as adults:
            # ad += 1
            pass

    return {"ad": ad, "chd": chd, "inf": inf, "total": ad + chd + inf}


def _enrich_rows_with_dcs(rows: list[dict], nz_day: date) -> None:
    if not rows:
        return

    max_workers = int(current_app.config.get("DCS_MAX_WORKERS", 8))
    app_obj = current_app._get_current_object()  # capture the real Flask app

    work: list[tuple[int, str, str, str]] = []
    for i, r in enumerate(rows):
        full_no = (r.get("flight_number") or "").strip().replace(" ", "").upper()
        origin  = (r.get("dep") or "").strip().upper()

        if not full_no or not origin:
            r.update({"error": "Missing flight/origin", "pax_count": 0, "bags_kg": 0.0, "adt": 0, "chd": 0, "inf": 0})
            continue

        desig, number = split_designator_and_number(full_no)
        if not desig or not number:
            r.update({"error": "Bad flight number format", "pax_count": 0, "bags_kg": 0.0, "adt": 0, "chd": 0, "inf": 0})
            continue

        r["designator"] = desig
        r["flight_numeric"] = number
        work.append((i, origin, desig, number))

    if not work:
        return

    def _fetch_one(idx: int, origin: str, desig: str, number: str):
        # Push an app context for this thread
        with app_obj.app_context():
            try:
                dcs = fetch_dcs_for_flight(
                    dep_airport=origin,
                    flight_date=nz_day,
                    airline_designator=desig,
                    flight_number=number,
                    only_status=True,
                )
                return (idx, dcs, None)
            except Exception as e:
                return (idx, None, e)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_fetch_one, *args) for args in work]
        for fut in as_completed(futures):
            idx, dcs, err = fut.result()
            r = rows[idx]

            # always stash raw Zenith response for debugging
            r["dcs_raw"] = dcs

            if err:
                r.update({
                    "error": f"DCS call failed: {err}",
                    "pax_count": 0,
                    "bags_kg": 0.0,
                    "adt": 0,
                    "chd": 0,
                    "inf": 0,
                })
                continue

            flights = (dcs or {}).get("Flights", []) if isinstance(dcs, dict) else (dcs or [])
            if not flights:
                r.update({
                    "error": "No DCS record",
                    "pax_count": 0,
                    "bags_kg": 0.0,
                    "adt": 0,
                    "chd": 0,
                    "inf": 0,
                })
                continue

            pax = flights[0].get("Passengers") or []
            counts = _count_pax_types(pax)
            # ✅ keep full DCS passenger list on the row
            r["pax_list"] = pax

            r["adt"] = counts["ad"]
            r["chd"] = counts["chd"]
            r["inf"] = counts["inf"]
            r["pax_count"] = counts["total"]
            r["pax_breakdown"] = (
                f"AD:{counts['ad']} / CHD:{counts['chd']} / INF:{counts['inf']} "
                f"(Tot:{counts['total']})"
            )

            total_bags = 0.0
            for p in pax:
                try:
                    total_bags += float(p.get("BaggageWeight") or 0)
                except (TypeError, ValueError):
                    pass

            r["bags_kg"] = total_bags
            r["error"] = None

@ui_bp.route("/sync/runs/<int:rid>")
def sync_run_detail(rid):
    r = SyncRun.query.get_or_404(rid)
    flights = SyncFlightLog.query.filter_by(sync_run_id=rid).order_by(SyncFlightLog.id.asc()).all()
    return render_template("sync_run_detail.html", r=r, flights=flights)

@ui_bp.route("/settings", methods=["GET", "POST"])
def settings_page():
    cfg = AppConfig.query.get(1)
    if request.method == "POST":
        auto_enabled = bool(request.form.get("auto_enabled"))
        interval_sec = int(request.form.get("interval_sec") or 300)
        if interval_sec < 60:
            interval_sec = 60
        if not cfg:
            cfg = AppConfig(id=1)
        cfg.auto_enabled = auto_enabled
        cfg.interval_sec = interval_sec
        db.session.add(cfg); db.session.commit()
        # API also reschedules; but you can reschedule here if desired.
        return redirect("/settings")
    return render_template("settings.html", cfg=cfg)

def _infer_designator(fnum: str) -> str | None:
    if not fnum:
        return None
    s = str(fnum).strip().upper()
    # take leading letters/digits until the first digit/letter boundary
    # common airline number formats: AB123, 3C220, L815, TB1751, OR1234
    i = 0
    while i < len(s) and s[i].isalnum() and (i == 0 or s[i-1].isalpha() == s[i].isalpha()):
        i += 1
        # stop once the next char flips from alpha<->digit (start of numeric part)
        if i < len(s) and s[i-1].isalpha() and s[i].isdigit():
            break
    # Fallback: consume leading alnum up to first digit
    if i == 0:
        m = re.match(r"^[A-Z0-9]+", s)
        return m.group(0) if m else None
    return s[:i]

def _parse_env_time_to_nz(s: str) -> datetime | None:
    """
    Envision departureScheduled/departureEstimate parser.
    - If offset is present (Z or ±hh:mm), respect it.
    - If naïve, TREAT AS UTC (Envision often returns naïve UTC).
    Returns timezone-aware NZ datetime.
    """
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

    if dt.tzinfo is None:
        # ✅ key change: naïve => UTC, not NZ
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(NZ)


# --- DCS flights page ---
@ui_bp.route("/dcs/flights")
def dcs_flights_page():
    dstr = request.args.get("date")
    if dstr:
        try:
            day = date.fromisoformat(dstr)
        except ValueError:
            day = date.today()
            flash("Invalid date format. Showing today.", "warning")
    else:
        day = date.today()

    dep = request.args.get("dep") or None
    arr = request.args.get("arr") or None
    designator = request.args.get("airline") or None

    # fetch safely
    try:
        data = fetch_flights_for_day(day, dep=dep, arr=arr,
                                     airline_designator=designator,
                                     only_dcs_status=True)
    except Exception as e:
        flash(f"DCS fetch failed: {e}", "danger")
        data = {"Flights": []}

    flights = (data or {}).get("Flights", []) or []

    def _agg(f):
        pax = f.get("Passengers") or []
        def ptype(p): return (p.get("PassengerType") or "").strip().upper()
        adults = sum(1 for p in pax if ptype(p) in {"ADT", "ADULT", "A"})
        children = sum(1 for p in pax if ptype(p) in {"CHD", "CHILD", "C", "INF", "INFANT"})
        def to_num(x):
            try:
                return float(x or 0)
            except (TypeError, ValueError):
                return 0.0
        total_bag_kg = sum(to_num(p.get("BaggageWeight")) for p in pax)
        return {"count": len(pax), "adults": adults, "children": children, "bags_kg": total_bag_kg}

    rows, totals = [], {"pax": 0, "adults": 0, "children": 0, "bags_kg": 0.0}
    for f in flights:
        a = _agg(f)
        rows.append({
            "flight_no": f.get("FlightNumber"),
            "date_utc": f.get("FlightDate"),
            "status": f.get("FlightStatus"),
            "origin": f.get("Origin"),
            "destination": f.get("Destination"),
            "dcs_status": f.get("FlightDcsStatus"),
            "pax_count": a["count"],
            "adults": a["adults"],
            "children": a["children"],
            "bags_kg": a["bags_kg"],
            "raw": f,
        })
        totals["pax"] += a["count"]
        totals["adults"] += a["adults"]
        totals["children"] += a["children"]
        totals["bags_kg"] += a["bags_kg"]

    return render_template(
        "dcs_flights.html",
        day=day,
        rows=rows,
        totals=totals,  # <-- pass totals so your template can render the totals row
        filters={"dep": dep, "arr": arr, "airline": designator}
    )

# --- JSON (with graceful error) ---
@ui_bp.route("/api/dcs/flights")
def dcs_flights_api():
    dstr = request.args.get("date")
    try:
        day = date.fromisoformat(dstr) if dstr else date.today()
    except ValueError:
        day = date.today()

    dep = request.args.get("dep") or None
    arr = request.args.get("arr") or None
    designator = request.args.get("airline") or None

    try:
        data = fetch_flights_for_day(day, dep=dep, arr=arr,
                                     airline_designator=designator,
                                     only_dcs_status=True)
    except Exception as e:
        return jsonify({"Flights": [], "error": str(e)}), 502

    return jsonify(data or {"Flights": []})

@ui_bp.route("/debug/dcs-ping")
def dcs_ping():
    from datetime import date
    from flask import current_app
    dep = request.args.get("dep") or None
    arr = request.args.get("arr") or None
    designator = request.args.get("airline") or None
    dstr = request.args.get("date")
    try:
        day = date.fromisoformat(dstr) if dstr else date.today()
    except ValueError:
        day = date.today()

    try:
        # reach into client and capture url/payload/resp
        from .zenith_client import _debug_call
        out = _debug_call(day, dep=dep, arr=arr, airline_designator=designator, only_dcs_status=True)
        return jsonify(out), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 502

def fetch_flights(from_dt: datetime, to_dt: datetime):
    base = current_app.config.get("ENVISION_BASE", "").rstrip("/")
    token = current_app.config.get("SOURCE_API_TOKEN", "")
    if not base:
        raise RuntimeError("Missing SOURCE_API_BASE")
    url = f"{base}/v1/flights?from={from_dt.isoformat()}&to={to_dt.isoformat()}"
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    current_app.logger.info(f"[ENVISION] GET {url}")
    if token:
        current_app.logger.info("[ENVISION] Using bearer token (masked)")
    else:
        current_app.logger.warning("[ENVISION] No SOURCE_API_TOKEN set")

    resp = requests.get(url, headers=headers, timeout=60)
    current_app.logger.info(f"[ENVISION] status={resp.status_code} elapsed={resp.elapsed.total_seconds():.3f}s")
    try:
        js = resp.json()
    except Exception:
        # show a preview for quick diagnosis
        preview = (resp.text or "")[:1000]
        current_app.logger.error(f"[ENVISION] Non-JSON response preview: {preview}")
        resp.raise_for_status()
        raise
    return js

@ui_bp.route("/debug/envision-ping")
def debug_envision_ping():
    dstr = request.args.get("date")
    try:
        day = date.fromisoformat(dstr) if dstr else date.today()
    except ValueError:
        day = date.today()
    start_utc = datetime.combine(day, time(0,0,0, tzinfo=timezone.utc))
    end_utc   = start_utc + timedelta(days=1)

    try:
        token = envision_authenticate()["token"]
        data = envision_get_flights(token, start_utc, end_utc)
        return jsonify({
            "window": {"from": start_utc.isoformat(), "to": end_utc.isoformat()},
            "count": len(data) if isinstance(data, list) else None,
            "first_item_keys": list(data[0].keys())[:40] if isinstance(data, list) and data else [],
            "preview": data[:2] if isinstance(data, list) else data
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 502


def _first_page_debug(token: str, start_utc: datetime, end_utc: datetime, limit: int = 5) -> dict:
    import json, requests
    url = f"{ENVISION_BASE.rstrip('/')}/Flights"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    params = {"dateFrom": start_utc.isoformat(), "dateTo": end_utc.isoformat(), "offset": 0, "limit": limit}
    out = {"url": url, "params": params, "status": None, "content_type": None, "raw_text": None, "json_preview": None}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=60)
        out["status"] = r.status_code
        out["content_type"] = r.headers.get("Content-Type")
        txt = r.text or ""
        out["raw_text"] = txt[:8000]
        try:
            js = r.json()
            out["json_preview"] = json.dumps(js[:2] if isinstance(js, list) else js, ensure_ascii=False, default=str, indent=2)
        except Exception:
            pass
    except Exception as e:
        out["raw_text"] = f"Request error: {e}"
    return out

### OLD WORKING ROUTE FOR REFERENCE ONLY; MAY BE DELETED LATER ###
@ui_bp.route("/dcs/from-envision/old")
def dcs_from_envision_page_old():
    dstr = request.args.get("date")
    try:
        day = date.fromisoformat(dstr) if dstr else date.today()
    except ValueError:
        day = date.today()
        flash("Invalid date format. Showing today.", "warning")

    # ---- Use NZ local day for the UI, convert to UTC for the API window ----
    start_nz = datetime.combine(day, time(0, 0, tzinfo=NZ))
    end_nz   = start_nz + timedelta(days=1)
    start_utc = start_nz.astimezone(timezone.utc)
    end_utc   = end_nz.astimezone(timezone.utc)

    # 1) Envision login -> token
    try:
        auth = envision_authenticate()
        token = auth["token"]
    except Exception as e:
        flash(f"Envision auth failed: {e}", "danger")
        return render_template(
            "dcs_from_envision.html",
            day=day,
            results=[],
            diag={
                "stage": "auth_failed",
                "window_used": f"{start_utc.isoformat()} → {end_utc.isoformat()}",
                "window_used_nz": f"{start_nz.isoformat()} → {end_nz.isoformat()}",
                "base": (current_app.config.get("ENVISION_BASE") or "").rstrip("/"),
                "has_token": False,
                "raw_type": None,
                "raw_preview": None,
            },
        )

    # 2) Pull Envision flights for the NZ local day (via UTC window)
    diag = {
        "stage": "fetch",
        "window_used": f"{start_utc.isoformat()} → {end_utc.isoformat()}",
        "window_used_nz": f"{start_nz.isoformat()} → {end_nz.isoformat()}",
        "base": (current_app.config.get("ENVISION_BASE") or "").rstrip("/"),
        "has_token": True,
        "raw_type": None,
        "raw_preview": None,
    }
    try:
        env_flights = envision_get_flights(token, start_utc, end_utc) or []
        diag["raw_type"] = type(env_flights).__name__
        import json as _json
        diag["raw_preview"] = (
            _json.dumps(env_flights[:2], ensure_ascii=False, default=str, indent=2)
            if isinstance(env_flights, list)
            else _json.dumps(env_flights, ensure_ascii=False, default=str)[:1000]
        )
    except Exception as e:
        flash(f"Envision fetch failed: {e}", "danger")
        return render_template("dcs_from_envision.html", day=day, results=[], diag=diag)

    if not env_flights:
        diag["note"] = (
            "No flights returned for this UTC window. "
            "Check the Raw HTTP block below (URL, params, status, body)."
        )
        return render_template("dcs_from_envision.html", day=day, results=[], diag=diag)

    # --- Map Envision -> table rows (convert to NZ, filter to selected NZ day) ---
    def _payload_to_list(payload):
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for k in ("flights", "items", "data"):
                v = payload.get(k)
                if isinstance(v, list):
                    return v
                if isinstance(v, dict):
                    for kk in ("flights", "items"):
                        vv = v.get(kk)
                        if isinstance(vv, list):
                            return vv
        return []

    items = _payload_to_list(env_flights)

    rows = []
    for f in items:
        dep = f.get("departurePlaceDescription") or f.get("departurePlaceId")
        arr = f.get("arrivalPlaceDescription") or f.get("arrivalPlaceId")
        std_nz = _parse_env_time_to_nz(
            f.get("departureEstimate") or f.get("departureScheduled")
        )
        fnum = f.get("flightNumberDescription")
        designator = _infer_designator(fnum)

        if not (dep and std_nz and fnum):
            continue
        if std_nz.date() != day:
            continue

        # --- Derive planned STA/ETA and duration ---
        arr_time = _parse_env_time_to_nz(
            f.get("arrivalEstimate") or f.get("arrivalScheduled")
        )
        etd = _parse_env_time_to_nz(
            f.get("departureEstimate") or f.get("departureScheduled")
        )
        block_mins = None
        if etd and arr_time:
            block_mins = round((arr_time - etd).total_seconds() / 60)

        # STD / STA (thin bar)
        std_sched_nz = _parse_env_time_to_nz(f.get("departureScheduled"))   # STD
        sta_sched_nz = _parse_env_time_to_nz(f.get("arrivalScheduled"))     # STA

        # ETD / ETA (estimate, what you were using for the thick bar)
        std_est_nz = _parse_env_time_to_nz(
            f.get("departureEstimate") or f.get("departureScheduled")
        )
        sta_est_nz = _parse_env_time_to_nz(
            f.get("arrivalEstimate") or f.get("arrivalScheduled")
        )

        # NEW: ATD / ATA – off-blocks / on-blocks actuals in NZ local
        dep_actual_nz = _parse_env_time_to_nz(
            f.get("departureActual")
            or f.get("departureOffBlocks")
            or f.get("gateOutActual")
        )
        arr_actual_nz = _parse_env_time_to_nz(
            f.get("arrivalActual")
            or f.get("arrivalOnBlocks")
            or f.get("gateInActual")
        )

        rows.append({
            # --- core identifiers used for matching ---
            "dep": str(dep),
            "dest": str(arr) if arr else None,
            "ades": str(arr) if arr else "",
            "envision_flight_id": f.get("id"),

            # ETD / ETA (what the thick bar uses)
            "std_nz": std_est_nz,
            "sta_nz": sta_est_nz,
            "std_utc": std_est_nz.astimezone(timezone.utc) if std_est_nz else None,
            "sta_utc": sta_est_nz.astimezone(timezone.utc) if sta_est_nz else None,

            # STD / STA (thin scheduled bar)
            "std_sched_nz": std_sched_nz,
            "sta_sched_nz": sta_sched_nz,

            # ✅ NEW: ATD / ATA in NZ local
            "dep_actual_nz": dep_actual_nz,
            "arr_actual_nz": arr_actual_nz,

            "block_mins": block_mins or 0,

            # --- flight identifiers ---
            "designator": designator or "",
            "flight_number": str(fnum),
            "flight": str(fnum),
            "reg": (
                f.get("flightRegistrationDescription")      # e.g. "ZK-MCU"
                or f.get("aircraftRegistration")
                or f.get("aircraftDescription")
                or f.get("flightLineDescription")           # e.g. "MCU (ATR72)"
                or ""
            ),
            "aircraft_type": f.get("aircraftType") or f.get("aircraftTypeId") or "",
            "service_type": f.get("serviceTypeDescription") or "",
            "flight_status": f.get("flightStatusDescription") or f.get("flightStatusId") or "",
            "crew": f.get("crewComposition") or "",
            "route": f.get("routeDescription") or "",

            # --- performance/planning extras ---
            "planned_block": block_mins,
            "departure_gate": f.get("departureGate") or "",
            "arrival_gate": f.get("arrivalGate") or "",
            "stand": f.get("stand") or "",
            "check_in_desk": f.get("checkInDeskDescription") or "",
            "remarks": f.get("remarks") or "",

            # --- flags for DCS + APG linking ---
            "ok": True,
            "pax_count": None,
            "bags_kg": 0.0,
            "adt": 0,
            "chd": 0,
            "inf": 0,
            "error": None,
        })

    rows.sort(key=lambda r: r["std_nz"])

    # 🔹 Pull DCS pax/bag data
    _enrich_rows_with_dcs(rows, day)

    # 🔹 Attach APG plan presence (plan_id per row)
    try:
        attach_apg_presence_to_rows(
            rows,
            window_from_utc=start_utc,
            window_to_utc=end_utc,
        )
    except Exception as e:
        current_app.logger.warning(f"APG presence attach failed: {e}")
        # Don't break the page — APG column will just show blanks

    # 🔹 Delay enrichment for initial page render
    try:
        for r in rows:
            fid = r.get("envision_flight_id")
            if not fid:
                r["delays"] = []
                continue
            try:
                r["delays"] = envision_get_delays(token, int(fid)) or []
            except Exception as e:
                current_app.logger.warning("Failed to load delays for %s: %s", fid, e)
                r["delays"] = []
    except Exception as e:
        current_app.logger.warning("Bulk delay enrichment failed: %s", e)
        for r in rows:
            # make sure key exists so template doesn't blow up
            r.setdefault("delays", [])

    # 🔹 APG /plan/list debug – for diagnostics panel
    if request.args.get("apg_debug") == "1":
        try:
            apg_auth = apg_login(APG_EMAIL, APG_PASSWORD)
            apg_bearer = apg_auth["authorization"]
            apg_plans = apg_get_plan_list(apg_bearer, page_size=50, after=None)

            import json as _json
            diag["apg_raw_type"] = type(apg_plans).__name__
            diag["apg_raw_preview"] = _json.dumps(
                apg_plans[:5] if isinstance(apg_plans, list) else apg_plans,
                ensure_ascii=False,
                default=str,
                indent=2,
            )
        except Exception as e:
            current_app.logger.warning(f"APG plan list debug failed: {e}")

    diag["raw_http"] = _first_page_debug(token, start_utc, end_utc, limit=1000)
    return render_template("dcs_from_envision.html", day=day, results=rows, diag=diag)

@ui_bp.route("/dcs/from-envision")
def dcs_from_envision():
    # parse ?date=… but default to today
    day_str = request.args.get("date")
    if day_str:
        try:
            day = date.fromisoformat(day_str)
        except ValueError:
            day = date.today()
    else:
        day = date.today()

    # ONLY render HTML – data will be loaded via JS from /api/dcs/gantt_data
    return render_template("dcs_from_envision.html", day=day)


@ui_bp.get("/api/dcs/gantt_data")
def api_dcs_gantt_data():
    """
    JSON endpoint used by the Gantt auto-refresh.
    Returns the same "rows" that dcs_from_envision_page builds, but as JSON.
    """
    dstr = request.args.get("date")
    try:
        day = date.fromisoformat(dstr) if dstr else date.today()
    except ValueError:
        day = date.today()

    # ---- NZ-local window → UTC for Envision API ----
    start_nz = datetime.combine(day, time(0, 0, tzinfo=NZ))
    end_nz   = start_nz + timedelta(days=1)
    start_utc = start_nz.astimezone(timezone.utc)
    end_utc   = end_nz.astimezone(timezone.utc)

    # 1) Envision auth + fetch
    try:
        auth = envision_authenticate()
        token = auth["token"]
        env_flights = envision_get_flights(token, start_utc, end_utc) or []
    except Exception as e:
        current_app.logger.exception("api_dcs_gantt_data: Envision error")
        return jsonify({"ok": False, "error": f"Envision error: {e}", "results": []}), 502

    # 2) Normalise payload → list
    items = _list_from_envision_payload(env_flights)

    # 3) Map Envision flights → "rows" (same as dcs_from_envision_page)
    rows = []
    for f in items:
        dep = f.get("departurePlaceDescription") or f.get("departurePlaceId")
        arr = f.get("arrivalPlaceDescription") or f.get("arrivalPlaceId")
        std_nz = _parse_env_time_to_nz(
            f.get("departureEstimate") or f.get("departureScheduled")
        )
        fnum = f.get("flightNumberDescription")
        designator = _infer_designator(fnum)

        # basic sanity + NZ-day filter
        if not (dep and std_nz and fnum):
            continue
        if std_nz.date() != day:
            continue

        arr_time = _parse_env_time_to_nz(
            f.get("arrivalEstimate") or f.get("arrivalScheduled")
        )
        etd = _parse_env_time_to_nz(
            f.get("departureEstimate") or f.get("departureScheduled")
        )
        block_mins = None
        if etd and arr_time:
            block_mins = round((arr_time - etd).total_seconds() / 60)

        std_sched_nz = _parse_env_time_to_nz(f.get("departureScheduled"))   # STD
        sta_sched_nz = _parse_env_time_to_nz(f.get("arrivalScheduled"))     # STA

        std_est_nz = _parse_env_time_to_nz(
            f.get("departureEstimate") or f.get("departureScheduled")
        )  # ETD
        sta_est_nz = _parse_env_time_to_nz(
            f.get("arrivalEstimate") or f.get("arrivalScheduled")
        )  # ETA

         # NEW: ATD/ATA
        dep_actual_nz = _parse_env_time_to_nz(
            f.get("departureActual")
            or f.get("departureOffBlocks")
            or f.get("gateOutActual")
        )
        arr_actual_nz = _parse_env_time_to_nz(
            f.get("arrivalActual")
            or f.get("arrivalOnBlocks")
            or f.get("gateInActual")
        )

        row = {
            # --- core identifiers used for matching ---
            "dep": str(dep),
            "dest": str(arr) if arr else None,
            "ades": str(arr) if arr else "",
            "envision_flight_id": f.get("id"),

            # ETD / ETA (thick bar)
            "std_nz": std_est_nz,
            "sta_nz": sta_est_nz,
            "std_utc": std_est_nz.astimezone(timezone.utc) if std_est_nz else None,
            "sta_utc": sta_est_nz.astimezone(timezone.utc) if sta_est_nz else None,

            # STD / STA (thin scheduled bar)
            "std_sched_nz": std_sched_nz,
            "sta_sched_nz": sta_sched_nz,

            # ✅ Actual off-blocks/on-blocks
            "dep_actual_nz": dep_actual_nz,
            "arr_actual_nz": arr_actual_nz,

            "block_mins": block_mins or 0,

            # --- flight identifiers ---
            "designator": designator or "",
            "flight_number": str(fnum),
            "flight": str(fnum),
            "reg": (
                f.get("flightRegistrationDescription")      # e.g. "ZK-MCU"
                or f.get("aircraftRegistration")
                or f.get("aircraftDescription")
                or f.get("flightLineDescription")           # e.g. "MCU (ATR72)"
                or ""
            ),
            "aircraft_type": f.get("aircraftType") or f.get("aircraftTypeId") or "",
            "service_type": f.get("serviceTypeDescription") or "",
            "flight_status": f.get("flightStatusDescription") or f.get("flightStatusId") or "",
            "crew": f.get("crewComposition") or "",
            "route": f.get("routeDescription") or "",

            # --- placeholders for DCS/APG enrichment ---
            "planned_block": block_mins,
            "departure_gate": f.get("departureGate") or "",
            "arrival_gate": f.get("arrivalGate") or "",
            "stand": f.get("stand") or "",
            "check_in_desk": f.get("checkInDeskDescription") or "",
            "remarks": f.get("remarks") or "",

            "ok": True,
            "pax_count": None,
            "bags_kg": 0.0,
            "adt": 0,
            "chd": 0,
            "inf": 0,
            "error": None,
            "apg_plan_id": "",
            "pax_list": [],

            # NEW: default delays
            "delays": [],
        }

        rows.append(row)

    rows.sort(key=lambda r: r["std_nz"] or _dt.min.replace(tzinfo=NZ))

    # 4) DCS enrichment
    try:
        _enrich_rows_with_dcs(rows, day)
    except Exception as e:
        current_app.logger.warning(f"api_dcs_gantt_data: _enrich_rows_with_dcs failed: {e}")

    # 5) APG plan presence
    try:
        attach_apg_presence_to_rows(
            rows,
            window_from_utc=start_utc,
            window_to_utc=end_utc,
        )
    except Exception as e:
        current_app.logger.warning(f"api_dcs_gantt_data: attach_apg_presence_to_rows failed: {e}")

    # 6) NEW: attach delays for each Envision flight
    try:
        for r in rows:
            fid = r.get("envision_flight_id")
            if not fid:
                r["delays"] = []
                continue

            try:
                delays = envision_get_delays(token, int(fid))
            except Exception as e:
                current_app.logger.warning(
                    "api_dcs_gantt_data: failed to load delays for flight %s: %s",
                    fid, e
                )
                delays = []

            # Delays look like:
            # {
            #   "id": 8824,
            #   "flightId": 68687,
            #   "delayCodeId": 92,
            #   "delayCode": "93",
            #   "delayCodeDescription": "...",
            #   "delayMinutes": 32,
            #   "isArrival": false,
            #   ...
            # }
            r["delays"] = delays or []
    except Exception as e:
        current_app.logger.warning(
            "api_dcs_gantt_data: top-level delay fetch error: %s", e
        )

    # 7) Make it JSON-serialisable (convert datetimes to ISO strings)
    def row_to_json(r):
        def dt_or_none(x):
            return x.isoformat() if isinstance(x, datetime) else None

        return {
            "reg": (r.get("reg") or "Unknown"),
            "dep": r.get("dep"),
            "ades": r.get("ades"),
            "std_nz": dt_or_none(r.get("std_nz")),
            "sta_nz": dt_or_none(r.get("sta_nz")),
            "std_sched_nz": dt_or_none(r.get("std_sched_nz")),
            "sta_sched_nz": dt_or_none(r.get("sta_sched_nz")),
            
            # ✅ Actuals
            "dep_actual_nz": dt_or_none(r.get("dep_actual_nz")),
            "arr_actual_nz": dt_or_none(r.get("arr_actual_nz")),

            "flight_number": r.get("flight_number"),
            "designator": r.get("designator"),
            "apg_plan_id": r.get("apg_plan_id") or "",
            "block_mins": r.get("block_mins") or 0,
            "aircraft_type": r.get("aircraft_type"),
            "flight_status": r.get("flight_status"),
            "adt": r.get("adt") or 0,
            "chd": r.get("chd") or 0,
            "inf": r.get("inf") or 0,
            "pax_count": r.get("pax_count") or 0,
            "bags_kg": float(r.get("bags_kg") or 0),
            "pax_list": r.get("pax_list") or [],
            "envision_flight_id": r.get("envision_flight_id"),
            "delays": r.get("delays") or [],   # <-- NEW: ship delays to JS
        }

    json_rows = [row_to_json(r) for r in rows]
    return jsonify({"ok": True, "results": json_rows})

@ui_bp.route("/debug/zenith-config")
def debug_zenith_config():
    from flask import jsonify, current_app
    keys = ["PROD_DCS_API_BASE", "DCS_API_FLIGHTS_PATH", "PROD_DCS_API_KEY"]
    masked = (current_app.config.get("PROD_DCS_API_KEY") or "")
    masked = masked[:4] + "…" + masked[-4:] if masked else ""
    return jsonify({
        "present": {k: bool(current_app.config.get(k)) for k in keys},
        "values": {
            "PROD_DCS_API_BASE": current_app.config.get("PROD_DCS_API_BASE"),
            "DCS_API_FLIGHTS_PATH": current_app.config.get("DCS_API_FLIGHTS_PATH"),
            "PROD_DCS_API_KEY(masked)": masked,
        },
    })

def _list_from_envision_payload(payload):
    """
    Envision may return one of:
      - list[flight]
      - {"flights": [...]}
      - {"items": [...]}
      - {"data": {"flights": [...]}} or {"data": [...]}
    This tries common variants and falls back to [].
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in ("flights", "items", "data"):
            v = payload.get(k)
            if isinstance(v, list):
                return v
            if isinstance(v, dict):
                # e.g. {"data":{"flights":[...]}}
                for kk in ("flights", "items"):
                    vv = v.get(kk)
                    if isinstance(vv, list):
                        return vv
    return []

@ui_bp.get("/api/envision/flight_times")
def api_envision_flight_times():
    """
    Return Envision actual times for a single flight.

    Expects:
      /api/envision/flight_times?flight_id=12345

    Uses /v1/Flights/{flightId} under the hood.
    """
    flight_id = request.args.get("flight_id") or request.args.get("id")
    if not flight_id:
        return jsonify({"ok": False, "error": "Missing flight_id"}), 400

    try:
        flight_id_int = int(flight_id)
    except ValueError:
        return jsonify({"ok": False, "error": "Bad flight_id"}), 400

    # 1) Envision auth
    try:
        auth = envision_authenticate()
        token = auth["token"]
    except Exception as e:
        current_app.logger.exception("Envision auth failed in api_envision_flight_times")
        return jsonify({"ok": False, "error": f"Envision auth failed: {e}"}), 502

    # 2) Get single-flight record
    try:
        raw = envision_get_flight_times(token, flight_id_int)
    except Exception as e:
        current_app.logger.exception("Envision /Flights/{id} failed")
        return jsonify({"ok": False, "error": f"Envision /Flights/{{id}} failed: {e}"}), 502

    # 3) Convert the four key timestamps to NZ local
    def as_local_iso(key: str):
        s = raw.get(key)
        dt = _parse_env_time_to_nz(s) if s else None
        return dt.isoformat() if dt else None

    def as_local_hm(key: str):
        s = raw.get(key)
        dt = _parse_env_time_to_nz(s) if s else None
        return dt.strftime("%H:%M") if dt else None

    payload = {
        "ok": True,
        "flight_id": flight_id_int,
        "flightStatusId": raw.get("flightStatusId"),

        # raw strings exactly as Envision returns them
        "raw": {
            "departureActual": raw.get("departureActual"),
            "departureTakeOff": raw.get("departureTakeOff"),
            "arrivalLanded": raw.get("arrivalLanded"),
            "arrivalActual": raw.get("arrivalActual"),
        },

        # Local ISO datetimes (NZ)
        "local_iso": {
            "departureActual": as_local_iso("departureActual"),
            "departureTakeOff": as_local_iso("departureTakeOff"),
            "arrivalLanded": as_local_iso("arrivalLanded"),
            "arrivalActual": as_local_iso("arrivalActual"),
        },

        # HH:MM strings for UI labels
        "local_hm": {
            "departureActual": as_local_hm("departureActual"),
            "departureTakeOff": as_local_hm("departureTakeOff"),
            "arrivalLanded": as_local_hm("arrivalLanded"),
            "arrivalActual": as_local_hm("arrivalActual"),
        },
    }

    return jsonify(payload), 200

@ui_bp.route("/dcs/manifest_preview")
def dcs_manifest_preview():
    # Pull query parameters safely
    dep         = request.args.get("dep")
    ades        = request.args.get("ades")
    date_str    = request.args.get("date")
    designator  = request.args.get("designator")
    flight_no   = request.args.get("flight_number")
    reg         = request.args.get("reg")

    current_app.logger.info(
        "Manifest preview request: dep=%s ades=%s date=%s designator=%s flight_no=%s reg=%s",
        dep, ades, date_str, designator, flight_no, reg,
    )

    # Validate required params
    missing = [name for name, value in [
        ("dep", dep),
        ("ades", ades),
        ("date", date_str),
        ("designator", designator),
        ("flight_number", flight_no),
        ("reg", reg),
    ] if not value]

    if missing:
        # Custom message instead of generic 400
        return (
            f"Missing required query parameter(s): {', '.join(missing)}",
            400,
        )

    # TODO: load passengers for this flight (however you already do it)
    # e.g. passengers = get_passengers_from_dcs(dep, ades, date_str, designator, flight_no, reg)

    # TODO: build the PDF bytes (you probably already have a helper for this)
    # pdf_bytes = build_manifest_pdf(dep, ades, date_str, designator, flight_no, reg, passengers)

    # For now, just prove it works with a dummy PDF or text:
    # pdf_bytes = generate_dummy_pdf(...)
    dummy = io.BytesIO()
    dummy.write(
        f"Manifest preview\n\n{designator}{flight_no} {dep}->{ades} {date_str} {reg}".encode("utf-8")
    )
    dummy.seek(0)

    return send_file(
        dummy,
        as_attachment=False,
        download_name="manifest-preview.txt",  # or .pdf if you’re returning a real PDF
        mimetype="text/plain",
    )






def _envision_first_page_debug(token: str, start_utc: datetime, end_utc: datetime, limit: int = 5) -> dict:
    import json as _json
    url = f"{ENVISION_BASE.rstrip('/')}/Flights"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    params = {"dateFrom": start_utc.isoformat(), "dateTo": end_utc.isoformat(), "offset": 0, "limit": limit}

    out = {"url": url, "params": params, "status": None, "content_type": None,
           "raw_text": None, "json_preview": None}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=60)
        out["status"] = r.status_code
        out["content_type"] = r.headers.get("Content-Type")
        txt = r.text or ""
        out["raw_text"] = txt[:8000]
        try:
            js = r.json()
            out["json_preview"] = _json.dumps(js[:2] if isinstance(js, list) else js,
                                              ensure_ascii=False, default=str, indent=2)
        except Exception:
            pass
    except Exception as e:
        out["raw_text"] = f"Request error: {e}"
    return out
