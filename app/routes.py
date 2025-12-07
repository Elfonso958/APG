import os
from flask import Blueprint, request, abort, send_file, make_response, current_app, jsonify, Response, render_template
from datetime import datetime, timezone, timedelta, date
from . import db
import requests
from sqlalchemy import func
from zoneinfo import ZoneInfo
from .models import SyncRun, SyncFlightLog, AppConfig
from .helpers_manifest import _seat_sort_key, _format_ssrs, _calc_age, _parse_dcs_dob, generate_manifest_pdf_from_html, generate_pdf_modern

from .sync.envision_apg_sync import (
    run_sync_once_return_summary,
    apg_login,
    APG_EMAIL,
    APG_PASSWORD,
    update_apg_plan_from_dcs_row,
    apg_plan_get,
    envision_update_flight_times,
    envision_get_flight_times,
    envision_authenticate,
    envision_put_delays,
    apg_upload_manifest_pdf,
    PAX_STD_WEIGHTS_KG,
    fetch_envision_crew_for_apg

)
import logging
import re
from .zenith_client import fetch_dcs_for_flight

api_bp = Blueprint("api", __name__)

# APG (RocketRoute / FlightPlan API)
APG_BASE = os.getenv("APG_BASE", "https://fly.rocketroute.com/api")
APG_APP_KEY = os.getenv("APG_APP_KEY", "")             # Provisioned by APG
APG_API_VERSION = os.getenv("APG_API_VERSION", "1.18") # Must be sent on each call
APG_EMAIL = os.getenv("APG_EMAIL", "")                 # API user email (from APG)
APG_PASSWORD = os.getenv("APG_PASSWORD", "")           # API user password (from APG)
ENVISION_BASE = os.getenv("ENVISION_BASE", "https://<envision-host>/v1")
ENVISION_USER = os.getenv("ENVISION_USER", "")   # e.g. "OJB"
ENVISION_PASS = os.getenv("ENVISION_PASS", "")   # e.g. "********"
NZ_TZ  = ZoneInfo("Pacific/Auckland")
UTC_TZ = ZoneInfo("UTC")

# ---- Manual run ----
@api_bp.post("/sync/run")
def api_sync_run_once():
    # read manual window from JSON body, form, or querystring
    data = request.get_json(silent=True) or {}
    date_from_utc = (
        data.get("date_from_utc")
        or request.form.get("date_from_utc")
        or request.args.get("date_from_utc")
    )
    date_to_utc = (
        data.get("date_to_utc")
        or request.form.get("date_to_utc")
        or request.args.get("date_to_utc")
    )

    logging.info("Manual run requested: from=%s to=%s", date_from_utc, date_to_utc)

    initiated_by = request.args.get("by") or "web"
    run = SyncRun(started_at=datetime.utcnow(), run_type="manual", initiated_by=initiated_by)
    db.session.add(run); db.session.commit()

    # pass the manual window through
    res = run_sync_once_return_summary(date_from_utc=date_from_utc, date_to_utc=date_to_utc)

    run.finished_at = datetime.utcnow()
    run.ok = bool(res.get("ok"))
    run.created = res.get("created")
    run.skipped = res.get("skipped")
    run.warnings = res.get("warnings")
    run.log_tail = res.get("log_tail")
    run.error = res.get("error")
    run.window_from_local = res.get("window_from_local")
    run.window_to_local   = res.get("window_to_local")
    run.window_from_utc   = res.get("window_from_utc")
    run.window_to_utc     = res.get("window_to_utc")
    db.session.add(run); db.session.commit()

    # Persist per-flight rows
    for ev in (res.get("flights") or []):
        row = SyncFlightLog(
            sync_run_id=run.id,
            envision_flight_id=str(ev.get("envision_flight_id") or ""),
            flight_no=ev.get("flight_no"),
            adep=ev.get("adep"),
            ades=ev.get("ades"),
            eobt=ev.get("eobt"),
            reg=ev.get("reg"),
            aircraft_id=ev.get("aircraft_id"),
            pic_name=ev.get("pic_name"),
            pic_empno=ev.get("pic_empno"),
            apg_pic_id=ev.get("apg_pic_id"),
            result=ev.get("result"),
            reason=ev.get("reason"),
            warnings=ev.get("warnings"),
        )
        db.session.add(row)
    db.session.commit()

    return {"id": run.id, "ok": run.ok}

# ---- List runs ----
@api_bp.get("/sync/runs")
def api_sync_runs():
    rows = SyncRun.query.order_by(SyncRun.id.desc()).limit(200).all()
    def fmt(dt): return dt.strftime("%d-%m-%y %H:%M") if dt else None
    return [{
        "id": r.id,
        "started_at": fmt(r.started_at),
        "finished_at": fmt(r.finished_at),
        "ok": r.ok,
        "created": r.created,
        "skipped": r.skipped,
        "warnings": r.warnings,
        "run_type": r.run_type,
        "initiated_by": r.initiated_by,
        "window_from_local": fmt(r.window_from_local),
        "window_to_local": fmt(r.window_to_local),
    } for r in rows]

@api_bp.get("/sync/runs/<int:rid>")
def api_sync_run_detail(rid):
    r = SyncRun.query.get_or_404(rid)
    def fmt(dt): return dt.strftime("%d-%m-%y %H:%M") if dt else None
    return {
        "id": r.id,
        "ok": r.ok,
        "created": r.created,
        "skipped": r.skipped,
        "warnings": r.warnings,
        "started_at": fmt(r.started_at),
        "finished_at": fmt(r.finished_at),
        "run_type": r.run_type,
        "initiated_by": r.initiated_by,
        "error": r.error,
        "log_tail": r.log_tail,
        "window_from_local": fmt(r.window_from_local),
        "window_to_local": fmt(r.window_to_local),
        "window_from_utc": fmt(r.window_from_utc),
        "window_to_utc": fmt(r.window_to_utc),
    }

@api_bp.get("/sync/runs/<int:rid>/flights")
def api_sync_run_flights(rid):
    rows = SyncFlightLog.query.filter_by(sync_run_id=rid).order_by(SyncFlightLog.id.asc()).all()
    def fmt(dt): return dt.strftime("%d-%m-%y %H:%M") if dt else None
    return [{
        "id": x.id,
        "envision_flight_id": x.envision_flight_id,
        "flight_no": x.flight_no,
        "adep": x.adep, "ades": x.ades,
        "eobt": fmt(x.eobt),
        "reg": x.reg, "aircraft_id": x.aircraft_id,
        "pic_name": x.pic_name, "pic_empno": x.pic_empno, "apg_pic_id": x.apg_pic_id,
        "result": x.result, "reason": x.reason,
        "warnings": x.warnings,
    } for x in rows]

# ---- Scheduler settings ----
@api_bp.get("/schedule")
def api_get_schedule():
    cfg = AppConfig.query.get(1)
    if not cfg:
        cfg = AppConfig(id=1, auto_enabled=False, interval_sec=300)
        db.session.add(cfg); db.session.commit()
    return {
        "auto_enabled": cfg.auto_enabled,
        "interval_sec": cfg.interval_sec,
        "last_auto_started": cfg.last_auto_started.isoformat() if cfg.last_auto_started else None,
        "last_auto_finished": cfg.last_auto_finished.isoformat() if cfg.last_auto_finished else None,
    }

@api_bp.post("/schedule")
def api_set_schedule():
    from . import _scheduler  # created in app.__init__
    data = request.get_json(silent=True) or {}
    auto_enabled = bool(data.get("auto_enabled", False))
    interval_sec = int(data.get("interval_sec", 300))
    if interval_sec < 60:
        interval_sec = 60  # minimum 1 minute to be safe

    cfg = AppConfig.query.get(1) or AppConfig(id=1)
    cfg.auto_enabled = auto_enabled
    cfg.interval_sec = interval_sec
    db.session.add(cfg); db.session.commit()

    # Reschedule job
    try:
        if _scheduler.get_job("sync_auto_job"):
            _scheduler.reschedule_job("sync_auto_job", trigger="interval", seconds=interval_sec)
        else:
            _scheduler.add_job(_run_sync_job_auto, "interval", seconds=interval_sec, id="sync_auto_job", replace_existing=True)
    except Exception:
        logging.exception("Failed to (re)schedule auto job")

    return {"ok": True, "auto_enabled": cfg.auto_enabled, "interval_sec": cfg.interval_sec}

@api_bp.post("/dcs/push_to_apg")
def api_dcs_push_to_apg():
    """
    Push DCS pax (already fetched on the UI) into an existing APG plan.

    Expects JSON like:
    {
      "apg_plan_id": 4595816,
      "dep": "WHK",
      "ades": "AKL",              # optional for APG update, useful for manifest
      "reg": "ZK-CIT",            # optional for APG update, useful for manifest
      "date": "2025-11-21",
      "designator": "3C",
      "flight_number": "823",
      "preview_only": true/false,
      "pax_list": [ ... DCS passenger objects ... ]
    }
    """
    logger = current_app.logger

    data = request.get_json(force=True) or {}

    plan_id      = data.get("apg_plan_id")
    dep          = (data.get("dep") or "").strip().upper()
    ades         = (data.get("ades") or "").strip().upper()     # NEW: for manifest + filename
    reg          = (data.get("reg") or "").strip().upper()      # NEW: for manifest
    flight_date  = data.get("date")               # "2025-11-21" (NZ-local string)
    designator   = (data.get("designator") or "").strip().upper()
    flight_no    = (data.get("flight_number") or "").strip()
    preview_only = bool(data.get("preview_only"))
    pax_list     = data.get("pax_list") or []

    raw_crew     = data.get("crew") or [] 

    # Basic validation (same as before)
    if not plan_id:
        return jsonify({"ok": False, "error": "Missing apg_plan_id"}), 400

    if not (dep and flight_date and designator and flight_no):
        return jsonify({
            "ok": False,
            "error": "Missing required fields dep/date/designator/flight_number"
        }), 400

    # Parse date (just for sanity / logging)
    try:
        nz_day = date.fromisoformat(flight_date)
    except ValueError:
        return jsonify({"ok": False, "error": "Bad date format (expected YYYY-MM-DD)"}), 400

    # Build a *stub* DCS flight in the shape your helper expects
    # (the important bit is the Passengers list) – DO NOT TOUCH THIS, it
    # is what drives the APG mass & balance update and is already working.
    dcs_flight = {
        "Origin": dep,
        "FlightDate": nz_day.isoformat(),
        "OperatingAirline": {
            "AirlineDesignator": designator,
            "FlightNumber": flight_no,
        },
        "Passengers": pax_list,
    }

    logger.info(
        "APG push: plan_id=%s dep=%s date=%s designator=%s flight_no=%s "
        "pax_count=%d preview_only=%s",
        plan_id, dep, flight_date, designator, flight_no, len(pax_list), preview_only
    )

    # 1) APG auth (unchanged pattern)
    try:
        auth = apg_login(APG_EMAIL, APG_PASSWORD)
        if isinstance(auth, dict):
            bearer = auth.get("authorization") or auth.get("Authorization")
        else:
            bearer = auth  # just in case you later change apg_login to return a string
    except Exception as e:
        current_app.logger.exception("APG login failed in api_dcs_push_to_apg")
        return jsonify({"ok": False, "error": f"APG login failed: {e}"}), 502

    # 2) Update APG plan from this *stub* DCS flight (MASS LOGIC UNCHANGED)
    try:
        result = update_apg_plan_from_dcs_row(
            bearer=bearer,
            plan_id=int(plan_id),
            dcs_flight=dcs_flight,
            preview_only=preview_only,
        )
    except Exception as e:
        logger.exception("update_apg_plan_from_dcs_row crashed")
        return jsonify({"ok": False, "error": f"APG update failed: {e}"}), 500

    # --- Only generate + upload manifest in LIVE mode (not preview) ---
    # --- Only generate + upload manifest in LIVE mode (not preview) ---
    manifest_pdf = None
    manifest_resp = None
    manifest_error = None
    plan_version = None

    if not preview_only:
        # Derive a version from APG response if possible
        if isinstance(result, dict):
            plan_version = (
                result.get("version")
                or result.get("data", {}).get("route", {}).get("version")
                or result.get("route", {}).get("version")
            )
        if plan_version is None:
            plan_version = 1

        # --- Build the SAME HTML as preview (includes crew + nice layout) ---
        env_id_raw = data.get("envision_flight_id")
        try:
            envision_flight_id = int(env_id_raw) if env_id_raw is not None else None
        except (TypeError, ValueError):
            envision_flight_id = None

        try:
            html, _flight_ctx = _build_manifest_html_and_ctx(
                dep=dep,
                ades=ades or "",
                date_str=flight_date,
                designator=designator,
                raw_number=flight_no,
                reg=reg or "",
                envision_flight_id=envision_flight_id,
            )
        except Exception as e:
            current_app.logger.exception(
                "Manifest HTML build failed in api_dcs_push_to_apg"
            )
            manifest_error = f"Manifest HTML build failed: {e}"
            html = None

        # 2a) Generate PDF **from that HTML** using wkhtmltopdf
        if html is not None:
            try:
                manifest_pdf = generate_pdf_modern(html)
            except Exception as e:
                current_app.logger.exception("Manifest PDF generation failed")
                manifest_error = f"Manifest PDF generation failed: {e}"
                manifest_pdf = None

        # 2b) Upload to APG (non-fatal if it fails)
        if manifest_pdf is not None:
            flight_code = f"{designator}{flight_no}".upper()
            route_str   = f"{dep}-{ades or 'UNK'}"
            date_local  = nz_day.isoformat()
            filename    = f"{flight_code} - {route_str} - {date_local} v{plan_version}.pdf"

            try:
                manifest_resp = apg_upload_manifest_pdf(
                    bearer=bearer,
                    plan_id=int(plan_id),
                    pdf_bytes=manifest_pdf,
                    filename=filename,
                )
            except Exception as e:
                current_app.logger.exception("APG manifest upload failed")
                manifest_error = f"APG manifest upload failed: {e}"


    # --- Normalise manifest response for the frontend ---
    doc_id = None
    if isinstance(manifest_resp, dict):
        data = manifest_resp.get("data")
        if isinstance(data, dict):
            # Just in case APG ever returns a single object
            doc_id = data.get("doc_id")
        elif isinstance(data, list) and data:
            first = data[0] or {}
            doc_id = first.get("doc_id")

    manifest_uploaded = bool(doc_id)


    return jsonify({
        "ok": True,
        "mode": "live",
        "apg_response": result,
        "manifest_uploaded": manifest_uploaded,
        "manifest_doc_id": doc_id,
        "manifest_error": manifest_error,
        "plan_version": plan_version,
    })


@api_bp.get("/apg/plan/<int:plan_id>")
def api_apg_plan_get(plan_id: int):
    """
    Fetch raw APG plan data for a single plan_id and return it as JSON.
    Used when clicking the Plan ID in the APG column.
    """
    try:
        auth = apg_login(APG_EMAIL, APG_PASSWORD)
        bearer = auth["authorization"]
    except Exception as e:
        current_app.logger.exception("APG login failed in api_apg_plan_get")
        return jsonify({"ok": False, "error": f"APG login failed: {e}"}), 502

    try:
        plan = apg_plan_get(bearer, plan_id)
    except Exception as e:
        current_app.logger.exception("APG plan/get failed")
        return jsonify({"ok": False, "error": f"APG plan/get failed: {e}"}), 502

    return jsonify({"ok": True, "plan_id": plan_id, "plan": plan})

@api_bp.post("/dcs/passenger_list")
def api_dcs_passenger_list():
    """
    Return raw passenger list for a single flight from Zenith DCS.

    Expects JSON:
      {
        "dep": "AKL",
        "date": "2025-11-19",
        "designator": "3C",
        "flight_number": "3C701"  # or "701"
      }
    """
    data = request.get_json(force=True) or {}
    dep = (data.get("dep") or "").strip().upper()
    date_str = (data.get("date") or "").strip()
    designator = (data.get("designator") or "").strip().upper()
    full_no = (data.get("flight_number") or "").strip().upper()

    if not (dep and date_str and full_no):
        return jsonify({"ok": False, "error": "Missing dep/date/flight_number"}), 400

    try:
        nz_day = date.fromisoformat(date_str)
    except ValueError:
        return jsonify({"ok": False, "error": "Bad date format (expected YYYY-MM-DD)"}), 400

    # Split numeric part for Zenith
    numeric = full_no
    if designator and full_no.startswith(designator):
        numeric = full_no[len(designator):]

    try:
        dcs = fetch_dcs_for_flight(
        dep,
        nz_day,
        designator,
        numeric,
        True,
    )
    except Exception as e:
        current_app.logger.exception("DCS passenger_list fetch failed")
        return jsonify({"ok": False, "error": f"DCS fetch failed: {e}"}), 502

    # Try to dig out the first flight + its passengers in a tolerant way
    passengers = []

    if isinstance(dcs, dict):
        flights = (
            dcs.get("Flights") or
            dcs.get("flights") or
            dcs.get("Data") or
            dcs.get("data") or
            []
        )
        if isinstance(flights, dict):
            # sometimes wrapped one level deeper
            flights = (
                flights.get("Flights") or flights.get("flights") or
                flights.get("Items")   or flights.get("items")   or
                []
            )

        if isinstance(flights, list) and flights:
            flight = flights[0]
            passengers = (
                flight.get("Passengers") or
                flight.get("passengers") or
                []
            )

    return jsonify({
        "ok": True,
        "passengers": passengers,
    })



@api_bp.post("/apg/reset_passengers")
def api_apg_reset_passengers():
    """
    Reset all 'Passenger ...' rows in an APG plan to 0 mass / 0 POB.
    Does NOT touch BEW, crew, baggage, cargo etc.

    Body:
    {
      "apg_plan_id": 4595491,
      "preview_only": true   # optional
    }
    """
    data = request.get_json(force=True) or {}

    plan_id = data.get("apg_plan_id") or data.get("plan_id")
    preview_only = bool(data.get("preview_only"))

    if not plan_id:
        return jsonify({"ok": False, "error": "Missing apg_plan_id"}), 400

    # 1) APG login
    try:
        auth = apg_login(APG_EMAIL, APG_PASSWORD)
        bearer = auth["authorization"]
    except Exception as e:
        current_app.logger.exception("APG login failed in api_apg_reset_passengers")
        return jsonify({"ok": False, "error": f"APG login failed: {e}"}), 502

    # 2) Call the existing updater with an EMPTY DCS flight:
    #    -> apply_dcs_passengers_to_apg_rows() will:
    #       - reset all 'Passenger ...' rows to 0
    #       - see no passengers, so it won't add anything back
    try:
        result = update_apg_plan_from_dcs_row(
            bearer=bearer,
            plan_id=int(plan_id),
            dcs_flight={"Passengers": []},
            preview_only=preview_only,
        )
    except Exception as e:
        current_app.logger.exception("APG reset passengers failed")
        return jsonify({"ok": False, "error": str(e)}), 500

    if preview_only:
        # update_apg_plan_from_dcs_row() returns {"payload": ..., "debug": ...}
        return jsonify({
            "ok": True,
            "mode": "preview",
            "payload": result.get("payload"),
            "debug": result.get("debug"),
        })

    # Non-preview: edit has been sent to APG
    return jsonify({
        "ok": True,
        "mode": "reset",
        "plan_id": int(plan_id),
        # if update_apg_plan_from_dcs_row returned raw APG response:
        "apg_response": result,
    })

@api_bp.post("/dcs/save_times")
def api_dcs_save_times():
    data = request.get_json(force=True) or {}

    # --- core fields from UI ---
    mode          = data.get("mode")            # "dep" or "arr"
    std_sched     = data.get("std_sched")       # ISO UTC from UI (optional)
    sta_sched     = data.get("sta_sched")       # ISO UTC from UI (optional)
    envision_id   = data.get("envision_flight_id")

    etd       = data.get("etd")        # HH:MM (NZ local)
    offblocks = data.get("offblocks")  # HH:MM (NZ local)
    airborne  = data.get("airborne")   # HH:MM (NZ local)

    eta       = data.get("eta")        # HH:MM (NZ local)
    landing   = data.get("landing")    # HH:MM (NZ local)
    onchocks  = data.get("onchocks")   # HH:MM (NZ local)

    # explicit local dates (YYYY-MM-DD) from UI
    dep_date  = data.get("dep_date")   # for departure times
    arr_date  = data.get("arr_date")   # for arrival times

    # NEW: delays payload from UI
    delays    = data.get("delays") or []

    current_app.logger.info("[DCS/TIMES] raw payload: %r", data)

    if not envision_id:
        return jsonify({"ok": False, "error": "Missing Envision flight ID"}), 400

    try:
        env_id_int = int(envision_id)
    except ValueError:
        return jsonify({"ok": False, "error": "Bad envision_flight_id"}), 400

    # 1) Envision auth
    try:
        env_auth = envision_authenticate()
        token = env_auth["token"]
    except Exception as exc:
        current_app.logger.exception("[DCS/TIMES] Envision auth failed")
        return jsonify({
            "ok": False,
            "error": f"Envision auth failed: {exc}",
        }), 502

    # 2) Get current flight so we can build a proper FlightUpdateRequest
    try:
        base = envision_get_flight_times(token, envision_id)
    except Exception as exc:
        current_app.logger.exception("[DCS/TIMES] Envision GET flight %s failed", envision_id)
        return jsonify({
            "ok": False,
            "error": f"Envision GET Flights/{envision_id} failed: {exc}",
        }), 502

    # 3) Start from Envision's own values
    update_body = {
        "id": int(base.get("id") or envision_id),
        "flightStatusId": base.get("flightStatusId") or 0,

        # DEPARTURE (we will NOT touch 'departureEstimate' in dep mode)
        "departureEstimate": base.get("departureEstimate"),
        "departureActual": base.get("departureActual"),
        "departureTakeOff": base.get("departureTakeOff"),

        # ARRIVAL (we will NOT touch 'arrivalEstimate' in arr mode unless we have ETA)
        "arrivalEstimate": base.get("arrivalEstimate"),
        "arrivalLanded": base.get("arrivalLanded"),
        "arrivalActual": base.get("arrivalActual"),

        "plannedFlightTime": base.get("plannedFlightTime") or 0,
        "calculatedTakeOffTime": base.get("calculatedTakeOffTime"),
    }

    # Anchor logic for combining date + HH:MM when UI dates not provided
    base_dep_sched = (
        std_sched
        or base.get("departureEstimate")
        or base.get("departureActual")
        or base.get("departureTakeOff")
    )
    base_arr_sched = (
        sta_sched
        or base.get("arrivalEstimate")
        or base.get("arrivalActual")
        or base.get("arrivalLanded")
    )

    current_app.logger.info(
        "[DCS/TIMES] using base_dep_sched=%r base_arr_sched=%r dep_date=%r arr_date=%r",
        base_dep_sched, base_arr_sched, dep_date, arr_date
    )

    if mode == "dep":
        # We DO NOT modify departureEstimate (ETD)
        if dep_date:
            dep_act = _local_date_hm_to_utc_iso(dep_date, offblocks)
            dep_to  = _local_date_hm_to_utc_iso(dep_date, airborne)
        else:
            dep_act = _combine_date_and_hm(base_dep_sched, offblocks)
            dep_to  = _combine_date_and_hm(base_dep_sched, airborne)

        if dep_act:
            update_body["departureActual"] = dep_act
        if dep_to:
            update_body["departureTakeOff"] = dep_to

    elif mode == "arr":
        if arr_date:
            arr_lan = _local_date_hm_to_utc_iso(arr_date, landing)
            arr_act = _local_date_hm_to_utc_iso(arr_date, onchocks)
            arr_est = _local_date_hm_to_utc_iso(arr_date, eta)
        else:
            arr_lan = _combine_date_and_hm(base_arr_sched, landing)
            arr_act = _combine_date_and_hm(base_arr_sched, onchocks)
            arr_est = _combine_date_and_hm(base_arr_sched, eta)

        # If you **don’t** want ETA touched, comment this block out
        if arr_est:
            update_body["arrivalEstimate"] = arr_est

        if arr_lan:
            update_body["arrivalLanded"] = arr_lan
        if arr_act:
            update_body["arrivalActual"] = arr_act

    else:
        return jsonify({"ok": False, "error": f"Unknown mode {mode!r}"}), 400

    # Strip Nones so we only send populated fields
    update_body = {k: v for k, v in update_body.items() if v is not None}

    current_app.logger.info(
        "[DCS/TIMES] FlightUpdateRequest payload (final): %r",
        update_body
    )

    # 4) Send time update to Envision
    try:
        env_resp = envision_update_flight_times(token, envision_id, update_body)
    except requests.HTTPError as http_err:
        resp = http_err.response
        body = resp.text[:500] if resp is not None and resp.text else ""
        current_app.logger.error(
            "[ENVISION] Flights/%s update failed: HTTP %s, body=%s",
            envision_id,
            resp.status_code if resp is not None else "?",
            body,
        )
        return jsonify({
            "ok": False,
            "error": f"Envision HTTP {resp.status_code if resp is not None else '?'}",
            "body": body,
        }), 502
    except Exception as exc:
        current_app.logger.error(
            "[DCS/TIMES] Envision HTTP error for flight %s: %s",
            envision_id, exc
        )
        return jsonify({
            "ok": False,
            "error": str(exc),
            "body": "",
        }), 502

    # 5) NEW: push delays to Envision
    updated_delays = []
    try:
        if delays:
            current_app.logger.info(
                "api_dcs_save_times: sending %d delay(s) to Envision for flight %s",
                len(delays), env_id_int
            )
            updated_delays = envision_put_delays(token, env_id_int, delays)
    except Exception as e:
        current_app.logger.exception("Envision delay update failed in api_dcs_save_times")
        # keep same behaviour you’re seeing now: treat as fatal so UI shows error
        return jsonify({"ok": False, "error": f"Envision delay update failed: {e}"}), 502

    # 6) Success response (times + delays)
    return jsonify({
        "ok": True,
        "envision_response": env_resp,
        "delays_sent": delays,
        "delays_result": updated_delays,
    }), 200


def _combine_date_and_hm(base_iso: str | None, hm: str | None) -> str | None:
    """
    base_iso: scheduled time from Envision, e.g. '2025-11-20T17:45:00.000Z'
    hm: '06:45' (local HH:MM, NZ time)

    Returns RFC3339 UTC string or None.
    """
    if not base_iso or not hm:
        return None

    try:
        base_utc   = datetime.fromisoformat(base_iso.replace("Z", "+00:00"))
        base_local = base_utc.astimezone(NZ_TZ)

        h, m = map(int, hm.split(":", 1))
        dt_local = base_local.replace(hour=h, minute=m, second=0, microsecond=0)
        dt_utc   = dt_local.astimezone(UTC_TZ)
        return dt_utc.isoformat().replace("+00:00", "Z")
    except Exception:
        current_app.logger.exception(
            "_combine_date_and_hm failed for base_iso=%r hm=%r",
            base_iso, hm
        )
        return None

def _local_date_hm_to_utc_iso(local_date: str | None, hm: str | None) -> str | None:
    """
    Combine a local NZ date 'YYYY-MM-DD' and 'HH:MM' into a UTC ISO8601 string.
    Returns None if inputs are missing/bad.
    """
    if not local_date or not hm:
        return None
    try:
        d = date.fromisoformat(local_date)
        hour_str, minute_str = hm.split(":", 1)
        hour = int(hour_str)
        minute = int(minute_str)
    except Exception:
        return None

    dt_local = datetime(d.year, d.month, d.day, hour, minute, tzinfo=NZ_TZ)
    dt_utc = dt_local.astimezone(timezone.utc).replace(microsecond=0)
    # Keep a 'Z' suffix like Envision's example
    return dt_utc.isoformat().replace("+00:00", "Z")

@api_bp.route("/dcs/manifest_preview", methods=["POST"])
def api_dcs_manifest_preview():
    data = request.get_json() or {}

    envision_flight_id = data.get("envision_flight_id")
    try:
        envision_flight_id = int(envision_flight_id) if envision_flight_id is not None else None
    except (TypeError, ValueError):
        envision_flight_id = None

    try:
        html, _flight_ctx = _build_manifest_html_and_ctx(
            dep=data.get("dep") or "",
            ades=data.get("ades") or "",
            date_str=data.get("date") or "",
            designator=data.get("designator") or "",
            raw_number=data.get("number") or data.get("flight_number") or "",
            reg=data.get("reg") or "",
            envision_flight_id=envision_flight_id,
        )
    except Exception as e:
        current_app.logger.exception("Manifest preview failed: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 400

    return jsonify({"ok": True, "html": html})


def _build_manifest_html_and_ctx(
    dep: str,
    ades: str,
    date_str: str,
    designator: str,
    raw_number: str,
    reg: str,
    envision_flight_id: int | None,
) -> tuple[str, dict]:
    """
    Core logic to fetch DCS + Envision crew and render manifest.html.
    Returns (html, flight_ctx).
    """

    dep = (dep or "").upper()
    ades = (ades or "").upper()
    designator = (designator or "").upper()
    reg = reg or ""

    raw_number = (raw_number or "").strip().upper()

    # Strip designator prefix if present (e.g. "3C702" → "702")
    if designator and raw_number.startswith(designator):
        number = raw_number[len(designator):]
    else:
        number = raw_number

    if not number:
        raise ValueError("Missing flight number for manifest build")

    # 1) Fetch from DCS – we want full passenger list
    dcs_json = fetch_dcs_for_flight(
        dep_airport=dep,
        flight_date=date_str,
        airline_designator=designator,
        flight_number=number,
        only_status=True,
    )

    flights = dcs_json.get("Flights") or []
    if not flights:
        # Graceful empty case – let caller decide what to show
        return render_template(
            "manifest.html",
            flight={
                "designator": designator,
                "number": number,
                "dep": dep,
                "ades": ades,
                "date": date_str,
                "reg": reg,
            },
            passengers=[],
            crew=[],
        ), {
            "designator": designator,
            "number": number,
            "dep": dep,
            "ades": ades,
            "date": date_str,
            "reg": reg,
        }


    dcs_f = flights[0]

    # 2) Build passenger list (unchanged from your route)
    passengers: list[dict] = []
    for r in dcs_f.get("Passengers", []):
        status = (r.get("Status") or "").strip().lower()
        iata_status = (r.get("IataStatus") or "").strip().upper()

        # Only include Boarded / Flown passengers
        # DCS example: Status="Flown", IataStatus="B"
        allowed_status = {"boarded", "flown"}
        allowed_iata   = {"B"}  # IATA 'B' = boarded

        if status not in allowed_status and iata_status not in allowed_iata:
            # Skip anything that isn't boarded or flown
            continue


        dob, dob_fmt = _parse_dcs_dob(r.get("DateOfBirth"))
        age = _calc_age(dob)

        ptype  = (r.get("PassengerType") or "").strip().upper()
        gender = (r.get("Gender") or "").strip().upper()
        ssrs   = r.get("Ssrs") or []

        has_um_code = any(
            (s.get("Code") or "").strip().upper().startswith("UM")
            for s in ssrs
        )
        has_um_text = any(
            "UMNR" in (s.get("FreeText") or "").strip().upper()
            for s in ssrs
        )
        is_um = ptype in {"UM", "UMN", "UMNR"} or has_um_code or has_um_text

        pax_weight_kg = float(
            PAX_STD_WEIGHTS_KG.get(
                ptype,
                PAX_STD_WEIGHTS_KG.get("AD", 0.0)
            )
        )

        bag_weight = float(r.get("BaggageWeight") or 0.0)
        bag_pcs    = 1 if bag_weight > 0 else 0

        is_infant = ptype in {"INF", "INFANT", "IN"}
        is_child  = ptype in {"CHD", "CHILD", "C", "CNN"}
        is_adult  = not (is_infant or is_child)

        passengers.append({
            "seat": r.get("Seat") or "",
            "name": " ".join(
                x for x in [
                    (r.get("NamePrefix") or "").strip(),
                    (r.get("GivenName") or "").strip(),
                    (r.get("Surname") or "").strip(),
                ] if x
            ),
            "dob": dob_fmt,
            "age": age,
            "pnr": r.get("BookingReferenceID") or "",
            "ssrs": _format_ssrs(ssrs),

            "ptype": ptype,
            "gender": gender,
            "pax_weight_kg": pax_weight_kg,
            "bags_pcs": bag_pcs,
            "bags_kg": bag_weight,
            "is_adult": is_adult,
            "is_child": is_child,
            "is_infant": is_infant,
            "is_um": is_um,
        })

    passengers.sort(key=lambda p: _seat_sort_key(p["seat"]))

    # 3) Flight metadata for header
    flight_date_raw = dcs_f.get("FlightDate")
    _, flight_date_fmt = _parse_dcs_dob(flight_date_raw)

    flight_ctx = {
        "designator": designator,
        "number": number,
        "dep": dcs_f.get("Origin") or dep,
        "ades": dcs_f.get("Destination") or ades,
        # this date is what we'll use for the filename (local date)
        "date": flight_date_fmt or date_str,
        "reg": reg,
    }

    # 4) Crew (optional)
    crew: list[dict] = []
    if envision_flight_id:
        try:
            crew = fetch_envision_crew_for_apg(envision_flight_id)
        except Exception as e:
            current_app.logger.exception(
                "Manifest build: fetch_envision_crew_for_apg failed for flight_id=%s: %s",
                envision_flight_id, e
            )

    html = render_template(
        "manifest.html",
        flight=flight_ctx,
        passengers=passengers,
        crew=crew,
    )
    return html, flight_ctx





@api_bp.get("/api/envision/flight_crew")
def api_envision_flight_crew():
    """
    Lightweight wrapper around fetch_envision_crew(envision_flight_id)
    so the Gantt / flight details modal can show crew.
    """
    flight_id = request.args.get("flight_id", type=int)
    if not flight_id:
        return jsonify(ok=False, error="Missing flight_id"), 400

    try:
        crew = fetch_envision_crew_for_apg(flight_id)
        # crew is already in the nice form:
        # { position, name, employee_id, is_operating }
        return jsonify(ok=True, crew=crew)
    except Exception as e:
        current_app.logger.exception(
            "flight_crew failed for Envision flight_id=%s", flight_id
        )
        return jsonify(ok=False, error=str(e)), 500


def _get_manifest_crew(
    designator: str,
    number: str,
    dep: str,
    ades: str,
    date_str: str | None,
    reg: str | None = None,
) -> list[dict]:
    """
    Look up the most relevant SyncFlightLog row and return a normalised
    crew list for the manifest template.

    Each returned item:
      { "position": "CPT", "name": "SMITH JOHN", "employee_id": "12345" }
    """

    full_no = f"{designator}{number}".replace(" ", "").upper()
    dep = (dep or "").upper()
    ades = (ades or "").upper()
    reg = (reg or "").upper() if reg else None

    day = None
    if date_str:
        try:
            day = date.fromisoformat(date_str)
        except Exception:
            current_app.logger.warning("manifest: bad date_str %r for crew lookup", date_str)

    base_q = SyncFlightLog.query.filter(
        SyncFlightLog.flight_no == full_no
    )

    # ---------- 1) strict: flight_no + adep + ades + reg + date ----------
    strict_q = base_q.filter(
        SyncFlightLog.adep == dep,
        SyncFlightLog.ades == ades,
    )

    if reg:
        strict_q = strict_q.filter(SyncFlightLog.reg == reg)

    if day is not None:
        strict_q = strict_q.filter(func.date(SyncFlightLog.eobt) == day)

    row = strict_q.order_by(SyncFlightLog.eobt.desc()).first()

    # ---------- 2) fallback: flight_no + date ----------
    if not row and day is not None:
        fallback_q = base_q.filter(func.date(SyncFlightLog.eobt) == day)
        row = fallback_q.order_by(SyncFlightLog.eobt.desc()).first()

    # ---------- 3) fallback: latest by flight_no ----------
    if not row:
        row = base_q.order_by(SyncFlightLog.eobt.desc()).first()

    if not row:
        current_app.logger.info(
            "manifest: no SyncFlightLog match for %s %s %s %s (reg=%s)",
            full_no, dep, ades, date_str, reg
        )
        return []

    crew: list[dict] = []

    # Flight deck
    if row.pic_name:
        crew.append({
            "position": "CPT",
            "name": row.pic_name,
            "employee_id": row.pic_empno or "",
        })
    if row.fo_name:
        crew.append({
            "position": "FO",
            "name": row.fo_name,
            "employee_id": row.fo_empno or "",
        })

    # Cabin crew – comma-separated lists
    if row.cc_names:
        names = [n.strip() for n in (row.cc_names or "").split(",") if n.strip()]
        empnos = [e.strip() for e in (row.cc_empnos or "").split(",")] if row.cc_empnos else []

        for idx, name in enumerate(names):
            empno = empnos[idx] if idx < len(empnos) else ""
            crew.append({
                "position": f"CC{idx+1}",
                "name": name,
                "employee_id": empno,
            })

    return crew
