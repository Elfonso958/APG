import os
from flask import Blueprint, request, current_app, jsonify
from datetime import datetime, timezone, timedelta, date
from . import db
from .models import SyncRun, SyncFlightLog, AppConfig
from .sync.envision_apg_sync import (
    run_sync_once_return_summary,
    apg_login,
    _canon_eobt_to_utc_min_str,
    normalize_flight_no,
    to_icao,
    build_existing_plan_index,
    _get_local_tz,
    APG_EMAIL,
    APG_PASSWORD,
    update_apg_plan_from_dcs_row,
    apg_plan_get,
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
    flight_date  = data.get("date")               # "2025-11-21" (NZ-local string)
    designator   = (data.get("designator") or "").strip().upper()
    flight_no    = (data.get("flight_number") or "").strip()
    preview_only = bool(data.get("preview_only"))
    pax_list     = data.get("pax_list") or []

    # Basic validation
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
    # (the important bit is the Passengers list)
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
        "APG push: plan_id=%s dep=%s date=%s designator=%s flight_no=%s pax_count=%d preview_only=%s",
        plan_id, dep, flight_date, designator, flight_no, len(pax_list), preview_only
    )

    # 1) APG auth (your existing pattern)
    try:
        auth = apg_login(APG_EMAIL, APG_PASSWORD)
        if isinstance(auth, dict):
            bearer = auth.get("authorization") or auth.get("Authorization")
        else:
            bearer = auth  # just in case you later change apg_login to return a string
    except Exception as e:
        current_app.logger.exception("APG login failed in api_apg_reset_passengers")
        return jsonify({"ok": False, "error": f"APG login failed: {e}"}), 502

    # 2) Update APG plan from this *stub* DCS flight (using the pax you already fetched)
    try:
        result = update_apg_plan_from_dcs_row(
            bearer=bearer,                 # adjust name if your helper uses a different param
            plan_id=int(plan_id),
            dcs_flight=dcs_flight,
            preview_only=preview_only,
        )
    except Exception as e:
        logger.exception("update_apg_plan_from_dcs_row crashed")
        return jsonify({"ok": False, "error": f"APG update failed: {e}"}), 500

    # 3) Return something valid in all cases
    if preview_only:
        # result should already contain "payload" and "debug"
        return jsonify({
            "ok": True,
            "mode": "preview",
            **result,
        })

    # live mode – we expect result to be the raw APG response from plan/edit
    return jsonify({
        "ok": True,
        "mode": "live",
        "apg_response": result,
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
            dep_airport=dep,
            flight_date_nz=nz_day,
            designator=designator,
            flight_number=numeric,
            only_status=False,          # we want full passenger list
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
