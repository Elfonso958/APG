import os
from flask import Blueprint, request, abort, send_file, make_response, current_app, jsonify, Response, render_template, session
from datetime import datetime, timezone, timedelta, date
from . import db
import requests
from sqlalchemy import func
from zoneinfo import ZoneInfo
from .models import SyncRun, SyncFlightLog, SyncFlightState, AppConfig, ManifestUploadState
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
    envision_get_flights,
    envision_authenticate,
    envision_put_delays,
    envision_change_registration,
    envision_cancel_flight,
    envision_divert_flight,
    envision_get_delay,
    envision_put_delay,
    envision_post_delay,
    envision_delete_delay,
    apg_upload_manifest_pdf,
    PAX_STD_WEIGHTS_KG,
    fetch_envision_crew_for_apg,
    envision_get_flight_crew,
    envision_get_crew_position_setups,
    envision_get_crew_position_setup_items,
    envision_get_line_registrations,
    envision_get_flight_notes,
    envision_post_flight_note,
    envision_put_flight_note,
    envision_get_flight_types,
    envision_change_type,
    envision_get_flight_passengers,
    envision_put_flight_passengers,
    is_dcs_passenger_boarded_or_flown,
    normalise_pax_type,
    get_envision_environment,
    set_envision_environment,
    ENVISION_BASE

)
import logging
import re
from .zenith_client import fetch_dcs_for_flight

api_bp = Blueprint("api", __name__)


@api_bp.before_app_request
def _apply_session_envision_environment():
    try:
        chosen = session.get("envision_env")
        if chosen:
            set_envision_environment(str(chosen))
    except Exception:
        current_app.logger.exception("Failed to apply session Envision environment")

# APG (RocketRoute / FlightPlan API)
APG_BASE = os.getenv("APG_BASE", "https://fly.rocketroute.com/api")
APG_APP_KEY = os.getenv("APG_APP_KEY", "")             # Provisioned by APG
APG_API_VERSION = os.getenv("APG_API_VERSION", "1.18") # Must be sent on each call
APG_EMAIL = os.getenv("APG_EMAIL", "")                 # API user email (from APG)
APG_PASSWORD = os.getenv("APG_PASSWORD", "")           # API user password (from APG)
NZ_TZ  = ZoneInfo("Pacific/Auckland")
UTC_TZ = ZoneInfo("UTC")

# ---- Manual run ----
@api_bp.post("/sync/run")
def api_sync_run_once():
    logger = current_app.logger

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

    logger.info("Manual run requested: from=%s to=%s", date_from_utc, date_to_utc)

    initiated_by = request.args.get("by") or "web"
    run = SyncRun(
        started_at=datetime.utcnow(),
        run_type="manual",
        initiated_by=initiated_by,
    )
    db.session.add(run)
    db.session.commit()  # get run.id so we can link SyncFlightLog rows

    # --- Call the core sync with protection so we ALWAYS save a row ---
    try:
        res = run_sync_once_return_summary(
            date_from_utc=date_from_utc,
            date_to_utc=date_to_utc,
        ) or {}
    except Exception as e:
        logger.exception("Manual sync run crashed")

        run.finished_at = datetime.utcnow()
        run.ok = False

        run.created  = 0
        run.skipped  = 0
        run.warnings = 0

        msg = f"Sync crashed: {e!r}"
        run.error    = msg
        run.log_tail = msg

        run.window_from_local = None
        run.window_to_local   = None
        run.window_from_utc   = None
        run.window_to_utc     = None

        db.session.add(run)
        db.session.commit()
        return jsonify({"id": run.id, "ok": False, "error": msg}), 500

    logger.info("Manual sync summary: %r", res)

    # ---------- NEW: persist per-flight logs ----------
    flights = res.get("flights") or []   # or whatever key you used in the summary
    for f in flights:
        row = SyncFlightLog(
            sync_run_id=run.id,
            envision_flight_id=str(f.get("envision_flight_id") or ""),
            flight_no=f.get("flight_no"),
            adep=f.get("adep"),
            ades=f.get("ades"),
            eobt=f.get("eobt"),          # this is already a datetime in your log tail
            reg=f.get("reg"),
            aircraft_id=f.get("aircraft_id"),
            pic_name=f.get("pic_name"),
            pic_empno=f.get("pic_empno"),
            apg_pic_id=f.get("apg_pic_id"),
            result=f.get("result"),
            reason=f.get("reason"),
            warnings=f.get("warnings"),
        )
        db.session.add(row)
    # do NOT commit yet – we’ll commit once after updating the SyncRun below
    # --------------------------------------------------

    # --- Normalise counters and text fields ---
    created  = int(res.get("created")  or 0)
    skipped  = int(res.get("skipped")  or 0)
    warnings = int(res.get("warnings") or 0)

    error_msg = (res.get("error")    or "").strip()
    log_tail  = (res.get("log_tail") or "").strip()

    ok_flag = None
    if error_msg:
        ok_flag = False
    elif res.get("ok") is True:
        ok_flag = True
    elif res.get("ok") is False:
        ok_flag = False
    else:
        ok_flag = True

    if not log_tail:
        if error_msg:
            log_tail = error_msg
        elif created == 0 and skipped == 0 and warnings == 0:
            log_tail = "Sync completed – no flights found in this window."
        else:
            log_tail = (
                f"Sync completed – created={created}, skipped={skipped}, "
                f"warnings={warnings}."
            )

    run.finished_at = datetime.utcnow()
    run.ok          = ok_flag
    run.created     = created
    run.skipped     = skipped
    run.warnings    = warnings
    run.error       = error_msg or None
    run.log_tail    = log_tail

    run.window_from_local = res.get("window_from_local")
    run.window_to_local   = res.get("window_to_local")
    run.window_from_utc   = res.get("window_from_utc")
    run.window_to_utc     = res.get("window_to_utc")

    db.session.add(run)
    db.session.commit()   # commits BOTH SyncRun + all SyncFlightLog rows

    return jsonify({"id": run.id, "ok": run.ok})

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


def _list_from_envision_payload(payload):
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


def _parse_envision_dt_utc(value):
    if not value:
        return None
    try:
        s = str(value).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _split_flight_designator_and_number(fnum: str) -> tuple[str, str]:
    txt = str(fnum or "").strip().upper().replace(" ", "")
    # Supports designators like "3C" (digit+letter), not only letters.
    m = re.match(r"^(.+?)(\d+)$", txt)
    if m:
        return m.group(1), m.group(2)
    return "", txt


def _is_dcs_passenger_flown(p: dict) -> bool:
    raw = (
        p.get("DCSStatus")
        or p.get("DcsStatus")
        or p.get("Status")
        or p.get("status")
        or p.get("IataStatus")
        or p.get("StatusCode")
        or ""
    )
    s = str(raw).strip().upper()
    if not s:
        return False
    if "FLOWN" in s:
        return True
    return s in {"F", "FLW", "FLWN"}


def _count_passengers_for_envision(passengers: list[dict], flown_only: bool = False) -> dict:
    adult = child = infant = male = female = 0
    for p in passengers if isinstance(passengers, list) else []:
        if flown_only and not _is_dcs_passenger_flown(p):
            continue
        ptype = normalise_pax_type(p.get("PassengerType"))
        if ptype == "INF":
            infant += 1
        elif ptype == "CH":
            child += 1
        else:
            adult += 1

        g = str(p.get("Gender") or "").strip().upper()
        if g.startswith("M"):
            male += 1
        elif g.startswith("F"):
            female += 1

    total = adult + child + infant
    return {
        "total": total,
        "adult": adult,
        "male": male,
        "female": female,
        "child": child,
        "infant": infant,
    }


def _build_envision_pax_payload(flight_id: int, expected_counts: dict, actual_counts: dict | None, existing_actuals: dict) -> dict:
    payload = {
        "flightId": int(flight_id),
        "expected": int(expected_counts.get("total", 0)),
        "adult": int(existing_actuals.get("adult", 0)),
        "male": int(existing_actuals.get("male", 0)),
        "female": int(existing_actuals.get("female", 0)),
        "child": int(existing_actuals.get("child", 0)),
        "infant": int(existing_actuals.get("infant", 0)),
        "expectedAdult": int(expected_counts.get("adult", 0)),
        "expectedMale": int(expected_counts.get("male", 0)),
        "expectedFemale": int(expected_counts.get("female", 0)),
        "expectedChild": int(expected_counts.get("child", 0)),
        "expectedInfant": int(expected_counts.get("infant", 0)),
    }
    if actual_counts:
        payload.update({
            "adult": int(actual_counts.get("adult", 0)),
            "male": int(actual_counts.get("male", 0)),
            "female": int(actual_counts.get("female", 0)),
            "child": int(actual_counts.get("child", 0)),
            "infant": int(actual_counts.get("infant", 0)),
        })
    return payload


def _enable_passenger_sync_log_focus():
    """
    Silence noisy non-passenger loggers so debug output is focused on pax sync.
    """
    quiet = [
        "werkzeug",
        "apscheduler",
        "zenith_client",
        "dcs_api_client",
        "dcs_sync",
        "urllib3",
        "requests",
    ]
    old_levels = {}
    for name in quiet:
        lg = logging.getLogger(name)
        old_levels[name] = lg.level
        lg.setLevel(logging.CRITICAL)

    root_logger = logging.getLogger()
    old_root = root_logger.level
    root_logger.setLevel(logging.CRITICAL)

    app_logger = current_app.logger
    old_app = app_logger.level
    app_logger.setLevel(logging.CRITICAL)

    def _restore():
        for name, lvl in old_levels.items():
            logging.getLogger(name).setLevel(lvl)
        root_logger.setLevel(old_root)
        app_logger.setLevel(old_app)

    return _restore


def _manifest_upload_doc_id(resp: dict | None) -> str | None:
    if not isinstance(resp, dict):
        return None
    data = resp.get("data")
    if isinstance(data, dict):
        if data.get("doc_id") not in (None, ""):
            return str(data.get("doc_id"))
        docs = data.get("docs")
        if isinstance(docs, list) and docs:
            first = docs[0] or {}
            if isinstance(first, dict) and first.get("doc_id") not in (None, ""):
                return str(first.get("doc_id"))
    return None


def _peek_manifest_upload_version(plan_id: int) -> int:
    state = ManifestUploadState.query.filter_by(apg_plan_id=int(plan_id)).first()
    current = int(state.upload_count or 0) if state else 0
    return current + 1


def _record_manifest_upload_success(plan_id: int, doc_id: str | None = None) -> int:
    state = ManifestUploadState.query.filter_by(apg_plan_id=int(plan_id)).first()
    now = datetime.utcnow()
    if state is None:
        state = ManifestUploadState(
            apg_plan_id=int(plan_id),
            upload_count=1,
            last_doc_id=doc_id,
            created_at=now,
            updated_at=now,
        )
        db.session.add(state)
    else:
        state.upload_count = int(state.upload_count or 0) + 1
        state.last_doc_id = doc_id
        state.updated_at = now
    db.session.commit()
    return int(state.upload_count or 0)


@api_bp.route("/envision/environment", methods=["GET", "POST"])
def api_envision_environment():
    if request.method == "GET":
        return jsonify({"ok": True, "environment": get_envision_environment()})

    data = request.get_json(silent=True) or {}
    env_name = str(data.get("environment") or data.get("env") or "").strip().lower()
    if env_name not in {"base", "live", "prod", "test", "uat", "staging"}:
        return jsonify({"ok": False, "error": "environment must be 'base' or 'test'"}), 400

    try:
        effective = "test" if env_name in {"test", "uat", "staging"} else "base"
        session["envision_env"] = effective
        env = set_envision_environment(effective)
        return jsonify({"ok": True, "environment": env})
    except Exception as exc:
        current_app.logger.exception("Failed to switch Envision environment")
        return jsonify({"ok": False, "error": str(exc)}), 400


def run_envision_passenger_sync_once() -> dict:
    """
    Update Envision flight passenger counts from DCS.
    - Window: flights departing between now+24h and now+48h.
    - Update expected counts always.
    - Update actual counts only if DCS passenger status is flown.
    """
    restore_logs = _enable_passenger_sync_log_focus()
    try:
        auth = envision_authenticate()
        token = auth["token"]

        now_utc = datetime.now(timezone.utc)
        date_from_utc = now_utc + timedelta(hours=24)
        date_to_utc = now_utc + timedelta(hours=48)

        env_payload = envision_get_flights(token, date_from_utc, date_to_utc)
        flights = _list_from_envision_payload(env_payload)
        print(f"[PAX_SYNC] start window={date_from_utc.isoformat()} -> {date_to_utc.isoformat()} envision_flights={len(flights)}")

        updates = []
        ok_count = 0
        fail_count = 0

        for f in flights:
            flight_id = f.get("id")
            dep = (f.get("departurePlaceDescription") or "").strip().upper()
            fnum = f.get("flightNumberDescription") or ""
            if not flight_id or not dep or not fnum:
                continue

            designator, number = _split_flight_designator_and_number(str(fnum))
            if not number:
                continue

            dep_dt_utc = _parse_envision_dt_utc(f.get("departureScheduled") or f.get("departureEstimate"))
            if not dep_dt_utc:
                continue
            # DCS + Envision both operate on UTC timestamps for this sync flow.
            # Use UTC departure date to avoid day-boundary mismatches.
            dep_utc_date = dep_dt_utc.date().isoformat()

            try:
                dcs = fetch_dcs_for_flight(dep, dep_utc_date, designator, number, False)
                dcs_flights = dcs.get("Flights") if isinstance(dcs, dict) else []
                pax = (dcs_flights[0].get("Passengers") or []) if dcs_flights else []

                expected_counts = _count_passengers_for_envision(pax, flown_only=False)
                flown_counts = _count_passengers_for_envision(pax, flown_only=True)
                has_flown = int(flown_counts.get("total", 0)) > 0

                try:
                    existing = envision_get_flight_passengers(token, int(flight_id)) or {}
                except Exception:
                    existing = {}

                existing_actuals = {
                    "adult": int(existing.get("adult") or 0),
                    "male": int(existing.get("male") or 0),
                    "female": int(existing.get("female") or 0),
                    "child": int(existing.get("child") or 0),
                    "infant": int(existing.get("infant") or 0),
                }

                payload = _build_envision_pax_payload(
                    int(flight_id),
                    expected_counts,
                    flown_counts if has_flown else None,
                    existing_actuals,
                )
                print(
                    f"[PAX_SYNC] PUT /Flights/{int(flight_id)}/Passengers "
                    f"flight={str(fnum)} dep={dep} expected={expected_counts} "
                    f"actual_mode={'flown' if has_flown else 'preserve'} payload={payload}"
                )
                put_resp = envision_put_flight_passengers(token, int(flight_id), payload)
                print(f"[PAX_SYNC] PUT success flightId={int(flight_id)} response={put_resp}")
                ok_count += 1
                updates.append({
                    "ok": True,
                    "flightId": int(flight_id),
                    "flightNumber": str(fnum),
                    "dep": dep,
                    "expected": expected_counts,
                    "actualUpdated": has_flown,
                    "actual": flown_counts if has_flown else existing_actuals,
                    "response": put_resp,
                })
            except Exception as e:
                fail_count += 1
                print(f"[PAX_SYNC] flight update failed flightId={flight_id} flight={str(fnum)} dep={dep} error={e}")
                updates.append({
                    "ok": False,
                    "flightId": int(flight_id) if str(flight_id).isdigit() else flight_id,
                    "flightNumber": str(fnum),
                    "dep": dep,
                    "error": str(e),
                })

        result = {
            "ok": fail_count == 0,
            "window": {
                "fromUtc": date_from_utc.isoformat(),
                "toUtc": date_to_utc.isoformat(),
            },
            "updated": ok_count,
            "failed": fail_count,
            "updates": updates,
        }
        print(f"[PAX_SYNC] done updated={ok_count} failed={fail_count}")
        return result
    finally:
        restore_logs()


@api_bp.post("/envision/passenger_sync/run")
@api_bp.post("/api/envision/passenger_sync/run")  # legacy path
def api_envision_passenger_sync_run():
    try:
        res = run_envision_passenger_sync_once()
        return jsonify(res), (200 if res.get("ok") else 207)
    except Exception as e:
        current_app.logger.exception("Passenger sync run failed")
        return jsonify({"ok": False, "error": str(e)}), 500

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
    manifest_version = None

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
                pax_override=pax_list,
            )
        except Exception as e:
            current_app.logger.exception(
                "Manifest HTML build failed in api_dcs_push_to_apg"
            )
            manifest_error = f"Manifest HTML build failed: {e}"
            html = None

        # 2a) Generate PDF **from that HTML**
        if html is not None:
            try:
                manifest_pdf = generate_pdf_modern(html)
            except Exception as e:
                current_app.logger.exception("Manifest PDF generation failed")
                # Fallback to simpler renderer if Playwright/Chromium fails
                try:
                    manifest_pdf = generate_manifest_pdf_from_html(html)
                    manifest_error = f"Manifest PDF generation failed (primary), used fallback: {e}"
                except Exception as e2:
                    current_app.logger.exception("Manifest PDF fallback generation failed")
                    manifest_error = f"Manifest PDF generation failed: {e}; fallback failed: {e2}"
                    manifest_pdf = None

        # 2b) Upload to APG (non-fatal if it fails)
        if manifest_pdf is not None:
            flight_code = f"{designator}{flight_no}".upper()
            route_str   = f"{dep}-{ades or 'UNK'}"
            date_local  = nz_day.isoformat()
            manifest_version = _peek_manifest_upload_version(int(plan_id))
            filename    = f"{flight_code} - {route_str} - {date_local} v{manifest_version}.pdf"

            try:
                manifest_resp = apg_upload_manifest_pdf(
                    bearer=bearer,
                    plan_id=int(plan_id),
                    pdf_bytes=manifest_pdf,
                    filename=filename,
                )
                manifest_version = _record_manifest_upload_success(
                    int(plan_id),
                    _manifest_upload_doc_id(manifest_resp),
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
        "manifest_version": manifest_version,
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


def _preserve_change_type_crew_positions(token: str, flight_id: int, new_type_id: int) -> tuple[list[dict], dict]:
    """
    Build crewPositions payload for ChangeType by retaining only crew that matches
    the crew setup for the target journey/type + current model/reg.
    """
    base = envision_get_flight_times(token, flight_id) or {}
    model_id = int(base.get("flightModelId") or 0)
    reg_id = int(base.get("flightRegistrationId") or 0)

    setups = envision_get_crew_position_setups(token)
    items = envision_get_crew_position_setup_items(token)
    existing_crew = envision_get_flight_crew(token, flight_id)

    matching = [
        s for s in (setups if isinstance(setups, list) else [])
        if int(s.get("journeyTypeId") or 0) == int(new_type_id)
        and int(s.get("modelId") or 0) == model_id
    ]
    exact_setup = next((s for s in matching if int(s.get("regId") or 0) == reg_id and reg_id > 0), None)
    fallback_setup = next((s for s in matching if int(s.get("regId") or 0) == 0), None)
    chosen_setup = exact_setup or fallback_setup

    def normalize_all_current():
        out = []
        for c in existing_crew if isinstance(existing_crew, list) else []:
            employee_id = c.get("employeeId")
            crew_position_id = c.get("crewPositionId") or c.get("positionId")
            if employee_id in (None, "") or crew_position_id in (None, ""):
                continue
            try:
                out.append({
                    "id": int(c.get("id") or 0),
                    "employeeId": int(employee_id),
                    "crewPositionId": int(crew_position_id),
                })
            except (TypeError, ValueError):
                continue
        return out

    if not chosen_setup:
        return normalize_all_current(), {
            "mode": "fallback_all",
            "modelId": model_id,
            "regId": reg_id,
            "newTypeId": int(new_type_id),
            "setupId": None,
        }

    setup_id = int(chosen_setup.get("id") or 0)
    setup_items = [
        i for i in (items if isinstance(items, list) else [])
        if int(i.get("crewPositionSetupId") or 0) == setup_id
    ]

    allowed_counts: dict[int, int] = {}
    for it in setup_items:
        try:
            pos_id = int(it.get("crewPositionId") or 0)
            cnt = int(it.get("crewCount") or 0)
        except (TypeError, ValueError):
            continue
        if pos_id <= 0 or cnt <= 0:
            continue
        allowed_counts[pos_id] = allowed_counts.get(pos_id, 0) + cnt

    used_counts: dict[int, int] = {}
    kept = []
    for c in existing_crew if isinstance(existing_crew, list) else []:
        employee_id = c.get("employeeId")
        crew_position_id = c.get("crewPositionId") or c.get("positionId")
        if employee_id in (None, "") or crew_position_id in (None, ""):
            continue
        try:
            pos_id = int(crew_position_id)
            emp_id = int(employee_id)
            row_id = int(c.get("id") or 0)
        except (TypeError, ValueError):
            continue

        max_for_pos = int(allowed_counts.get(pos_id, 0))
        if max_for_pos <= 0:
            continue
        if used_counts.get(pos_id, 0) >= max_for_pos:
            continue
        used_counts[pos_id] = used_counts.get(pos_id, 0) + 1
        kept.append({
            "id": row_id,
            "employeeId": emp_id,
            "crewPositionId": pos_id,
        })

    return kept, {
        "mode": "filtered_by_setup",
        "modelId": model_id,
        "regId": reg_id,
        "newTypeId": int(new_type_id),
        "setupId": setup_id,
        "allowedCounts": allowed_counts,
        "keptCount": len(kept),
        "existingCount": len(existing_crew if isinstance(existing_crew, list) else []),
    }


@api_bp.post("/envision/flight_action")
def api_envision_flight_action():
    data = request.get_json(force=True) or {}
    action = str(data.get("action") or "").strip().lower()
    payload = data.get("payload") or {}
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "payload must be an object"}), 400

    flight_id_raw = data.get("flight_id") or payload.get("flightId") or payload.get("id")
    if flight_id_raw in (None, ""):
        return jsonify({"ok": False, "error": "Missing flight_id"}), 400
    try:
        flight_id = int(flight_id_raw)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Invalid flight_id"}), 400

    try:
        auth = envision_authenticate()
        token = auth["token"]
    except Exception as exc:
        current_app.logger.exception("api_envision_flight_action: Envision auth failed")
        return jsonify({"ok": False, "error": f"Envision auth failed: {exc}"}), 502

    try:
        if action == "update_flight":
            base = envision_get_flight_times(token, flight_id)
            update_body = {
                "id": int(base.get("id") or flight_id),
                "flightStatusId": payload.get("flightStatusId", base.get("flightStatusId") or 0),
                "departureEstimate": payload.get("departureEstimate", base.get("departureEstimate")),
                "departureActual": payload.get("departureActual", base.get("departureActual")),
                "departureTakeOff": payload.get("departureTakeOff", base.get("departureTakeOff")),
                "arrivalEstimate": payload.get("arrivalEstimate", base.get("arrivalEstimate")),
                "arrivalLanded": payload.get("arrivalLanded", base.get("arrivalLanded")),
                "arrivalActual": payload.get("arrivalActual", base.get("arrivalActual")),
                "plannedFlightTime": payload.get("plannedFlightTime", base.get("plannedFlightTime") or 0),
                "calculatedTakeOffTime": payload.get("calculatedTakeOffTime", base.get("calculatedTakeOffTime")),
            }
            result = envision_update_flight_times(token, flight_id, update_body)

        elif action == "change_registration":
            body = dict(payload)
            body["flightId"] = int(body.get("flightId") or flight_id)
            result = envision_change_registration(token, flight_id, body)

        elif action == "change_type":
            body = dict(payload)
            body["flightId"] = int(body.get("flightId") or flight_id)
            type_id = body.get("typeId") if body.get("typeId") not in (None, "") else body.get("type_id")
            if type_id in (None, ""):
                return jsonify({"ok": False, "error": "typeId is required for change_type"}), 400
            body["typeId"] = int(type_id)
            crew_positions = body.get("crewPositions")
            if isinstance(crew_positions, list) and crew_positions:
                body["crewPositions"] = crew_positions
            else:
                kept, crew_diag = _preserve_change_type_crew_positions(token, flight_id, int(type_id))
                body["crewPositions"] = kept
            result = envision_change_type(token, flight_id, body)
            if "crew_diag" in locals():
                if isinstance(result, dict):
                    result["crewFilter"] = crew_diag

        elif action == "cancel":
            body = dict(payload)
            body["flightId"] = int(body.get("flightId") or flight_id)
            result = envision_cancel_flight(token, flight_id, body)

        elif action == "divert":
            body = dict(payload)
            body["flightId"] = int(body.get("flightId") or flight_id)
            result = envision_divert_flight(token, flight_id, body)

        elif action == "delay_get":
            delay_id_raw = data.get("delay_id") or payload.get("id") or payload.get("delayId")
            if delay_id_raw in (None, ""):
                return jsonify({"ok": False, "error": "delay_id is required for delay_get"}), 400
            result = envision_get_delay(token, flight_id, int(delay_id_raw))

        elif action == "delay_put":
            delay_id_raw = data.get("delay_id") or payload.get("id") or payload.get("delayId")
            if delay_id_raw in (None, ""):
                return jsonify({"ok": False, "error": "delay_id is required for delay_put"}), 400
            body = dict(payload)
            body["id"] = int(body.get("id") or delay_id_raw)
            body["flightId"] = int(body.get("flightId") or flight_id)
            result = envision_put_delay(token, flight_id, int(delay_id_raw), body)

        elif action == "delay_post":
            delay_id_raw = data.get("delay_id") or payload.get("id") or payload.get("delayId")
            body = dict(payload)
            body["flightId"] = int(body.get("flightId") or flight_id)
            delay_id = int(delay_id_raw) if delay_id_raw not in (None, "") else None
            result = envision_post_delay(token, flight_id, body, delay_id=delay_id)

        else:
            return jsonify({"ok": False, "error": f"Unsupported action '{action}'"}), 400

    except requests.HTTPError as http_err:
        resp = http_err.response
        body = (resp.text or "")[:2000] if resp is not None else ""
        return jsonify({
            "ok": False,
            "error": f"Envision HTTP {resp.status_code if resp is not None else '?'}",
            "body": body,
        }), 502
    except Exception as exc:
        current_app.logger.exception("api_envision_flight_action failed action=%s flight_id=%s", action, flight_id)
        return jsonify({"ok": False, "error": str(exc)}), 500

    return jsonify({"ok": True, "action": action, "flight_id": flight_id, "result": result}), 200


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

    status_mode = (data.get("status_mode") or "exclude_booked").strip().lower()
    include_all_status = status_mode == "all"

    try:
        html, _flight_ctx = _build_manifest_html_and_ctx(
            dep=data.get("dep") or "",
            ades=data.get("ades") or "",
            date_str=data.get("date") or "",
            designator=data.get("designator") or "",
            raw_number=data.get("number") or data.get("flight_number") or "",
            reg=data.get("reg") or "",
            envision_flight_id=envision_flight_id,
            pax_override=data.get("pax_list") or [],
            include_all_status=include_all_status,
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
    pax_override: list[dict] | None = None,
    include_all_status: bool = False,
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

    def _pick_dcs_flight(rows: list[dict], expect_origin: str, expect_dest: str) -> dict | None:
        expect_origin = (expect_origin or "").upper()
        expect_dest = (expect_dest or "").upper()
        matched = None
        for row in rows or []:
            if (row.get("Origin") or "").upper() == expect_origin and (row.get("Destination") or "").upper() == expect_dest:
                matched = row
                break
        if matched is not None:
            if (matched.get("Passengers") or []):
                return matched
            # Through services can place pax on a longer-sector record.
            richest = max(rows or [], key=lambda r: len((r or {}).get("Passengers") or []), default=matched)
            if (richest or {}).get("Passengers"):
                return richest
            return matched
        return (rows or [None])[0]

    def _find_previous_manifest_sector() -> tuple[str | None, datetime | None]:
        full_no = f"{designator}{number}".replace(" ", "").upper()
        day = None
        try:
            day = date.fromisoformat(date_str) if date_str else None
        except Exception:
            day = None

        current_row = None
        if envision_flight_id is not None:
            current_row = SyncFlightLog.query.filter(
                SyncFlightLog.envision_flight_id == str(envision_flight_id)
            ).order_by(SyncFlightLog.eobt.desc()).first()

        base_q = SyncFlightLog.query.filter(
            SyncFlightLog.flight_no == full_no,
            SyncFlightLog.ades == dep,
        )
        if reg:
            base_q = base_q.filter(SyncFlightLog.reg == reg)
        if day is not None:
            base_q = base_q.filter(func.date(SyncFlightLog.eobt) == day)
        if current_row is not None and current_row.eobt is not None:
            base_q = base_q.filter(SyncFlightLog.eobt < current_row.eobt)

        prev_row = base_q.order_by(SyncFlightLog.eobt.desc()).first()
        if prev_row is None and day is not None:
            fallback_q = SyncFlightLog.query.filter(
                SyncFlightLog.flight_no == full_no,
                SyncFlightLog.ades == dep,
                func.date(SyncFlightLog.eobt) == day,
            )
            prev_row = fallback_q.order_by(SyncFlightLog.eobt.desc()).first()

        if prev_row is None:
            return None, None
        return (prev_row.adep or "").upper() or None, prev_row.eobt

    def _candidate_previous_manifest_sectors() -> list[tuple[str, datetime | None]]:
        full_no = f"{designator}{number}".replace(" ", "").upper()
        day = None
        try:
            day = date.fromisoformat(date_str) if date_str else None
        except Exception:
            day = None

        current_row = None
        if envision_flight_id is not None:
            current_row = SyncFlightLog.query.filter(
                SyncFlightLog.envision_flight_id == str(envision_flight_id)
            ).order_by(SyncFlightLog.eobt.desc()).first()

        q = SyncFlightLog.query.filter(
            SyncFlightLog.flight_no == full_no,
            SyncFlightLog.ades == dep,
        )
        if reg:
            q = q.filter(SyncFlightLog.reg == reg)
        if day is not None:
            q = q.filter(func.date(SyncFlightLog.eobt) == day)
        if current_row is not None and current_row.eobt is not None:
            q = q.filter(SyncFlightLog.eobt < current_row.eobt)

        rows = q.order_by(SyncFlightLog.eobt.desc()).all() or []
        out: list[tuple[str, datetime | None]] = []
        seen: set[str] = set()
        for row in rows:
            pdep = (row.adep or "").upper().strip()
            if not pdep or pdep == dep or pdep in seen:
                continue
            seen.add(pdep)
            out.append((pdep, row.eobt))
        return out

    def _build_manifest_passenger(r: dict, origin_code: str, dest_code: str) -> dict | None:
        if not include_all_status and not is_dcs_passenger_boarded_or_flown(r):
            return None

        dob, dob_fmt = _parse_dcs_dob(r.get("DateOfBirth"))
        age = _calc_age(dob)

        raw_ptype = (r.get("PassengerType") or "").strip().upper()
        ptype  = normalise_pax_type(raw_ptype)
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
        is_um = raw_ptype in {"UM", "UMN", "UMNR"} or ptype == "UMNR" or has_um_code or has_um_text
        weight_key = "UMNR" if is_um else (ptype or "AD")

        pax_weight_kg = float(
            PAX_STD_WEIGHTS_KG.get(
                weight_key,
                PAX_STD_WEIGHTS_KG.get("AD", 0.0)
            )
        )

        bag_weight = float(r.get("BaggageWeight") or 0.0)
        bag_pcs    = 1 if bag_weight > 0 else 0

        is_infant = ptype == "INF"
        is_child  = ptype == "CHD" or is_um
        is_adult  = not (is_infant or is_child)

        return {
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
            "origin": (r.get("__manifest_origin") or origin_code or dep).upper(),
            "dest": (r.get("__manifest_dest") or dest_code or ades).upper(),
            "ptype": ptype,
            "gender": gender,
            "pax_weight_kg": pax_weight_kg,
            "bags_pcs": bag_pcs,
            "bags_kg": bag_weight,
            "is_adult": is_adult,
            "is_child": is_child,
            "is_infant": is_infant,
            "is_um": is_um,
        }

    # 1) Fetch from DCS – we want full passenger list
    # First try with DCS-only status; if empty, fall back to full list.
    dcs_json = fetch_dcs_for_flight(
        dep_airport=dep,
        arr_airport=ades,
        flight_date=date_str,
        airline_designator=designator,
        flight_number=number,
        only_status=True,
    )

    flights = dcs_json.get("Flights") or []
    if flights:
        dcs_f = _pick_dcs_flight(flights, dep, ades)
        if not (dcs_f.get("Passengers") or []):
            # No pax with OnlyDCSStatus=True, retry with full list.
            dcs_json = fetch_dcs_for_flight(
                dep_airport=dep,
                arr_airport=ades,
                flight_date=date_str,
                airline_designator=designator,
                flight_number=number,
                only_status=False,
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
    dcs_f = _pick_dcs_flight(flights, dep, ades) or flights[0]

    # 2) Build passenger list
    # If the UI already supplied a merged pax list (through-sectors included),
    # prefer that to keep manifest aligned with the Gantt card totals.
    raw_passengers = (pax_override or []) if isinstance(pax_override, list) and pax_override else (dcs_f.get("Passengers", []) or [])
    passengers: list[dict] = []
    filtered_out_booked = 0
    seen_manifest_keys: set[str] = set()
    for r in raw_passengers:
        manifest_pax = _build_manifest_passenger(
            r,
            dep,
            ades,
        )
        if manifest_pax is None:
            filtered_out_booked += 1
            continue
        pax_key = f"{manifest_pax['pnr']}|{manifest_pax['seat']}|{manifest_pax['name']}".upper()
        seen_manifest_keys.add(pax_key)
        passengers.append(manifest_pax)

    if not (isinstance(pax_override, list) and pax_override):
        prev_dep, prev_eobt = _find_previous_manifest_sector()
        prev_candidates = _candidate_previous_manifest_sectors()
        if prev_dep and prev_dep != dep and not any(p == prev_dep for p, _ in prev_candidates):
            prev_candidates.insert(0, (prev_dep, prev_eobt))

        for cand_dep, cand_eobt in prev_candidates:
            if not cand_dep or cand_dep == dep:
                continue
            try:
                upstream_json = fetch_dcs_for_flight(
                    dep_airport=cand_dep,
                    arr_airport=ades,
                    flight_date=date_str,
                    airline_designator=designator,
                    flight_number=number,
                    only_status=True,
                )
                upstream_flights = upstream_json.get("Flights") or []
                upstream_f = _pick_dcs_flight(upstream_flights, cand_dep, ades)
                if upstream_f and not (upstream_f.get("Passengers") or []):
                    upstream_json = fetch_dcs_for_flight(
                        dep_airport=cand_dep,
                        arr_airport=ades,
                        flight_date=date_str,
                        airline_designator=designator,
                        flight_number=number,
                        only_status=False,
                    )
                    upstream_flights = upstream_json.get("Flights") or []
                    upstream_f = _pick_dcs_flight(upstream_flights, cand_dep, ades)

                upstream_added = 0
                for r in (upstream_f or {}).get("Passengers", []) or []:
                    manifest_pax = _build_manifest_passenger(
                        r,
                        upstream_f.get("Origin") or cand_dep,
                        upstream_f.get("Destination") or ades,
                    )
                    if manifest_pax is None:
                        continue
                    pax_key = f"{manifest_pax['pnr']}|{manifest_pax['seat']}|{manifest_pax['name']}".upper()
                    if pax_key in seen_manifest_keys:
                        continue
                    seen_manifest_keys.add(pax_key)
                    passengers.append(manifest_pax)
                    upstream_added += 1

                current_app.logger.info(
                    "Manifest through-pax merge: flight=%s%s date=%s current=%s-%s upstream=%s-%s prior_eobt=%s added=%s",
                    designator,
                    number,
                    date_str,
                    dep,
                    ades,
                    cand_dep,
                    ades,
                    cand_eobt,
                    upstream_added,
                )
            except Exception:
                current_app.logger.exception(
                    "Manifest through-pax merge failed for %s%s %s %s-%s via previous dep %s",
                    designator,
                    number,
                    date_str,
                    dep,
                    ades,
                    cand_dep,
                )

    passengers.sort(key=lambda p: _seat_sort_key(p["seat"]))
    status_counts: dict[str, int] = {}
    for rp in raw_passengers:
        sval = str(
            rp.get("Status")
            or rp.get("DCSStatus")
            or rp.get("DcsStatus")
            or ""
        ).strip() or "<blank>"
        status_counts[sval] = status_counts.get(sval, 0) + 1
    if not include_all_status:
        current_app.logger.info(
            "Manifest preview status filter: mode=exclude_booked raw=%s kept=%s filtered=%s statuses=%s flight=%s%s date=%s",
            len(raw_passengers),
            len(passengers),
            filtered_out_booked,
            status_counts,
            designator,
            number,
            date_str,
        )

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





@api_bp.get("/envision/flight_crew")
@api_bp.get("/api/envision/flight_crew")  # legacy path (kept for backward compatibility)
def api_envision_flight_crew():
    """
    Lightweight wrapper around fetch_envision_crew(envision_flight_id)
    so the Gantt / flight details modal can show crew.
    """
    flight_id = request.args.get("flight_id", type=int)
    if not flight_id:
        return jsonify(ok=False, error="Missing flight_id"), 400

    debug = request.args.get("debug") == "1"
    try:
        crew = fetch_envision_crew_for_apg(flight_id)
        payload = {"ok": True, "crew": crew}
        if debug:
            # Lightweight diagnostics: count + raw response from Envision
            env_auth = envision_authenticate()
            env_token = env_auth["token"]
            raw = envision_get_flight_crew(env_token, flight_id)
            payload["diag"] = {
                "raw_count": len(raw) if isinstance(raw, list) else None,
                "raw_type": type(raw).__name__,
                "raw_preview": raw[:5] if isinstance(raw, list) else raw,
            }
        # crew is already in the nice form:
        # { position, name, employee_id, is_operating }
        return jsonify(payload)
    except Exception as e:
        current_app.logger.exception(
            "flight_crew failed for Envision flight_id=%s", flight_id
        )
        return jsonify(ok=False, error=str(e)), 500


@api_bp.get("/envision/flight_crew_raw")
@api_bp.get("/api/envision/flight_crew_raw")  # legacy path
def api_envision_flight_crew_raw():
    """
    Debug: return the raw Envision /Flights/{id}/Crew payload for a single flight.
    """
    flight_id = request.args.get("flight_id", type=int)
    if not flight_id:
        return jsonify(ok=False, error="Missing flight_id"), 400

    try:
        auth = envision_authenticate()
        token = auth["token"]
        raw = envision_get_flight_crew(token, flight_id)
        return jsonify(ok=True, flight_id=flight_id, raw=raw)
    except Exception as e:
        current_app.logger.exception(
            "flight_crew_raw failed for Envision flight_id=%s", flight_id
        )
        return jsonify(ok=False, error=str(e)), 500


@api_bp.get("/envision/line_registrations")
@api_bp.get("/api/envision/line_registrations")  # legacy path
def api_envision_line_registrations():
    """
    Proxy Envision GET /v1/Lines/Registrations for frontend dropdowns.
    """
    try:
        auth = envision_authenticate()
        token = auth["token"]
        items = envision_get_line_registrations(token)
        return jsonify(ok=True, items=items)
    except Exception as e:
        current_app.logger.exception("line_registrations failed")
        return jsonify(ok=False, error=str(e)), 502


@api_bp.get("/envision/flight_notes")
@api_bp.get("/api/envision/flight_notes")  # legacy path
def api_envision_flight_notes():
    flight_id = request.args.get("flight_id", type=int)
    if not flight_id:
        return jsonify(ok=False, error="Missing flight_id"), 400
    crew_view = str(request.args.get("crew_view") or "").strip().lower() in {"1", "true", "yes", "on"}
    try:
        auth = envision_authenticate()
        token = auth["token"]
        notes = envision_get_flight_notes(token, flight_id, crew_view=crew_view)
        return jsonify(ok=True, notes=notes)
    except Exception as e:
        current_app.logger.exception("flight_notes failed")
        return jsonify(ok=False, error=str(e)), 502


@api_bp.post("/envision/flight_notes_upsert")
@api_bp.post("/api/envision/flight_notes_upsert")  # legacy path
def api_envision_flight_notes_upsert():
    data = request.get_json(force=True) or {}
    flight_id = data.get("flight_id")
    note_id = data.get("note_id")
    note_type_id = data.get("note_type_id")
    text = data.get("text")
    is_important = bool(data.get("is_important", False))

    if not flight_id:
        return jsonify(ok=False, error="Missing flight_id"), 400
    try:
        flight_id = int(flight_id)
    except Exception:
        return jsonify(ok=False, error="Invalid flight_id"), 400

    text = (str(text or "")).strip()
    if text == "":
        return jsonify(ok=False, error="text is required"), 400

    try:
        auth = envision_authenticate()
        token = auth["token"]
        if note_id not in (None, ""):
            note_id = int(note_id)
            if note_type_id in (None, ""):
                notes = envision_get_flight_notes(token, flight_id, crew_view=False)
                found = next((n for n in notes if int(n.get("id") or 0) == note_id), None)
                note_type_id = int(found.get("noteTypeId")) if found and found.get("noteTypeId") is not None else int(os.getenv("ENVISION_DEFAULT_NOTE_TYPE_ID", "1"))
            payload = {
                "id": note_id,
                "flightId": flight_id,
                "noteTypeId": int(note_type_id),
                "text": text,
                "isImportant": is_important,
            }
            result = envision_put_flight_note(token, flight_id, note_id, payload)
            return jsonify(ok=True, mode="put", note=result, sent=payload)
        else:
            note_type_id = int(note_type_id) if note_type_id not in (None, "") else int(os.getenv("ENVISION_DEFAULT_NOTE_TYPE_ID", "1"))
            payload = {
                "flightId": flight_id,
                "noteTypeId": note_type_id,
                "text": text,
                "isImportant": is_important,
            }
            result = envision_post_flight_note(token, flight_id, payload)
            return jsonify(ok=True, mode="post", note=result, sent=payload)
    except Exception as e:
        current_app.logger.exception("flight_notes_upsert failed")
        return jsonify(ok=False, error=str(e)), 502


@api_bp.get("/envision/flight_types")
@api_bp.get("/api/envision/flight_types")  # legacy path
def api_envision_flight_types():
    """
    Proxy Envision GET /v1/Flights/Types.
    """
    try:
        auth = envision_authenticate()
        token = auth["token"]
        items = envision_get_flight_types(token)
        return jsonify(ok=True, items=items)
    except Exception as e:
        current_app.logger.exception("flight_types failed")
        return jsonify(ok=False, error=str(e)), 502


@api_bp.get("/envision/flight_raw")
@api_bp.get("/api/envision/flight_raw")  # legacy path
def api_envision_flight_raw():
    """
    Debug: return the raw Envision /Flights/{id} payload for a single flight.
    """
    flight_id = request.args.get("flight_id", type=int)
    if not flight_id:
        return jsonify(ok=False, error="Missing flight_id"), 400

    try:
        auth = envision_authenticate()
        token = auth["token"]
        raw = envision_get_flight_times(token, flight_id)
        return jsonify(ok=True, flight_id=flight_id, raw=raw)
    except Exception as e:
        current_app.logger.exception(
            "flight_raw failed for Envision flight_id=%s", flight_id
        )
        return jsonify(ok=False, error=str(e)), 500


@api_bp.get("/envision/flights_raw")
@api_bp.get("/api/envision/flights_raw")  # legacy path
def api_envision_flights_raw():
    """
    Debug: return the raw Envision /Flights payload for a date window.
    Only dateFrom/dateTo are passed through (no offset/limit).
    """
    date_from = request.args.get("dateFrom") or request.args.get("date_from")
    date_to = request.args.get("dateTo") or request.args.get("date_to")
    if not date_from or not date_to:
        return jsonify(ok=False, error="Missing dateFrom or dateTo"), 400

    try:
        auth = envision_authenticate()
        token = auth["token"]
        headers = {"Authorization": f"Bearer {token}"}
        params = {"dateFrom": date_from, "dateTo": date_to}
        url = f"{get_envision_environment().get('base', ENVISION_BASE)}/Flights"
        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        raw = r.json()
        return jsonify(ok=True, dateFrom=date_from, dateTo=date_to, raw=raw)
    except Exception as e:
        current_app.logger.exception(
            "flights_raw failed for Envision dateFrom=%s dateTo=%s", date_from, date_to
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
