from flask import Blueprint, request
from datetime import datetime
from . import db
from .models import SyncRun, SyncFlightLog, AppConfig
from .sync.envision_apg_sync import run_sync_once_return_summary
import logging

api_bp = Blueprint("api", __name__)

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
