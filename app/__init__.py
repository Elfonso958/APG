# app/__init__.py
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from sqlalchemy import inspect
import threading
import logging
import os

db = SQLAlchemy()
migrate = Migrate()

# single-process lock & scheduler
_run_lock = threading.Lock()
_scheduler: BackgroundScheduler | None = None

def _should_start_scheduler(app: Flask) -> bool:
    """Avoid starting the scheduler twice under the debug reloader."""
    if not app.debug:
        return True
    return os.environ.get("WERKZEUG_RUN_MAIN") == "true"

def create_app():
    app = Flask(__name__, static_folder="static", template_folder="templates")
    app.config.setdefault("DCS_MAX_WORKERS", 12)  # tune as needed, e.g. 4–10

    noisy_loggers = [
    "zenith_client",   # [DCS] POST ... status logs
    "apscheduler",     # "Scheduler started", "Added job..."
    "dcs_api_client",  # [DCS] INFO in ...
    "dcs_sync",        # [DCS] INFO in ...

    #"werkzeug",        # 127.0.0.1 - - GET /... access logs
    # Add any others you see in the "INFO in X" lines
    # e.g. "routes" or your APG sync logger name if you have one
    ]

    for name in noisy_loggers:
        logging.getLogger(name).setLevel(logging.WARNING)
    
    # 0) Load config FIRST so everything else can use it
    from .config import Config  # <-- adjust path if needed
    app.config.from_object(Config)
    # Optional: allow an extra config file path via env var
    if os.environ.get("APP_SETTINGS"):
        app.config.from_envvar("APP_SETTINGS", silent=True)

    # ---- DB config (keep if you still want to override)
    app.config.setdefault("SQLALCHEMY_DATABASE_URI", "sqlite:///apg_importer.db")
    app.config.setdefault("SQLALCHEMY_TRACK_MODIFICATIONS", False)
    app.secret_key = app.config.get("SECRET_KEY", "dev")

    # Log (masked) DCS config so you can confirm it’s loaded
    dcs_base = app.config.get("DCS_API_BASE")
    dcs_path = app.config.get("DCS_API_FLIGHTS_PATH")
    dcs_key  = app.config.get("DCS_API_KEY") or ""
    app.logger.info(
        "[BOOT] DCS base=%s path=%s key=%s",
        dcs_base,
        dcs_path,
        (dcs_key[:4] + "…" + dcs_key[-4:]) if dcs_key else "(missing)"
    )

    # ---- init extensions
    db.init_app(app)
    migrate.init_app(app, db)

    # ---- blueprints
    from .routes import api_bp
    from .views import ui_bp
    app.register_blueprint(api_bp, url_prefix="/api")
    app.register_blueprint(ui_bp)
    
    with app.app_context():
        from .models import AppConfig, SyncRun, SyncFlightLog  # noqa: F401
        from .sync.envision_apg_sync import run_sync_once_return_summary
        from . import db as _db

        def _tables_ready() -> bool:
            try:
                insp = inspect(_db.engine)
                return (
                    insp.has_table("app_config")
                    and insp.has_table("sync_runs")
                    and insp.has_table("sync_flight_logs")
                )
            except Exception:
                return False

        def _ensure_config_row():
            if not _tables_ready():
                app.logger.info("Tables not ready yet; skipping AppConfig bootstrap.")
                return None
            cfg = AppConfig.query.get(1)
            if not cfg:
                cfg = AppConfig(id=1, auto_enabled=False, interval_sec=300)
                _db.session.add(cfg); _db.session.commit()
            return cfg

        # inside create_app(), where _run_sync_job_auto is defined…

        def _run_sync_job_auto():
            # run DB/Flask things inside app context
            with app.app_context():
                if not _run_lock.acquire(blocking=False):
                    logging.info("Auto job skipped: previous run still in progress.")
                    return
                try:
                    from .models import AppConfig, SyncRun, SyncFlightLog
                    from . import db as _db
                    from .sync.envision_apg_sync import run_sync_once_return_summary

                    cfg = AppConfig.query.get(1)
                    if not cfg or not cfg.auto_enabled:
                        logging.info("Auto job disabled—no-op.")
                        return

                    cfg.last_auto_started = datetime.utcnow()
                    _db.session.add(cfg); _db.session.commit()

                    run = SyncRun(started_at=datetime.utcnow(), run_type="auto", initiated_by="scheduler")
                    _db.session.add(run); _db.session.commit()

                    res = run_sync_once_return_summary()

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
                    _db.session.add(run); _db.session.commit()

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

                            # PIC
                            pic_name=ev.get("pic_name"),
                            pic_empno=ev.get("pic_empno"),
                            apg_pic_id=ev.get("apg_pic_id"),

                            # FO
                            fo_name=ev.get("fo_name"),
                            fo_empno=ev.get("fo_empno"),
                            apg_fo_id=ev.get("apg_fo_id"),

                            # CC
                            cc_names=ev.get("cc_names"),
                            cc_empnos=ev.get("cc_empnos"),
                            apg_cc_ids=ev.get("apg_cc_ids"),

                            result=ev.get("result"),
                            reason=ev.get("reason"),
                            warnings=ev.get("warnings"),
                        )
                        _db.session.add(row)

                    _db.session.commit()

                    cfg.last_auto_finished = datetime.utcnow()
                    _db.session.add(cfg); _db.session.commit()

                except Exception:
                    logging.exception("Auto job failed")
                finally:
                    _run_lock.release()

        def _start_or_reschedule_scheduler():
            # allow disabling during migrations/ops
            if os.environ.get("DISABLE_SCHEDULER") == "1":
                app.logger.info("Scheduler disabled by DISABLE_SCHEDULER=1")
                return
            if not _tables_ready():
                app.logger.info("Skipping scheduler init: tables not created yet.")
                return
            global _scheduler
            cfg = _ensure_config_row()
            interval = max(int((cfg.interval_sec if cfg else 300)), 60)
            if _scheduler is None:
                _scheduler = BackgroundScheduler(daemon=True, timezone="UTC")
                _scheduler.add_job(
                    _run_sync_job_auto, "interval",
                    seconds=interval, id="sync_auto_job", replace_existing=True
                )
                if _should_start_scheduler(app):
                    _scheduler.start()
                    app.logger.info(f"Scheduler started (interval={interval}s)")
            else:
                try:
                    _scheduler.reschedule_job("sync_auto_job", trigger="interval", seconds=interval)
                    app.logger.info(f"Scheduler rescheduled (interval={interval}s)")
                except Exception:
                    _scheduler.add_job(
                        _run_sync_job_auto, "interval",
                        seconds=interval, id="sync_auto_job", replace_existing=True
                    )
                    app.logger.info(f"Scheduler job added (interval={interval}s)")

        _start_or_reschedule_scheduler()

    return app
