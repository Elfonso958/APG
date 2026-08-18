# app/__init__.py
from dotenv import load_dotenv
from werkzeug.middleware.proxy_fix import ProxyFix
import os

# Resolve "<project_root>/.env" no matter where we run from
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)

# app/__init__.py
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
from sqlalchemy import inspect
from werkzeug.middleware.proxy_fix import ProxyFix
import threading
import logging
import os

db = SQLAlchemy()
migrate = Migrate()

# single-process lock & scheduler
_run_lock = threading.Lock()
_scheduler: BackgroundScheduler | None = None


class PrefixHeaderMiddleware:
    """
    Normalise proxy prefix headers before ProxyFix applies them to the WSGI environ.
    """

    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        forwarded_prefix = environ.get("HTTP_X_FORWARDED_PREFIX", "").strip()
        script_name = environ.get("HTTP_X_SCRIPT_NAME", "").strip()
        prefix = forwarded_prefix or script_name

        if prefix:
            environ["HTTP_X_FORWARDED_PREFIX"] = "/" + prefix.strip("/")

        return self.app(environ, start_response)


def _normalise_application_root(value: str | None) -> str:
    if not value:
        return ""

    cleaned = "/" + str(value).strip().strip("/")
    return "" if cleaned == "/" else cleaned

def _should_start_scheduler(app: Flask) -> bool:
    """Avoid starting the scheduler twice under the debug reloader."""
    if not app.debug:
        return True
    return os.environ.get("WERKZEUG_RUN_MAIN") == "true"


def _normalise_sync_result(res: dict | None) -> dict:
    res = res or {}

    created = int(res.get("created") or 0)
    skipped = int(res.get("skipped") or 0)
    warnings = int(res.get("warnings") or 0)
    flights = res.get("flights") or []
    successful_events = sum(
        1 for f in flights
        if f.get("result") in {"created", "updated", "deleted", "skipped"}
    )
    failed_events = sum(1 for f in flights if f.get("result") == "failed")

    if created == 0 and skipped == 0 and flights:
        created = sum(1 for f in flights if f.get("result") in {"created", "updated"})
        skipped = sum(1 for f in flights if f.get("result") == "skipped")

    error_msg = (res.get("error") or "").strip()
    log_tail = (res.get("log_tail") or "").strip()

    if error_msg:
        ok_flag = False
    elif res.get("ok") is False:
        ok_flag = False
    elif failed_events and not successful_events:
        ok_flag = False
    else:
        ok_flag = True

    if not log_tail:
        if error_msg:
            log_tail = error_msg
        elif failed_events and not successful_events:
            log_tail = f"Sync finished with {failed_events} failed flight event(s)."
        elif created == 0 and skipped == 0 and warnings == 0:
            if successful_events:
                log_tail = (
                    f"Sync completed - {successful_events} flight event(s) recorded, "
                    "but summary counters were missing."
                )
            else:
                log_tail = "Sync completed - no flights found in this window."
        else:
            log_tail = (
                f"Sync completed - created={created}, skipped={skipped}, "
                f"warnings={warnings}."
            )

    return {
        "ok": ok_flag,
        "created": created,
        "skipped": skipped,
        "warnings": warnings,
        "error": error_msg or None,
        "log_tail": log_tail,
    }

def create_app():
    app = Flask(__name__, static_folder="static", template_folder="templates")
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1, x_prefix=1)
    app.config.setdefault("DCS_MAX_WORKERS", 12)  # tune as needed, e.g. 4–10

    noisy_loggers = [
        "zenith_client",   # [DCS] logs
        "apscheduler",     # scheduler lifecycle logs
        "dcs_api_client",
        "dcs_sync",
        "werkzeug",        # dev server/access logs
        "urllib3",
        "requests",
    ]

    for name in noisy_loggers:
        lg = logging.getLogger(name)
        lg.setLevel(logging.CRITICAL)
        lg.propagate = False

    # Keep app logs quiet by default; pax sync debug uses explicit [PAX_SYNC] prints.
    app.logger.setLevel(logging.WARNING)
    
    # 0) Load config FIRST so everything else can use it
    from .config import Config  # <-- adjust path if needed
    app.config.from_object(Config)
    # Optional: allow an extra config file path via env var
    if os.environ.get("APP_SETTINGS"):
        app.config.from_envvar("APP_SETTINGS", silent=True)

    app.config["APPLICATION_ROOT"] = _normalise_application_root(
        app.config.get("APPLICATION_ROOT")
    )
    app.wsgi_app = PrefixHeaderMiddleware(app.wsgi_app)
    app.wsgi_app = ProxyFix(
        app.wsgi_app,
        x_for=1,
        x_proto=1,
        x_host=1,
        x_port=1,
        x_prefix=1,
    )

    # ---- DB config (keep if you still want to override)
    app.config.setdefault("SQLALCHEMY_DATABASE_URI", "sqlite:///apg_importer.db")
    app.config.setdefault("SQLALCHEMY_TRACK_MODIFICATIONS", False)
    app.secret_key = app.config.get("SECRET_KEY", "dev")
    app.permanent_session_lifetime = timedelta(hours=8)

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
        from .models import AppConfig, SyncRun, SyncFlightLog, EnvisionOtpFlightCache  # noqa: F401
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

                    res = run_sync_once_return_summary() or {}
                    outcome = _normalise_sync_result(res)

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

                    run.finished_at = datetime.utcnow()
                    run.ok = outcome["ok"]
                    run.created = outcome["created"]
                    run.skipped = outcome["skipped"]
                    run.warnings = outcome["warnings"]
                    run.log_tail = outcome["log_tail"]
                    run.error = outcome["error"]
                    run.window_from_local = res.get("window_from_local")
                    run.window_to_local   = res.get("window_to_local")
                    run.window_from_utc   = res.get("window_from_utc")
                    run.window_to_utc     = res.get("window_to_utc")
                    _db.session.add(run)
                    _db.session.commit()

                    cfg.last_auto_finished = datetime.utcnow()
                    _db.session.add(cfg); _db.session.commit()

                except Exception:
                    logging.exception("Auto job failed")
                finally:
                    _run_lock.release()

        def _run_passenger_sync_job():
            with app.app_context():
                try:
                    from .routes import run_envision_passenger_sync_once
                    result = run_envision_passenger_sync_once()
                    app.logger.info(
                        "Passenger sync job finished: ok=%s updated=%s failed=%s",
                        bool(result.get("ok")),
                        int(result.get("updated") or 0),
                        int(result.get("failed") or 0),
                    )
                except Exception:
                    logging.exception("Passenger sync scheduled job failed")

        def _run_otp_cache_refresh_job():
            from .otp_cache_job import rolling_otp_cache_dates, start_otp_cache_job

            date_from, date_to = rolling_otp_cache_dates(lookback_days=2)
            started, status = start_otp_cache_job(
                app,
                date_from=date_from,
                date_to=date_to,
                window_days=3,
                chunk_days=1,
                page_size=100,
                include_details=False,
            )
            if started:
                app.logger.info("OTP cache refresh started for %s to %s", date_from, date_to)
            else:
                app.logger.warning(
                    "OTP cache refresh skipped because a refresh is already running: %s",
                    status.get("currentWindow") or "starting",
                )

        def _ensure_otp_cache_job():
            if _scheduler.get_job("envision_otp_cache_four_daily") is None:
                _scheduler.add_job(
                    _run_otp_cache_refresh_job,
                    "cron",
                    hour="0,6,12,18",
                    minute=0,
                    timezone="Pacific/Auckland",
                    id="envision_otp_cache_four_daily",
                    replace_existing=True,
                    coalesce=True,
                    max_instances=1,
                )

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
                _scheduler.add_job(
                    _run_passenger_sync_job,
                    "cron",
                    hour=12,
                    minute=0,
                    timezone="Pacific/Auckland",
                    id="envision_pax_sync_daily",
                    replace_existing=True,
                )
                _ensure_otp_cache_job()
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
                if _scheduler.get_job("envision_pax_sync_daily") is None:
                    _scheduler.add_job(
                        _run_passenger_sync_job,
                        "cron",
                        hour=12,
                        minute=0,
                        timezone="Pacific/Auckland",
                        id="envision_pax_sync_daily",
                        replace_existing=True,
                    )
                _ensure_otp_cache_job()

        _start_or_reschedule_scheduler()

    return app
