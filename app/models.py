from . import db
from datetime import datetime


class SyncRun(db.Model):
    __tablename__ = "sync_runs"

    id = db.Column(db.Integer, primary_key=True)
    started_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    finished_at = db.Column(db.DateTime)

    # Time window used for the Envision fetch
    window_from_local = db.Column(db.DateTime)
    window_to_local   = db.Column(db.DateTime)
    window_from_utc   = db.Column(db.DateTime)
    window_to_utc     = db.Column(db.DateTime)

    # Outcome
    ok = db.Column(db.Boolean, default=False)
    created = db.Column(db.Integer)
    skipped = db.Column(db.Integer)
    warnings = db.Column(db.Integer)

    # Logs
    log_tail = db.Column(db.Text)
    error = db.Column(db.Text)

    # NEW: manual / auto, and who kicked it off
    run_type = db.Column(db.String(16), default="manual", index=True)  # "manual" | "auto"
    initiated_by = db.Column(db.String(64), nullable=True)
    flights = db.relationship("SyncFlightLog", backref="run", lazy=True, cascade="all,delete-orphan")


class SyncFlightLog(db.Model):
    __tablename__ = "sync_flight_logs"

    id = db.Column(db.Integer, primary_key=True)
    sync_run_id = db.Column(db.Integer, db.ForeignKey("sync_runs.id"), nullable=False)

    envision_flight_id = db.Column(db.String(32), index=True)
    flight_no = db.Column(db.String(16), index=True)
    adep = db.Column(db.String(8), index=True)
    ades = db.Column(db.String(8), index=True)
    eobt = db.Column(db.DateTime, index=True)

    reg = db.Column(db.String(16), index=True)
    aircraft_id = db.Column(db.Integer)

    pic_name = db.Column(db.String(128))
    pic_empno = db.Column(db.String(32), index=True)
    apg_pic_id = db.Column(db.Integer)

    # FO
    fo_name = db.Column(db.String(128))
    fo_empno = db.Column(db.String(32), index=True)
    apg_fo_id = db.Column(db.Integer)

    # Cabin Crew (you can normalise this later if multiple, but simplest is flat fields)
    cc_names = db.Column(db.Text)       # comma-separated list
    cc_empnos = db.Column(db.Text)      # comma-separated list
    apg_cc_ids = db.Column(db.Text)     # comma-separated list of ints

    result = db.Column(db.String(16), index=True)      # created|updated|skipped|failed
    reason = db.Column(db.String(256))                 # explanation for skip/fail
    warnings = db.Column(db.Text)                      # JSON/text from APG if any
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

# NEW: simple key/value schedule settings (singleton row: id=1)
class AppConfig(db.Model):
    __tablename__ = "app_config"
    id = db.Column(db.Integer, primary_key=True, default=1)
    auto_enabled = db.Column(db.Boolean, default=False, nullable=False)
    interval_sec = db.Column(db.Integer, default=300, nullable=False)  # default 5 min
    last_auto_started = db.Column(db.DateTime, nullable=True)
    last_auto_finished = db.Column(db.DateTime, nullable=True)