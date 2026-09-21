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


class SyncFlightState(db.Model):
    __tablename__ = "sync_flight_state"

    id = db.Column(db.Integer, primary_key=True)
    envision_flight_id = db.Column(db.String(32), unique=True, index=True, nullable=False)

    # Persisted snapshot for diffing
    core_json = db.Column(db.Text)          # JSON-serialized core
    fp = db.Column(db.String(128))          # fingerprint
    apg_id = db.Column(db.Integer)          # last known APG plan id

    last_run_id = db.Column(db.Integer, db.ForeignKey("sync_runs.id"), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class ManifestUploadState(db.Model):
    __tablename__ = "manifest_upload_state"

    id = db.Column(db.Integer, primary_key=True)
    apg_plan_id = db.Column(db.Integer, unique=True, index=True, nullable=False)
    upload_count = db.Column(db.Integer, default=0, nullable=False)
    last_doc_id = db.Column(db.String(64), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class CharterManifest(db.Model):
    __tablename__ = "charter_manifests"

    id = db.Column(db.Integer, primary_key=True)
    envision_flight_id = db.Column(db.String(32), unique=True, index=True, nullable=False)
    flight_no = db.Column(db.String(16), index=True, nullable=True)
    dep = db.Column(db.String(8), nullable=True)
    ades = db.Column(db.String(8), nullable=True)
    gate = db.Column(db.String(16), nullable=True)
    pax_json = db.Column(db.Text, nullable=False, default="[]")
    uploaded_filename = db.Column(db.String(255), nullable=True)
    closed_at = db.Column(db.DateTime, nullable=True)
    closure_email_sent_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class CharterBrief(db.Model):
    """A controlled operational brief. Detail is stored as JSON so the form can evolve safely."""
    __tablename__ = "charter_briefs"

    id = db.Column(db.Integer, primary_key=True)
    reference = db.Column(db.String(48), unique=True, index=True, nullable=False)
    title = db.Column(db.String(255), nullable=False)
    charterer = db.Column(db.String(255), nullable=True)
    start_date = db.Column(db.Date, nullable=True, index=True)
    end_date = db.Column(db.Date, nullable=True, index=True)
    status = db.Column(db.String(24), nullable=False, default="Draft")
    version = db.Column(db.Integer, nullable=False, default=1)
    details_json = db.Column(db.Text, nullable=False, default="{}")
    published_at = db.Column(db.DateTime, nullable=True)
    published_by = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class AppUser(db.Model):
    """APG access assignments, optionally authenticated by Envision."""
    __tablename__ = "app_users"

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, index=True, nullable=False)
    display_name = db.Column(db.String(120), nullable=True)
    password_hash = db.Column(db.String(255), nullable=False)
    auth_provider = db.Column(db.String(24), nullable=False, default="local")
    envision_username = db.Column(db.String(120), unique=True, index=True, nullable=True)
    envision_employee_id = db.Column(db.String(64), unique=True, index=True, nullable=True)
    envision_crew_code = db.Column(db.String(32), unique=True, index=True, nullable=True)
    envision_job_title = db.Column(db.String(160), nullable=True)
    crew_briefing_private = db.Column(db.Boolean, nullable=False, default=False)
    directory_last_seen_at = db.Column(db.DateTime, nullable=True)
    is_admin = db.Column(db.Boolean, nullable=False, default=False)
    permissions_json = db.Column(db.Text, nullable=False, default="[]")
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    last_login_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class EmailSettings(db.Model):
    """Admin-managed recipients; service credentials remain in the protected server environment."""
    __tablename__ = "email_settings"

    id = db.Column(db.Integer, primary_key=True, default=1)
    flight_operations_email = db.Column(db.String(1000), nullable=True)
    from_email = db.Column(db.String(255), nullable=True)
    charter_closure_emails_enabled = db.Column(db.Boolean, nullable=False, default=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class EnvisionOtpFlightCache(db.Model):
    __tablename__ = "envision_otp_flight_cache"

    id = db.Column(db.Integer, primary_key=True)
    envision_flight_id = db.Column(db.String(32), unique=True, index=True, nullable=False)
    flight_date = db.Column(db.Date, index=True, nullable=True)
    departure_scheduled = db.Column(db.DateTime, index=True, nullable=True)
    reg = db.Column(db.String(16), index=True, nullable=True)
    row_json = db.Column(db.Text, nullable=False, default="{}")
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


# NEW: simple key/value schedule settings (singleton row: id=1)
class AppConfig(db.Model):
    __tablename__ = "app_config"
    id = db.Column(db.Integer, primary_key=True, default=1)
    auto_enabled = db.Column(db.Boolean, default=False, nullable=False)
    interval_sec = db.Column(db.Integer, default=300, nullable=False)  # default 5 min
    apg_create_ahead_hours = db.Column(db.Integer, default=48, nullable=False)
    seat_bag_tare_kg = db.Column(db.Float, default=7.0, nullable=False)
    charter_passenger_weights_json = db.Column(db.Text, default="{}", nullable=False)
    catering_services_json = db.Column(db.Text, default="[]", nullable=False)
    airport_handling_json = db.Column(db.Text, default="[]", nullable=False)
    last_auto_started = db.Column(db.DateTime, nullable=True)
    last_auto_finished = db.Column(db.DateTime, nullable=True)
    last_envision_user_sync_at = db.Column(db.DateTime, nullable=True)


class FlightFreightAllocation(db.Model):
    __tablename__ = "flight_freight_allocations"

    id = db.Column(db.Integer, primary_key=True)
    envision_flight_id = db.Column(db.String(32), unique=True, index=True, nullable=False)
    seats_json = db.Column(db.Text, nullable=False, default="[]")
    freight_kg = db.Column(db.Float, nullable=False, default=0.0)
    tare_kg = db.Column(db.Float, nullable=False, default=7.0)
    revision = db.Column(db.Integer, nullable=False, default=1)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class FlightCargoAllocation(db.Model):
    __tablename__ = "flight_cargo_allocations"

    id = db.Column(db.Integer, primary_key=True)
    envision_flight_id = db.Column(db.String(32), unique=True, index=True, nullable=False)
    allocations_json = db.Column(db.Text, nullable=False, default="[]")
    atr_rows_json = db.Column(db.Text, nullable=False, default="[]")
    revision = db.Column(db.Integer, nullable=False, default=1)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
