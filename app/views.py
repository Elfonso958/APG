from flask import Blueprint, render_template, request, redirect, jsonify, flash, current_app,send_file, abort, url_for, make_response
from datetime import date, datetime, time, timezone, timedelta
from flask import session
from .models import SyncRun, SyncFlightLog, AppConfig, CharterManifest, CharterBrief, CharterRequest, CharterHandlingRequest, CharterHandlingEvent, AppUser, EmailSettings, PowerBiApiKey, ManualDcsFlightLink, AirportHandlingProvider
from .airport_handling import AIRPORT_HANDLERS, DEFAULT_CATERING_SERVICES, handlers_for_airports
from . import db
from .kmh_auth import create_kmh_session, clear_kmh_session, get_kmh_session
from .zenith_client import fetch_dcs_for_flight
from .otp_cache_job import get_otp_cache_job_status, start_otp_cache_job
from functools import lru_cache, wraps
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime as _dt
from datetime import date as _date
import io
import os
from io import BytesIO

import json, requests
import html
import time as _time
import re
import secrets
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from werkzeug.security import check_password_hash, generate_password_hash
from markupsafe import Markup, escape
from zoneinfo import ZoneInfo
NZ = ZoneInfo("Pacific/Auckland")


def _nz_today() -> date:
    """Return the current operational date in New Zealand time."""
    return datetime.now(NZ).date()

# ✅ use your existing Envision helpers

from .sync.envision_apg_sync import (
    envision_authenticate,
    envision_authenticate_with_credentials,
    envision_get_flights,
    ENVISION_BASE,               # diagnostics
    ENVISION_ACTIVE_NAME,
    ENVISION_ACTIVE_HOST,
    get_envision_environment,
    set_envision_environment,
    attach_apg_presence_to_rows, # APG plan presence
    apg_login,                   #add
    apg_get_plan_list,           #add
    APG_EMAIL,                   #add
    APG_PASSWORD,                #add
    envision_get_flight_times,
    fetch_flights_for_day,       #add
    envision_get_delays,
    envision_get_employees,
    envision_get_places,
    envision_get_line_registrations,
    envision_get_lines,
    envision_get_flight_types,
    envision_create_flight,
    envision_cancel_flight,
    envision_get_cancel_codes,
    envision_get_flight_crew,
    envision_change_registration,
    fetch_envision_crew_for_apg,
    calculate_dcs_passenger_seat_loads,
)

# Simple in-memory cache for Envision flights per date window
_ENVISION_FLIGHT_CACHE: dict[tuple[str, str], dict] = {}
# Simple in-memory cache for Envision defects by registration id
_ENVISION_DEFECT_CACHE: dict[int, dict] = {}
# Simple in-memory cache for Envision scheduled maintenance by registration id
_ENVISION_MAINT_CACHE: dict[int, dict] = {}


def clear_gantt_flight_cache() -> None:
    """Clear cached Envision flight rows used by the live Gantt endpoint."""
    _ENVISION_FLIGHT_CACHE.clear()

# ✅ use your DCS single-flight call
from .zenith_client import fetch_dcs_for_flight

ui_bp = Blueprint("ui", __name__)

APP_PERMISSIONS = {
    "operations": "Operations",
    "crew_briefing": "Crew Briefing",
    "live_gantt": "Live Gantt",
    "charter_checkin": "Charter Check-in",
    "charter_requests": "Charter Requests",
    "maintenance_dashboard": "Maintenance Dashboard",
}


def _normalise_user_email(value: str | None) -> str:
    return str(value or "").strip().lower()


def _normalise_envision_username(value: str | None) -> str:
    return str(value or "").strip().lower()


def _directory_value(employee: dict, *keys: str) -> str:
    for key in keys:
        value = employee.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def _is_cabin_crew_user(user: AppUser | None) -> bool:
    """Cabin crew receive the read-only, passenger-focused briefing view."""
    if not user or user.is_admin or user.auth_provider != "envision":
        return False
    title = " ".join(str(user.envision_job_title or "").casefold().split())
    return "cabin crew" in title


def sync_envision_user_directory() -> dict:
    """Discover Envision users without granting access or retaining their passwords."""
    auth = envision_authenticate()
    employees = envision_get_employees(auth["token"], ttl_seconds=0)
    now = datetime.utcnow()
    discovered = updated = skipped = 0
    seen_usernames: set[str] = set()
    seen_employee_ids: set[str] = set()
    for employee in employees:
        if not isinstance(employee, dict):
            skipped += 1
            continue
        username = _normalise_envision_username(_directory_value(employee, "username", "userName", "employeeUsername", "loginName", "employeeNo"))
        if not username:
            skipped += 1
            continue
        employee_id = _directory_value(employee, "id", "employeeId") or None
        crew_code = _normalise_envision_username(_directory_value(employee, "employeeNo", "employeeNumber", "employerCode", "crewCode", "code")) or None
        seen_usernames.add(username)
        if employee_id:
            seen_employee_ids.add(employee_id)
        email = _normalise_user_email(_directory_value(employee, "email", "emailAddress", "workEmail"))
        first = _directory_value(employee, "firstName", "givenName")
        last = _directory_value(employee, "surname", "lastName", "familyName")
        display_name = " ".join(part for part in (first, last) if part).strip() or _directory_value(employee, "displayName", "employeeName", "shortDisplayName") or username
        job_title = _directory_value(employee, "jobTitle", "job_title", "title", "role", "roleName", "jobRole", "position", "positionName", "crewPosition", "crewPositionDescription") or None
        user = None
        if employee_id:
            user = AppUser.query.filter_by(envision_employee_id=employee_id).first()
        if not user:
            user = AppUser.query.filter(db.func.lower(AppUser.envision_username) == username).first()
        if not user and email:
            user = AppUser.query.filter_by(email=email).first()
        if user:
            user.envision_username = username
            user.envision_employee_id = employee_id
            user.envision_crew_code = crew_code
            user.auth_provider = "envision"
            user.display_name = display_name
            user.envision_job_title = job_title
            user.directory_last_seen_at = now
            updated += 1
        else:
            # Some Envision records do not contain an email. A private placeholder
            # keeps the existing non-null APG email schema intact and is never used to sign in.
            local_email = email or f"{username}@envision.local"
            if AppUser.query.filter_by(email=local_email).first():
                local_email = f"{username}-{employee_id or secrets.token_hex(3)}@envision.local"
            db.session.add(AppUser(
                email=local_email,
                display_name=display_name,
                password_hash=generate_password_hash(secrets.token_urlsafe(32)),
                auth_provider="envision",
                envision_username=username,
                envision_employee_id=employee_id,
                envision_crew_code=crew_code,
                envision_job_title=job_title,
                directory_last_seen_at=now,
                is_active=False,
                permissions_json="[]",
            ))
            discovered += 1

    # Envision does not return an explicit active/invalid status. Its employee
    # directory is authoritative instead: a previously synced Envision identity
    # that is absent from a completed refresh can no longer sign in to APG.
    deactivated = 0
    for user in AppUser.query.filter_by(auth_provider="envision", is_active=True).all():
        username = _normalise_envision_username(user.envision_username)
        employee_id = str(user.envision_employee_id or "").strip()
        if (username and username in seen_usernames) or (employee_id and employee_id in seen_employee_ids):
            continue
        user.is_active = False
        deactivated += 1
    cfg = db.session.get(AppConfig, 1) or AppConfig(id=1)
    cfg.last_envision_user_sync_at = now
    db.session.add(cfg)
    db.session.commit()
    return {"discovered": discovered, "updated": updated, "deactivated": deactivated, "skipped": skipped, "total": len(employees)}


def _bootstrap_initial_admin() -> None:
    """Create the first local administrator from protected environment variables, once."""
    try:
        if AppUser.query.count():
            return
        email = _normalise_user_email(os.getenv("APG_INITIAL_ADMIN_EMAIL"))
        password = os.getenv("APG_INITIAL_ADMIN_PASSWORD") or ""
        if not email or not password:
            return
        db.session.add(AppUser(
            email=email,
            display_name=os.getenv("APG_INITIAL_ADMIN_NAME") or email.split("@", 1)[0],
            password_hash=generate_password_hash(password),
            is_admin=True,
        ))
        db.session.commit()
        current_app.logger.info("Created initial APG administrator account for %s", email)
    except Exception:
        db.session.rollback()


def _current_apg_user() -> AppUser | None:
    user_id = session.get("apg_user_id")
    if not user_id:
        return None
    try:
        user = db.session.get(AppUser, int(user_id))
        return user if user and user.is_active else None
    except (TypeError, ValueError):
        return None


def _user_permissions(user: AppUser | None) -> set[str]:
    if not user:
        return set()
    if user.is_admin:
        return set(APP_PERMISSIONS)
    try:
        raw = json.loads(user.permissions_json or "[]")
    except (TypeError, ValueError):
        raw = []
    return {value for value in raw if value in APP_PERMISSIONS}


def _can_access(permission: str, user: AppUser | None = None) -> bool:
    return permission in _user_permissions(user or _current_apg_user())


def _csrf_token() -> str:
    token = session.get("apg_csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["apg_csrf_token"] = token
    return token


def _csrf_is_valid() -> bool:
    return bool(session.get("apg_csrf_token")) and secrets.compare_digest(
        str(request.form.get("csrf_token") or ""), str(session.get("apg_csrf_token") or "")
    )


def _login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not _current_apg_user():
            return redirect(url_for("ui.apg_login", next=_request_next_url()))
        return view(*args, **kwargs)
    return wrapped


def _admin_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        user = _current_apg_user()
        if not user:
            return redirect(url_for("ui.apg_login", next=_request_next_url()))
        if not user.is_admin:
            abort(403)
        return view(*args, **kwargs)
    return wrapped


def _permission_required(permission: str):
    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            user = _current_apg_user()
            if not user:
                return redirect(url_for("ui.apg_login", next=_request_next_url()))
            if not _can_access(permission, user):
                return redirect(url_for("ui.dcs_landing", denied=permission))
            return view(*args, **kwargs)
        return wrapped
    return decorator


def _any_permission_required(*permissions: str):
    """Allow a shared read-only page/API to be used by any listed role."""
    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            user = _current_apg_user()
            if not user:
                return redirect(url_for("ui.apg_login", next=_request_next_url()))
            if not any(_can_access(permission, user) for permission in permissions):
                return redirect(url_for("ui.dcs_landing", denied=permissions[0] if permissions else "module"))
            return view(*args, **kwargs)
        return wrapped
    return decorator


def _operations_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        user = _current_apg_user()
        if not user:
            return redirect(url_for("ui.apg_login", next=_request_next_url()))
        if not _user_permissions(user):
            abort(403)
        return view(*args, **kwargs)
    return wrapped


def _charter_request_access(user=None):
    user = user or _current_apg_user()
    return bool(user and (user.is_admin or "operations" in _user_permissions(user) or "charter_requests" in _user_permissions(user)))


@ui_bp.app_context_processor
def _inject_apg_account_context():
    user = _current_apg_user()
    config = db.session.get(AppConfig, 1) if user and user.is_admin else None
    return {
        "apg_current_user": user,
        "apg_is_admin": bool(user and user.is_admin),
        "apg_permissions": _user_permissions(user),
        "apg_is_cabin_crew": _is_cabin_crew_user(user),
        "apg_csrf_token": _csrf_token(),
        "charter_tail_debug_enabled": bool(config and config.charter_tail_debug_enabled),
    }


def _seed_airport_handling_providers() -> None:
    """Import the bundled directory for installations whose legacy JSON was empty."""
    if AirportHandlingProvider.query.first():
        return
    for entry in AIRPORT_HANDLERS:
        if not isinstance(entry, dict) or not entry.get("airport") or not entry.get("label"):
            continue
        db.session.add(AirportHandlingProvider(
            airport=str(entry.get("airport") or "").upper(),
            label=str(entry.get("label") or ""), handler=str(entry.get("handler") or ""),
            contact=str(entry.get("contact") or ""), phone=str(entry.get("phone") or ""),
            additional_phone=str(entry.get("additional_phone") or ""),
            email_addresses="\n".join(str(email).strip() for email in (entry.get("emails") or []) if str(email).strip()),
            frequency=str(entry.get("frequency") or ""), gpu=str(entry.get("gpu") or ""),
            fuel=str(entry.get("fuel") or ""), notes=str(entry.get("notes") or ""),
        ))
    db.session.commit()


def _charter_operations_directory():
    cfg = db.session.get(AppConfig, 1)
    try:
        catering = json.loads(cfg.catering_services_json or "[]") if cfg else []
    except (TypeError, ValueError):
        catering = []
    _seed_airport_handling_providers()
    providers = AirportHandlingProvider.query.order_by(AirportHandlingProvider.airport, AirportHandlingProvider.label).all()
    handlers = [{"id": item.id, "airport": item.airport, "label": item.label, "handler": item.handler or "", "contact": item.contact or "", "phone": item.phone or "", "additional_phone": item.additional_phone or "", "emails": [email.strip() for email in (item.email_addresses or "").splitlines() if email.strip()], "frequency": item.frequency or "", "gpu": item.gpu or "", "fuel": item.fuel or "", "notes": item.notes or ""} for item in providers]
    return (catering if isinstance(catering, list) and catering else DEFAULT_CATERING_SERVICES,
            handlers if isinstance(handlers, list) and handlers else AIRPORT_HANDLERS)


def _safe_apg_next(value: str | None) -> str:
    candidate = str(value or "").strip()
    if not candidate.startswith("/") or candidate.startswith("//"):
        return url_for("ui.home")

    # Older sign-in pages may still carry a pre-proxy next value such as
    # ``/dcs/charter-checkin``. Keep those links within the mounted /APG app.
    prefix = request.script_root.rstrip("/")
    if prefix and candidate != prefix and not candidate.startswith(f"{prefix}/"):
        return f"{prefix}{candidate}"
    return candidate


def _request_next_url() -> str:
    """Preserve the reverse-proxy prefix when returning from sign-in."""
    prefix = request.script_root.rstrip("/")
    path = request.full_path if request.full_path.startswith("/") else f"/{request.full_path}"
    return f"{prefix}{path}" if prefix and not path.startswith(f"{prefix}/") else path


@ui_bp.route("/account/login", methods=["GET", "POST"])
def apg_login():
    _bootstrap_initial_admin()
    next_url = _safe_apg_next(request.values.get("next"))
    error = None
    if request.method == "POST":
        if not _csrf_is_valid():
            error = "Your sign-in form expired. Please try again."
        else:
            identity = str(request.form.get("identity") or request.form.get("email") or "").strip()
            password = request.form.get("password") or ""
            user = AppUser.query.filter(db.func.lower(AppUser.envision_username) == _normalise_envision_username(identity)).first()
            if not user:
                user = AppUser.query.filter_by(email=_normalise_user_email(identity)).first()
            authenticated = False
            if user and user.is_active:
                if user.auth_provider == "envision" and user.envision_username:
                    try:
                        envision_auth = envision_authenticate_with_credentials(user.envision_username, password)
                        authenticated = True
                    except Exception:
                        authenticated = False
                else:
                    authenticated = check_password_hash(user.password_hash, password)
            if not authenticated:
                error = "Incorrect Envision username or password, or you have not been granted APG access."
            else:
                session.clear()
                session["apg_user_id"] = user.id
                if user.auth_provider == "envision":
                    # Flask's default session is a signed browser cookie, so never put the
                    # Envision bearer token in it. Store only an opaque server-side ID.
                    session["apg_envision_session_id"] = create_kmh_session(
                        token=envision_auth["token"],
                        username=user.envision_username,
                        refresh_token=envision_auth.get("refreshToken"),
                        expires_at=envision_auth.get("expires_at"),
                    )
                    session.permanent = True
                _csrf_token()
                user.last_login_at = datetime.utcnow()
                db.session.add(user)
                db.session.commit()
                return redirect(next_url)
    return render_template("account_login.html", error=error, next_url=next_url)


@ui_bp.post("/account/logout")
@_login_required
def apg_logout():
    clear_kmh_session(session.get("apg_envision_session_id"))
    session.clear()
    return redirect(url_for("ui.apg_login"))


@ui_bp.post("/account/crew-briefing-privacy")
@_permission_required("crew_briefing")
def crew_briefing_privacy():
    """Let a crew member prevent others from searching their roster by code."""
    if not _csrf_is_valid():
        flash("Your privacy setting could not be saved. Please try again.", "danger")
    else:
        user = _current_apg_user()
        user.crew_briefing_private = request.form.get("crew_briefing_private") == "1"
        db.session.add(user)
        db.session.commit()
        flash("Your crew-briefing privacy setting was saved.", "success")
    return redirect(_safe_apg_next(request.form.get("next")))


@ui_bp.route("/admin/users", methods=["GET", "POST"])
@_admin_required
def admin_users():
    if request.method == "POST":
        if not _csrf_is_valid():
            flash("Your form expired. Please try again.", "danger")
            return redirect(url_for("ui.admin_users"))
        action = str(request.form.get("action") or "").strip()
        if action == "sync_directory":
            try:
                outcome = sync_envision_user_directory()
                flash(f"Envision directory refreshed: {outcome['discovered']} new, {outcome['updated']} updated, {outcome['deactivated']} deactivated.", "success")
            except Exception:
                current_app.logger.exception("Envision user directory refresh failed")
                flash("Envision directory refresh failed. Check the server Envision connection and try again.", "danger")
        elif action == "create":
            email = _normalise_user_email(request.form.get("email"))
            password = request.form.get("password") or ""
            if not email or "@" not in email or len(password) < 8:
                flash("Enter a valid email address and a password of at least 8 characters.", "danger")
            elif AppUser.query.filter_by(email=email).first():
                flash("An APG account already exists for that email address.", "danger")
            else:
                db.session.add(AppUser(
                    email=email,
                    display_name=str(request.form.get("display_name") or "").strip() or None,
                    password_hash=generate_password_hash(password),
                    is_admin=bool(request.form.get("is_admin")),
                    permissions_json=json.dumps(list(APP_PERMISSIONS) if request.form.get("is_admin") else [key for key in APP_PERMISSIONS if request.form.get(f"permission_{key}")]),
                    is_active=True,
                ))
                db.session.commit()
                flash("APG user account created.", "success")
        elif action == "update":
            try:
                user_id = int(request.form.get("user_id") or 0)
            except (TypeError, ValueError):
                user_id = 0
            user = db.session.get(AppUser, user_id)
            if not user:
                flash("User account not found.", "danger")
            else:
                requested_admin = bool(request.form.get("is_admin"))
                requested_active = bool(request.form.get("is_active"))
                final_admin = AppUser.query.filter_by(is_admin=True, is_active=True).count() == 1 and user.is_admin and user.is_active and (not requested_admin or not requested_active)
                if final_admin:
                    flash("Keep at least one active APG administrator account.", "danger")
                else:
                    user.display_name = str(request.form.get("display_name") or "").strip() or None
                    user.is_admin = requested_admin
                    user.is_active = requested_active
                    user.permissions_json = json.dumps(list(APP_PERMISSIONS) if requested_admin else [key for key in APP_PERMISSIONS if request.form.get(f"permission_{key}")])
                    new_password = request.form.get("new_password") or ""
                    if new_password:
                        if len(new_password) < 8:
                            flash("Password changes must be at least 8 characters.", "danger")
                            return redirect(url_for("ui.admin_users"))
                        user.password_hash = generate_password_hash(new_password)
                    db.session.add(user)
                    db.session.commit()
                    flash("APG user account updated.", "success")
        return redirect(url_for("ui.admin_users"))
    users = AppUser.query.order_by(AppUser.is_admin.desc(), AppUser.email.asc()).all()
    cfg = db.session.get(AppConfig, 1)
    return render_template(
        "admin_users.html",
        users=users,
        permissions=APP_PERMISSIONS,
        user_permissions={user.id: _user_permissions(user) for user in users},
        last_directory_sync_at=cfg.last_envision_user_sync_at if cfg else None,
    )


@ui_bp.route("/admin/email-settings", methods=["GET", "POST"])
@_admin_required
def admin_email_settings():
    settings = db.session.get(EmailSettings, 1)
    sender_options = []
    for value in (os.getenv("MAIL_FROM"), os.getenv("MAIL_DEFAULT_SENDER"), os.getenv("GRAPH_MAILBOX_UPN"), os.getenv("TEAMS_ORGANIZER_UPN")):
        value = _normalise_user_email(value)
        if value and value not in sender_options:
            sender_options.append(value)
    if "info@accharters.co.nz" not in sender_options:
        sender_options.append("info@accharters.co.nz")
    if request.method == "POST":
        if not _csrf_is_valid():
            flash("Your form expired. Please try again.", "danger")
            return redirect(url_for("ui.admin_email_settings"))
        recipients = ", ".join(part.strip() for part in str(request.form.get("flight_operations_email") or "").split(",") if part.strip())
        charter_recipients = ", ".join(part.strip() for part in str(request.form.get("charter_request_recipients") or "").split(",") if part.strip())
        if (recipients and any("@" not in address for address in recipients.split(", "))) or (charter_recipients and any("@" not in address for address in charter_recipients.split(", "))):
            flash("Enter one or more valid email addresses, separated by commas.", "danger")
        else:
            settings = settings or EmailSettings(id=1)
            selected_sender = _normalise_user_email(request.form.get("from_email"))
            if selected_sender and selected_sender not in sender_options:
                flash("Choose one of the configured sender addresses.", "danger")
                return redirect(url_for("ui.admin_email_settings"))
            settings.flight_operations_email = recipients or None
            settings.charter_request_recipients = charter_recipients or None
            settings.from_email = selected_sender or None
            settings.charter_closure_emails_enabled = bool(request.form.get("charter_closure_emails_enabled"))
            db.session.add(settings)
            db.session.commit()
            flash("Email settings saved.", "success")
        return redirect(url_for("ui.admin_email_settings", embed="1") if request.args.get("embed") else url_for("ui.admin_email_settings"))
    configured_recipients = (settings.flight_operations_email if settings else None) or os.getenv("FLIGHT_OPERATIONS_EMAIL", "")
    return render_template(
        "admin_email_settings_embed.html" if request.args.get("embed") else "admin_email_settings_v2.html",
        settings=settings,
        configured_recipients=configured_recipients,
        charter_request_recipients=(settings.charter_request_recipients if settings else "") or configured_recipients,
        sender_options=sender_options,
        graph_configured=bool(os.getenv("GRAPH_TENANT_ID") or os.getenv("MS_TENANT_ID")),
        smtp_configured=bool(os.getenv("MAIL_USERNAME") or os.getenv("SMTP_USERNAME")),
    )


@ui_bp.route("/admin/powerbi-api-keys", methods=["GET", "POST"])
@_admin_required
def admin_powerbi_api_keys():
    new_key = None
    if request.method == "POST":
        if not _csrf_is_valid():
            flash("Your form expired. Please try again.", "danger")
            return redirect(url_for("ui.admin_powerbi_api_keys"))

        action = str(request.form.get("action") or "").strip()
        if action == "create":
            name = str(request.form.get("name") or "").strip()
            if not name or len(name) > 120:
                flash("Enter a key name of up to 120 characters.", "danger")
            else:
                secret = secrets.token_urlsafe(32)
                user = _current_apg_user()
                db.session.add(PowerBiApiKey(
                    name=name,
                    key_prefix=secret[:12],
                    key_hash=PowerBiApiKey.hash_secret(secret),
                    created_by_user_id=user.id if user else None,
                ))
                db.session.commit()
                new_key = secret
                flash("API key created. Copy it now; it will not be shown again.", "success")
        elif action == "revoke":
            try:
                key_id = int(request.form.get("key_id") or 0)
            except (TypeError, ValueError):
                key_id = 0
            api_key = db.session.get(PowerBiApiKey, key_id)
            if not api_key or api_key.revoked_at:
                flash("Active API key not found.", "danger")
            else:
                api_key.revoked_at = datetime.utcnow()
                db.session.commit()
                flash(f"API key '{api_key.name}' was revoked.", "success")
            return redirect(url_for("ui.admin_powerbi_api_keys"))

    keys = PowerBiApiKey.query.filter_by(revoked_at=None).order_by(PowerBiApiKey.created_at.desc()).all()
    return render_template("admin_powerbi_api_keys.html", keys=keys, new_key=new_key)


@ui_bp.before_app_request
def _apply_session_envision_environment():
    try:
        chosen = session.get("envision_env")
        if chosen:
            set_envision_environment(str(chosen))
    except Exception:
        current_app.logger.exception("Failed to apply session Envision environment")


def _runtime_envision_base() -> str:
    env = get_envision_environment()
    return str(env.get("base") or "").rstrip("/")


def _kmh_cookie_path() -> str:
    return request.script_root or "/"

@ui_bp.get("/ops/otp-cache")
def ops_otp_cache():
    return render_template("otp_cache.html")


@ui_bp.get("/ops/otp-cache/status")
def ops_otp_cache_status():
    return jsonify(get_otp_cache_job_status())


@ui_bp.post("/ops/otp-cache/start")
def ops_otp_cache_start():
    payload = request.get_json(silent=True) or {}

    def as_int(name: str, default: int) -> int:
        try:
            value = int(payload.get(name) or default)
        except (TypeError, ValueError):
            value = default
        return max(1, value)

    date_from = str(payload.get("dateFrom") or "2022-01-01").strip()
    date_to = str(payload.get("dateTo") or "2035-12-31").strip()
    include_details = str(payload.get("includeDetails") or "").strip().lower() in {"1", "true", "yes", "on"}

    started, status = start_otp_cache_job(
        current_app._get_current_object(),
        date_from=date_from,
        date_to=date_to,
        window_days=as_int("windowDays", 7),
        chunk_days=as_int("chunkDays", 1),
        page_size=as_int("pageSize", 100),
        include_details=include_details,
    )
    return jsonify(ok=started, status=status), (202 if started else 409)


@ui_bp.route("/ops/modify-leg")
def ops_modify_leg():
    return render_template("flight_details.html")


@ui_bp.route("/ops/kmh-login", methods=["GET", "POST"])
def ops_kmh_login():
    existing_session_id = request.cookies.get("kmh_session_id") or session.get("kmh_session_id")
    if get_kmh_session(existing_session_id):
        return redirect(url_for("ui.ops_kmh_calendar"))

    next_url = request.values.get("next") or url_for("ui.ops_kmh_calendar")
    if not str(next_url).startswith("/"):
        next_url = url_for("ui.ops_kmh_calendar")

    error = None
    info = ""
    username = ""
    if str(request.args.get("reason") or "").strip().lower() == "expired":
        info = "Your Envision session expired. Sign in again to continue."
    if request.method == "POST":
        username = str(request.form.get("username") or "").strip()
        password = str(request.form.get("password") or "")
        try:
            auth = envision_authenticate_with_credentials(username, password, base=ENVISION_BASE)
            session.permanent = True
            session_id = create_kmh_session(
                token=auth["token"],
                username=username,
                refresh_token=auth.get("refreshToken"),
                expires_at=auth.get("expires_at"),
            )
            session["kmh_session_id"] = session_id
            session["kmh_envision_username"] = username
            session["envision_env"] = "base"
            set_envision_environment("base")
            response = make_response(redirect(next_url))
            response.set_cookie(
                "kmh_session_id",
                session_id,
                max_age=8 * 60 * 60,
                path=_kmh_cookie_path(),
                httponly=True,
                samesite="Lax",
            )
            return response
        except Exception as exc:
            error = str(exc)

    return render_template(
        "kmh_login.html",
        next_url=next_url,
        error=error,
        info=info,
        username=username,
        kmh_session_id=session.get("kmh_session_id") or "",
    )


@ui_bp.route("/ops/kmh-logout", methods=["POST", "GET"])
def ops_kmh_logout():
    for key in (
        "kmh_session_id",
        "kmh_envision_username",
    ):
        if key == "kmh_session_id":
            clear_kmh_session(session.get(key))
        session.pop(key, None)
    response = make_response(redirect(url_for("ui.ops_kmh_login")))
    response.delete_cookie("kmh_session_id", path=_kmh_cookie_path())
    return response


@ui_bp.route("/ops/kmh-calendar")
def ops_kmh_calendar():
    session_id = request.cookies.get("kmh_session_id") or session.get("kmh_session_id")
    kmh_session = get_kmh_session(session_id)
    if not kmh_session:
        session.pop("kmh_session_id", None)
        session.pop("kmh_envision_username", None)
        return redirect(url_for("ui.ops_kmh_login", next=url_for("ui.ops_kmh_calendar")))

    session["envision_env"] = "base"
    set_envision_environment("base")
    return render_template(
        "kmh_calendar.html",
        today=date.today(),
        kmh_user=str(kmh_session.get("username") or session.get("kmh_envision_username") or ""),
        kmh_session_id=session_id or "",
    )

@ui_bp.route("/", endpoint="home")
@ui_bp.route("/sync/runs")
@ui_bp.route("/sync/runs/")
def sync_runs_page():
    runs = SyncRun.query.order_by(SyncRun.id.desc()).limit(50).all()
    ok_count = sum(1 for r in runs if r.ok)
    fail_count = sum(1 for r in runs if r.ok is False)
    warn_count = sum(1 for r in runs if (r.warnings or 0) > 0)
    summary = {
        "total": len(runs),
        "ok": ok_count,
        "failed": fail_count,
        "warnings": warn_count,
    }
    return render_template("sync_runs.html", runs=runs, summary=summary)

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

    ADULT_TAGS = {"AD", "ADT", "ADULT", "A", "T"}
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

    manual_links = {}
    try:
        manual_links = {
            str(link.envision_flight_id): json.loads(link.dcs_flight_json or "{}")
            for link in ManualDcsFlightLink.query.all()
        }
    except Exception:
        current_app.logger.exception("Unable to load manual DCS links")
    for row in rows:
        manual = manual_links.get(str(row.get("envision_flight_id") or ""))
        if not isinstance(manual, dict):
            continue
        pax = manual.get("Passengers") or []
        counts = _count_pax_types(pax)
        row["pax_list"] = pax
        row["dcs_linked"] = True
        row["dcs_origin"] = str(manual.get("Origin") or row.get("dep") or "").upper()
        row["dcs_destination"] = str(manual.get("Destination") or manual.get("ArrivalAirport") or row.get("ades") or "").upper()
        row["adt"], row["chd"], row["inf"], row["pax_count"] = counts["ad"], counts["chd"], counts["inf"], counts["total"]
        bags_kg = 0.0
        for passenger in pax:
            try:
                bags_kg += float(passenger.get("BaggageWeight") or 0) if isinstance(passenger, dict) else 0.0
            except (TypeError, ValueError):
                pass
        row["bags_kg"] = bags_kg
        row["error"] = None

    max_workers = int(current_app.config.get("DCS_MAX_WORKERS", 8))
    app_obj = current_app._get_current_object()  # capture the real Flask app

    work: list[tuple[int, str, str, str, str]] = []
    for i, r in enumerate(rows):
        if str(r.get("envision_flight_id") or "") in manual_links:
            continue
        full_no = (r.get("flight_number") or "").strip().replace(" ", "").upper()
        origin  = (r.get("dep") or "").strip().upper()
        dest    = (r.get("ades") or r.get("dest") or "").strip().upper()

        if not full_no or not origin:
            r.update({"error": "Missing flight/origin", "pax_count": 0, "bags_kg": 0.0, "adt": 0, "chd": 0, "inf": 0, "dcs_linked": False})
            continue

        desig, number = split_designator_and_number(full_no)
        if not desig or not number:
            r.update({"error": "Bad flight number format", "pax_count": 0, "bags_kg": 0.0, "adt": 0, "chd": 0, "inf": 0, "dcs_linked": False})
            continue

        r["designator"] = desig
        r["flight_numeric"] = number
        work.append((i, origin, desig, number, dest))

    if not work:
        return

    def _fetch_one(idx: int, origin: str, desig: str, number: str, dest: str):
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
                flights = (dcs or {}).get("Flights", []) if isinstance(dcs, dict) else (dcs or [])
                if not flights:
                    # Fallback: include all statuses if strict DCS-status view is empty.
                    dcs = fetch_dcs_for_flight(
                        dep_airport=origin,
                        flight_date=nz_day,
                        airline_designator=desig,
                        flight_number=number,
                        only_status=False,
                    )
                return (idx, dcs, None)
            except Exception as e:
                return (idx, None, e)

    def _annotate_pax_origin_dest(pax_list: list[dict], origin_code: str, dest_code: str) -> list[dict]:
        out: list[dict] = []
        o = (origin_code or "").strip().upper()
        d = (dest_code or "").strip().upper()
        for p in pax_list or []:
            if isinstance(p, dict):
                px = dict(p)
                px["__manifest_origin"] = o
                px["__manifest_dest"] = d
                out.append(px)
            else:
                out.append(p)
        return out

    def _pax_key(p: dict) -> str:
        return "|".join(
            [
                str(p.get("BookingReferenceID") or "").strip().upper(),
                str(p.get("GivenName") or "").strip().upper(),
                str(p.get("Surname") or "").strip().upper(),
                str(p.get("Seat") or p.get("SeatNumber") or "").strip().upper(),
            ]
        )

    def _merge_annotated_pax(*pax_groups: list[dict]) -> list[dict]:
        out: list[dict] = []
        seen: set[str] = set()
        for group in pax_groups:
            for p in group or []:
                key = _pax_key(p) if isinstance(p, dict) else str(p)
                if key in seen:
                    continue
                seen.add(key)
                out.append(p)
        return out

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
                    "dcs_linked": False,
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
                    "dcs_linked": False,
                })
                continue

            chosen = None
            if len(flights) == 1:
                chosen = flights[0]
            else:
                # Prefer exact destination match when multiple legs are returned.
                dest = (r.get("ades") or r.get("dest") or "").strip().upper()
                for fl in flights:
                    fl_dest = str(fl.get("Destination") or fl.get("ArrivalAirport") or "").strip().upper()
                    if dest and fl_dest and fl_dest == dest:
                        chosen = fl
                        break
                if chosen is None:
                    chosen = flights[0]

                # Some DCS responses return pax on through-sector records (e.g. PPQ->AKL)
                # while the intermediate matched sector has an empty passenger list.
                chosen_pax = (chosen or {}).get("Passengers") or []
                if not chosen_pax:
                    richest = max(
                        flights,
                        key=lambda fl: len((fl or {}).get("Passengers") or []),
                        default=chosen,
                    )
                    richest_pax = (richest or {}).get("Passengers") or []
                    if richest_pax:
                        chosen = richest

            chosen_origin = str((chosen or {}).get("Origin") or r.get("dep") or "").strip().upper()
            chosen_dest = str((chosen or {}).get("Destination") or r.get("ades") or r.get("dest") or "").strip().upper()
            pax = _annotate_pax_origin_dest((chosen or {}).get("Passengers") or [], chosen_origin, chosen_dest)

            # DCS can return multiple records for the same departure under one flight number,
            # e.g. AKL->WAG plus AKL->PPQ for a through service. Keep both on the first leg.
            if len(flights) > 1 and chosen_origin:
                same_origin_through = []
                for fl in flights:
                    fl_origin = str(fl.get("Origin") or "").strip().upper()
                    fl_dest = str(fl.get("Destination") or "").strip().upper()
                    if fl is chosen or fl_origin != chosen_origin or not fl_dest or fl_dest == chosen_dest:
                        continue
                    fl_pax = _annotate_pax_origin_dest(
                        (fl.get("Passengers") or []),
                        fl_origin,
                        fl_dest,
                    )
                    if fl_pax:
                        same_origin_through.append(fl_pax)
                if same_origin_through:
                    pax = _merge_annotated_pax(pax, *same_origin_through)

            r["dcs_linked"] = bool(chosen)
            r["dcs_origin"] = chosen_origin
            r["dcs_destination"] = chosen_dest
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

def _propagate_through_pax(rows: list[dict]) -> None:
    """
    For through-services split into multiple legs under the same flight number/reg,
    share the DCS pax set across connected sectors, then keep only passengers
    whose origin/destination means they are onboard each sector.
    """
    if not rows:
        return

    def _key(r: dict) -> tuple[str, str]:
        return (
            str(r.get("reg") or "").strip().upper(),
            str(r.get("flight_number") or "").strip().upper(),
        )

    def _pax_key(p: dict) -> str:
        return "|".join(
            [
                str(p.get("BookingReferenceID") or "").strip().upper(),
                str(p.get("GivenName") or "").strip().upper(),
                str(p.get("Surname") or "").strip().upper(),
                str(p.get("Seat") or p.get("SeatNumber") or "").strip().upper(),
            ]
        )

    def _merge_pax(primary: list[dict], extra: list[dict]) -> list[dict]:
        out: list[dict] = []
        seen: set[str] = set()
        for p in primary or []:
            k = _pax_key(p)
            if k in seen:
                continue
            seen.add(k)
            out.append(p)
        for p in extra or []:
            k = _pax_key(p)
            if k in seen:
                continue
            seen.add(k)
            out.append(p)
        return out

    def _merge_many_pax(groups: list[list[dict]]) -> list[dict]:
        merged: list[dict] = []
        for group in groups:
            merged = _merge_pax(merged, group)
        return merged

    def _apply_pax(rr: dict, pax_list: list[dict]) -> None:
        counts = _count_pax_types(pax_list)
        bag_kg = 0.0
        for p in pax_list:
            try:
                bag_kg += float(p.get("BaggageWeight") or 0)
            except (TypeError, ValueError):
                pass
        rr["pax_list"] = pax_list
        rr["adt"] = counts["ad"]
        rr["chd"] = counts["chd"]
        rr["inf"] = counts["inf"]
        rr["pax_count"] = counts["total"]
        rr["bags_kg"] = bag_kg
        rr["dcs_linked"] = True
        rr["error"] = None

    def _chain_station_indexes(items: list[dict]) -> dict[str, int]:
        stations: list[str] = []
        for rr in items:
            dep = str(rr.get("dep") or "").strip().upper()
            arr = str(rr.get("ades") or rr.get("dest") or "").strip().upper()
            if dep and (not stations or stations[-1] != dep):
                stations.append(dep)
            if arr and (not stations or stations[-1] != arr):
                stations.append(arr)
        return {station: idx for idx, station in enumerate(stations)}

    def _pax_flies_sector(p: dict, rr: dict, station_idx: dict[str, int]) -> bool:
        dep = str(rr.get("dep") or "").strip().upper()
        arr = str(rr.get("ades") or rr.get("dest") or "").strip().upper()
        origin = str(p.get("__manifest_origin") or "").strip().upper()
        dest = str(p.get("__manifest_dest") or "").strip().upper()

        # If DCS did not provide/derive route metadata, preserve the passenger.
        if not origin and not dest:
            return True

        if origin == dep and dest == arr:
            return True

        dep_i = station_idx.get(dep)
        arr_i = station_idx.get(arr)
        origin_i = station_idx.get(origin)
        dest_i = station_idx.get(dest)
        if None in (dep_i, arr_i, origin_i, dest_i):
            return False

        # Passenger is onboard this sector if they boarded at/before the sector
        # origin and their destination is at/after this sector destination.
        return origin_i <= dep_i < arr_i <= dest_i

    groups: dict[tuple[str, str], list[dict]] = {}
    for r in rows:
        groups.setdefault(_key(r), []).append(r)

    for _, group in groups.items():
        group.sort(key=lambda r: r.get("std_nz") or _dt.min.replace(tzinfo=NZ))
        chain: list[dict] = []

        def _apply_chain(items: list[dict]) -> None:
            if not items:
                return
            pax_groups = [list((rr.get("pax_list") or [])) for rr in items]
            carry = _merge_many_pax(pax_groups)
            if not carry:
                return

            has_route_metadata = any(
                isinstance(p, dict) and (p.get("__manifest_origin") or p.get("__manifest_dest"))
                for p in carry
            )
            station_idx = _chain_station_indexes(items)

            for rr in items:
                if len(items) > 1 and has_route_metadata:
                    sector_pax = [
                        p for p in carry
                        if isinstance(p, dict) and _pax_flies_sector(p, rr, station_idx)
                    ]
                    _apply_pax(rr, sector_pax)
                else:
                    current = list((rr.get("pax_list") or []))
                    if not current:
                        _apply_pax(rr, carry)
                    else:
                        merged = _merge_pax(current, carry)
                        if len(merged) != len(current):
                            _apply_pax(rr, merged)

        for r in group:
            if not chain:
                chain = [r]
                continue
            prev = chain[-1]
            prev_dest = str(prev.get("ades") or prev.get("dest") or "").strip().upper()
            cur_dep = str(r.get("dep") or "").strip().upper()
            prev_end = prev.get("sta_nz") or prev.get("std_nz")
            cur_start = r.get("std_nz")
            gap_min = None
            if isinstance(prev_end, datetime) and isinstance(cur_start, datetime):
                gap_min = (cur_start - prev_end).total_seconds() / 60.0

            connected = bool(prev_dest and cur_dep and prev_dest == cur_dep)
            close_in_time = (gap_min is None) or (0 <= gap_min <= 360)
            if connected and close_in_time:
                chain.append(r)
            else:
                _apply_chain(chain)
                chain = [r]
        _apply_chain(chain)


def _is_charter_service(row: dict) -> bool:
    service = str(row.get("service_type") or row.get("flight_type") or "").strip().lower()
    return "charter" in service


def _apply_charter_manifests(rows: list[dict]) -> None:
    ids = [str(r.get("envision_flight_id")) for r in rows if r.get("envision_flight_id")]
    if not ids:
        return
    manifests = {
        str(m.envision_flight_id): m
        for m in CharterManifest.query.filter(CharterManifest.envision_flight_id.in_(ids)).all()
    }
    for r in rows:
        fid = str(r.get("envision_flight_id") or "")
        manifest = manifests.get(fid)
        if not manifest:
            r["charter_manifest_uploaded"] = False
            continue
        try:
            pax = json.loads(manifest.pax_json or "[]")
        except Exception:
            pax = []
        if not isinstance(pax, list):
            pax = []

        r["charter_manifest_uploaded"] = True
        r["charter_manifest_linked"] = True
        r["charter_manifest_filename"] = manifest.uploaded_filename or ""
        r["charter_manifest_updated_at"] = manifest.updated_at.isoformat() if manifest.updated_at else None
        r["charter_flight_closed_at"] = manifest.closed_at.isoformat() if manifest.closed_at else None
        r["charter_gate"] = manifest.gate or ""
        if _is_charter_service(r):
            counts = _count_pax_types(pax)
            r["pax_list"] = pax
            r["adt"] = counts["ad"]
            r["chd"] = counts["chd"]
            r["inf"] = counts["inf"]
            r["pax_count"] = counts["total"]
            bags_kg = 0.0
            for p in pax:
                try:
                    bags_kg += float(p.get("BaggageWeight") or 0)
                except (TypeError, ValueError):
                    pass
            r["bags_kg"] = bags_kg
            r["error"] = None

@ui_bp.route("/sync/runs/<int:rid>")
def sync_run_detail(rid):
    r = SyncRun.query.get_or_404(rid)
    flights = SyncFlightLog.query.filter_by(sync_run_id=rid).order_by(SyncFlightLog.id.asc()).all()
    return render_template("sync_run_detail.html", r=r, flights=flights)

@ui_bp.route("/admin/charter-operations", methods=["GET", "POST"])
@_admin_required
def admin_charter_operations():
    cfg = db.session.get(AppConfig, 1) or AppConfig(id=1)
    if request.method == "POST":
        if not _csrf_is_valid():
            flash("Your form expired. Please try again.", "danger")
            return redirect(url_for("ui.admin_charter_operations"))
        action = str(request.form.get("action") or "").strip()
        if action == "save_catering":
            cfg.catering_services_json = json.dumps([line.strip() for line in str(request.form.get("catering_services") or "").splitlines() if line.strip()])
            db.session.add(cfg)
            flash("Catering services saved.", "success")
        elif action == "delete_provider":
            provider = db.session.get(AirportHandlingProvider, request.form.get("provider_id"))
            if provider: db.session.delete(provider); flash("Handling provider removed.", "success")
        elif action in {"create_provider", "update_provider"}:
            airport = re.sub(r"[^A-Z0-9]", "", str(request.form.get("airport") or "").upper())
            label = str(request.form.get("label") or "").strip()
            if not airport or not label:
                flash("Airport code and provider label are required.", "danger")
                return redirect(url_for("ui.admin_charter_operations"))
            provider = db.session.get(AirportHandlingProvider, request.form.get("provider_id")) if action == "update_provider" else AirportHandlingProvider(airport=airport, label=label)
            if provider is None:
                flash("Handling provider was not found.", "danger")
                return redirect(url_for("ui.admin_charter_operations"))
            for field in ("airport", "label", "handler", "contact", "phone", "additional_phone", "frequency", "gpu", "fuel", "notes"):
                setattr(provider, field, airport if field == "airport" else label if field == "label" else str(request.form.get(field) or "").strip())
            provider.email_addresses = "\n".join(line.strip() for line in str(request.form.get("email_addresses") or "").splitlines() if line.strip())
            db.session.add(provider)
            flash("Handling provider saved.", "success")
        db.session.commit()
        return redirect(url_for("ui.admin_charter_operations"))
    catering, _ = _charter_operations_directory()
    providers = AirportHandlingProvider.query.order_by(AirportHandlingProvider.airport, AirportHandlingProvider.label).all()
    return render_template("admin_charter_operations.html", catering_services="\n".join(catering), providers=providers)


@ui_bp.route("/settings", methods=["GET", "POST"])
def settings_page():
    cfg = AppConfig.query.get(1)
    if request.method == "POST":
        auto_enabled = bool(request.form.get("auto_enabled"))
        interval_sec = int(request.form.get("interval_sec") or 300)
        apg_create_ahead_hours = int(request.form.get("apg_create_ahead_hours") or 48)
        if interval_sec < 60:
            interval_sec = 60
        apg_create_ahead_hours = min(max(apg_create_ahead_hours, 1), 336)
        if not cfg:
            cfg = AppConfig(id=1)
        cfg.auto_enabled = auto_enabled
        cfg.interval_sec = interval_sec
        cfg.apg_create_ahead_hours = apg_create_ahead_hours
        db.session.add(cfg); db.session.commit()
        # API also reschedules; but you can reschedule here if desired.
        return redirect(url_for("ui.settings_page"))
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
        adults = sum(1 for p in pax if ptype(p) in {"AD", "ADT", "ADULT", "A", "T"})
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
    url = f"{_runtime_envision_base()}/Flights"
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
            "service_type": (
                f.get("flightTypeDescription")
                or f.get("flightType")
                or f.get("serviceTypeDescription")
                or ""
            ),
            "flight_type": f.get("flightTypeDescription") or f.get("flightType") or "",
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


@ui_bp.route("/dcs")
def dcs_landing():
    """Operations entry point for Charter Check-in, Live Gantt, and Crew Briefing."""
    return render_template("dcs_landing.html", day=_nz_today())


@ui_bp.route("/dcs/new-live-gantt")
@_permission_required("live_gantt")
def dcs_new_live_gantt():
    day_str = request.args.get("date")
    if day_str:
        try:
            day = date.fromisoformat(day_str)
        except ValueError:
            day = _nz_today()
    else:
        day = _nz_today()
    # Production is the default for every non-admin session.  Only admins may
    # retain a test-session selection.
    if not _current_apg_user().is_admin:
        session["envision_env"] = "base"
        set_envision_environment("base")
    env = get_envision_environment()
    return render_template(
        "New_Gantt/live_gantt.html",
        day=day,
        envision_env_name=env.get("name"),
        envision_env_host=env.get("host"),
        envision_env_key=env.get("key"),
        envision_test_available=env.get("test_available"),
    )


@ui_bp.route("/dcs/charter-checkin")
@_permission_required("charter_checkin")
def dcs_charter_checkin():
    day_str = request.args.get("date")
    try:
        day = date.fromisoformat(day_str) if day_str else _nz_today()
    except ValueError:
        day = _nz_today()
    return render_template("charter_checkin.html", day=day)


def _charter_brief_details(brief: CharterBrief) -> dict:
    try:
        value = json.loads(brief.details_json or "{}")
        return value if isinstance(value, dict) else {}
    except (TypeError, ValueError):
        return {}


def _charter_brief_request_ids(brief: CharterBrief) -> set[str]:
    """Return every charter request represented by a brief (including old briefs)."""
    details = _charter_brief_details(brief)
    ids = {str(value) for value in details.get("source_charter_request_ids", []) if str(value)}
    legacy_id = str(details.get("source_charter_request_id") or "").strip()
    if legacy_id:
        ids.add(legacy_id)
    return ids


def _published_charter_brief_links() -> dict[str, CharterBrief]:
    """Map Envision flight IDs to the published charter brief containing them."""
    links: dict[str, CharterBrief] = {}
    for brief in CharterBrief.query.filter(CharterBrief.status == "Published").all():
        for sector in _charter_brief_details(brief).get("sectors", []):
            flight_id = str(sector.get("source_flight_id") or "").strip()
            if flight_id:
                links[flight_id] = brief
    return links


def _brief_print_date(value) -> str:
    try:
        return date.fromisoformat(str(value)[:10]).strftime("%A %-d %B %Y")
    except (TypeError, ValueError):
        return str(value or "Date TBC")


def _brief_print_time(value) -> str:
    raw = str(value or "")
    if not raw:
        return "—"
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.strftime("%H:%M")
    except ValueError:
        return raw[11:16] if "T" in raw and len(raw) >= 16 else raw


def _request_sectors(request_row):
    try:
        value = json.loads(request_row.sectors_json or "[]")
        return value if isinstance(value, list) else []
    except (TypeError, ValueError):
        return []


def _request_handling_ports(request_row) -> list[str]:
    """Unique stations needing handling coordination, in route order."""
    ports = []
    for sector in _request_sectors(request_row):
        for key in ("dep", "arr"):
            airport = str(sector.get(key) or "").strip().upper()
            if airport and airport not in ports:
                ports.append(airport)
    return ports


def _handling_provider_for_port(request_row, airport, providers):
    """Use the saved allocation, otherwise auto-select a sole provider."""
    record = CharterHandlingRequest.query.filter_by(charter_request_id=request_row.id, airport=airport).order_by(CharterHandlingRequest.updated_at.desc()).first()
    if record:
        return next((provider for provider in providers if provider.id == record.provider_id), None), record
    choices = [provider for provider in providers if provider.airport == airport]
    return (choices[0], None) if len(choices) == 1 else (None, None)


def _log_handling_event(record, event_type, actor, detail=None):
    db.session.add(CharterHandlingEvent(
        handling_request_id=record.id,
        event_type=event_type,
        detail=detail,
        actor=actor,
    ))


def _handling_provider_update_token(provider_id):
    """A time-limited bearer link for a handler to correct its own directory record."""
    serializer = URLSafeTimedSerializer(current_app.config["SECRET_KEY"], salt="charter-handling-provider-update")
    return serializer.dumps({"provider_id": int(provider_id)})


def _handling_confirmation_token(record_id):
    serializer = URLSafeTimedSerializer(current_app.config["SECRET_KEY"], salt="charter-handling-confirmation")
    return serializer.dumps({"handling_request_id": int(record_id)})


def _handling_request_text(charter_request, airport, provider, tracking_id, provider_update_url=None, confirmation_url=None):
    matching_sectors = [sector for sector in _request_sectors(charter_request) if airport in {str(sector.get("dep") or "").upper(), str(sector.get("arr") or "").upper()}]
    lines = [
        f"Ground handling request — {charter_request.reference}",
        "",
        f"Reference: {charter_request.reference}",
        f"Charter: {charter_request.title}",
        f"Airport: {airport}",
        f"Provider: {provider.label}",
        "",
        "Please confirm handling, parking, GPU availability and any local requirements for the following charter sectors:",
        "",
    ]
    for sector in matching_sectors:
        lines.append(f"{sector.get('date') or 'Date TBC'} · {sector.get('flight_number') or 'Flight TBC'} · {sector.get('dep') or '---'}–{sector.get('arr') or '---'} · STD {sector.get('std') or 'TBC'} · STA {sector.get('sta') or 'TBC'} · {sector.get('tail') or sector.get('aircraft_type') or 'Aircraft TBC'}")
    lines.extend(["", f"Fuel: {provider.fuel or 'Please advise availability and arrangements.'}", f"GPU: {provider.gpu or 'Please advise availability.'}", ""])
    if confirmation_url:
        lines.extend(["Please confirm that handling is accepted by opening this link:", confirmation_url])
    lines.extend(["", "You can also reply all to this email so Charter Operations can record your response."])
    if provider_update_url:
        lines.extend(["", "Need to correct your handling, fuel or contact details? Update your port information here:", provider_update_url])
    lines.extend(["", f"Tracking reference: [APG-HANDLING-{tracking_id}]"])
    return "\n".join(lines)


def _handling_request_html(charter_request, airport, provider, tracking_id, provider_update_url, confirmation_url):
    """Conservative table markup so the message remains readable in Outlook."""
    safe = lambda value: html.escape(str(value or "—"))
    safe_url = html.escape(provider_update_url, quote=True)
    safe_confirmation_url = html.escape(confirmation_url, quote=True)
    logo_url = url_for("ui.charter_brand_asset", asset="main-logo", _external=True)
    matching_sectors = [sector for sector in _request_sectors(charter_request) if airport in {str(sector.get("dep") or "").upper(), str(sector.get("arr") or "").upper()}]
    rows = "".join(
        "<tr>"
        f"<td style='padding:10px;border-bottom:1px solid #d8e5e8'>{safe(sector.get('date'))}</td>"
        f"<td style='padding:10px;border-bottom:1px solid #d8e5e8'><strong>{safe(sector.get('flight_number') or 'TBC')}</strong></td>"
        f"<td style='padding:10px;border-bottom:1px solid #d8e5e8'>{safe(sector.get('dep'))} – {safe(sector.get('arr'))}</td>"
        f"<td style='padding:10px;border-bottom:1px solid #d8e5e8'>{safe(sector.get('std'))} – {safe(sector.get('sta'))}</td>"
        f"<td style='padding:10px;border-bottom:1px solid #d8e5e8'>{safe(sector.get('tail') or sector.get('aircraft_type'))}</td>"
        "</tr>" for sector in matching_sectors
    ) or "<tr><td colspan='5' style='padding:10px'>Sector details to be confirmed.</td></tr>"
    return f"""<!doctype html><html><body style='margin:0;padding:0;background:#eef3f4;font-family:Arial,Helvetica,sans-serif;color:#173743'>
<table role='presentation' width='100%' cellspacing='0' cellpadding='0' border='0' bgcolor='#eef3f4'><tr><td align='center' style='padding:24px 12px'>
<table role='presentation' width='680' cellspacing='0' cellpadding='0' border='0' bgcolor='#ffffff' style='width:680px;max-width:680px'>
<tr><td bgcolor='#075c74' style='padding:22px 28px;color:#ffffff'><img src='{html.escape(logo_url, quote=True)}' alt='AC Charters' width='180' border='0' style='display:block;height:auto;margin:0 0 18px'><div style='font-size:11px;font-weight:bold;letter-spacing:1px'>AIR CHATHAMS · CHARTER OPERATIONS</div><div style='font-size:27px;font-weight:bold;line-height:32px;margin-top:7px'>Ground handling request</div><div style='font-size:15px;line-height:21px;margin-top:5px;color:#dceff0'>Please confirm the arrangements below.</div></td></tr>
<tr><td style='padding:26px 28px'><table role='presentation' width='100%' cellspacing='0' cellpadding='0' border='0' bgcolor='#eef7f6'><tr><td width='5' bgcolor='#8dc63f'>&nbsp;</td><td style='padding:13px 16px'><strong style='font-size:16px'>{safe(charter_request.reference)}</strong><br><span style='color:#587479;font-size:13px'>{safe(charter_request.title)} · {safe(airport)} · {safe(provider.label)}</span></td></tr></table>
<p style='margin:22px 0 12px;line-height:22px'>Hello {safe(provider.contact or provider.handler)},</p><p style='margin:0 0 20px;line-height:22px'>Please confirm ground handling, parking, GPU availability and any local requirements for the following charter operation at <strong>{safe(airport)}</strong>.</p>
<table role='presentation' width='100%' cellspacing='0' cellpadding='0' border='0' style='border-collapse:collapse;font-size:13px'><tr bgcolor='#173f50' style='color:#ffffff'><th align='left' style='padding:10px 7px'>Date</th><th align='left' style='padding:10px 7px'>Flight</th><th align='left' style='padding:10px 7px'>Sector</th><th align='left' style='padding:10px 7px'>Schedule</th><th align='left' style='padding:10px 7px'>Aircraft</th></tr><tbody>{rows}</tbody></table>
<table role='presentation' width='100%' cellspacing='0' cellpadding='0' border='0' style='margin-top:20px;font-size:14px'><tr><td width='130' style='padding:6px 0;color:#587479;font-weight:bold'>Fuel</td><td style='padding:6px 0'>{safe(provider.fuel or 'Please advise availability and arrangements.')}</td></tr><tr><td style='padding:6px 0;color:#587479;font-weight:bold'>GPU</td><td style='padding:6px 0'>{safe(provider.gpu or 'Please advise availability.')}</td></tr><tr><td style='padding:6px 0;color:#587479;font-weight:bold'>Contact</td><td style='padding:6px 0'>{safe(provider.contact)} · {safe(provider.phone)}</td></tr></table>
<table role='presentation' width='100%' cellspacing='0' cellpadding='0' border='0' bgcolor='#e7f4ea' style='margin-top:20px'><tr><td style='padding:16px;color:#214d31;font-size:14px;line-height:20px'><strong>Ready to confirm?</strong><br>Please select the button below to confirm handling arrangements. Charter Operations will be notified automatically.<br><table role='presentation' cellspacing='0' cellpadding='0' border='0' style='margin-top:12px'><tr><td bgcolor='#24744d' style='padding:12px 18px'><a href='{safe_confirmation_url}' style='color:#ffffff;font-size:14px;font-weight:bold;text-decoration:none'>Accept and confirm handling</a></td></tr></table><span style='display:block;margin-top:12px;font-size:12px'>Or reply all with your confirmation. Tracking reference: [APG-HANDLING-{tracking_id}]</span></td></tr></table>
<p style='margin:22px 0 12px;font-size:14px;line-height:20px'>Are any contact, fuel, GPU or local handling details out of date? Please use the button below to update your port information.</p>
<table role='presentation' cellspacing='0' cellpadding='0' border='0'><tr><td bgcolor='#075c74' style='padding:12px 18px'><a href='{safe_url}' style='color:#ffffff;font-size:14px;font-weight:bold;text-decoration:none'>Update your port information</a></td></tr></table>
<p style='margin:24px 0 0;line-height:20px'>Kind regards,<br><strong>Air Chathams Charter Operations</strong><br><a href='mailto:charters@airchathams.co.nz' style='color:#075c74'>charters@airchathams.co.nz</a></p></td></tr>
<tr><td bgcolor='#f2f7f7' style='padding:14px 28px;color:#6d8589;font-size:11px'>Air Chathams Ltd · Charter Operations</td></tr></table>
</td></tr></table></body></html>"""


_CHARTER_AIRPORT_LATITUDES = {"AKL":-37.0,"WLG":-41.3,"CHC":-43.5,"DUD":-45.9,"IVC":-46.4,"ZQN":-45.0,"ROT":-38.1,"TRG":-37.7,"NPL":-39.0,"NPE":-39.5,"PMR":-40.3,"NSN":-41.3,"WSZ":-41.7,"TUO":-38.7,"TBU":-21.2,"VAV":-18.6,"HPA":-19.8}


def _charter_sector_direction(sector):
    dep, arr = str(sector.get("dep") or "").upper(), str(sector.get("arr") or "").upper()
    if dep in _CHARTER_AIRPORT_LATITUDES and arr in _CHARTER_AIRPORT_LATITUDES:
        return "south" if _CHARTER_AIRPORT_LATITUDES[arr] < _CHARTER_AIRPORT_LATITUDES[dep] else "north"
    return "south"


def _allocate_charter_numbers(sectors, group_digit=0):
    counters = {"south": 0, "north": 1}
    for sector in sectors:
        if str(sector.get("flight_number") or "").strip():
            continue
        direction = _charter_sector_direction(sector)
        suffix = counters[direction]
        sector["flight_number"] = f"3C9{group_digit}{suffix}"
        counters[direction] += 2
    return sectors


def _resolve_charter_place(token, code):
    wanted = str(code or "").strip().upper()
    for place in envision_get_places(token):
        if wanted in {str(place.get(key) or "").strip().upper() for key in ("place", "iataCode", "icaoCode")} and place.get("id") is not None:
            return int(place["id"])
    raise RuntimeError(f"Envision does not recognise airport {wanted}.")


def _parse_charter_local(value, day):
    raw = str(value or "").strip().replace(":", "")
    if not re.fullmatch(r"\d{4}", raw): raise RuntimeError("Each sector must have STD and STA in HHMM format.")
    return datetime.combine(date.fromisoformat(str(day)), time(int(raw[:2]), int(raw[2:])), tzinfo=NZ)


def _friendly_charter_envision_error(exc, sector=None, tail=""):
    """Turn Envision's verbose placement response into an actionable Ops message."""
    detail = str(exc)
    match = re.search(r"Place mismatch with previous flight\s*-\s*([^/]+)/(?:[^/]+/){3}([A-Z]{3})", detail, re.I)
    if match:
        previous_flight, last_airport = match.groups()
        requested_dep = str((sector or {}).get("dep") or "the requested departure station").upper()
        requested_flight = str((sector or {}).get("flight_number") or "this sector").upper()
        tail_label = f" {tail}" if tail else ""
        return (
            f"Envision cannot create {requested_flight} on{tail_label}: its previous flight "
            f"({previous_flight}) leaves it at {last_airport}, but this sector departs {requested_dep}. "
            f"Select a tail positioned at {requested_dep}, or add a positioning sector from {last_airport} to {requested_dep}."
        )
    if "406" in detail:
        return "Envision rejected this sector during validation. Check the selected tail's position, maintenance and schedule, then try again."
    return f"Envision could not create the request: {detail}"


@ui_bp.post("/ops/charter-requests/sync-envision")
@_login_required
def ops_charter_requests_sync_envision():
    user = _current_apg_user()
    if not (user and (user.is_admin or "operations" in _user_permissions(user))) or not _csrf_is_valid(): abort(403)
    approved = CharterRequest.query.filter_by(status="Approved").all()
    sectors_by_request = [(row, _request_sectors(row)) for row in approved]
    dates = [str(s.get("date") or "") for _, sectors in sectors_by_request for s in sectors if s.get("date")]
    if not dates: flash("There are no approved dated sectors to sync.", "danger"); return redirect(url_for("ui.ops_charter_requests"))
    try:
        first, last = date.fromisoformat(min(dates)), date.fromisoformat(max(dates))
        token = envision_authenticate()["token"]
        payload = envision_get_flights(token, datetime.combine(first, time.min, tzinfo=NZ).astimezone(timezone.utc), datetime.combine(last + timedelta(days=1), time.min, tzinfo=NZ).astimezone(timezone.utc))
        flights = _list_from_envision_payload(payload)
        linked = 0
        for row, sectors in sectors_by_request:
            changed = False
            for sector in sectors:
                if sector.get("envision_flight_id"): continue
                try:
                    requested_etd = _parse_charter_local(sector.get("std"), sector.get("date"))
                    requested_eta = _parse_charter_local(sector.get("sta"), sector.get("date"))
                    if requested_eta <= requested_etd: requested_eta += timedelta(days=1)
                except Exception:
                    continue
                best_match, best_score = None, None
                for flight in flights:
                    dep = str(flight.get("departurePlaceDescription") or "").upper().strip()
                    arr = str(flight.get("arrivalPlaceDescription") or "").upper().strip()
                    etd = _parse_env_time_to_nz(flight.get("departureScheduled")); eta = _parse_env_time_to_nz(flight.get("arrivalScheduled"))
                    if not etd or not eta or (dep, arr, etd.date().isoformat()) != (str(sector.get("dep") or "").upper(), str(sector.get("arr") or "").upper(), str(sector.get("date"))): continue
                    score = abs((etd - requested_etd).total_seconds()) + abs((eta - requested_eta).total_seconds())
                    if abs((etd - requested_etd).total_seconds()) <= 7200 and abs((eta - requested_eta).total_seconds()) <= 7200 and (best_score is None or score < best_score): best_match, best_score = flight, score
                if best_match:
                    sector.update({"envision_flight_id":str(best_match.get("id") or ""),"flight_number":str(best_match.get("flightNumberDescription") or sector.get("flight_number") or ""),"tail":str(best_match.get("flightRegistrationDescription") or best_match.get("aircraftRegistration") or "")}); linked += 1; changed = True
            if sectors and all(str(sector.get("envision_flight_id") or "").strip() for sector in sectors):
                row.status = "Pushed to Envision"
                changed = True
            if changed: row.sectors_json = json.dumps(sectors); db.session.add(row)
        db.session.commit(); flash(f"Envision sync linked {linked} existing sector(s).", "success")
    except Exception as exc:
        current_app.logger.exception("Charter Envision matching failed"); flash(f"Envision sync failed: {exc}", "danger")
    return redirect(url_for("ui.ops_charter_requests"))


@ui_bp.post("/ops/charter-requests/<int:request_id>/create-brief")
@_login_required
def ops_charter_request_create_brief(request_id):
    """Create one draft crew brief from one or more connected charter requests."""
    user = _current_apg_user()
    if not (user and (user.is_admin or "operations" in _user_permissions(user))) or not _csrf_is_valid(): abort(403)
    row = db.session.get(CharterRequest, request_id)
    if not row or row.status not in {"Approved", "Pushed to Envision"}: abort(404)
    requested_related_id = request.form.get("include_request_id", type=int)
    source_rows = [row]
    if requested_related_id and requested_related_id != row.id:
        related = db.session.get(CharterRequest, requested_related_id)
        if not related or related.status not in {"Approved", "Pushed to Envision"}:
            flash("The connected charter is no longer available for a combined brief.", "danger")
            return redirect(url_for("ui.ops_charter_requests", tab="approved"))
        source_rows.append(related)
    source_ids = {str(item.id) for item in source_rows}
    for existing in CharterBrief.query.filter_by(status="Draft").all():
        if source_ids & _charter_brief_request_ids(existing):
            flash("A draft charter brief already exists for one of these requests.", "success")
            return redirect(url_for("ui.ops_charter_brief_edit", brief_id=existing.id))
    selected_sector_keys = {str(value) for value in request.form.getlist("sector_key") if str(value)}
    sectors = []
    for source_row in source_rows:
        for sector_index, sector in enumerate(_allocate_charter_numbers(_request_sectors(source_row))):
            sector_key = f"{source_row.id}:{sector_index}"
            if selected_sector_keys and sector_key not in selected_sector_keys:
                continue
            sectors.append({**sector, "source_charter_request_id": source_row.id, "source_sector_key": sector_key})
    if not sectors:
        flash("Select at least one flight to include in the charter brief.", "danger")
        return redirect(url_for("ui.ops_charter_requests", tab="approved"))
    sectors.sort(key=lambda sector: (str(sector.get("date") or ""), str(sector.get("std") or "")))
    dated = sorted({str(sector.get("date") or "") for sector in sectors if sector.get("date")})
    if not dated:
        flash("This request has no dated sectors to put into a brief.", "danger")
        return redirect(url_for("ui.ops_charter_requests", tab="approved"))
    try:
        token = envision_authenticate()["token"]
        start_day, end_day = date.fromisoformat(dated[0]), date.fromisoformat(dated[-1])
        envision_flights = _list_from_envision_payload(envision_get_flights(token, datetime.combine(start_day, time.min, tzinfo=NZ).astimezone(timezone.utc), datetime.combine(end_day + timedelta(days=1), time.min, tzinfo=NZ).astimezone(timezone.utc)))
        for sector in sectors:
            if sector.get("envision_flight_id"):
                continue
            try:
                requested_etd = _parse_charter_local(sector.get("std"), sector.get("date"))
            except Exception:
                continue
            candidates = []
            for flight in envision_flights:
                etd = _parse_env_time_to_nz(flight.get("departureEstimate") or flight.get("departureScheduled"))
                if not etd or etd.date().isoformat() != str(sector.get("date")): continue
                if (str(flight.get("departurePlaceDescription") or "").upper(), str(flight.get("arrivalPlaceDescription") or "").upper()) != (str(sector.get("dep") or "").upper(), str(sector.get("arr") or "").upper()): continue
                delta = abs((etd - requested_etd).total_seconds())
                if delta <= 7200: candidates.append((delta, flight))
            if candidates:
                _, flight = min(candidates, key=lambda item: item[0])
                sector["envision_flight_id"] = str(flight.get("id") or "")
                sector["flight_number"] = str(flight.get("flightNumberDescription") or sector.get("flight_number") or "")
                sector["tail"] = str(flight.get("flightRegistrationDescription") or sector.get("tail") or "")
    except Exception:
        current_app.logger.exception("Unable to enrich charter brief from Envision")
        flash("Draft created, but Envision flights/crew could not be refreshed. Use Refresh operational data in the brief.", "warning")
    brief_sectors, crew, crew_keys = [], [], set()
    for sector in sectors:
        flight_id = str(sector.get("envision_flight_id") or "")
        codes = []
        if flight_id:
            try:
                for member in fetch_envision_crew_for_apg(int(flight_id), include_available_employees=False):
                    code = str(member.get("employee_no") or member.get("employeeNo") or member.get("code") or "").upper()
                    name = str(member.get("name") or "")
                    role_raw = str(member.get("position") or member.get("role") or "")
                    role = "Captain" if role_raw.upper() in {"PIC", "CPT"} or "CAPTAIN" in role_raw.upper() else "First Officer" if role_raw.upper() == "FO" or "FIRST" in role_raw.upper() else "FA Crew" if role_raw.upper() == "FA" or "CABIN" in role_raw.upper() else role_raw or "Crew"
                    if code: codes.append(code)
                    key = code or name.upper()
                    if key and key not in crew_keys:
                        crew.append({"code": code, "name": name, "role": role, "phone": "", "source": "envision"}); crew_keys.add(key)
            except Exception:
                current_app.logger.warning("Could not load crew for charter brief flight %s", flight_id)
        passenger = str(sector.get("passengers") or "").strip()
        brief_sectors.append({"date": sector.get("date") or "", "source_flight_id": flight_id, "flight": sector.get("flight_number") or "", "dep": sector.get("dep") or "", "arr": sector.get("arr") or "", "std": sector.get("std") or "", "sta": sector.get("sta") or "", "aircraft": sector.get("tail") or sector.get("aircraft_type") or "", "flight_type": sector.get("flight_type") or "", "passenger_info": f"{passenger} pax (manual)" if passenger.isdigit() else (passenger or "No passenger total entered"), "manual_pax_count": int(passenger) if passenger.isdigit() else None, "crew_codes": ", ".join(codes), "catering": "" if "position" in str(sector.get("flight_type") or "").lower() else sector.get("catering") or "", "notes": sector.get("notes") or ""})
    start_date, end_date = date.fromisoformat(dated[0]), date.fromisoformat(dated[-1])
    reference = f"CB-{start_date:%Y%m%d}-{secrets.token_hex(2).upper()}"
    source_label = " + ".join(item.reference for item in source_rows)
    providers = AirportHandlingProvider.query.all()
    handler_selections = {}
    for source_row in source_rows:
        for airport in _request_handling_ports(source_row):
            provider, _record = _handling_provider_for_port(source_row, airport, providers)
            if provider:
                handler_selections[airport] = provider.label
    details = {"source_charter_request_id": row.id, "source_charter_request_ids": [item.id for item in source_rows], "source_sector_keys": [sector.get("source_sector_key") for sector in sectors], "sectors": brief_sectors, "crew": crew, "accommodation": [], "transport": [], "ports": [], "handler_selections": handler_selections, "bag_weights_text": "", "operations_notes": "", "crew_notes": ""}
    brief = CharterBrief(reference=reference, title=" + ".join(item.title or item.reference for item in source_rows), charterer=source_label, start_date=start_date, end_date=end_date, details_json=json.dumps(details, ensure_ascii=False))
    db.session.add(brief); db.session.commit()
    flash("Combined draft charter brief created." if len(source_rows) > 1 else "Draft charter brief created from the approved request.", "success")
    return redirect(url_for("ui.ops_charter_brief_edit", brief_id=brief.id))


@ui_bp.post("/ops/charter-requests/<int:request_id>/request-cancellation")
@_login_required
def ops_charter_request_cancellation_request(request_id):
    user = _current_apg_user()
    if not _charter_request_access(user) or not _csrf_is_valid(): abort(403)
    row = db.session.get(CharterRequest, request_id)
    if not row or row.status not in {"Approved", "Pushed to Envision"}: abort(404)
    reason = str(request.form.get("cancellation_reason") or "").strip()
    if not reason:
        flash("Enter a reason for the cancellation request.", "danger")
        return redirect(url_for("ui.ops_charter_requests", tab="approved"))
    row.status = "Cancellation requested"
    row.decision_note = reason
    row.decided_at = datetime.utcnow()
    db.session.add(row); db.session.commit()
    recipients = [item.strip() for item in str((db.session.get(EmailSettings, 1) or EmailSettings()).charter_request_recipients or "").split(",") if "@" in item]
    if recipients:
        try:
            from .routes import _send_email_via_graph, _charter_email_sender
            _send_email_via_graph(_charter_email_sender(), recipients, f"Charter cancellation approval required — {row.reference}", f"{user.display_name or user.email} requested cancellation of {row.reference}. Reason: {reason}. Review it at {url_for('ui.ops_charter_requests', _external=True)}.")
        except Exception:
            current_app.logger.exception("Charter cancellation notification could not be sent")
    flash("Cancellation request sent to Operations for approval.", "success")
    return redirect(url_for("ui.ops_charter_requests"))


@ui_bp.post("/ops/charter-requests/<int:request_id>/approve-cancellation")
@_login_required
def ops_charter_request_cancellation_approve(request_id):
    user = _current_apg_user()
    if not (user and (user.is_admin or "operations" in _user_permissions(user))) or not _csrf_is_valid(): abort(403)
    row = db.session.get(CharterRequest, request_id)
    if not row or row.status != "Cancellation requested": abort(404)
    try:
        cancel_code_id = int(request.form.get("cancel_code_id") or 0)
        if cancel_code_id <= 0: raise RuntimeError("Choose an Envision cancellation code.")
        token = envision_authenticate()["token"]
        sectors = _request_sectors(row)
        assigned = []
        for sector in sectors:
            flight_id = str(sector.get("envision_flight_id") or "").strip()
            if not flight_id: continue
            if envision_get_flight_crew(token, int(flight_id)):
                assigned.append(str(sector.get("flight_number") or flight_id))
        if assigned and not request.form.get("crew_standby_confirmed"):
            raise RuntimeError("Crew are assigned to " + ", ".join(assigned) + ". In Envision please ensure you allocate existing crew SBY duties in lieu of this charter flight, then tick the confirmation and approve again.")
        for sector in sectors:
            flight_id = str(sector.get("envision_flight_id") or "").strip()
            if flight_id:
                envision_cancel_flight(token, flight_id, {"flightId": int(flight_id), "cancelCodeId": cancel_code_id, "remarks": f"Charter cancellation approved: {row.decision_note or 'No reason supplied'}"})
        row.status = "Cancelled"; row.decision_by = user.display_name or user.email; row.decided_at = datetime.utcnow()
        db.session.add(row); db.session.commit()
        flash("Charter cancellation approved and linked Envision flights cancelled.", "success")
    except Exception as exc:
        flash(f"Cancellation was not approved: {exc}", "danger")
    return redirect(url_for("ui.ops_charter_requests"))


@ui_bp.route("/ops/charter-requests", methods=["GET", "POST"])
@_login_required
def ops_charter_requests():
    user = _current_apg_user()
    if not _charter_request_access(user): abort(403)
    if request.method == "POST":
        if not _csrf_is_valid():
            flash("Your form expired. Please try again.", "danger")
            return redirect(url_for("ui.ops_charter_requests"))
        try:
            sectors = json.loads(request.form.get("sectors_json") or "[]")
            if not isinstance(sectors, list) or not sectors: raise ValueError
        except ValueError:
            flash("Add at least one valid charter sector.", "danger")
            return redirect(url_for("ui.ops_charter_requests"))
        if any(str(sector.get("aircraft_type") or "") not in {"SF34", "ATR72"} for sector in sectors):
            flash("Choose an aircraft type for every requested aircraft group.", "danger")
            return redirect(url_for("ui.ops_charter_requests"))
        ref = str(request.form.get("reference") or "").strip().upper() or f"CR-{_nz_today():%Y%m%d}-{secrets.token_hex(2).upper()}"
        active_reference_statuses = {
            "Pending approval",
            "Approved",
            "Pushed to Envision",
            "Cancellation requested",
        }
        if CharterRequest.query.filter(
            CharterRequest.reference == ref,
            CharterRequest.status.in_(active_reference_statuses),
        ).first():
            flash("That booking reference already has an active request. Cancel or complete the existing request before submitting it again.", "danger")
            return redirect(url_for("ui.ops_charter_requests"))
        row = CharterRequest(reference=ref, title=str(request.form.get("title") or ref).strip(), sectors_json=json.dumps(sectors), created_by=user.display_name or user.email)
        db.session.add(row); db.session.commit()
        recipients = [item.strip() for item in str((db.session.get(EmailSettings, 1) or EmailSettings()).charter_request_recipients or "").split(",") if "@" in item]
        if recipients:
            try:
                from .routes import _send_email_via_graph, _charter_email_sender
                _send_email_via_graph(_charter_email_sender(), recipients, f"Charter approval required — {ref}", f"{row.title} has been submitted by {row.created_by}. Review it at {url_for('ui.ops_charter_request_detail', request_id=row.id, _external=True)}.")
            except Exception:
                current_app.logger.exception("Charter approval notification could not be sent")
        flash("Charter request submitted for Operations approval.", "success")
        return redirect(url_for("ui.ops_charter_request_detail", request_id=row.id))
    tab = request.args.get("tab") if request.args.get("tab") in {"approved", "cancelled"} else "pending"
    rows = CharterRequest.query.order_by(CharterRequest.created_at.desc()).all()
    pending_count = sum(row.status in {"Pending approval", "Cancellation requested"} for row in rows)
    if tab == "pending" and not pending_count:
        tab = "approved"
    if tab == "approved": rows = [row for row in rows if row.status in {"Approved", "Pushed to Envision"}]
    elif tab == "cancelled": rows = [row for row in rows if row.status == "Cancelled"]
    else: rows = [row for row in rows if row.status in {"Pending approval", "Cancellation requested"}]
    request_rows = []
    for item in rows:
        sectors = _allocate_charter_numbers(_request_sectors(item))
        for sector_index, sector in enumerate(sectors):
            sector["_request_id"] = item.id
            sector["_sector_key"] = f"{item.id}:{sector_index}"
        days = {}
        for sector in sectors:
            days.setdefault(str(sector.get("date") or "Date TBC"), []).append(sector)
        dated = sorted((day for day in days if re.fullmatch(r"\d{4}-\d{2}-\d{2}", day)))
        start = date.fromisoformat(dated[0]) if dated else None
        request_rows.append({"request": item, "days": days, "start_date": start, "days_to_charter": (start - _nz_today()).days if start else None})
    request_rows.sort(key=lambda item: (item["start_date"] is None, item["start_date"] or date.max, min((str(sector.get("std") or "9999").replace(":", "") for sectors in item["days"].values() for sector in sectors), default="9999")))
    rotations = []
    for group in request_rows:
        for sectors in group["days"].values():
            for sector in sectors:
                if not sector.get("tail") or not sector.get("date"): continue
                try:
                    etd = _parse_charter_local(sector.get("std"), sector.get("date")); eta = _parse_charter_local(sector.get("sta"), sector.get("date"))
                    if eta <= etd: eta += timedelta(days=1)
                    rotations.append((str(sector["tail"]).upper(), etd, eta, group, sector))
                except Exception: continue
    rotations.sort(key=lambda item: (item[0], item[1]))
    for previous, following in zip(rotations, rotations[1:]):
        tail, _, previous_eta, previous_group, previous_sector = previous
        next_tail, next_etd, _, next_group, next_sector = following
        handover_airport = str(previous_sector.get("arr") or "").upper()
        if tail != next_tail or previous_group is next_group or handover_airport == "AKL" or handover_airport != str(next_sector.get("dep") or "").upper(): continue
        gap = (next_etd - previous_eta).total_seconds() / 60
        if 0 <= gap <= 24 * 60:
            previous_sector["continues_to"] = Markup(f'<a href="#charter-request-{next_group["request"].id}">{escape(next_group["request"].reference)}</a>')
            previous_sector["continues_to_id"] = next_group["request"].id
            next_sector["continues_from"] = Markup(f'<a href="#charter-request-{previous_group["request"].id}">{escape(previous_group["request"].reference)}</a>')
            next_sector["continues_from_id"] = previous_group["request"].id
            next_sector["connection_gap_minutes"] = round(gap)
    cancellation_codes = []
    if any(group["request"].status == "Cancellation requested" for group in request_rows) and (user.is_admin or "operations" in _user_permissions(user)):
        try:
            cancellation_codes = envision_get_cancel_codes(envision_authenticate()["token"])
        except Exception:
            current_app.logger.exception("Could not load Envision cancellation codes")
    groups_by_request_id = {group["request"].id: group for group in request_rows}
    for group in request_rows:
        reference = str(group["request"].reference)
        request_row = group["request"]
        group["brief"] = next((brief for brief in CharterBrief.query.order_by(CharterBrief.updated_at.desc()).all() if str(request_row.id) in _charter_brief_request_ids(brief)), None)
        brief_details = _charter_brief_details(group["brief"]) if group["brief"] else {}
        group["brief_sector_keys"] = set(brief_details.get("source_sector_keys", []))
        group["brief_has_sector_selection"] = "source_sector_keys" in brief_details
        related_ids = set()
        for sectors in group["days"].values():
            for sector in sectors:
                related_ids.update(str(value) for value in (sector.get("continues_from_id"), sector.get("continues_to_id")) if value)
        group["related_request_ids"] = sorted(related_ids, key=int)
        connected_tails = set()
        for sectors in group["days"].values():
            for sector in sectors:
                if sector.get("continues_from_id") or sector.get("continues_to_id"):
                    connected_tails.add(str(sector.get("tail") or "").upper())
        group["combined_brief_options"] = []
        for related_id in group["related_request_ids"]:
            related_group = groups_by_request_id.get(int(related_id))
            if not related_group:
                continue
            options = []
            for candidate in (group, related_group):
                for candidate_sectors in candidate["days"].values():
                    for sector in candidate_sectors:
                        options.append({"request_id": candidate["request"].id, "reference": str(candidate["request"].reference), "sector": sector, "checked": bool(str(sector.get("tail") or "").upper() in connected_tails)})
            group["combined_brief_options"].append({"request_id": related_id, "options": options})
        if request_row.status == "Pending approval" or (request_row.status in {"Approved", "Pushed to Envision"} and (user.is_admin or "operations" in _user_permissions(user))):
            group["request"].reference = Markup(f'<a id="charter-request-{request_row.id}" href="{escape(url_for("ui.ops_charter_request_edit", request_id=request_row.id))}">{escape(reference)}</a>')
        else:
            group["request"].reference = Markup(f'<span id="charter-request-{request_row.id}">{escape(reference)}</span>')
    catering_services, _ = _charter_operations_directory()
    return render_template("charter_requests_tabs.html", request_rows=request_rows, can_operate=bool(user.is_admin or "operations" in _user_permissions(user)), format_date=_brief_print_date, active_tab=tab, pending_count=pending_count, cancellation_codes=cancellation_codes, catering_services=catering_services)


@ui_bp.route("/ops/charter-planner-settings", methods=["GET", "POST"])
@_admin_required
def ops_charter_planner_settings():
    config = db.session.get(AppConfig, 1) or AppConfig(id=1)
    if request.method == "POST":
        if not _csrf_is_valid(): abort(403)
        selected = sorted({str(value).strip() for value in request.form.getlist("registration_id") if str(value).strip().isdigit()}, key=int)
        config.charter_planner_registrations_json = json.dumps({registration_id: str(request.form.get(f"aircraft_type_{registration_id}") or "").strip() for registration_id in selected})
        db.session.add(config); db.session.commit()
        flash("Tail-planning aircraft settings saved.", "success")
        return redirect(url_for("ui.ops_charter_planner_settings"))
    try:
        saved = json.loads(config.charter_planner_registrations_json or "[]")
    except (TypeError, ValueError):
        saved = []
    selected = set(saved if isinstance(saved, list) else saved.keys())
    aircraft_types = {} if isinstance(saved, list) else saved
    try:
        token = envision_authenticate()["token"]
        registrations = sorted(envision_get_line_registrations(token), key=lambda item: str(item.get("registration") or ""))
    except Exception as exc:
        registrations = []
        flash(f"Current Envision aircraft could not be loaded: {exc}", "danger")
    return render_template("charter_planner_settings.html", registrations=registrations, selected=selected, aircraft_types=aircraft_types)


@ui_bp.get("/ops/charter-requests/<int:request_id>/planning")
@_login_required
def ops_charter_request_planning(request_id):
    """Planning board: scheduled Envision flights plus an uncommitted charter overlay."""
    user = _current_apg_user()
    if not _charter_request_access(user): abort(403)
    row = db.session.get(CharterRequest, request_id)
    if not row or row.status not in {"Pending approval", "Approved", "Pushed to Envision"}: abort(404)
    if not (user.is_admin or "operations" in _user_permissions(user)): abort(403)
    config = db.session.get(AppConfig, 1) or AppConfig(id=1)
    tail_debug_enabled = bool(config.charter_tail_debug_enabled)
    tail_change_debug = session.get("charter_tail_change_debug")
    if tail_debug_enabled and isinstance(tail_change_debug, dict) and str(tail_change_debug.get("request_id")) == str(row.id):
        session.pop("charter_tail_change_debug", None)
    else:
        session.pop("charter_tail_change_debug", None)
        tail_change_debug = None
    sectors = _allocate_charter_numbers(_request_sectors(row))
    available_days = sorted({str(sector.get("date") or "") for sector in sectors if sector.get("date")})
    requested_day = request.args.get("date")
    day_value = requested_day if requested_day in available_days else (available_days[0] if available_days else _nz_today().isoformat())
    try:
        day = date.fromisoformat(day_value)
        token = envision_authenticate()["token"]
        raw_flights = _list_from_envision_payload(envision_get_flights(token, datetime.combine(day, time.min, tzinfo=NZ).astimezone(timezone.utc), datetime.combine(day + timedelta(days=1), time.min, tzinfo=NZ).astimezone(timezone.utc)))
        prior_flights = _list_from_envision_payload(envision_get_flights(token, datetime.combine(day - timedelta(days=14), time.min, tzinfo=NZ).astimezone(timezone.utc), datetime.combine(day, time.min, tzinfo=NZ).astimezone(timezone.utc)))
        registrations = sorted(envision_get_line_registrations(token), key=lambda item: str(item.get("registration") or ""))
        line_by_registration_id = {str(item.get("regIdDefault") or ""): item for item in envision_get_lines(token)}
        for item in registrations:
            line = line_by_registration_id.get(str(item.get("id") or ""))
            item["planner_line_id"] = int(line.get("id") or 0) if line else 0
        try:
            saved_registrations = json.loads((config.charter_planner_registrations_json if config else "[]") or "[]")
        except (TypeError, ValueError):
            saved_registrations = []
        allowed_registration_ids = set(saved_registrations if isinstance(saved_registrations, list) else saved_registrations.keys())
        aircraft_type_overrides = {} if isinstance(saved_registrations, list) else saved_registrations
        if allowed_registration_ids:
            registrations = [item for item in registrations if str(item.get("id") or "") in allowed_registration_ids]
        for item in registrations:
            item["planner_aircraft_type"] = str(aircraft_type_overrides.get(str(item.get("id") or "")) or item.get("model") or "Aircraft")
    except Exception as exc:
        current_app.logger.exception("Charter planning board could not load Envision")
        flash(f"The live Envision schedule could not be loaded: {exc}", "danger")
        raw_flights, prior_flights, registrations = [], [], []
    scheduled = []
    for flight in raw_flights:
        etd = _parse_env_time_to_nz(flight.get("departureEstimate") or flight.get("departureScheduled"))
        eta = _parse_env_time_to_nz(flight.get("arrivalEstimate") or flight.get("arrivalScheduled"))
        if not etd or etd.date() != day: continue
        scheduled.append({"tail": str(flight.get("flightRegistrationDescription") or flight.get("aircraftRegistration") or "Unassigned"), "flight": str(flight.get("flightNumberDescription") or "Flight"), "dep": str(flight.get("departurePlaceDescription") or ""), "arr": str(flight.get("arrivalPlaceDescription") or ""), "std": etd.strftime("%H%M"), "sta": eta.strftime("%H%M") if eta else "", "flight_type": str(flight.get("flightTypeDescription") or flight.get("flightType") or "")})
    ghosts = [sector for sector in sectors if str(sector.get("date")) == day_value]
    aircraft_groups = []
    seen_groups = set()
    for index, sector in enumerate(sectors, start=1):
        group_id = str(sector.get("aircraft_group") or "aircraft-1")
        if group_id in seen_groups:
            continue
        seen_groups.add(group_id)
        aircraft_groups.append({
            "id": group_id,
            "label": f"Aircraft {len(aircraft_groups) + 1}",
            "aircraft_type": str(sector.get("aircraft_type") or "Aircraft type not selected"),
        })
    ground_positions = {}
    day_start = datetime.combine(day, time.min, tzinfo=NZ)
    for flight in prior_flights:
        arrival = _parse_env_time_to_nz(flight.get("arrivalEstimate") or flight.get("arrivalScheduled"))
        tail = str(flight.get("flightRegistrationDescription") or flight.get("aircraftRegistration") or "").strip()
        airport = str(flight.get("arrivalPlaceDescription") or "").strip()
        if not tail or not airport or not arrival or arrival > day_start:
            continue
        current = ground_positions.get(tail)
        if not current or arrival > current["arrival"]:
            ground_positions[tail] = {"airport": airport, "arrival": arrival}
    ground_positions = {tail: {"airport": item["airport"], "arrival": item["arrival"].strftime("%d %b %H%M")} for tail, item in ground_positions.items()}
    maintenance_by_registration = {}
    # Work orders are deliberately advisory in planning: a failed lookup must not
    # hide the scheduled board or prevent Operations assigning an aircraft.
    if registrations and token:
        day_start = datetime.combine(day, time.min, tzinfo=NZ)
        day_end = day_start + timedelta(days=1)
        for registration in registrations:
            registration_id = int(registration.get("id") or 0)
            if not registration_id:
                continue
            try:
                work_orders = _fetch_work_orders_for_registration(token, registration_id)
                items = []
                for work_order in work_orders:
                    start = _parse_env_time_to_nz(work_order.get("plannedStartDate") or work_order.get("actualStartDate") or work_order.get("openDate"))
                    end = _parse_env_time_to_nz(work_order.get("plannedFinishDate") or work_order.get("actualFinishDate") or work_order.get("closeDate"))
                    if not start and not end:
                        continue
                    start = start or (end - timedelta(hours=2))
                    end = end or (start + timedelta(hours=2))
                    if end <= start:
                        end = start + timedelta(hours=1)
                    if start < day_end and end > day_start:
                        items.append({"std": max(start, day_start).strftime("%H%M"), "sta": min(end, day_end).strftime("%H%M"), "title": str(work_order.get("description") or work_order.get("title") or "Maintenance"), "order": str(work_order.get("orderNo") or work_order.get("workOrderNo") or "")})
                maintenance_by_registration[str(registration_id)] = items
            except Exception:
                current_app.logger.warning("Charter planner maintenance lookup failed for registration %s", registration_id)
    registration_by_tail = {
        str(item.get("registration") or item.get("registrationDescription") or "").upper(): str(item.get("id") or "")
        for item in registrations
    }
    saved_tail_assignments = {}
    for sector in sectors:
        group_id = str(sector.get("aircraft_group") or "aircraft-1")
        registration_id = registration_by_tail.get(str(sector.get("tail") or "").upper())
        if registration_id:
            saved_tail_assignments[group_id] = registration_id
    return render_template("charter_request_planning.html", charter_request=row, day=day_value, available_days=available_days, ghosts=ghosts, aircraft_groups=aircraft_groups, scheduled=scheduled, registrations=registrations, maintenance_by_registration=maintenance_by_registration, ground_positions=ground_positions, saved_tail_assignments=saved_tail_assignments, tail_change_debug=tail_change_debug, tail_debug_enabled=tail_debug_enabled, planning_mode="change_tail" if row.status == "Pushed to Envision" else "approve")


@ui_bp.post("/ops/charter-planner-debug-settings")
@_admin_required
def ops_charter_planner_debug_settings():
    if not _csrf_is_valid(): abort(403)
    config = db.session.get(AppConfig, 1) or AppConfig(id=1)
    config.charter_tail_debug_enabled = request.form.get("tail_debug_enabled") == "1"
    db.session.add(config); db.session.commit()
    flash(f"Envision tail-change debugging {'enabled' if config.charter_tail_debug_enabled else 'disabled'}.", "success")
    request_id = request.form.get("request_id", type=int)
    day_value = str(request.form.get("date") or "")
    if request_id:
        return redirect(url_for("ui.ops_charter_request_planning", request_id=request_id, date=day_value or None))
    return redirect(url_for("ui.ops_charter_requests"))


@ui_bp.post("/ops/charter-requests/import")
@_login_required
def ops_charter_requests_import():
    user = _current_apg_user()
    if not _charter_request_access(user): abort(403)
    if not _csrf_is_valid():
        flash("Your form expired. Please try again.", "danger"); return redirect(url_for("ui.ops_charter_requests"))
    groups, current = [], []
    for line in str(request.form.get("schedule_text") or "").splitlines():
        cells = [cell.strip() for cell in line.split("\t")]
        if not any(cells):
            if current: groups.append(current); current = []
            continue
        if len(cells) < 7 or cells[0].lower() in {"booking ref", "saab one", "saab two"}: continue
        current.append(cells)
    if current: groups.append(current)
    created = 0
    for rows in groups:
        reference = rows[0][0]
        if CharterRequest.query.filter_by(reference=reference).first():
            reference = f"{reference} ({rows[0][1] if len(rows[0]) > 1 else created + 1})"
        sectors = []
        for row in rows:
            def col(index): return row[index] if len(row) > index else ""
            raw_date = col(1)
            try: sector_date = datetime.strptime(raw_date.replace(",", ""), "%A %d %B %Y").date().isoformat()
            except ValueError: continue
            sectors.append({"date":sector_date,"dep":col(2).upper(),"arr":col(3).upper(),"std":col(4).replace(":", ""),"sta":col(5).replace(":", ""),"aircraft_type":col(6),"flight_type":col(7),"passengers":col(8),"baggage":col(9),"catering":col(10),"ground_handling":col(11),"notes":col(12)})
        if sectors:
            db.session.add(CharterRequest(reference=reference, title=reference, status="Approved", sectors_json=json.dumps(sectors), created_by="Spreadsheet import", decision_by="Spreadsheet import", decided_at=datetime.utcnow())); created += 1
    db.session.commit(); flash(f"Imported {created} approved charter request(s).", "success")
    return redirect(url_for("ui.ops_charter_requests"))


@ui_bp.route("/ops/charter-requests/<int:request_id>/edit", methods=["GET", "POST"])
@_login_required
def ops_charter_request_edit(request_id):
    user = _current_apg_user()
    if not _charter_request_access(user): abort(403)
    row = db.session.get(CharterRequest, request_id)
    can_operate = bool(user.is_admin or "operations" in _user_permissions(user))
    if not row or row.status not in {"Pending approval", "Approved", "Pushed to Envision"}: abort(404)
    if row.status != "Pending approval" and not can_operate: abort(403)
    if request.method == "POST":
        if not _csrf_is_valid(): abort(403)
        try:
            sectors = json.loads(request.form.get("sectors_json") or "[]")
            if not isinstance(sectors, list) or not sectors: raise ValueError
        except ValueError:
            flash("Add at least one complete sector before resubmitting.", "danger")
            return redirect(url_for("ui.ops_charter_request_edit", request_id=row.id))
        existing_sectors = _request_sectors(row)
        sectors = [{**(existing_sectors[index] if index < len(existing_sectors) else {}), **sector} for index, sector in enumerate(sectors)]
        row.reference = str(request.form.get("reference") or row.reference).strip().upper(); row.title = str(request.form.get("title") or row.title).strip(); row.sectors_json = json.dumps(sectors); row.created_by = user.display_name or user.email
        if row.status == "Pending approval":
            row.decision_by = row.decision_note = None; row.decided_at = None
        db.session.add(row); db.session.commit(); clear_gantt_flight_cache()
        flash("Approved charter details updated." if row.status in {"Approved", "Pushed to Envision"} else "Charter request updated and resubmitted for approval.", "success")
        return redirect(url_for("ui.ops_charter_requests"))
    return render_template("charter_request_edit_v2.html", charter_request=row, sectors=_request_sectors(row))


@ui_bp.route("/ops/charter-requests/<int:request_id>/ground-handling", methods=["GET", "POST"])
@_login_required
def ops_charter_request_ground_handling(request_id):
    """Operations checklist and outbound requests for charter handling providers."""
    user = _current_apg_user()
    can_operate = bool(user and (user.is_admin or "operations" in _user_permissions(user)))
    row = db.session.get(CharterRequest, request_id)
    if not row or not can_operate or row.status not in {"Approved", "Pushed to Envision"}:
        abort(404)
    providers = AirportHandlingProvider.query.order_by(AirportHandlingProvider.airport, AirportHandlingProvider.label).all()
    if request.method == "POST":
        if not _csrf_is_valid(): abort(403)
        airport = str(request.form.get("airport") or "").upper().strip()
        provider_id = request.form.get("provider_id", type=int)
        provider = db.session.get(AirportHandlingProvider, provider_id) if provider_id else None
        if airport not in _request_handling_ports(row) or not provider or provider.airport != airport:
            flash("Choose a valid handling provider for that airport.", "danger")
            return redirect(url_for("ui.ops_charter_request_ground_handling", request_id=row.id))
        record = CharterHandlingRequest.query.filter_by(charter_request_id=row.id, airport=airport).first()
        is_new_record = record is None
        provider_changed = False
        if not record:
            record = CharterHandlingRequest(charter_request_id=row.id, airport=airport, provider_id=provider.id)
        elif record.provider_id != provider.id:
            record.provider_id, record.status, record.sent_at, record.sent_by = provider.id, "Not sent", None, None
            provider_changed = True
        db.session.add(record); db.session.flush()
        actor = user.display_name or user.email or "Operations"
        if is_new_record or provider_changed:
            _log_handling_event(record, "Provider allocated", actor, provider.label)
        record.recipient_emails = "\n".join(email.strip() for email in (provider.email_addresses or "").splitlines() if "@" in email)
        record.subject = f"Ground handling request — {row.reference} — {airport} [APG-HANDLING-{record.id}]"
        provider_update_url = url_for(
            "ui.charter_handling_provider_update",
            token=_handling_provider_update_token(provider.id),
            _external=True,
        )
        confirmation_url = url_for(
            "ui.charter_handling_confirmation",
            token=_handling_confirmation_token(record.id),
            _external=True,
        )
        record.body = _handling_request_text(row, airport, provider, record.id, provider_update_url, confirmation_url)
        action = request.form.get("action")
        if action == "confirm":
            record.status = "Confirmed"
            _log_handling_event(record, "Handling confirmed", actor, str(request.form.get("confirmation_note") or "").strip() or None)
            flash(f"Handling confirmed for {airport} with {provider.label}.", "success")
        elif action == "send":
            recipients = [email.strip() for email in record.recipient_emails.splitlines() if "@" in email]
            cc = [email.strip() for email in str((db.session.get(EmailSettings, 1) or EmailSettings()).charter_request_recipients or "").split(",") if "@" in email]
            if not recipients:
                db.session.rollback()
                flash(f"{provider.label} has no saved email address. Add one in Charter operations first.", "danger")
                return redirect(url_for("ui.ops_charter_request_ground_handling", request_id=row.id))
            try:
                from .routes import _send_email_via_graph, _charter_email_sender
                if not _send_email_via_graph(_charter_email_sender(), recipients, record.subject, record.body, html_body=_handling_request_html(row, airport, provider, record.id, provider_update_url, confirmation_url), cc_recipients=cc):
                    raise RuntimeError("Microsoft Graph did not accept the handling request.")
                record.status = "Confirmed" if record.status == "Confirmed" else "Sent"
                record.sent_at, record.sent_by = datetime.utcnow(), actor
                _log_handling_event(record, "Request sent", actor, f"Sent to {', '.join(recipients)}")
                flash(f"Handling request sent to {provider.label}.", "success")
            except Exception as exc:
                current_app.logger.exception("Unable to send charter handling request")
                flash(f"Handling request was not sent: {exc}", "danger")
        else:
            flash(f"{provider.label} allocated for {airport}.", "success")
        db.session.add(record); db.session.commit()
        return redirect(url_for("ui.ops_charter_request_ground_handling", request_id=row.id))
    records = CharterHandlingRequest.query.filter_by(charter_request_id=row.id).all()
    events_by_record = {}
    if records:
        event_rows = CharterHandlingEvent.query.filter(
            CharterHandlingEvent.handling_request_id.in_([record.id for record in records])
        ).order_by(CharterHandlingEvent.created_at.desc()).all()
        for event in event_rows:
            events_by_record.setdefault(event.handling_request_id, []).append(event)
    checklist = []
    for airport in _request_handling_ports(row):
        choices = [provider for provider in providers if provider.airport == airport]
        selected, record = _handling_provider_for_port(row, airport, providers)
        events = events_by_record.get(record.id, []) if record else []
        send_count = sum(1 for event in events if event.event_type == "Request sent") + (1 if record and record.sent_at and not any(event.event_type == "Request sent" for event in events) else 0)
        checklist.append({"airport": airport, "providers": choices, "selected": selected, "record": record, "events": events, "send_count": send_count, "auto_selected": bool(selected and not record and len(choices) == 1)})
    return render_template("charter_request_ground_handling.html", charter_request=row, checklist=checklist, sectors=_request_sectors(row))


@ui_bp.route("/ops/charter-requests/<int:request_id>", methods=["GET", "POST"])
@_login_required
def ops_charter_request_detail(request_id):
    user = _current_apg_user()
    if not _charter_request_access(user): abort(403)
    row = db.session.get(CharterRequest, request_id)
    if not row: abort(404)
    can_operate = bool(user.is_admin or "operations" in _user_permissions(user))
    if request.method == "POST":
        if not _csrf_is_valid() or not can_operate: abort(403)
        action = request.form.get("action")
        if action in {"approve", "reject"}:
            row.status = "Approved" if action == "approve" else "Rejected"
            if action == "approve":
                try:
                    tour_group = int(request.form.get("tour_group") or 0)
                    if not 0 <= tour_group <= 9: raise ValueError
                except ValueError:
                    flash("Choose one flight-number middle digit from 0 to 9.", "danger")
                    return redirect(url_for("ui.ops_charter_request_planning", request_id=row.id))
                sectors = _request_sectors(row)
                _allocate_charter_numbers(sectors, tour_group)
                try:
                    tail_assignments = json.loads(request.form.get("tail_assignments") or "{}")
                except (TypeError, ValueError):
                    tail_assignments = {}
                if not isinstance(tail_assignments, dict):
                    tail_assignments = {}
                legacy_registration_id = request.form.get("registration_id", type=int)
                current_sector, current_tail = None, ""
                try:
                    token = envision_authenticate()["token"]
                    # A flight number must be unique on its operating day. Check both
                    # Envision (authoritative schedule) and approved local requests.
                    flights_by_day = {}
                    for sector in sectors:
                        day_text = str(sector.get("date") or "")
                        if not day_text or day_text in flights_by_day: continue
                        day_value = date.fromisoformat(day_text)
                        flights_by_day[day_text] = _list_from_envision_payload(envision_get_flights(token, datetime.combine(day_value, time.min, tzinfo=NZ).astimezone(timezone.utc), datetime.combine(day_value + timedelta(days=1), time.min, tzinfo=NZ).astimezone(timezone.utc)))
                    conflicts = []
                    for sector in sectors:
                        number = str(sector.get("flight_number") or "").strip().upper()
                        day_text = str(sector.get("date") or "")
                        if not number or not day_text: continue
                        if any(str(flight.get("flightNumberDescription") or "").strip().upper() == number for flight in flights_by_day.get(day_text, [])):
                            conflicts.append(f"{number} on {day_text}")
                    for other in CharterRequest.query.filter(CharterRequest.id != row.id, CharterRequest.status.in_(["Approved", "Pushed to Envision"])).all():
                        for other_sector in _request_sectors(other):
                            candidate = f"{str(other_sector.get('flight_number') or '').strip().upper()} on {str(other_sector.get('date') or '')}"
                            if candidate in conflicts or not other_sector.get("flight_number"): continue
                            if any(candidate == f"{str(sector.get('flight_number') or '').strip().upper()} on {str(sector.get('date') or '')}" for sector in sectors): conflicts.append(candidate)
                    if conflicts:
                        flash(f"Approval stopped: flight number already exists — {', '.join(conflicts[:5])}. Choose another middle digit.", "danger")
                        return redirect(url_for("ui.ops_charter_request_planning", request_id=row.id))
                    registrations_by_id = {str(item.get("id") or ""): item for item in envision_get_line_registrations(token)}
                    group_ids = {str(sector.get("aircraft_group") or "aircraft-1") for sector in sectors}
                    if legacy_registration_id and not tail_assignments:
                        tail_assignments = {group_id: legacy_registration_id for group_id in group_ids}
                    missing_groups = [group_id for group_id in group_ids if not str(tail_assignments.get(group_id) or "") in registrations_by_id]
                    if missing_groups: raise RuntimeError("Assign an operating tail to every requested aircraft group in the planning Gantt before approval.")
                    for sector in sectors:
                        registration = registrations_by_id[str(tail_assignments[str(sector.get("aircraft_group") or "aircraft-1")])]
                        sector["tail"] = str(registration.get("registration") or registration.get("registrationDescription") or "")
                    types = envision_get_flight_types(token)
                    for sector in sectors:
                        if sector.get("envision_flight_id"): continue
                        current_sector = sector
                        ftype = str(sector.get("flight_type") or "Charter").lower()
                        type_row = next((item for item in types if "charter" in str(item.get("description") or item.get("flightTypeDescription") or "").lower() and (("position" in ftype) == ("position" in str(item.get("description") or item.get("flightTypeDescription") or "").lower()))), None)
                        if not type_row: raise RuntimeError(f"No Envision flight type found for {sector.get('flight_type') or 'Charter'}.")
                        etd, eta = _parse_charter_local(sector.get("std"), sector.get("date")), _parse_charter_local(sector.get("sta"), sector.get("date"))
                        if eta <= etd: eta += timedelta(days=1)
                        registration = registrations_by_id[str(tail_assignments[str(sector.get("aircraft_group") or "aircraft-1")])]
                        current_tail = str(registration.get("registration") or registration.get("registrationDescription") or "")
                        created = envision_create_flight(token, {"ignoreValidations":False,"flightDate":etd.astimezone(timezone.utc).isoformat().replace("+00:00","Z"),"departurePlaceId":_resolve_charter_place(token, sector.get("dep")),"arrivalPlaceId":_resolve_charter_place(token, sector.get("arr")),"scheduledTimeDeparture":etd.astimezone(timezone.utc).isoformat().replace("+00:00","Z"),"scheduledTimeArrival":eta.astimezone(timezone.utc).isoformat().replace("+00:00","Z"),"flightTypeId":int(type_row["id"]),"modelId":int(registration.get("modelId") or 0),"registrationId":int(registration.get("id") or 0),"flightNumber":sector["flight_number"]})
                        sector["envision_flight_id"] = str(created.get("id") or "")
                    row.sectors_json = json.dumps(sectors)
                    row.status = "Pushed to Envision"
                    clear_gantt_flight_cache()
                except Exception as exc:
                    flash(_friendly_charter_envision_error(exc, current_sector, current_tail), "danger")
                    return redirect(url_for("ui.ops_charter_request_planning", request_id=row.id))
            row.decision_by = user.display_name or user.email; row.decision_note = str(request.form.get("decision_note") or "").strip() or None; row.decided_at = datetime.utcnow()
            db.session.add(row); db.session.commit(); flash(f"Request {row.status.lower()}.", "success")
        elif action == "change_tail" and row.status == "Pushed to Envision":
            debug_enabled = bool((db.session.get(AppConfig, 1) or AppConfig()).charter_tail_debug_enabled)
            change_debug = []
            try:
                try:
                    tail_assignments = json.loads(request.form.get("tail_assignments") or "{}")
                except (TypeError, ValueError):
                    tail_assignments = {}
                token = envision_authenticate()["token"]
                sectors = _request_sectors(row)
                registrations_by_id = {str(item.get("id") or ""): item for item in envision_get_line_registrations(token)}
                line_by_registration_id = {str(item.get("regIdDefault") or ""): item for item in envision_get_lines(token)}
                group_ids = {str(sector.get("aircraft_group") or "aircraft-1") for sector in sectors}
                legacy_registration_id = request.form.get("registration_id", type=int)
                if legacy_registration_id and not tail_assignments:
                    tail_assignments = {group_id: legacy_registration_id for group_id in group_ids}
                if any(str(tail_assignments.get(group_id) or "") not in registrations_by_id for group_id in group_ids):
                    raise RuntimeError("Assign an operating tail to every requested aircraft group in the planning Gantt.")
                for sector in sectors:
                    registration = registrations_by_id[str(tail_assignments[str(sector.get("aircraft_group") or "aircraft-1")])]
                    flight_id = str(sector.get("envision_flight_id") or "").strip()
                    if not flight_id: raise RuntimeError(f"{sector.get('flight_number') or 'A sector'} is not linked to an Envision flight.")
                    line_id = int((line_by_registration_id.get(str(registration.get("id") or "")) or {}).get("id") or 0)
                    if not line_id: raise RuntimeError(f"No Envision operating line is configured for {registration.get('registration') or registration.get('registrationDescription') or 'the selected aircraft'}.")
                    payload = {"ignoreValidations": True, "flightId": int(flight_id), "lineId": line_id, "crewPositions": [{"id": 0, "employeeId": 0, "crewPositionId": 0}]}
                    try:
                        response = envision_change_registration(token, int(flight_id), payload)
                        if debug_enabled: change_debug.append({"flight": sector.get("flight_number"), "target_tail": registration.get("registration") or registration.get("registrationDescription"), "request": payload, "response": response})
                    except Exception as exc:
                        if debug_enabled: change_debug.append({"flight": sector.get("flight_number"), "target_tail": registration.get("registration") or registration.get("registrationDescription"), "request": payload, "error": str(exc)})
                        raise
                    sector["tail"] = str(registration.get("registration") or registration.get("registrationDescription") or "")
                row.sectors_json = json.dumps(sectors); db.session.add(row); db.session.commit(); clear_gantt_flight_cache()
                if debug_enabled: session["charter_tail_change_debug"] = {"request_id": row.id, "attempts": change_debug}
                flash("Operating tail changed in Envision and the flight board refreshed.", "success")
            except Exception as exc:
                if debug_enabled: session["charter_tail_change_debug"] = {"request_id": row.id, "attempts": change_debug}
                current_app.logger.exception("Charter Envision tail change failed")
                flash(f"Tail was not changed: {exc}", "danger")
            return redirect(url_for("ui.ops_charter_request_planning", request_id=row.id))
        elif action == "push_envision" and row.status == "Approved":
            sectors = _request_sectors(row)
            try:
                supplied = json.loads(request.form.get("sectors_json") or "[]")
                if isinstance(supplied, list): sectors = supplied
                group = int(request.form.get("tour_group") or 0)
                if group < 0 or group > 9: raise RuntimeError("Tour group must be one digit (0–9).")
                _allocate_charter_numbers(sectors, group)
                token = envision_authenticate()["token"]
                registration_id = int(request.form.get("registration_id") or 0)
                registration = next((item for item in envision_get_line_registrations(token) if int(item.get("id") or 0) == registration_id), None)
                if not registration: raise RuntimeError("Select a current Envision aircraft registration.")
                types = envision_get_flight_types(token)
                for sector in sectors:
                    if sector.get("envision_flight_id"): continue
                    ftype = str(sector.get("flight_type") or "Charter").lower()
                    type_row = next((item for item in types if "charter" in str(item.get("description") or item.get("flightTypeDescription") or "").lower() and (("position" in ftype) == ("position" in str(item.get("description") or item.get("flightTypeDescription") or "").lower()))), None)
                    if not type_row: raise RuntimeError(f"No Envision flight type found for {sector.get('flight_type') or 'Charter'}.")
                    etd, eta = _parse_charter_local(sector.get("std"), sector.get("date")), _parse_charter_local(sector.get("sta"), sector.get("date"))
                    if eta <= etd: eta += timedelta(days=1)
                    created = envision_create_flight(token, {"ignoreValidations":False,"flightDate":etd.astimezone(timezone.utc).isoformat().replace("+00:00","Z"),"departurePlaceId":_resolve_charter_place(token, sector.get("dep")),"arrivalPlaceId":_resolve_charter_place(token, sector.get("arr")),"scheduledTimeDeparture":etd.astimezone(timezone.utc).isoformat().replace("+00:00","Z"),"scheduledTimeArrival":eta.astimezone(timezone.utc).isoformat().replace("+00:00","Z"),"flightTypeId":int(type_row["id"]),"modelId":int(registration.get("modelId") or 0),"registrationId":registration_id,"flightNumber":sector["flight_number"]})
                    sector["envision_flight_id"] = str(created.get("id") or "")
                row.sectors_json = json.dumps(sectors); row.status = "Pushed to Envision"; db.session.add(row); db.session.commit(); flash("Selected charter sectors were created in Envision.", "success")
            except Exception as exc:
                current_app.logger.exception("Charter Envision push failed")
                flash(f"Nothing further was pushed: {exc}", "danger")
        return redirect(url_for("ui.ops_charter_request_detail", request_id=row.id))
    registrations = []
    if can_operate and row.status == "Approved":
        try:
            token = envision_authenticate()["token"]
            registrations = sorted(envision_get_line_registrations(token), key=lambda item: str(item.get("registration") or ""))
        except Exception:
            flash("Aircraft could not be loaded from Envision. You can retry the page before pushing.", "danger")
    return render_template("charter_request_detail_v2.html", charter_request=row, sectors=_allocate_charter_numbers(_request_sectors(row)), can_operate=can_operate, registrations=registrations)


@ui_bp.get("/ops/charter-briefs")
@_login_required
def ops_charter_briefs():
    briefs = CharterBrief.query.order_by(CharterBrief.start_date.desc(), CharterBrief.updated_at.desc()).all()
    return render_template("charter_briefs.html", briefs=briefs)


@ui_bp.route("/ops/charter-briefs/new", methods=["GET", "POST"])
@_login_required
def ops_charter_brief_new():
    if request.method == "POST":
        if not _csrf_is_valid():
            flash("Your form expired. Please try again.", "danger")
            return redirect(url_for("ui.ops_charter_brief_new"))
        start = request.form.get("start_date") or None
        try:
            start_date = date.fromisoformat(start) if start else _nz_today()
        except ValueError:
            start_date = _nz_today()
        reference = f"CB-{start_date:%Y%m%d}-{secrets.token_hex(2).upper()}"
        brief = CharterBrief(reference=reference, title="New charter brief", start_date=start_date, end_date=start_date)
        db.session.add(brief)
        db.session.commit()
        return redirect(url_for("ui.ops_charter_brief_edit", brief_id=brief.id))
    return render_template("charter_brief_new.html", today=_nz_today())


@ui_bp.post("/ops/charter-briefs/<int:brief_id>/delete")
@_login_required
def ops_charter_brief_delete(brief_id: int):
    brief = db.session.get(CharterBrief, brief_id)
    if not brief:
        abort(404)
    if not _csrf_is_valid():
        flash("Your form expired. Please try again.", "danger")
        return redirect(url_for("ui.ops_charter_brief_edit", brief_id=brief.id))
    if str(brief.status or "").lower() != "draft":
        flash("Published charter briefs are retained and cannot be deleted.", "danger")
        return redirect(url_for("ui.ops_charter_brief_edit", brief_id=brief.id))
    db.session.delete(brief)
    db.session.commit()
    flash("Draft charter brief deleted.", "success")
    return redirect(url_for("ui.ops_charter_briefs"))


@ui_bp.route("/ops/charter-briefs/<int:brief_id>", methods=["GET", "POST"])
@_login_required
def ops_charter_brief_edit(brief_id: int):
    brief = db.session.get(CharterBrief, brief_id)
    if not brief:
        abort(404)
    if request.method == "POST":
        if not _csrf_is_valid():
            flash("Your form expired. Please try again.", "danger")
            return redirect(url_for("ui.ops_charter_brief_edit", brief_id=brief.id))
        try:
            details = json.loads(request.form.get("details_json") or "{}")
            if not isinstance(details, dict):
                raise ValueError
        except ValueError:
            flash("The briefing detail could not be saved. Please reload and try again.", "danger")
            return redirect(url_for("ui.ops_charter_brief_edit", brief_id=brief.id))
        brief.title = str(request.form.get("title") or "").strip() or "Untitled charter brief"
        brief.charterer = str(request.form.get("charterer") or "").strip() or None
        for field in ("start_date", "end_date"):
            value = str(request.form.get(field) or "").strip()
            try:
                setattr(brief, field, date.fromisoformat(value) if value else None)
            except ValueError:
                pass
        brief.details_json = json.dumps(details, ensure_ascii=False)
        action = request.form.get("action")
        if action == "publish":
            brief.status = "Published"
            brief.version += 1
            brief.published_at = datetime.utcnow()
            user = _current_apg_user()
            brief.published_by = user.display_name or user.email if user else None
            flash(f"Version {brief.version} published.", "success")
        else:
            brief.status = "Draft"
            flash("Charter brief saved as a draft.", "success")
        db.session.add(brief)
        db.session.commit()
        return redirect(url_for("ui.ops_charter_brief_edit", brief_id=brief.id))
    catering_services, handler_directory = _charter_operations_directory()
    return render_template("charter_brief_edit.html", brief=brief, details=_charter_brief_details(brief), handler_directory=handler_directory, catering_services=catering_services)


@ui_bp.get("/ops/charter-briefs/<int:brief_id>/print")
@_login_required
def ops_charter_brief_print(brief_id: int):
    brief = db.session.get(CharterBrief, brief_id)
    if not brief:
        abort(404)
    details = _charter_brief_details(brief)
    airports = [sector.get(field) for sector in details.get("sectors", []) for field in ("dep", "arr")]
    _, handler_directory = _charter_operations_directory()
    airport_codes = {str(code or "").upper().strip() for code in airports}
    selections = details.get("handler_selections") if isinstance(details.get("handler_selections"), dict) else {}
    handler_details = [
        entry for entry in handler_directory
        if selections.get(entry.get("airport")) == entry.get("label")
    ]
    return_date = str(request.args.get("return_date") or "").strip()
    crew_briefing_url = url_for("ui.dcs_crew_briefing", date=return_date) if return_date else None
    return render_template("charter_brief_print.html", brief=brief, details=details, handler_details=handler_details, format_date=_brief_print_date, format_time=_brief_print_time, crew_briefing_url=crew_briefing_url)


@ui_bp.get("/charter/check-in/<token>")
def charter_self_checkin(token: str):
    return render_template("charter_self_checkin.html", token=token)


@ui_bp.route("/charter/handling-provider-update/<token>", methods=["GET", "POST"])
def charter_handling_provider_update(token: str):
    """Public, time-limited correction page linked only from handling emails."""
    serializer = URLSafeTimedSerializer(current_app.config["SECRET_KEY"], salt="charter-handling-provider-update")
    try:
        payload = serializer.loads(token, max_age=60 * 60 * 24 * 90)
        provider_id = int(payload["provider_id"])
    except (BadSignature, SignatureExpired, KeyError, TypeError, ValueError):
        abort(404)
    provider = db.session.get(AirportHandlingProvider, provider_id)
    if not provider:
        abort(404)
    if request.method == "POST":
        provider.handler = str(request.form.get("handler") or "").strip()
        provider.contact = str(request.form.get("contact") or "").strip()
        provider.phone = str(request.form.get("phone") or "").strip()
        provider.additional_phone = str(request.form.get("additional_phone") or "").strip()
        provider.email_addresses = "\n".join(
            line.strip() for line in str(request.form.get("email_addresses") or "").splitlines() if line.strip()
        )
        provider.frequency = str(request.form.get("frequency") or "").strip()
        provider.gpu = str(request.form.get("gpu") or "").strip()
        provider.fuel = str(request.form.get("fuel") or "").strip()
        provider.notes = str(request.form.get("notes") or "").strip()
        db.session.add(provider)
        db.session.commit()
        return render_template("charter_provider_update.html", provider=provider, saved=True)
    return render_template("charter_provider_update.html", provider=provider, saved=False)


@ui_bp.route("/charter/handling-confirmation/<token>", methods=["GET", "POST"])
def charter_handling_confirmation(token: str):
    """Handler-facing acceptance flow; confirmation is then emailed to Operations."""
    serializer = URLSafeTimedSerializer(current_app.config["SECRET_KEY"], salt="charter-handling-confirmation")
    try:
        payload = serializer.loads(token, max_age=60 * 60 * 24 * 90)
        record = db.session.get(CharterHandlingRequest, int(payload["handling_request_id"]))
    except (BadSignature, SignatureExpired, KeyError, TypeError, ValueError):
        abort(404)
    if not record:
        abort(404)
    provider = db.session.get(AirportHandlingProvider, record.provider_id)
    request_row = db.session.get(CharterRequest, record.charter_request_id)
    if not provider or not request_row:
        abort(404)
    already_confirmed = record.status == "Confirmed"
    if request.method == "POST" and not already_confirmed:
        note = str(request.form.get("note") or "").strip()
        record.status = "Confirmed"
        _log_handling_event(record, "Handling confirmed", provider.label, note or "Confirmed through handler email")
        db.session.add(record)
        db.session.commit()
        recipients = [item.strip() for item in str((db.session.get(EmailSettings, 1) or EmailSettings()).charter_request_recipients or "").split(",") if "@" in item]
        if recipients:
            try:
                from .routes import _send_email_via_graph, _charter_email_sender
                subject = f"Handling confirmed — {request_row.reference} — {record.airport}"
                body = f"{provider.label} has confirmed ground handling for {request_row.reference} at {record.airport}."
                if note:
                    body += f"\n\nHandler note: {note}"
                body += f"\n\nView the handling checklist: {url_for('ui.ops_charter_request_ground_handling', request_id=request_row.id, _external=True)}"
                if _send_email_via_graph(_charter_email_sender(), recipients, subject, body):
                    _log_handling_event(record, "Operations notified", "APG", f"Confirmation email sent to {', '.join(recipients)}")
                    db.session.commit()
            except Exception:
                current_app.logger.exception("Unable to notify Operations of handling confirmation")
        already_confirmed = True
    return render_template("charter_handling_confirmation.html", provider=provider, charter_request=request_row, handling_request=record, confirmed=already_confirmed)


@ui_bp.get("/dcs/charter-brand/<asset>")
def charter_brand_asset(asset: str):
    filenames = {
        "main-logo": "ACCharter Main Logo.png",
        "brand-banner": "ACCharter.jpg",
    }
    filename = filenames.get(asset)
    if not filename:
        abort(404)
    return send_file(os.path.join(current_app.root_path, "..", filename), conditional=True)


@ui_bp.route("/dcs/crew-briefing")
@_permission_required("crew_briefing")
def dcs_crew_briefing():
    day_str = request.args.get("date")
    if day_str:
        try:
            day = date.fromisoformat(day_str)
        except ValueError:
            day = _nz_today()
    else:
        day = _nz_today()
    # Crew briefing is operational-only.  Always start it on the live Envision
    # environment, rather than inheriting a test selection from the Gantt view.
    session["envision_env"] = "base"
    env = set_envision_environment("base")
    return render_template(
        "New_Gantt/live_gantt.html",
        day=day,
        page_view="briefing",
        envision_env_name=env.get("name"),
        envision_env_host=env.get("host"),
        envision_env_key=env.get("key"),
        envision_test_available=env.get("test_available"),
        is_cabin_crew=_is_cabin_crew_user(_current_apg_user()),
        signed_in_crew_code=(_current_apg_user().envision_crew_code or _current_apg_user().envision_username or "").upper(),
    )


@ui_bp.get("/crew-briefing.webmanifest")
def crew_briefing_manifest():
    response = jsonify({
        "name": "Air Chathams AC Crew Brief",
        "short_name": "AC Crew Brief",
        "description": "Live mobile flight briefing for Air Chathams crew.",
        "start_url": url_for("ui.dcs_crew_briefing"),
        "scope": request.script_root.rstrip("/") + "/",
        "display": "standalone",
        "background_color": "#ffffff",
        "theme_color": "#009f4d",
        "icons": [{
            "src": url_for("static", filename="images/ac-crew-brief-icon.png"),
            "sizes": "512x512",
            "type": "image/png",
            "purpose": "any",
        }, {
            "src": url_for("static", filename="images/ac-crew-brief-icon.png"),
            "sizes": "512x512",
            "type": "image/png",
            "purpose": "maskable",
        }],
    })
    response.headers["Content-Type"] = "application/manifest+json"
    response.headers["Cache-Control"] = "no-cache"
    return response


@ui_bp.get("/crew-briefing-sw.js")
def crew_briefing_service_worker():
    response = make_response(current_app.send_static_file("New_Gantt/crew_briefing_sw.js"))
    response.headers["Content-Type"] = "application/javascript"
    response.headers["Service-Worker-Allowed"] = request.script_root.rstrip("/") + "/"
    response.headers["Cache-Control"] = "no-cache"
    return response


@ui_bp.get("/api/dcs/gantt_data")
@_any_permission_required("live_gantt", "crew_briefing")
def api_dcs_gantt_data():
    """
    JSON endpoint used by the Gantt auto-refresh.
    Returns the same "rows" that dcs_from_envision_page builds, but as JSON.
    """
    dstr = request.args.get("date")
    try:
        day = date.fromisoformat(dstr) if dstr else _nz_today()
    except ValueError:
        day = _nz_today()

    # ---- NZ-local window → UTC for Envision API ----
    start_nz = datetime.combine(day, time(0, 0, tzinfo=NZ))
    end_nz   = start_nz + timedelta(days=1)
    start_utc = start_nz.astimezone(timezone.utc)
    end_utc   = end_nz.astimezone(timezone.utc)

    # 1) Envision auth + fetch (with short TTL cache)
    cache_ttl = int(current_app.config.get("ENVISION_CACHE_TTL", 60))
    cache_key = (start_utc.isoformat(), end_utc.isoformat())
    force_refresh = request.args.get("force") == "1"
    cache_hit = False
    token = None
    env_flights = []

    cached = _ENVISION_FLIGHT_CACHE.get(cache_key)
    if cached and not force_refresh:
        age = _time.time() - cached.get("ts", 0)
        if age <= cache_ttl:
            env_flights = cached.get("data") or []
            cache_hit = True

    if not cache_hit:
        try:
            auth = envision_authenticate()
            token = auth["token"]
            env_flights = envision_get_flights(token, start_utc, end_utc) or []
            _ENVISION_FLIGHT_CACHE[cache_key] = {"ts": _time.time(), "data": env_flights}
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
            "registration_id": _extract_registration_id(f),
            "aircraft_type": f.get("aircraftType") or f.get("aircraftTypeId") or "",
            "service_type": (
                f.get("flightTypeDescription")
                or f.get("flightType")
                or f.get("serviceTypeDescription")
                or ""
            ),
            "flight_type": f.get("flightTypeDescription") or f.get("flightType") or "",
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
            "dcs_linked": False,
            "defect_count": None,
            "defect_total": None,

            # NEW: default delays
            "delays": [],
        }

        rows.append(row)

    rows.sort(key=lambda r: r["std_nz"] or _dt.min.replace(tzinfo=NZ))

    # 4) Envision registration defects (open + total) per aircraft.
    #
    # The Gantt refreshes in the browser background.  Refresh defect snapshots
    # along with it so a defect closed in Envision is not left on the board by
    # an old in-memory cache.  The cached snapshot remains a fallback only if
    # Envision is temporarily unavailable.
    reg_ids = sorted({int(r["registration_id"]) for r in rows if r.get("registration_id")})
    if reg_ids:
        defect_counts: dict[int, tuple[int, int]] = {}

        for reg_id in reg_ids:
            cached = _ENVISION_DEFECT_CACHE.get(reg_id)
            if cached:
                defect_counts[reg_id] = (
                    int(cached.get("open", 0)),
                    int(cached.get("total", 0)),
                )

        if reg_ids:
            if token is None:
                try:
                    auth = envision_authenticate()
                    token = auth["token"]
                except Exception as e:
                    current_app.logger.warning(
                        "api_dcs_gantt_data: Envision auth failed for defects: %s", e
                    )

            if token:
                max_workers = int(current_app.config.get("ENVISION_DEFECT_MAX_WORKERS", 6))
                with ThreadPoolExecutor(max_workers=max_workers) as ex:
                    future_map = {
                        ex.submit(_fetch_defect_snapshot_for_registration, token, reg_id): reg_id
                        for reg_id in reg_ids
                    }
                    for fut in as_completed(future_map):
                        reg_id = future_map[fut]
                        try:
                            open_count, total_count, details = fut.result()
                        except Exception as e:
                            current_app.logger.warning(
                                "api_dcs_gantt_data: defects fetch failed reg_id=%s: %s",
                                reg_id,
                                e,
                            )
                            # Retain the last known result rather than briefly
                            # showing a misleading zero while Envision is down.
                            continue
                        defect_counts[reg_id] = (open_count, total_count)
                        _ENVISION_DEFECT_CACHE[reg_id] = {
                            "ts": _time.time(),
                            "open": open_count,
                            "total": total_count,
                            "details": details,
                        }

        for r in rows:
            reg_id = r.get("registration_id")
            if not reg_id:
                continue
            open_count, total_count = defect_counts.get(int(reg_id), (0, 0))
            r["defect_count"] = open_count
            r["defect_total"] = total_count

    # 5) DCS enrichment
    try:
        _enrich_rows_with_dcs(rows, day)
    except Exception as e:
        current_app.logger.warning(f"api_dcs_gantt_data: _enrich_rows_with_dcs failed: {e}")
    try:
        _propagate_through_pax(rows)
    except Exception as e:
        current_app.logger.warning(f"api_dcs_gantt_data: _propagate_through_pax failed: {e}")
    try:
        _apply_charter_manifests(rows)
    except Exception as e:
        current_app.logger.warning(f"api_dcs_gantt_data: _apply_charter_manifests failed: {e}")

    try:
        published_briefs = _published_charter_brief_links()
        for row in rows:
            brief = published_briefs.get(str(row.get("envision_flight_id") or ""))
            if brief:
                row["charter_brief_url"] = url_for("ui.ops_charter_brief_print", brief_id=brief.id, return_date=day.isoformat())
                row["charter_brief_version"] = brief.version
    except Exception as e:
        current_app.logger.warning("api_dcs_gantt_data: unable to attach published charter briefs: %s", e)

    # 6) APG plan presence
    try:
        attach_apg_presence_to_rows(
            rows,
            window_from_utc=start_utc,
            window_to_utc=end_utc,
        )
    except Exception as e:
        current_app.logger.warning(f"api_dcs_gantt_data: attach_apg_presence_to_rows failed: {e}")

    # 7) OPTIONAL: attach delays for each Envision flight (expensive)
    include_delays = request.args.get("include_delays") == "1"
    if include_delays:
        if token is None:
            try:
                auth = envision_authenticate()
                token = auth["token"]
            except Exception as e:
                current_app.logger.exception("api_dcs_gantt_data: Envision auth failed for delays")
                return jsonify({"ok": False, "error": f"Envision auth failed: {e}", "results": []}), 502
        try:
            delay_rows = [r for r in rows if r.get("envision_flight_id")]
            for r in rows:
                r["delays"] = []

            max_workers = min(
                len(delay_rows),
                max(1, int(current_app.config.get("ENVISION_DELAY_MAX_WORKERS", 8))),
            )
            if delay_rows:
                with ThreadPoolExecutor(max_workers=max_workers) as ex:
                    future_map = {
                        ex.submit(envision_get_delays, token, int(r["envision_flight_id"])): r
                        for r in delay_rows
                    }
                    for fut in as_completed(future_map):
                        row = future_map[fut]
                        fid = row.get("envision_flight_id")
                        try:
                            row["delays"] = fut.result() or []
                        except Exception as e:
                            current_app.logger.warning(
                                "api_dcs_gantt_data: failed to load delays for flight %s: %s",
                                fid, e
                            )
        except Exception as e:
            current_app.logger.warning(
                "api_dcs_gantt_data: top-level delay fetch error: %s", e
            )

    # 8) Make it JSON-serialisable (convert datetimes to ISO strings)
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
            "registration_id": r.get("registration_id"),
            "defect_count": r.get("defect_count"),
            "defect_total": r.get("defect_total"),
            "block_mins": r.get("block_mins") or 0,
            "aircraft_type": r.get("aircraft_type"),
            "service_type": r.get("service_type"),
            "flight_type": r.get("flight_type"),
            "flight_status": r.get("flight_status"),
            "adt": r.get("adt") or 0,
            "chd": r.get("chd") or 0,
            "inf": r.get("inf") or 0,
            "pax_count": r.get("pax_count") or 0,
            "bags_kg": float(r.get("bags_kg") or 0),
            "pax_list": r.get("pax_list") or [],
            "apg_passenger_seat_loads": calculate_dcs_passenger_seat_loads({
                "Passengers": r.get("pax_list") or []
            }),
            "charter_manifest_uploaded": bool(r.get("charter_manifest_uploaded")),
            "charter_manifest_filename": r.get("charter_manifest_filename") or "",
            "charter_manifest_updated_at": r.get("charter_manifest_updated_at"),
            "charter_flight_closed_at": r.get("charter_flight_closed_at"),
            "charter_brief_url": r.get("charter_brief_url") or "",
            "charter_brief_version": r.get("charter_brief_version"),
            "charter_gate": r.get("charter_gate") or "",
            "dcs_linked": bool(r.get("dcs_linked")),
            "envision_flight_id": r.get("envision_flight_id"),
            "delays": r.get("delays") or [],   # <-- NEW: ship delays to JS
        }

    json_rows = [row_to_json(r) for r in rows]
    return jsonify({"ok": True, "results": json_rows})

@ui_bp.get("/api/envision/flight_delays")
@_permission_required("live_gantt")
def api_envision_flight_delays():
    """
    /api/envision/flight_delays?flight_id=12345
    Returns the raw Envision delays list for a single flight.
    """
    flight_id = request.args.get("flight_id", type=int)
    if not flight_id:
        return jsonify({"ok": False, "error": "Missing flight_id"}), 400

    try:
        auth = envision_authenticate()
        token = auth["token"]
    except Exception as e:
        current_app.logger.exception("Envision auth failed in api_envision_flight_delays")
        return jsonify({"ok": False, "error": f"Envision auth failed: {e}"}), 502

    try:
        delays = envision_get_delays(token, int(flight_id)) or []
    except Exception as e:
        current_app.logger.exception("Envision delays failed for flight_id=%s", flight_id)
        return jsonify({"ok": False, "error": f"Envision delays failed: {e}"}), 502

    return jsonify({"ok": True, "delays": delays})


@ui_bp.get("/api/envision/registration_defects")
@_permission_required("live_gantt")
def api_envision_registration_defects():
    """
    /api/envision/registration_defects?registration_id=123
    Returns full defects for a registration plus open/total counts.
    """
    registration_id = request.args.get("registration_id", type=int)
    if not registration_id:
        return jsonify({"ok": False, "error": "Missing registration_id"}), 400

    ttl = int(current_app.config.get("ENVISION_DEFECT_CACHE_TTL", 180))
    now_ts = _time.time()
    cached = _ENVISION_DEFECT_CACHE.get(registration_id)
    if cached and (now_ts - cached.get("ts", 0) <= ttl) and isinstance(cached.get("details"), list):
        cached_filtered = [d for d in (cached.get("details") or []) if _is_open_or_deferred_defect(d)]
        return jsonify({
            "ok": True,
            "registration_id": registration_id,
            "open_count": len(cached_filtered),
            "total_count": len(cached_filtered),
            "defects": cached_filtered,
            "cached": True,
        })

    try:
        auth = envision_authenticate()
        token = auth["token"]
    except Exception as e:
        current_app.logger.exception("Envision auth failed in api_envision_registration_defects")
        return jsonify({"ok": False, "error": f"Envision auth failed: {e}"}), 502

    try:
        defects = _fetch_defects_for_registration(token, int(registration_id))
    except Exception as e:
        current_app.logger.exception(
            "Envision defects fetch failed for registration_id=%s", registration_id
        )
        return jsonify({"ok": False, "error": f"Envision defects failed: {e}"}), 502

    filtered = [d for d in defects if _is_open_or_deferred_defect(d)]
    open_count = len(filtered)
    total_count = len(filtered)
    _ENVISION_DEFECT_CACHE[registration_id] = {
        "ts": _time.time(),
        "open": open_count,
        "total": total_count,
        "details": filtered,
    }

    return jsonify({
        "ok": True,
        "registration_id": registration_id,
        "open_count": open_count,
        "total_count": total_count,
        "defects": filtered,
        "cached": False,
    })


@ui_bp.get("/api/envision/registration_maintenance")
@_permission_required("live_gantt")
def api_envision_registration_maintenance():
    """
    /api/envision/registration_maintenance?registration_id=123
    Returns registration work orders (maintenance) for a registration.
    """
    registration_id = request.args.get("registration_id", type=int)
    if not registration_id:
        return jsonify({"ok": False, "error": "Missing registration_id"}), 400

    ttl = int(current_app.config.get("ENVISION_MAINT_CACHE_TTL", 300))
    now_ts = _time.time()
    cached = _ENVISION_MAINT_CACHE.get(registration_id)
    if cached and (now_ts - cached.get("ts", 0) <= ttl) and isinstance(cached.get("items"), list):
        return jsonify({
            "ok": True,
            "registration_id": registration_id,
            "maintenance": cached.get("items") or [],
            "cached": True,
        })

    try:
        auth = envision_authenticate()
        token = auth["token"]
    except Exception as e:
        current_app.logger.exception("Envision auth failed in api_envision_registration_maintenance")
        return jsonify({"ok": False, "error": f"Envision auth failed: {e}"}), 502

    try:
        maintenance = _fetch_work_orders_for_registration(token, int(registration_id))
    except Exception as e:
        current_app.logger.exception(
            "Envision work orders fetch failed for registration_id=%s", registration_id
        )
        return jsonify({"ok": False, "error": f"Envision work orders failed: {e}"}), 502

    _ENVISION_MAINT_CACHE[registration_id] = {
        "ts": _time.time(),
        "items": maintenance,
    }

    return jsonify({
        "ok": True,
        "registration_id": registration_id,
        "maintenance": maintenance,
        "cached": False,
    })

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


def _extract_registration_id(f: dict) -> int | None:
    """Best-effort extraction of registration id from Envision flight payload."""
    candidates = (
        f.get("flightRegistrationId"),
        f.get("registrationId"),
        f.get("regId"),
        f.get("aircraftRegistrationId"),
    )
    for value in candidates:
        if value is None or value == "":
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _count_open_defects(defects: list[dict]) -> int:
    """
    Operational defect count:
    - Prefer unresolved defects where closeDate is empty/null.
    - Fall back to status text if closeDate is unavailable.
    """
    if not defects:
        return 0
    open_count = 0
    for d in defects:
        close_date = d.get("closeDate")
        if close_date:
            continue
        status = str(d.get("defectStatus") or "").strip().lower()
        if status in {"closed", "complete", "completed", "cleared", "resolved", "deferred closed"}:
            continue
        open_count += 1
    return open_count


def _is_open_or_deferred_defect(defect: dict) -> bool:
    """True only for unresolved Open/Deferred defects."""
    status = str(defect.get("defectStatus") or "").strip().lower()
    if defect.get("closeDate"):
        return False
    if any(s in status for s in ("closed", "complete", "completed", "cleared", "resolved")):
        return False
    # Explicit Open/Deferred are always allowed.
    if ("open" in status) or ("defer" in status):
        return True
    # Fallback: unresolved defect with unclear status is treated as open.
    return True


def _fetch_defect_count_for_registration(token: str, registration_id: int) -> tuple[int, int]:
    """
    Returns (open_defect_count, total_defect_count) for a registration.
    """
    defects = _fetch_defects_for_registration(token, registration_id)
    return _count_open_defects(defects), len(defects)


def _fetch_defect_snapshot_for_registration(
    token: str, registration_id: int
) -> tuple[int, int, list[dict]]:
    """Fetch current counts and modal-ready unresolved defects for a Gantt refresh."""
    defects = _fetch_defects_for_registration(token, registration_id)
    active_details = [d for d in defects if _is_open_or_deferred_defect(d)]
    return len(active_details), len(defects), active_details


def _fetch_defects_for_registration(token: str, registration_id: int) -> list[dict]:
    """Return full defect list for one Envision registration id."""
    url = f"{_runtime_envision_base()}/Registrations/{registration_id}/Defects"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def _fetch_work_orders_for_registration(token: str, registration_id: int, status_ids_raw: str | None = None) -> list[dict]:
    """Return work orders list for one Envision registration id."""
    url = f"{_runtime_envision_base()}/Registrations/{registration_id}/WorkOrders"
    headers = {"Authorization": f"Bearer {token}"}
    # Optional status filter can be supplied via ENV var, e.g. "1,2,3"
    if status_ids_raw is None:
        status_ids_raw = str(current_app.config.get("ENVISION_WORK_ORDER_STATUS_IDS") or "").strip()
    params = None
    if status_ids_raw:
        ids = []
        for x in status_ids_raw.split(","):
            x = x.strip()
            if not x:
                continue
            try:
                ids.append(int(x))
            except ValueError:
                continue
        if ids:
            params = [("workOrderStatusIds", i) for i in ids]

    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def _fetch_registration_life_codes(token: str, registration_id: int) -> list[dict]:
    """Return installed life-coded parts and their current Envision life totals."""
    url = f"{_runtime_envision_base()}/Registrations/{registration_id}/LifeCodes"
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, list) else []


def _fetch_registration_life_values(token: str, registration_id: int) -> dict:
    """Return the aircraft's current flying hours and cycles."""
    url = f"{_runtime_envision_base()}/Registrations/{registration_id}/LifeValues"
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, dict) else {}


def _fetch_registration_scheduled_maintenance(token: str, registration_id: int) -> list[dict]:
    """Return Envision maintenance intervals, including its remaining-life calculation."""
    url = f"{_runtime_envision_base()}/Registrations/{registration_id}/ScheduledMaintenance"
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, list) else []


def _fetch_components(token: str) -> list[dict]:
    """Return Envision's fitted-component index for matching life-code assets."""
    response = requests.get(
        f"{_runtime_envision_base()}/Components",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, list) else []


def _fetch_registration_configuration(token: str, registration_id: int) -> list[dict]:
    """Return the fitted asset tree, including asset-parent relationships."""
    response = requests.get(
        f"{_runtime_envision_base()}/Registrations/{registration_id}/Configuration",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, list) else []


def _fetch_maintenance_registration_snapshot(token: str, registration: dict, status_ids_raw: str, components: list[dict]) -> dict:
    """Build one dashboard card from the registration-scoped Envision endpoints."""
    registration_id = int(registration["id"])
    life_values = _fetch_registration_life_values(token, registration_id)
    try:
        configuration = _fetch_registration_configuration(token, registration_id)
    except requests.RequestException:
        configuration = []
    return {
        "id": registration_id,
        "registration": registration.get("registration") or "Unassigned",
        "model": registration.get("model") or "",
        "serial_no": registration.get("serialNo") or "",
        "status": registration.get("status") or "",
        "life_values": life_values,
        "life_codes": _fetch_registration_life_codes(token, registration_id),
        "components": components,
        "configuration": configuration,
        "scheduled_maintenance": _fetch_registration_scheduled_maintenance(token, registration_id),
        "work_orders": _fetch_work_orders_for_registration(token, registration_id, status_ids_raw),
    }


@ui_bp.route("/maintenance")
@_login_required
def maintenance_dashboard():
    """Fleet maintenance overview; detail data is loaded on demand by the page."""
    return render_template("maintenance_dashboard.html")


@ui_bp.get("/api/maintenance/dashboard")
@_login_required
def api_maintenance_dashboard():
    """Read-only fleet maintenance data sourced from Envision's v1 API."""
    try:
        user_session = get_kmh_session(session.get("apg_envision_session_id"))
        token = str((user_session or {}).get("token") or "").strip()
        if not token:
            return jsonify({"ok": False, "session_expired": True, "error": "Your Envision session expired. Please sign in again."}), 401
        response = requests.get(
            f"{_runtime_envision_base()}/Registrations",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        response.raise_for_status()
        registrations = response.json()
        if not isinstance(registrations, list):
            registrations = []
        # The fleet dashboard is operationally scoped: do not show or query
        # retired, stored, or otherwise inactive registrations.
        registrations = [
            row for row in registrations
            if isinstance(row, dict) and str(row.get("status") or "").strip().casefold() == "active"
        ]
        try:
            components = _fetch_components(token)
        except Exception:
            # Components are an enrichment only; retain the life-code dashboard
            # if an Envision role cannot access this optional endpoint.
            current_app.logger.warning("Maintenance dashboard could not load the Envision Components index", exc_info=True)
            components = []
    except Exception as exc:
        if getattr(getattr(exc, "response", None), "status_code", None) == 401:
            clear_kmh_session(session.get("apg_envision_session_id"))
            session.pop("apg_envision_session_id", None)
            return jsonify({"ok": False, "session_expired": True, "error": "Your Envision session expired. Please sign in again."}), 401
        current_app.logger.exception("Maintenance dashboard could not load registrations")
        return jsonify({"ok": False, "error": f"Envision registrations could not be loaded: {exc}"}), 502

    components_by_registration: dict[int, list[dict]] = {}
    for component in components:
        if not isinstance(component, dict):
            continue
        try:
            component_registration_id = int(component.get("registrationId"))
        except (TypeError, ValueError):
            continue
        components_by_registration.setdefault(component_registration_id, []).append(component)

    snapshots, errors = [], []
    status_ids_raw = str(current_app.config.get("ENVISION_WORK_ORDER_STATUS_IDS") or "").strip()
    # Registration endpoints are independent. Parallel reads keep fleet refreshes responsive.
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(registrations)))) as executor:
        futures = {
            executor.submit(
                _fetch_maintenance_registration_snapshot,
                token,
                row,
                status_ids_raw,
                components_by_registration.get(int(row["id"]), []),
            ): row
            for row in registrations if isinstance(row, dict) and row.get("id") is not None
        }
        for future in as_completed(futures):
            row = futures[future]
            try:
                snapshots.append(future.result())
            except Exception as exc:
                current_app.logger.warning("Maintenance snapshot failed for registration %s: %s", row.get("id"), exc)
                errors.append({"registration": row.get("registration") or str(row.get("id")), "error": str(exc)})

    snapshots.sort(key=lambda item: item["registration"])
    return jsonify({"ok": True, "aircraft": snapshots, "errors": errors, "generated_at": datetime.now(timezone.utc).isoformat()})

@ui_bp.get("/api/envision/flight_times")
@_permission_required("live_gantt")
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
    url = f"{_runtime_envision_base()}/Flights"
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
