from flask import Blueprint, render_template, request, redirect
from .models import SyncRun, SyncFlightLog, AppConfig
from . import db

ui_bp = Blueprint("ui", __name__)

@ui_bp.route("/")
@ui_bp.route("/sync/runs")
def sync_runs_page():
    runs = SyncRun.query.order_by(SyncRun.id.desc()).limit(50).all()
    return render_template("sync_runs.html", runs=runs)

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
