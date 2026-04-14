"""FastAPI application for Wednesday Frog."""

from __future__ import annotations

from collections import Counter
from contextlib import asynccontextmanager
from functools import lru_cache
import re
import time
from typing import Any
from zoneinfo import available_timezones

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.orm import Session
from starlette.middleware.sessions import SessionMiddleware

from .assets import AssetProcessor, create_pending_asset, resolve_asset_path
from .config import AppConfig
from .db import create_session_factory, session_scope
from .http_client import OutboundHttpClient
from .logging_utils import configure_logging
from .metrics import MetricsCollector, render_metric_lines
from .models import AdminUser, AssetRecord, DeliveryAttempt, DestinationChannel, RunTrigger, ServiceDestination, UserRole
from .plugins import LoadedPlugin, render_schema_fields
from .scheduler import SchedulerService
from .security import PasswordManager, SecretManager, issue_csrf_token
from .services import (
    DEFAULT_SCHEDULE_CRON,
    DeliveryManager,
    add_channel,
    build_plugin_manager,
    clear_secret_value,
    count_admin_users,
    create_admin_user,
    create_user,
    create_destination,
    delete_user,
    delete_channel,
    delete_destination,
    describe_secret_state,
    ensure_defaults,
    get_channel_for_user,
    get_destination_for_user,
    get_settings,
    get_user_by_id,
    get_user_by_username,
    has_admin_user,
    is_admin_user,
    list_attempts_for_runs,
    list_destinations,
    list_recent_runs,
    list_users,
    rekey_all_secrets,
    resolve_active_asset,
    set_user_password,
    set_secret_value,
    update_user,
    update_channel,
    update_destination,
    validate_all_destinations,
)


COMMON_TIMEZONES = (
    "UTC",
    "America/Los_Angeles",
    "America/Denver",
    "America/Chicago",
    "America/New_York",
    "Europe/London",
    "Europe/Berlin",
    "Asia/Tokyo",
    "Australia/Sydney",
)
USER_ROLE_OPTIONS = (UserRole.ADMIN.value, UserRole.STANDARD.value)
WEDNESDAY_CRON_TOKEN = "wed"
SUCCESS_FLASH_TIMEOUT_MS = 15_000
IDLE_TIMEOUT_SECONDS = 15 * 60

DAY_NAME_MAP = {
    0: "Sunday",
    1: "Monday",
    2: "Tuesday",
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
    6: "Saturday",
    7: "Sunday",
}


def _bool_from_form(form, key: str) -> bool:
    """Interpret checkbox-style form values."""
    return str(form.get(key, "")).lower() in {"1", "true", "on", "yes"}


def _flash(request: Request, message: str, *, level: str = "info") -> None:
    """Append a flash message to the session."""
    flashes = request.session.setdefault("flashes", [])
    flashes.append({"level": level, "message": message})
    request.session["flashes"] = flashes


def _consume_flashes(request: Request) -> list[dict[str, str]]:
    """Pop and return the current flash messages."""
    flashes = list(request.session.get("flashes", []))
    request.session["flashes"] = []
    return flashes


def _ensure_csrf(request: Request) -> str:
    """Guarantee a CSRF token exists in the signed session."""
    token = request.session.get("csrf_token")
    if not token:
        token = issue_csrf_token()
        request.session["csrf_token"] = token
    return token


def _validate_csrf(request: Request, submitted: str) -> bool:
    """Check the submitted CSRF token against the session."""
    return bool(submitted) and submitted == request.session.get("csrf_token")


def _session_timestamp() -> int:
    """Return the current timestamp for idle-session tracking."""
    return int(time.time())


def _touch_session_activity(request: Request) -> None:
    """Refresh the activity timestamp for an authenticated session."""
    if request.session.get("user_id"):
        request.session["last_activity_at"] = _session_timestamp()


def _clear_authenticated_session(request: Request) -> None:
    """Clear authenticated session state while preserving anonymous flash support."""
    request.session.pop("user_id", None)
    request.session.pop("last_activity_at", None)


def _session_timed_out(request: Request) -> bool:
    """Return whether the current authenticated session has exceeded the idle timeout."""
    if not request.session.get("user_id"):
        return False
    last_activity_at = request.session.get("last_activity_at")
    if not isinstance(last_activity_at, int):
        request.session["last_activity_at"] = _session_timestamp()
        return False
    return _session_timestamp() - last_activity_at > IDLE_TIMEOUT_SECONDS


def _timeout_login_path() -> str:
    """Return the shared login redirect used for idle timeouts."""
    return "/login?reason=timeout"


def _timeout_page_response(request: Request) -> RedirectResponse:
    """Clear the session and redirect a page request to the timeout login page."""
    _clear_authenticated_session(request)
    return _redirect(_timeout_login_path())


def _timeout_api_response(request: Request) -> JSONResponse:
    """Clear the session and return a timeout response for API requests."""
    _clear_authenticated_session(request)
    return JSONResponse(
        {"detail": "Session timed out after 15 minutes of inactivity.", "reason": "timeout"},
        status_code=401,
    )


def _current_user(session: Session, request: Request) -> AdminUser | None:
    """Return the logged-in admin, if any."""
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    return session.get(AdminUser, user_id)


def _redirect(location: str) -> RedirectResponse:
    """Build a standard POST-redirect response."""
    return RedirectResponse(location, status_code=303)


def _csp_nonce(request: Request) -> str:
    """Return a per-request CSP nonce for inline bootstrap scripts."""
    nonce = getattr(request.state, "csp_nonce", None)
    if not nonce:
        nonce = issue_csrf_token()
        request.state.csp_nonce = nonce
    return nonce


def _require_page_user(session: Session, request: Request) -> AdminUser | RedirectResponse:
    """Ensure a page request has a logged-in admin."""
    if _session_timed_out(request):
        return _timeout_page_response(request)
    user = _current_user(session, request)
    if user is None:
        return _redirect("/login")
    _touch_session_activity(request)
    return user


def _require_api_user(session: Session, request: Request) -> AdminUser | JSONResponse:
    """Ensure an API request has a logged-in admin."""
    if _session_timed_out(request):
        return _timeout_api_response(request)
    user = _current_user(session, request)
    if user is None:
        return JSONResponse({"detail": "Authentication required."}, status_code=401)
    _touch_session_activity(request)
    return user


def _require_admin_page(session: Session, request: Request) -> AdminUser | RedirectResponse:
    """Ensure a page request is made by an admin user."""
    user = _require_page_user(session, request)
    if isinstance(user, RedirectResponse):
        return user
    if not is_admin_user(user):
        _flash(request, "Admin access is required for that page.", level="error")
        return _redirect("/")
    return user


def _require_admin_api(session: Session, request: Request) -> AdminUser | JSONResponse:
    """Ensure an API request is made by an admin user."""
    user = _require_api_user(session, request)
    if isinstance(user, JSONResponse):
        return user
    if not is_admin_user(user):
        return JSONResponse({"detail": "Admin access is required."}, status_code=403)
    return user


def _validate_api_csrf(request: Request) -> bool:
    """Validate the CSRF token carried by a JS API request."""
    return request.headers.get("X-CSRF-Token", "") == request.session.get("csrf_token", "")


def _values_from_fields(form, fields) -> dict[str, Any]:
    """Extract values from a form using schema-derived fields."""
    values: dict[str, Any] = {}
    for field in fields:
        if field.input_type == "checkbox":
            values[field.name] = _bool_from_form(form, field.name)
        else:
            raw = str(form.get(field.name, "")).strip()
            if field.field_type == "integer":
                values[field.name] = int(raw) if raw else field.default
            else:
                values[field.name] = raw
    return values


def _selected_role(raw: str) -> str:
    """Normalize a submitted user role."""
    return raw if raw in USER_ROLE_OPTIONS else UserRole.STANDARD.value


@lru_cache(maxsize=1)
def timezone_options() -> tuple[str, ...]:
    """Return the available IANA time zones with UTC pinned first."""
    zones = set(available_timezones())
    zones.add("UTC")
    ordered = list(COMMON_TIMEZONES)
    ordered.extend(zone for zone in sorted(zones) if zone not in COMMON_TIMEZONES)
    return tuple(ordered)


def is_valid_timezone(value: str) -> bool:
    """Return whether the submitted timezone exists in the available IANA set."""
    return value in timezone_options()


def humanize_timezone_name(value: str) -> str:
    """Render an IANA timezone in a friendlier UI form."""
    return value.replace("_", " ")


def _parse_single_int(token: str, *, minimum: int, maximum: int) -> int | None:
    """Parse a plain integer token within a cron field range."""
    if not token.isdigit():
        return None
    value = int(token)
    if value < minimum or value > maximum:
        return None
    return value


def _format_clock_time(hour: int, minute: int) -> str:
    """Render a 24-hour time-of-day value."""
    return f"{hour:02d}:{minute:02d}"


def _hour_option_label(hour: int) -> str:
    """Render a select label for one hour value."""
    suffix = "AM" if hour < 12 else "PM"
    display_hour = hour % 12 or 12
    return f"{hour:02d} ({display_hour}:00 {suffix})"


def schedule_hour_options() -> tuple[dict[str, str | int], ...]:
    """Return hour dropdown values with readable labels."""
    return tuple({"value": hour, "label": _hour_option_label(hour)} for hour in range(24))


def schedule_minute_options() -> tuple[dict[str, str | int], ...]:
    """Return minute dropdown values."""
    return tuple({"value": minute, "label": f"{minute:02d}"} for minute in range(60))


def _cron_for_wednesday_time(hour: int, minute: int) -> str:
    """Build the internal cron expression for the fixed Wednesday cadence."""
    return f"{minute} {hour} * * {WEDNESDAY_CRON_TOKEN}"


def _normalize_schedule_time_from_cron(cron: str) -> tuple[str, int, int]:
    """Return a Wednesday-only cron plus its hour/minute components."""
    parts = cron.split()
    if len(parts) != 5:
        return DEFAULT_SCHEDULE_CRON, 12, 0
    minute = _parse_single_int(parts[0], minimum=0, maximum=59)
    hour = _parse_single_int(parts[1], minimum=0, maximum=23)
    if minute is None or hour is None:
        return DEFAULT_SCHEDULE_CRON, 12, 0
    return _cron_for_wednesday_time(hour, minute), hour, minute


def parse_schedule_time_input(value: str) -> tuple[int, int] | None:
    """Parse a manual time entry in 12-hour or 24-hour format."""
    raw = value.strip().lower()
    if not raw:
        return None
    match = re.fullmatch(r"(\d{1,2}):(\d{2})\s*([ap]\.?m\.?)?", raw)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2))
    meridiem = match.group(3)
    if minute < 0 or minute > 59:
        return None
    if meridiem:
        normalized = meridiem.replace(".", "")
        if hour < 1 or hour > 12:
            return None
        if normalized == "am":
            hour = 0 if hour == 12 else hour
        else:
            hour = 12 if hour == 12 else hour + 12
        return hour, minute
    if hour < 0 or hour > 23:
        return None
    return hour, minute


def _humanize_day_of_week(token: str) -> str | None:
    """Return an English description for a simple day-of-week cron token."""
    if token == "*":
        return "Every day"
    if token == "1-5":
        return "Every weekday"
    if token == "0,6" or token == "6,0":
        return "Every weekend"
    if "," in token:
        names: list[str] = []
        for piece in token.split(","):
            value = _parse_single_int(piece, minimum=0, maximum=7)
            if value is None:
                return None
            names.append(DAY_NAME_MAP[value])
        return "Every " + ", ".join(names)
    value = _parse_single_int(token, minimum=0, maximum=7)
    if value is not None:
        return f"Every {DAY_NAME_MAP[value]}"
    return None


def _humanize_time(minute: str, hour: str) -> str | None:
    """Return an English description for the time portion of a cron expression."""
    minute_value = _parse_single_int(minute, minimum=0, maximum=59)
    hour_value = _parse_single_int(hour, minimum=0, maximum=23)
    if minute_value is not None and hour_value is not None:
        return f"at {hour_value:02d}:{minute_value:02d}"
    if minute.startswith("*/") and hour == "*":
        step = _parse_single_int(minute.removeprefix("*/"), minimum=1, maximum=59)
        if step is not None:
            unit = "minute" if step == 1 else "minutes"
            return f"every {step} {unit}"
    if minute_value is not None and hour == "*":
        return f"at minute {minute_value:02d} past every hour"
    if minute == "*" and hour == "*":
        return "every minute"
    return None


def describe_cron_schedule(cron: str, timezone: str) -> str:
    """Convert a saved cron expression into a UI-friendly sentence."""
    timezone_label = humanize_timezone_name(timezone)
    _, hour, minute = _normalize_schedule_time_from_cron(cron)
    return f"Every Wednesday at {_format_clock_time(hour, minute)} in {timezone_label}"


def _plugin_context(plugin: LoadedPlugin | None) -> dict[str, Any]:
    if plugin is None:
        return {
            "plugin": None,
            "destination_fields": [],
            "destination_secret_fields": [],
            "channel_fields": [],
            "channel_secret_fields": [],
        }
    return {
        "plugin": plugin,
        "destination_fields": render_schema_fields(plugin.connector.destination_config_schema()),
        "destination_secret_fields": render_schema_fields(plugin.connector.destination_secret_schema()),
        "channel_fields": render_schema_fields(plugin.connector.channel_config_schema()),
        "channel_secret_fields": render_schema_fields(plugin.connector.channel_secret_schema()),
    }


def _template_context(
    request: Request,
    *,
    session: Session,
    config: AppConfig,
    **extra: Any,
) -> dict[str, Any]:
    """Build the common Jinja context for templates."""
    settings = get_settings(session, config)
    normalized_schedule_cron, schedule_hour, schedule_minute = _normalize_schedule_time_from_cron(settings.schedule_cron)
    _, active_asset, fallback_asset_active, fallback_warning = resolve_active_asset(session, config)
    current_user = _current_user(session, request)
    return {
        "request": request,
        "current_user": current_user,
        "current_user_is_admin": is_admin_user(current_user),
        "csrf_token": _ensure_csrf(request),
        "flashes": _consume_flashes(request),
        "plugin_list": request.app.state.plugin_manager.available_plugins(),
        "plugin_failures": request.app.state.plugin_manager.failures(),
        "timezone_options": timezone_options(),
        "common_timezones": COMMON_TIMEZONES,
        "user_role_options": USER_ROLE_OPTIONS,
        "settings": settings,
        "schedule_hour_options": schedule_hour_options(),
        "schedule_minute_options": schedule_minute_options(),
        "selected_schedule_hour": schedule_hour,
        "selected_schedule_minute": schedule_minute,
        "selected_schedule_time_text": _format_clock_time(schedule_hour, schedule_minute),
        "settings_timezone_label": humanize_timezone_name(settings.timezone),
        "schedule_summary": describe_cron_schedule(normalized_schedule_cron, settings.timezone),
        "active_asset": active_asset,
        "active_asset_url": f"/assets/{active_asset.id}",
        "fallback_asset_active": fallback_asset_active,
        "fallback_warning": fallback_warning,
        "success_flash_timeout_ms": SUCCESS_FLASH_TIMEOUT_MS,
        "session_idle_timeout_seconds": IDLE_TIMEOUT_SECONDS,
        "timeout_login_path": _timeout_login_path(),
        "csp_nonce": _csp_nonce(request),
        **extra,
    }


def _refresh_scheduler(app: FastAPI) -> None:
    """Reload the APScheduler job from the current database settings."""
    with session_scope(app.state.session_factory) as session:
        settings = get_settings(session, app.state.config)
        normalized_cron, _, _ = _normalize_schedule_time_from_cron(settings.schedule_cron)
        if settings.schedule_cron != normalized_cron:
            settings.schedule_cron = normalized_cron
            session.flush()
        enabled = settings.schedule_enabled and not app.state.config.scheduler_disabled
        app.state.scheduler.configure(
            cron=normalized_cron,
            timezone=settings.timezone,
            enabled=enabled,
            job=lambda: app.state.delivery_manager.run(trigger=RunTrigger.SCHEDULED, initiated_by="scheduler"),
        )


def _metrics_authorized(request: Request, token: str | None) -> bool:
    """Check whether the metrics request carries the correct token."""
    if not token:
        return False
    header = request.headers.get("Authorization", "")
    if header.startswith("Bearer ") and header.removeprefix("Bearer ").strip() == token:
        return True
    return request.headers.get("X-Metrics-Token", "") == token


def _render_metrics(app: FastAPI) -> str:
    """Render a Prometheus-style metrics payload."""
    lines: list[str] = []
    snapshot = app.state.metrics.snapshot()
    with session_scope(app.state.session_factory) as session:
        validation = validate_all_destinations(session, app.state.config, app.state.secret_manager, app.state.plugin_manager)
        runs = list_recent_runs(session, limit=500)
        attempts = list(session.scalars(select(DeliveryAttempt).order_by(DeliveryAttempt.id.desc()).limit(2_000)))
        destinations = list(session.scalars(select(ServiceDestination).order_by(ServiceDestination.id.asc())))
    run_counts = Counter(run.status for run in runs)
    for status, count in sorted(run_counts.items()):
        lines.append(render_metric_lines("wednesday_frog_runs_total", count, {"status": status}))
    attempt_counts = Counter((attempt.plugin_id or "unknown", attempt.status) for attempt in attempts)
    for (plugin_id, status), count in sorted(attempt_counts.items()):
        lines.append(render_metric_lines("wednesday_frog_delivery_attempts_total", count, {"plugin_id": plugin_id, "status": status}))
    lines.append(render_metric_lines("wednesday_frog_plugin_failures", len(app.state.plugin_manager.failures())))
    enabled_counts = Counter(destination.plugin_id for destination in destinations if destination.enabled)
    for plugin_id, count in sorted(enabled_counts.items()):
        lines.append(render_metric_lines("wednesday_frog_enabled_destinations", count, {"plugin_id": plugin_id}))
    lines.append(render_metric_lines("wednesday_frog_scheduler_enabled", 0 if app.state.config.scheduler_disabled else 1))
    lines.append(render_metric_lines("wednesday_frog_fallback_asset_active", 1 if validation["fallback_asset_active"] else 0))
    for outcome, count in sorted(snapshot["lock_outcomes"].items()):
        lines.append(render_metric_lines("wednesday_frog_schedule_lock_total", count, {"outcome": outcome}))
    return "\n".join(lines) + "\n"


def create_app(config: AppConfig | None = None) -> FastAPI:
    """Create and configure the FastAPI application."""
    configure_logging()
    resolved_config = config or AppConfig.from_env()
    bootstrap_issues = resolved_config.bootstrap_issues()
    if bootstrap_issues:
        raise RuntimeError("; ".join(bootstrap_issues))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        resolved_config.ensure_runtime_dirs()
        session_factory = create_session_factory(resolved_config)
        secret_manager = SecretManager(resolved_config.master_key, resolved_config.previous_master_key)
        password_manager = PasswordManager()
        plugin_manager = build_plugin_manager(resolved_config)
        http_client = OutboundHttpClient(resolved_config)
        metrics = MetricsCollector()
        delivery_manager = DeliveryManager(
            config=resolved_config,
            session_factory=session_factory,
            secret_manager=secret_manager,
            plugin_manager=plugin_manager,
            http_client=http_client,
            metrics=metrics,
        )
        asset_processor = AssetProcessor(session_factory=session_factory, config=resolved_config)
        scheduler = SchedulerService(enabled=not resolved_config.scheduler_disabled)
        app.state.config = resolved_config
        app.state.session_factory = session_factory
        app.state.secret_manager = secret_manager
        app.state.password_manager = password_manager
        app.state.plugin_manager = plugin_manager
        app.state.http_client = http_client
        app.state.metrics = metrics
        app.state.delivery_manager = delivery_manager
        app.state.asset_processor = asset_processor
        app.state.scheduler = scheduler
        with session_scope(session_factory) as session:
            ensure_defaults(session, resolved_config)
        scheduler.start()
        _refresh_scheduler(app)
        try:
            yield
        finally:
            scheduler.shutdown()
            delivery_manager.shutdown(resolved_config.shutdown_grace_seconds)
            asset_processor.shutdown()
            http_client.close()

    app = FastAPI(title="Wednesday Frog", lifespan=lifespan)
    app.add_middleware(
        SessionMiddleware,
        secret_key=resolved_config.session_secret,
        max_age=IDLE_TIMEOUT_SECONDS,
        https_only=resolved_config.secure_cookies,
    )
    app.mount("/static", StaticFiles(directory=str(resolved_config.static_dir)), name="static")
    templates = Jinja2Templates(directory=str(resolved_config.template_dir))
    app.state.templates = templates

    @app.middleware("http")
    async def add_security_headers(request: Request, call_next):
        nonce = _csp_nonce(request)
        response = await call_next(request)
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            f"script-src 'self' https://storage.ko-fi.com 'nonce-{nonce}'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data: https://storage.ko-fi.com https://*.ko-fi.com https://ko-fi.com; "
            "connect-src 'self' https://storage.ko-fi.com https://*.ko-fi.com https://ko-fi.com; "
            "frame-src https://*.ko-fi.com https://ko-fi.com;"
        )
        return response

    @app.get("/health/live")
    def health_live():
        return {"status": "ok"}

    @app.get("/health/ready")
    def health_ready(request: Request):
        with session_scope(app.state.session_factory) as session:
            validation = validate_all_destinations(session, resolved_config, app.state.secret_manager, app.state.plugin_manager)
            current_user = _current_user(session, request)
        if is_admin_user(current_user):
            payload = {
                "status": "ok" if validation["ok"] else "degraded",
                "bootstrap_issues": [],
                "validation": validation,
            }
        else:
            payload = {
                "status": "ok" if validation["ok"] else "degraded",
                "bootstrap_issues": [],
                "validation": {
                    "ok": validation["ok"],
                    "issues": validation["issues"],
                    "plugin_failure_count": len(validation["plugin_failures"]),
                    "destination_count": len(validation["destinations"]),
                    "destination_issue_count": sum(1 for item in validation["destinations"] if item["issues"]),
                    "fallback_asset_active": validation["fallback_asset_active"],
                },
            }
        return JSONResponse(payload, status_code=200 if payload["status"] == "ok" else 503)

    @app.get("/metrics")
    async def metrics(request: Request):
        if not resolved_config.metrics_token:
            return PlainTextResponse("Not Found\n", status_code=404)
        if not _metrics_authorized(request, resolved_config.metrics_token):
            return PlainTextResponse("Unauthorized\n", status_code=401)
        return PlainTextResponse(_render_metrics(app), media_type="text/plain; version=0.0.4")

    @app.get("/assets/{asset_id}")
    async def asset_preview(request: Request, asset_id: int):
        with session_scope(app.state.session_factory) as session:
            user = _require_page_user(session, request)
            if isinstance(user, RedirectResponse):
                return user
            asset = session.get(AssetRecord, asset_id)
            if asset is None:
                return PlainTextResponse("Not found\n", status_code=404)
            asset_path = resolve_asset_path(resolved_config, asset)
            if not asset_path.is_file():
                if asset.is_default:
                    asset_path = resolved_config.bundled_asset_path
                else:
                    return PlainTextResponse("Not found\n", status_code=404)
            return FileResponse(path=asset_path, media_type=asset.media_type, filename=asset.original_filename)

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request):
        with session_scope(app.state.session_factory) as session:
            if not has_admin_user(session):
                return _redirect("/setup")
            user = _require_page_user(session, request)
            if isinstance(user, RedirectResponse):
                return user
            validation = validate_all_destinations(
                session,
                resolved_config,
                app.state.secret_manager,
                app.state.plugin_manager,
                user=user,
            )
            recent_runs = list_recent_runs(session, limit=5, user=user)
            context = _template_context(
                request,
                session=session,
                config=resolved_config,
                validation=validation,
                recent_runs=recent_runs,
                next_run_time=app.state.scheduler.next_run_time(),
            )
            return templates.TemplateResponse(request, "dashboard.html", context)

    @app.get("/setup", response_class=HTMLResponse)
    async def setup_page(request: Request):
        with session_scope(app.state.session_factory) as session:
            if has_admin_user(session):
                return _redirect("/")
            return templates.TemplateResponse(request, "setup.html", _template_context(request, session=session, config=resolved_config))

    @app.post("/setup")
    async def setup_submit(request: Request):
        form = await request.form()
        with session_scope(app.state.session_factory) as session:
            if has_admin_user(session):
                return _redirect("/")
            if not _validate_csrf(request, str(form.get("csrf_token", ""))):
                _flash(request, "The setup form expired. Please try again.", level="error")
                return _redirect("/setup")
            if str(form.get("setup_token", "")) != resolved_config.setup_token:
                _flash(request, "The setup token is incorrect.", level="error")
                return _redirect("/setup")
            username = str(form.get("username", "")).strip()
            password = str(form.get("password", ""))
            if not username or not password:
                _flash(request, "Username and password are required.", level="error")
                return _redirect("/setup")
            admin = create_admin_user(session, username, password, app.state.password_manager)
            request.session["user_id"] = admin.id
            _touch_session_activity(request)
            _flash(request, "Admin account created.", level="success")
            return _redirect("/")

    @app.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request):
        with session_scope(app.state.session_factory) as session:
            if not has_admin_user(session):
                return _redirect("/setup")
            if request.query_params.get("reason") == "timeout":
                _clear_authenticated_session(request)
                _flash(request, "Session timed out after 15 minutes of inactivity.", level="warn")
            return templates.TemplateResponse(request, "login.html", _template_context(request, session=session, config=resolved_config))

    @app.post("/login")
    async def login_submit(request: Request):
        form = await request.form()
        with session_scope(app.state.session_factory) as session:
            if not _validate_csrf(request, str(form.get("csrf_token", ""))):
                _flash(request, "The login form expired. Please try again.", level="error")
                return _redirect("/login")
            admin = get_user_by_username(session, str(form.get("username", "")).strip())
            password = str(form.get("password", ""))
            if admin is None or not app.state.password_manager.verify(admin.password_hash, password):
                _flash(request, "Invalid username or password.", level="error")
                return _redirect("/login")
            request.session["user_id"] = admin.id
            _touch_session_activity(request)
            _flash(request, "Signed in.", level="success")
            return _redirect("/")

    @app.post("/logout")
    async def logout_submit(request: Request):
        form = await request.form()
        if not _validate_csrf(request, str(form.get("csrf_token", ""))):
            return _redirect("/")
        request.session.clear()
        return _redirect("/login")

    @app.get("/account", response_class=HTMLResponse)
    async def account_page(request: Request):
        with session_scope(app.state.session_factory) as session:
            user = _require_page_user(session, request)
            if isinstance(user, RedirectResponse):
                return user
            return templates.TemplateResponse(
                request,
                "account.html",
                _template_context(request, session=session, config=resolved_config, account_user=user),
            )

    @app.post("/account/password")
    async def account_password_submit(request: Request):
        form = await request.form()
        with session_scope(app.state.session_factory) as session:
            user = _require_page_user(session, request)
            if isinstance(user, RedirectResponse):
                return user
            if not _validate_csrf(request, str(form.get("csrf_token", ""))):
                _flash(request, "The password form expired. Please try again.", level="error")
                return _redirect("/account")
            current_password = str(form.get("current_password", ""))
            new_password = str(form.get("new_password", ""))
            confirm_password = str(form.get("confirm_password", ""))
            if not app.state.password_manager.verify(user.password_hash, current_password):
                _flash(request, "Current password is incorrect.", level="error")
                return _redirect("/account")
            if not new_password:
                _flash(request, "A new password is required.", level="error")
                return _redirect("/account")
            if new_password != confirm_password:
                _flash(request, "The new passwords do not match.", level="error")
                return _redirect("/account")
            set_user_password(session, user, password=new_password, password_manager=app.state.password_manager)
            _flash(request, "Password updated.", level="success")
            return _redirect("/account")

    @app.get("/users", response_class=HTMLResponse)
    async def users_page(request: Request):
        with session_scope(app.state.session_factory) as session:
            user = _require_admin_page(session, request)
            if isinstance(user, RedirectResponse):
                return user
            return templates.TemplateResponse(
                request,
                "users.html",
                _template_context(request, session=session, config=resolved_config, users=list_users(session)),
            )

    @app.get("/users/new", response_class=HTMLResponse)
    async def user_new_page(request: Request):
        with session_scope(app.state.session_factory) as session:
            user = _require_admin_page(session, request)
            if isinstance(user, RedirectResponse):
                return user
            return templates.TemplateResponse(
                request,
                "user_detail.html",
                _template_context(
                    request,
                    session=session,
                    config=resolved_config,
                    managed_user=None,
                    role_options=USER_ROLE_OPTIONS,
                    can_edit_role=False,
                ),
            )

    @app.post("/users")
    async def users_create(request: Request):
        form = await request.form()
        with session_scope(app.state.session_factory) as session:
            actor = _require_admin_page(session, request)
            if isinstance(actor, RedirectResponse):
                return actor
            if not _validate_csrf(request, str(form.get("csrf_token", ""))):
                _flash(request, "The user form expired. Please try again.", level="error")
                return _redirect("/users/new")
            username = str(form.get("username", "")).strip()
            password = str(form.get("password", ""))
            if not username or not password:
                _flash(request, "Username and password are required.", level="error")
                return _redirect("/users/new")
            if get_user_by_username(session, username) is not None:
                _flash(request, "That username is already in use.", level="error")
                return _redirect("/users/new")
            managed_user = create_user(session, username, password, app.state.password_manager, role=UserRole.STANDARD.value)
            _flash(request, f"Created user {managed_user.username}.", level="success")
            return _redirect(f"/users/{managed_user.id}")

    @app.get("/users/{user_id}", response_class=HTMLResponse)
    async def user_detail_page(request: Request, user_id: int):
        with session_scope(app.state.session_factory) as session:
            actor = _require_admin_page(session, request)
            if isinstance(actor, RedirectResponse):
                return actor
            managed_user = get_user_by_id(session, user_id)
            if managed_user is None:
                return HTMLResponse("Not found", status_code=404)
            can_edit_role = not (managed_user.id == actor.id and count_admin_users(session) <= 1)
            return templates.TemplateResponse(
                request,
                "user_detail.html",
                _template_context(
                    request,
                    session=session,
                    config=resolved_config,
                    managed_user=managed_user,
                    role_options=USER_ROLE_OPTIONS,
                    can_edit_role=can_edit_role,
                ),
            )

    @app.post("/users/{user_id}")
    async def user_save(request: Request, user_id: int):
        form = await request.form()
        with session_scope(app.state.session_factory) as session:
            actor = _require_admin_page(session, request)
            if isinstance(actor, RedirectResponse):
                return actor
            managed_user = get_user_by_id(session, user_id)
            if managed_user is None:
                return _redirect("/users")
            if not _validate_csrf(request, str(form.get("csrf_token", ""))):
                _flash(request, "The user form expired. Please try again.", level="error")
                return _redirect(f"/users/{user_id}")
            username = str(form.get("username", "")).strip()
            if not username:
                _flash(request, "Username is required.", level="error")
                return _redirect(f"/users/{user_id}")
            existing = get_user_by_username(session, username)
            if existing is not None and existing.id != managed_user.id:
                _flash(request, "That username is already in use.", level="error")
                return _redirect(f"/users/{user_id}")
            role = _selected_role(str(form.get("role", UserRole.STANDARD.value)).strip())
            if managed_user.id == actor.id and managed_user.is_admin and role != UserRole.ADMIN.value and count_admin_users(session) <= 1:
                _flash(request, "You cannot remove admin access from the last admin.", level="error")
                return _redirect(f"/users/{user_id}")
            update_user(session, managed_user, username=username, role=role)
            new_password = str(form.get("new_password", ""))
            if new_password:
                set_user_password(session, managed_user, password=new_password, password_manager=app.state.password_manager)
            _flash(request, "User saved.", level="success")
            return _redirect(f"/users/{user_id}")

    @app.post("/users/{user_id}/delete")
    async def user_remove(request: Request, user_id: int):
        form = await request.form()
        with session_scope(app.state.session_factory) as session:
            actor = _require_admin_page(session, request)
            if isinstance(actor, RedirectResponse):
                return actor
            managed_user = get_user_by_id(session, user_id)
            if managed_user is None:
                return _redirect("/users")
            if not _validate_csrf(request, str(form.get("csrf_token", ""))):
                _flash(request, "The user form expired. Please try again.", level="error")
                return _redirect(f"/users/{user_id}")
            if managed_user.id == actor.id:
                _flash(request, "Use your account page for your own password; deleting your own account is disabled.", level="error")
                return _redirect(f"/users/{user_id}")
            if managed_user.is_admin and count_admin_users(session) <= 1:
                _flash(request, "You cannot delete the last admin.", level="error")
                return _redirect(f"/users/{user_id}")
            delete_user(session, managed_user)
            _flash(request, "User deleted.", level="success")
            return _redirect("/users")

    @app.get("/settings", response_class=HTMLResponse)
    async def settings_page(request: Request):
        with session_scope(app.state.session_factory) as session:
            user = _require_admin_page(session, request)
            if isinstance(user, RedirectResponse):
                return user
            assets = list(session.scalars(select(AssetRecord).order_by(AssetRecord.id.desc())))
            return templates.TemplateResponse(
                request,
                "settings.html",
                _template_context(request, session=session, config=resolved_config, assets=assets, next_run_time=app.state.scheduler.next_run_time()),
            )

    @app.post("/settings")
    async def settings_submit(request: Request):
        form = await request.form()
        with session_scope(app.state.session_factory) as session:
            user = _require_admin_page(session, request)
            if isinstance(user, RedirectResponse):
                return user
            if not _validate_csrf(request, str(form.get("csrf_token", ""))):
                _flash(request, "The settings form expired. Please try again.", level="error")
                return _redirect("/settings")
            settings = get_settings(session, resolved_config)
            _, current_hour, current_minute = _normalize_schedule_time_from_cron(settings.schedule_cron)
            timezone = str(form.get("timezone", "UTC")).strip() or "UTC"
            if not is_valid_timezone(timezone):
                _flash(request, "Choose a valid timezone from the list.", level="error")
                return _redirect("/settings")
            manual_time = str(form.get("schedule_time_text", "")).strip()
            parsed_time = parse_schedule_time_input(manual_time) if manual_time else None
            if manual_time and parsed_time is None:
                _flash(request, "Enter a valid time like 9:05 AM or 21:05.", level="error")
                return _redirect("/settings")
            if parsed_time is None:
                hour = _parse_single_int(str(form.get("schedule_hour", current_hour)), minimum=0, maximum=23)
                minute = _parse_single_int(str(form.get("schedule_minute", current_minute)), minimum=0, maximum=59)
                if hour is None or minute is None:
                    _flash(request, "Choose a valid Wednesday time.", level="error")
                    return _redirect("/settings")
            else:
                hour, minute = parsed_time
            cron = _cron_for_wednesday_time(hour, minute)
            from apscheduler.triggers.cron import CronTrigger

            try:
                CronTrigger.from_crontab(cron, timezone=timezone)
            except Exception as exc:  # noqa: BLE001
                _flash(request, f"Invalid cron expression: {exc}", level="error")
                return _redirect("/settings")
            settings.timezone = timezone
            schedule_enabled_value = str(form.get("schedule_enabled", "enabled")).strip().lower()
            settings.schedule_enabled = schedule_enabled_value not in {"disabled", "false", "off", "0"}
            settings.schedule_cron = cron
            settings.caption_text = str(form.get("caption_text", "")).strip()
            chosen_asset_id = str(form.get("asset_id", "")).strip()
            asset_file = form.get("asset_file")
            if asset_file and getattr(asset_file, "filename", ""):
                payload = await asset_file.read()
                media_type = asset_file.content_type or "image/png"
                try:
                    pending_asset = create_pending_asset(
                        session,
                        resolved_config,
                        filename=asset_file.filename,
                        payload=payload,
                        media_type=media_type,
                    )
                except ValueError as exc:
                    _flash(request, str(exc), level="error")
                    return _redirect("/settings")
                app.state.asset_processor.queue(pending_asset.id)
                _flash(request, "Uploaded image queued for background processing. Activate it after it becomes ready.", level="success")
            elif chosen_asset_id:
                asset = session.get(AssetRecord, int(chosen_asset_id))
                if asset is None:
                    _flash(request, "Selected asset does not exist.", level="error")
                    return _redirect("/settings")
                if asset.processing_status != "ready":
                    _flash(request, "Choose an asset that is ready before activating it.", level="error")
                    return _redirect("/settings")
                settings.active_asset_id = asset.id
            session.flush()
        _refresh_scheduler(app)
        _flash(request, "Settings saved.", level="success")
        return _redirect("/settings")

    @app.get("/destinations", response_class=HTMLResponse)
    async def destinations_page(request: Request):
        with session_scope(app.state.session_factory) as session:
            user = _require_page_user(session, request)
            if isinstance(user, RedirectResponse):
                return user
            destinations = list_destinations(session, user=user)
            return templates.TemplateResponse(
                request,
                "destinations.html",
                _template_context(request, session=session, config=resolved_config, destinations=destinations),
            )

    @app.post("/destinations")
    async def destinations_create(request: Request):
        form = await request.form()
        with session_scope(app.state.session_factory) as session:
            user = _require_page_user(session, request)
            if isinstance(user, RedirectResponse):
                return user
            if not _validate_csrf(request, str(form.get("csrf_token", ""))):
                _flash(request, "The destination form expired. Please try again.", level="error")
                return _redirect("/destinations")
            plugin_id = str(form.get("plugin_id", "")).strip()
            plugin = app.state.plugin_manager.get(plugin_id)
            if plugin is None:
                _flash(request, "Choose an available plugin.", level="error")
                return _redirect("/destinations")
            name = str(form.get("name", "")).strip() or plugin.manifest.display_name
            destination = create_destination(session, owner=user, plugin_id=plugin_id, name=name)
            _flash(request, f"Created {name}.", level="success")
            return _redirect(f"/destinations/{destination.id}")

    @app.get("/destinations/{destination_id}", response_class=HTMLResponse)
    async def destination_detail(request: Request, destination_id: int):
        with session_scope(app.state.session_factory) as session:
            user = _require_page_user(session, request)
            if isinstance(user, RedirectResponse):
                return user
            destination = get_destination_for_user(session, user, destination_id)
            if destination is None:
                return HTMLResponse("Not found", status_code=404)
            plugin = app.state.plugin_manager.get(destination.plugin_id)
            validation = validate_all_destinations(
                session,
                resolved_config,
                app.state.secret_manager,
                app.state.plugin_manager,
                user=user,
            )
            destination_validation = next((item for item in validation["destinations"] if item["id"] == destination.id), {"issues": []})
            plugin_context = _plugin_context(plugin)
            destination_secret_state = {
                field.name: describe_secret_state(session, destination=destination, secret_key=field.name)
                for field in plugin_context["destination_secret_fields"]
            }
            channel_secret_state = {
                channel.id: {
                    field.name: describe_secret_state(session, channel=channel, secret_key=field.name)
                    for field in plugin_context["channel_secret_fields"]
                }
                for channel in destination.channels
            }
            return templates.TemplateResponse(
                request,
                "destination_detail.html",
                _template_context(
                    request,
                    session=session,
                    config=resolved_config,
                    destination=destination,
                    destination_validation=destination_validation["issues"],
                    destination_secret_state=destination_secret_state,
                    channel_secret_state=channel_secret_state,
                    **plugin_context,
                ),
            )

    @app.post("/destinations/{destination_id}")
    async def destination_save(request: Request, destination_id: int):
        form = await request.form()
        with session_scope(app.state.session_factory) as session:
            user = _require_page_user(session, request)
            if isinstance(user, RedirectResponse):
                return user
            destination = get_destination_for_user(session, user, destination_id)
            if destination is None:
                return _redirect("/destinations")
            if not _validate_csrf(request, str(form.get("csrf_token", ""))):
                _flash(request, "The destination form expired. Please try again.", level="error")
                return _redirect(f"/destinations/{destination_id}")
            plugin = app.state.plugin_manager.get(destination.plugin_id)
            plugin_context = _plugin_context(plugin)
            update_destination(
                session,
                destination,
                name=str(form.get("name", "")).strip() or destination.name,
                enabled=_bool_from_form(form, "enabled"),
                config_values=_values_from_fields(form, plugin_context["destination_fields"]),
            )
            _flash(request, "Destination saved.", level="success")
            return _redirect(f"/destinations/{destination_id}")

    @app.post("/destinations/{destination_id}/delete")
    async def destination_remove(request: Request, destination_id: int):
        form = await request.form()
        with session_scope(app.state.session_factory) as session:
            user = _require_page_user(session, request)
            if isinstance(user, RedirectResponse):
                return user
            destination = get_destination_for_user(session, user, destination_id)
            if destination is not None and _validate_csrf(request, str(form.get("csrf_token", ""))):
                delete_destination(session, destination)
                _flash(request, "Destination deleted.", level="success")
        return _redirect("/destinations")

    @app.post("/destinations/{destination_id}/secrets")
    async def destination_secret_submit(request: Request, destination_id: int):
        form = await request.form()
        with session_scope(app.state.session_factory) as session:
            user = _require_page_user(session, request)
            if isinstance(user, RedirectResponse):
                return user
            destination = get_destination_for_user(session, user, destination_id)
            if destination is None:
                return _redirect("/destinations")
            if not _validate_csrf(request, str(form.get("csrf_token", ""))):
                _flash(request, "The secret form expired. Please try again.", level="error")
                return _redirect(f"/destinations/{destination_id}")
            secret_key = str(form.get("secret_key", "")).strip()
            action = str(form.get("action", "save")).strip()
            if action == "clear":
                clear_secret_value(session, destination=destination, secret_key=secret_key)
                _flash(request, "Secret cleared.", level="success")
            else:
                value = str(form.get("secret_value", "")).strip()
                if value:
                    set_secret_value(
                        session,
                        secret_manager=app.state.secret_manager,
                        destination=destination,
                        secret_key=secret_key,
                        label=secret_key,
                        value=value,
                    )
                    _flash(request, "Secret saved.", level="success")
        return _redirect(f"/destinations/{destination_id}")

    @app.post("/destinations/{destination_id}/channels")
    async def channel_create(request: Request, destination_id: int):
        form = await request.form()
        with session_scope(app.state.session_factory) as session:
            user = _require_page_user(session, request)
            if isinstance(user, RedirectResponse):
                return user
            destination = get_destination_for_user(session, user, destination_id)
            if destination is None:
                return _redirect("/destinations")
            if not _validate_csrf(request, str(form.get("csrf_token", ""))):
                _flash(request, "The channel form expired. Please try again.", level="error")
                return _redirect(f"/destinations/{destination_id}")
            plugin = app.state.plugin_manager.get(destination.plugin_id)
            plugin_context = _plugin_context(plugin)
            channel = add_channel(
                session,
                destination,
                name=str(form.get("name", "")).strip() or "New channel",
                enabled=_bool_from_form(form, "enabled"),
                config_values=_values_from_fields(form, plugin_context["channel_fields"]),
            )
            for field in plugin_context["channel_secret_fields"]:
                value = str(form.get(field.name, "")).strip()
                if value:
                    set_secret_value(
                        session,
                        secret_manager=app.state.secret_manager,
                        channel=channel,
                        secret_key=field.name,
                        label=field.label,
                        value=value,
                    )
            _flash(request, "Channel added.", level="success")
        return _redirect(f"/destinations/{destination_id}")

    @app.post("/destinations/{destination_id}/channels/{channel_id}")
    async def channel_save(request: Request, destination_id: int, channel_id: int):
        form = await request.form()
        with session_scope(app.state.session_factory) as session:
            user = _require_page_user(session, request)
            if isinstance(user, RedirectResponse):
                return user
            destination = get_destination_for_user(session, user, destination_id)
            channel = get_channel_for_user(session, user, destination_id, channel_id)
            if destination is None or channel is None:
                return _redirect(f"/destinations/{destination_id}")
            if not _validate_csrf(request, str(form.get("csrf_token", ""))):
                _flash(request, "The channel form expired. Please try again.", level="error")
                return _redirect(f"/destinations/{destination_id}")
            plugin = app.state.plugin_manager.get(destination.plugin_id)
            plugin_context = _plugin_context(plugin)
            update_channel(
                session,
                channel,
                name=str(form.get("name", "")).strip() or channel.name,
                enabled=_bool_from_form(form, "enabled"),
                config_values=_values_from_fields(form, plugin_context["channel_fields"]),
            )
            for field in plugin_context["channel_secret_fields"]:
                value = str(form.get(field.name, "")).strip()
                if value:
                    set_secret_value(
                        session,
                        secret_manager=app.state.secret_manager,
                        channel=channel,
                        secret_key=field.name,
                        label=field.label,
                        value=value,
                    )
            _flash(request, "Channel saved.", level="success")
        return _redirect(f"/destinations/{destination_id}")

    @app.post("/destinations/{destination_id}/channels/{channel_id}/secrets")
    async def channel_secret_submit(request: Request, destination_id: int, channel_id: int):
        form = await request.form()
        with session_scope(app.state.session_factory) as session:
            user = _require_page_user(session, request)
            if isinstance(user, RedirectResponse):
                return user
            channel = get_channel_for_user(session, user, destination_id, channel_id)
            if channel is None:
                return _redirect(f"/destinations/{destination_id}")
            if not _validate_csrf(request, str(form.get("csrf_token", ""))):
                _flash(request, "The secret form expired. Please try again.", level="error")
                return _redirect(f"/destinations/{destination_id}")
            secret_key = str(form.get("secret_key", "")).strip()
            action = str(form.get("action", "save")).strip()
            if action == "clear":
                clear_secret_value(session, channel=channel, secret_key=secret_key)
                _flash(request, "Channel secret cleared.", level="success")
            else:
                value = str(form.get("secret_value", "")).strip()
                if value:
                    set_secret_value(
                        session,
                        secret_manager=app.state.secret_manager,
                        channel=channel,
                        secret_key=secret_key,
                        label=secret_key,
                        value=value,
                    )
                    _flash(request, "Channel secret saved.", level="success")
        return _redirect(f"/destinations/{destination_id}")

    @app.post("/destinations/{destination_id}/channels/{channel_id}/delete")
    async def channel_remove(request: Request, destination_id: int, channel_id: int):
        form = await request.form()
        with session_scope(app.state.session_factory) as session:
            user = _require_page_user(session, request)
            if isinstance(user, RedirectResponse):
                return user
            channel = get_channel_for_user(session, user, destination_id, channel_id)
            if channel is not None and _validate_csrf(request, str(form.get("csrf_token", ""))):
                delete_channel(session, channel)
                _flash(request, "Channel deleted.", level="success")
        return _redirect(f"/destinations/{destination_id}")

    @app.get("/test", response_class=HTMLResponse)
    async def test_page(request: Request):
        with session_scope(app.state.session_factory) as session:
            user = _require_page_user(session, request)
            if isinstance(user, RedirectResponse):
                return user
            destinations = list_destinations(session, user=user)
            return templates.TemplateResponse(
                request,
                "test.html",
                _template_context(request, session=session, config=resolved_config, destinations=destinations),
            )

    @app.get("/history", response_class=HTMLResponse)
    async def history_page(request: Request):
        with session_scope(app.state.session_factory) as session:
            user = _require_page_user(session, request)
            if isinstance(user, RedirectResponse):
                return user
            runs = list_recent_runs(session, limit=30, user=user)
            attempts = list_attempts_for_runs(session, runs, user=user)
            attempts_by_run: dict[int, list[DeliveryAttempt]] = {}
            for attempt in attempts:
                attempts_by_run.setdefault(attempt.run_id, []).append(attempt)
            return templates.TemplateResponse(
                request,
                "history.html",
                _template_context(request, session=session, config=resolved_config, runs=runs, attempts_by_run=attempts_by_run),
            )

    @app.post("/api/v1/runs")
    async def api_run(request: Request):
        with session_scope(app.state.session_factory) as session:
            user = _require_api_user(session, request)
            if isinstance(user, JSONResponse):
                return user
        if not _validate_api_csrf(request):
            return JSONResponse({"detail": "CSRF token missing or invalid."}, status_code=403)
        result = app.state.delivery_manager.run(
            trigger=RunTrigger.MANUAL,
            initiated_by=user.username,
            initiated_by_user_id=user.id,
            acting_as_admin=is_admin_user(user),
        )
        return JSONResponse(result)

    @app.get("/api/v1/runs")
    async def api_runs(request: Request):
        with session_scope(app.state.session_factory) as session:
            user = _require_api_user(session, request)
            if isinstance(user, JSONResponse):
                return user
            runs = list_recent_runs(session, limit=20, user=user)
            payload = [
                {
                    "id": run.id,
                    "trigger_kind": run.trigger_kind,
                    "status": run.status,
                    "summary": run.summary_json,
                    "scheduled_slot": run.scheduled_slot.isoformat() if run.scheduled_slot else None,
                    "started_at": run.started_at.isoformat(),
                    "finished_at": run.finished_at.isoformat() if run.finished_at else None,
                }
                for run in runs
            ]
            return JSONResponse({"runs": payload})

    @app.post("/api/v1/destinations/{destination_id}/test")
    async def api_destination_test(request: Request, destination_id: int):
        with session_scope(app.state.session_factory) as session:
            user = _require_api_user(session, request)
            if isinstance(user, JSONResponse):
                return user
            destination = get_destination_for_user(session, user, destination_id)
            if destination is None:
                return JSONResponse({"detail": "Destination not found."}, status_code=404)
        if not _validate_api_csrf(request):
            return JSONResponse({"detail": "CSRF token missing or invalid."}, status_code=403)
        result = app.state.delivery_manager.run(
            trigger=RunTrigger.TEST,
            initiated_by=user.username,
            initiated_by_user_id=user.id,
            acting_as_admin=is_admin_user(user),
            destination_id=destination_id,
        )
        return JSONResponse(result)

    @app.get("/api/v1/config/validate")
    async def api_validate(request: Request):
        with session_scope(app.state.session_factory) as session:
            user = _require_api_user(session, request)
            if isinstance(user, JSONResponse):
                return user
            return JSONResponse(
                validate_all_destinations(
                    session,
                    resolved_config,
                    app.state.secret_manager,
                    app.state.plugin_manager,
                    user=user,
                )
            )

    @app.post("/api/v1/admin/rekey")
    async def api_rekey(request: Request):
        with session_scope(app.state.session_factory) as session:
            user = _require_admin_api(session, request)
            if isinstance(user, JSONResponse):
                return user
            if not _validate_api_csrf(request):
                return JSONResponse({"detail": "CSRF token missing or invalid."}, status_code=403)
            count = rekey_all_secrets(session, secret_manager=app.state.secret_manager)
            return JSONResponse({"rekeyed": count})

    return app
