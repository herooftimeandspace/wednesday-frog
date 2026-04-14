"""Core application services and delivery orchestration."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
import logging
import threading
import time
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import delete, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, load_only, selectinload, sessionmaker

from .assets import ensure_default_asset, load_asset_bytes, resolve_asset_path
from .config import AppConfig
from .db import session_scope
from .delivery.base import AdapterResult, PreparedAsset
from .http_client import OutboundHttpClient
from .metrics import MetricsCollector
from .models import (
    AdminUser,
    AppMetricCounter,
    AppSettings,
    AssetRecord,
    DeliveryAttempt,
    DeliveryRun,
    DestinationChannel,
    EncryptedSecret,
    RunTrigger,
    ServiceDestination,
    UserRole,
)
from .plugins import PluginErrorContext, PluginManager, PluginSendContext, PluginValidationContext
from .schedule_coordination import RedisScheduleCoordinator
from .security import PasswordManager, SecretManager


LOGGER = logging.getLogger(__name__)
DEFAULT_SCHEDULE_CRON = "0 12 * * wed"
DEFAULT_TIMEZONE = "UTC"
RUN_COUNTER_METRIC = "runs_total"
ATTEMPT_COUNTER_METRIC = "delivery_attempts_total"
MAX_RESPONSE_EXCERPT_LENGTH = 240
MAX_ERROR_MESSAGE_LENGTH = 500


def time_to_datetime() -> datetime:
    """Return an aware UTC timestamp."""
    return datetime.now(UTC)


def _truncate_text(value: str | None, *, limit: int) -> str | None:
    """Cap stored response and error text so history rows stay compact."""
    if not value:
        return value
    if len(value) <= limit:
        return value
    return f"{value[: limit - 1]}…"


def _plugin_requires_asset_for_validation(loaded_plugin) -> bool:
    """Return whether one plugin needs an asset reference during validation."""
    return bool(loaded_plugin and getattr(loaded_plugin.connector, "requires_asset_for_validation", False))


def _build_prepared_asset(
    session: Session,
    config: AppConfig,
    *,
    include_payload: bool,
) -> tuple[AppSettings, PreparedAsset, bool, str | None]:
    """Prepare the active asset once for validation or delivery."""
    settings, asset_record, fallback_active, fallback_warning = resolve_active_asset(session, config)
    asset_path = resolve_asset_path(config, asset_record)
    payload = load_asset_bytes(config, asset_record) if include_payload else b""
    return settings, PreparedAsset(
        filename=asset_record.original_filename,
        media_type=asset_record.media_type,
        payload=payload,
        size_bytes=asset_record.size_bytes,
        source_path=asset_path,
    ), fallback_active, fallback_warning


def build_plugin_manager(config: AppConfig) -> PluginManager:
    """Build the plugin manager from the current config."""
    return PluginManager([config.package_plugins_dir])


def has_admin_user(session: Session) -> bool:
    """Return whether the database already has at least one admin."""
    return session.scalar(select(AdminUser.id).where(AdminUser.role == UserRole.ADMIN.value).limit(1)) is not None


def get_user_by_username(session: Session, username: str) -> AdminUser | None:
    """Look up a local user by username."""
    return session.scalar(select(AdminUser).where(AdminUser.username == username))


def get_user_by_id(session: Session, user_id: int) -> AdminUser | None:
    """Return a user row by primary key."""
    return session.get(AdminUser, user_id)


def is_admin_user(user: AdminUser | None) -> bool:
    """Return whether the supplied user has admin privileges."""
    return bool(user and user.is_admin)


def list_users(session: Session) -> list[AdminUser]:
    """Return all local users."""
    return list(
        session.scalars(
            select(AdminUser)
            .options(load_only(AdminUser.id, AdminUser.username, AdminUser.role, AdminUser.created_at))
            .order_by(AdminUser.username.asc())
        )
    )


def count_admin_users(session: Session) -> int:
    """Return the number of admin-role users."""
    return len(list(session.scalars(select(AdminUser.id).where(AdminUser.role == UserRole.ADMIN.value))))


def create_user(
    session: Session,
    username: str,
    password: str,
    password_manager: PasswordManager,
    *,
    role: str = UserRole.STANDARD.value,
) -> AdminUser:
    """Create a local user with the supplied role."""
    admin = AdminUser(username=username.strip(), role=role, password_hash=password_manager.hash_password(password))
    session.add(admin)
    session.flush()
    return admin


def create_admin_user(session: Session, username: str, password: str, password_manager: PasswordManager) -> AdminUser:
    """Create the first local admin user."""
    return create_user(session, username, password, password_manager, role=UserRole.ADMIN.value)


def update_user(session: Session, user: AdminUser, *, username: str, role: str | None = None) -> None:
    """Update a user's public metadata."""
    user.username = username.strip()
    if role is not None:
        user.role = role
    session.flush()


def set_user_password(session: Session, user: AdminUser, *, password: str, password_manager: PasswordManager) -> None:
    """Replace a user's password hash."""
    user.password_hash = password_manager.hash_password(password)
    session.flush()


def delete_user(session: Session, user: AdminUser) -> None:
    """Delete a user account."""
    for run in session.scalars(select(DeliveryRun).where(DeliveryRun.initiated_by_user_id == user.id)):
        run.initiated_by_user_id = None
    session.delete(user)
    session.flush()


def ensure_defaults(session: Session, config: AppConfig) -> AppSettings:
    """Create the default asset and singleton settings row when missing."""
    asset = ensure_default_asset(session, config)
    settings = session.get(AppSettings, 1)
    if settings is None:
        settings = AppSettings(
            id=1,
            timezone=DEFAULT_TIMEZONE,
            schedule_enabled=True,
            schedule_cron=DEFAULT_SCHEDULE_CRON,
            caption_text="",
            active_asset_id=asset.id,
        )
        session.add(settings)
        session.flush()
    elif settings.active_asset_id is None:
        settings.active_asset_id = asset.id
        session.flush()
    return settings


def get_settings(session: Session, config: AppConfig) -> AppSettings:
    """Return the singleton settings row, seeding defaults if necessary."""
    return ensure_defaults(session, config)


def get_secret_record(
    session: Session,
    *,
    destination: ServiceDestination | None = None,
    channel: DestinationChannel | None = None,
    secret_key: str,
) -> EncryptedSecret | None:
    """Find a stored secret for a destination or channel."""
    query = select(EncryptedSecret).where(EncryptedSecret.secret_key == secret_key)
    if destination is not None:
        query = query.where(EncryptedSecret.destination_id == destination.id)
    if channel is not None:
        query = query.where(EncryptedSecret.channel_id == channel.id)
    return session.scalar(query.limit(1))


def get_secret_value(
    session: Session,
    *,
    secret_manager: SecretManager,
    destination: ServiceDestination | None = None,
    channel: DestinationChannel | None = None,
    secret_key: str,
) -> str | None:
    """Decrypt and return a secret value if one exists."""
    record = get_secret_record(session, destination=destination, channel=channel, secret_key=secret_key)
    if record is None:
        return None
    return secret_manager.decrypt(record.ciphertext, record.nonce)


def describe_secret_state(
    session: Session,
    *,
    destination: ServiceDestination | None = None,
    channel: DestinationChannel | None = None,
    secret_key: str,
) -> dict[str, str]:
    """Return masked secret metadata for the UI."""
    record = get_secret_record(session, destination=destination, channel=channel, secret_key=secret_key)
    if record is None:
        return {"state": "empty", "label": ""}
    return {"state": "saved", "label": f"Saved ending in {record.last_four}"}


def set_secret_value(
    session: Session,
    *,
    secret_manager: SecretManager,
    secret_key: str,
    label: str,
    value: str,
    destination: ServiceDestination | None = None,
    channel: DestinationChannel | None = None,
) -> None:
    """Create or replace an encrypted secret value."""
    ciphertext, nonce, last_four = secret_manager.encrypt(value)
    record = get_secret_record(session, destination=destination, channel=channel, secret_key=secret_key)
    if record is None:
        record = EncryptedSecret(
            destination_id=destination.id if destination else None,
            channel_id=channel.id if channel else None,
            secret_key=secret_key,
            label=label,
            ciphertext=ciphertext,
            nonce=nonce,
            last_four=last_four,
        )
        session.add(record)
    else:
        record.label = label
        record.ciphertext = ciphertext
        record.nonce = nonce
        record.last_four = last_four
    session.flush()


def clear_secret_value(
    session: Session,
    *,
    secret_key: str,
    destination: ServiceDestination | None = None,
    channel: DestinationChannel | None = None,
) -> None:
    """Delete a stored secret value."""
    record = get_secret_record(session, destination=destination, channel=channel, secret_key=secret_key)
    if record is not None:
        session.delete(record)
        session.flush()


def rekey_all_secrets(session: Session, *, secret_manager: SecretManager) -> int:
    """Re-encrypt every stored secret with the active key."""
    count = 0
    for record in session.scalars(select(EncryptedSecret).order_by(EncryptedSecret.id.asc())):
        plaintext = secret_manager.decrypt(record.ciphertext, record.nonce)
        record.ciphertext, record.nonce, record.last_four = secret_manager.encrypt(plaintext)
        count += 1
    session.flush()
    return count


def increment_metric_counter(
    session: Session,
    *,
    metric_name: str,
    label_primary: str = "",
    label_secondary: str = "",
    amount: int = 1,
) -> None:
    """Increment one persisted aggregate counter."""
    counter = session.get(
        AppMetricCounter,
        {
            "metric_name": metric_name,
            "label_primary": label_primary,
            "label_secondary": label_secondary,
        },
    )
    if counter is None:
        counter = AppMetricCounter(
            metric_name=metric_name,
            label_primary=label_primary,
            label_secondary=label_secondary,
            value=amount,
        )
        session.add(counter)
    else:
        counter.value += amount


def list_metric_counters(session: Session, *, metric_name: str) -> list[AppMetricCounter]:
    """Return all persisted counters for one metric family."""
    return list(
        session.scalars(
            select(AppMetricCounter)
            .options(
                load_only(
                    AppMetricCounter.metric_name,
                    AppMetricCounter.label_primary,
                    AppMetricCounter.label_secondary,
                    AppMetricCounter.value,
                )
            )
            .where(AppMetricCounter.metric_name == metric_name)
            .order_by(AppMetricCounter.label_primary.asc(), AppMetricCounter.label_secondary.asc())
        )
    )


def enabled_destination_counts(session: Session) -> dict[str, int]:
    """Return enabled destination counts grouped by plugin id."""
    rows = session.execute(
        select(ServiceDestination.plugin_id, func.count(ServiceDestination.id))
        .where(ServiceDestination.enabled.is_(True))
        .group_by(ServiceDestination.plugin_id)
    )
    return {plugin_id: count for plugin_id, count in rows}


def list_destinations(session: Session, *, user: AdminUser | None = None) -> list[ServiceDestination]:
    """Return destinations visible to the supplied user."""
    query = (
        select(ServiceDestination)
        .options(
            load_only(
                ServiceDestination.id,
                ServiceDestination.owner_user_id,
                ServiceDestination.plugin_id,
                ServiceDestination.name,
                ServiceDestination.enabled,
                ServiceDestination.auto_disabled_at,
                ServiceDestination.disable_reason,
                ServiceDestination.created_at,
            ),
            selectinload(ServiceDestination.owner).load_only(AdminUser.id, AdminUser.username),
        )
        .order_by(ServiceDestination.id.asc())
    )
    if user is not None and not is_admin_user(user):
        query = query.where(ServiceDestination.owner_user_id == user.id)
    return list(session.scalars(query))


def get_destination_for_user(session: Session, user: AdminUser, destination_id: int) -> ServiceDestination | None:
    """Return one destination if it is visible to the supplied user."""
    destination = session.get(ServiceDestination, destination_id)
    if destination is None:
        return None
    if is_admin_user(user) or destination.owner_user_id == user.id:
        return destination
    return None


def get_channel_for_user(session: Session, user: AdminUser, destination_id: int, channel_id: int) -> DestinationChannel | None:
    """Return one channel when its parent destination is visible to the supplied user."""
    destination = get_destination_for_user(session, user, destination_id)
    if destination is None:
        return None
    channel = session.get(DestinationChannel, channel_id)
    if channel is None or channel.destination_id != destination.id:
        return None
    return channel


def create_destination(
    session: Session,
    *,
    owner: AdminUser,
    plugin_id: str | None = None,
    service_type: str | None = None,
    name: str,
) -> ServiceDestination:
    """Create a new destination skeleton."""
    resolved_plugin_id = plugin_id or service_type
    if not resolved_plugin_id:
        raise ValueError("plugin_id is required")
    destination = ServiceDestination(
        owner_user_id=owner.id,
        plugin_id=resolved_plugin_id,
        name=name,
        enabled=True,
        config_json={},
    )
    session.add(destination)
    session.flush()
    return destination


def update_destination(session: Session, destination: ServiceDestination, *, name: str, enabled: bool, config_values: dict[str, str]) -> None:
    """Update destination metadata and plain-text config fields."""
    destination.name = name.strip()
    destination.enabled = enabled
    destination.config_json = config_values
    if enabled:
        destination.auto_disabled_at = None
        destination.disable_reason = None
    session.flush()


def add_channel(session: Session, destination: ServiceDestination, *, name: str, enabled: bool, config_values: dict[str, str]) -> DestinationChannel:
    """Append a new channel to a destination."""
    channel = DestinationChannel(destination_id=destination.id, name=name.strip(), enabled=enabled, config_json=config_values)
    session.add(channel)
    session.flush()
    return channel


def update_channel(session: Session, channel: DestinationChannel, *, name: str, enabled: bool, config_values: dict[str, str]) -> None:
    """Update one existing channel."""
    channel.name = name.strip()
    channel.enabled = enabled
    channel.config_json = config_values
    session.flush()


def delete_channel(session: Session, channel: DestinationChannel) -> None:
    """Delete one channel."""
    session.delete(channel)
    session.flush()


def delete_destination(session: Session, destination: ServiceDestination) -> None:
    """Delete an entire destination and its channels and secrets."""
    session.delete(destination)
    session.flush()


def resolve_active_asset(session: Session, config: AppConfig) -> tuple[AppSettings, AssetRecord, bool, str | None]:
    """Return a usable asset, falling back to the bundled frog if necessary."""
    settings = get_settings(session, config)
    default_asset = ensure_default_asset(session, config)
    asset = settings.active_asset or default_asset
    fallback_warning = None
    fallback_active = False
    if asset.processing_status != "ready" or not resolve_asset_path(config, asset).is_file():
        settings.active_asset_id = default_asset.id
        session.flush()
        asset = default_asset
        fallback_active = True
        fallback_warning = "The selected uploaded asset was unavailable, so the bundled frog image is active."
    return settings, asset, fallback_active, fallback_warning


def validate_destination(
    session: Session,
    config: AppConfig,
    destination: ServiceDestination,
    secret_manager: SecretManager,
    plugin_manager: PluginManager,
    *,
    prepared_asset: PreparedAsset | None = None,
) -> list[str]:
    """Validate one destination and return human-readable issue strings."""
    loaded = plugin_manager.get(destination.plugin_id)
    if loaded is None:
        return [f"Plugin '{destination.plugin_id}' is unavailable."]
    context = PluginValidationContext(session=session, destination=destination, secret_manager=secret_manager, asset=prepared_asset)
    try:
        return [issue.message for issue in loaded.connector.validate_config(context)]
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Plugin validation crashed for plugin_id=%s destination_id=%s: %s", destination.plugin_id, destination.id, exc)
        return [f"Plugin validation crashed: {exc}"]


def validate_all_destinations(
    session: Session,
    config: AppConfig,
    secret_manager: SecretManager,
    plugin_manager: PluginManager,
    *,
    user: AdminUser | None = None,
) -> dict[str, Any]:
    """Return a health-style validation summary for all enabled destinations."""
    destination_query = (
        select(ServiceDestination)
        .options(selectinload(ServiceDestination.channels))
        .order_by(ServiceDestination.id.asc())
    )
    if user is not None and not is_admin_user(user):
        destination_query = destination_query.where(ServiceDestination.owner_user_id == user.id)
    destinations = list(session.scalars(destination_query))
    _settings, asset_record, fallback_active, fallback_warning = resolve_active_asset(session, config)
    validation_asset = None
    if any(_plugin_requires_asset_for_validation(plugin_manager.get(destination.plugin_id)) for destination in destinations):
        validation_asset = PreparedAsset(
            filename=asset_record.original_filename,
            media_type=asset_record.media_type,
            payload=b"",
            size_bytes=asset_record.size_bytes,
            source_path=resolve_asset_path(config, asset_record),
        )
    issues: list[str] = []
    if fallback_warning:
        issues.append(fallback_warning)
    results: list[dict[str, Any]] = []
    for destination in destinations:
        destination_issues = validate_destination(
            session,
            config,
            destination,
            secret_manager,
            plugin_manager,
            prepared_asset=validation_asset if _plugin_requires_asset_for_validation(plugin_manager.get(destination.plugin_id)) else None,
        )
        results.append(
            {
                "id": destination.id,
                "name": destination.name,
                "plugin_id": destination.plugin_id,
                "enabled": destination.enabled,
                "owner_user_id": destination.owner_user_id,
                "issues": destination_issues,
                "auto_disabled_at": destination.auto_disabled_at.isoformat() if destination.auto_disabled_at else None,
                "disable_reason": destination.disable_reason,
            }
        )
    return {
        "ok": not issues and not plugin_manager.failures() and all(not result["issues"] or not result["enabled"] for result in results),
        "issues": issues,
        "plugin_failures": [{"plugin_id": item.plugin_id, "reason": item.reason} for item in plugin_manager.failures()],
        "fallback_asset_active": fallback_active,
        "active_asset_filename": asset_record.original_filename,
        "destinations": results,
    }


def _channel_attempts_for_validation(run_id: int, destination: ServiceDestination, channels: list[DestinationChannel], message: str) -> list[DeliveryAttempt]:
    capped_message = _truncate_text(message, limit=MAX_ERROR_MESSAGE_LENGTH)
    attempts: list[DeliveryAttempt] = []
    if not channels:
        attempts.append(
            DeliveryAttempt(
                run_id=run_id,
                destination_id=destination.id,
                channel_id=None,
                plugin_id=destination.plugin_id,
                status="permanent_failure",
                attempt_index=1,
                error_message=capped_message,
                finished_at=time_to_datetime(),
            )
        )
    else:
        for channel in channels:
            attempts.append(
                DeliveryAttempt(
                    run_id=run_id,
                    destination_id=destination.id,
                    channel_id=channel.id,
                    plugin_id=destination.plugin_id,
                    status="permanent_failure",
                    attempt_index=1,
                    error_message=capped_message,
                    finished_at=time_to_datetime(),
                )
            )
    return attempts


class DeliveryManager:
    """Run scheduled, manual, and test deliveries."""

    def __init__(
        self,
        *,
        config: AppConfig,
        session_factory: sessionmaker[Session],
        secret_manager: SecretManager,
        plugin_manager: PluginManager,
        http_client: OutboundHttpClient,
        metrics: MetricsCollector,
    ) -> None:
        self._config = config
        self._session_factory = session_factory
        self._secret_manager = secret_manager
        self._plugin_manager = plugin_manager
        self._http_client = http_client
        self._metrics = metrics
        self._coordinator = RedisScheduleCoordinator(config.redis_url)
        self._run_lock = threading.Lock()
        self._idle_condition = threading.Condition()
        self._active_runs = 0
        self._closing = False

    def _prepare_asset(self, session: Session) -> tuple[AppSettings, PreparedAsset, bool, str | None]:
        return _build_prepared_asset(session, self._config, include_payload=True)

    def _is_retryable_error(self, error_message: str | None) -> bool:
        """Best-effort retry classifier for transport and platform throttling errors."""
        if not error_message:
            return False
        lowered = error_message.lower()
        return any(token in lowered for token in ("timed out", "timeout", "connection", "429", "502", "503", "504"))

    def _send_with_retry(self, sender, error_handler, *, max_attempts: int = 3):
        """Run one plugin send with bounded retries."""
        last_result = None
        for attempt_index in range(1, max_attempts + 1):
            try:
                result = sender()
            except httpx.TransportError as exc:
                if attempt_index == max_attempts:
                    return attempt_index, {
                        "status": "retryable_failure",
                        "error_message": _truncate_text(str(exc), limit=MAX_ERROR_MESSAGE_LENGTH),
                        "response_excerpt": None,
                    }
                time.sleep(attempt_index)
                continue
            except Exception as exc:  # noqa: BLE001
                result = error_handler(exc)
            response_excerpt = _truncate_text(result.response_excerpt, limit=MAX_RESPONSE_EXCERPT_LENGTH)
            error_message = _truncate_text(result.error_message, limit=MAX_ERROR_MESSAGE_LENGTH)
            if result.status == "success":
                return attempt_index, {"status": result.status, "error_message": error_message, "response_excerpt": response_excerpt}
            if result.status == "retryable_failure" or self._is_retryable_error(error_message):
                last_result = result
                if attempt_index == max_attempts:
                    return attempt_index, {"status": "retryable_failure", "error_message": error_message, "response_excerpt": response_excerpt}
                time.sleep(attempt_index)
                continue
            return attempt_index, {"status": result.status, "error_message": error_message, "response_excerpt": response_excerpt}
        assert last_result is not None
        return max_attempts, {
            "status": last_result.status,
            "error_message": _truncate_text(last_result.error_message, limit=MAX_ERROR_MESSAGE_LENGTH),
            "response_excerpt": _truncate_text(last_result.response_excerpt, limit=MAX_RESPONSE_EXCERPT_LENGTH),
        }

    def _begin_run(self) -> bool:
        with self._idle_condition:
            if self._closing:
                return False
        acquired = self._run_lock.acquire(blocking=False)
        if not acquired:
            return False
        with self._idle_condition:
            self._active_runs += 1
        return True

    def _end_run(self) -> None:
        self._run_lock.release()
        with self._idle_condition:
            self._active_runs = max(0, self._active_runs - 1)
            self._idle_condition.notify_all()

    def shutdown(self, timeout: int) -> None:
        """Stop accepting new work and wait for active runs to finish."""
        with self._idle_condition:
            self._closing = True
            end_at = time.monotonic() + timeout
            while self._active_runs > 0:
                remaining = end_at - time.monotonic()
                if remaining <= 0:
                    break
                self._idle_condition.wait(timeout=remaining)

    def _compute_scheduled_slot(self, settings: AppSettings) -> datetime:
        """Infer the scheduled slot from the current cron within the grace window."""
        from apscheduler.triggers.cron import CronTrigger

        timezone = ZoneInfo(settings.timezone)
        now_local = datetime.now(timezone).replace(second=0, microsecond=0)
        trigger = CronTrigger.from_crontab(settings.schedule_cron, timezone=settings.timezone)
        for minutes_back in range(0, 16):
            candidate = now_local - timedelta(minutes=minutes_back)
            previous = candidate - timedelta(minutes=1)
            if trigger.get_next_fire_time(previous, previous) == candidate:
                return candidate.astimezone(UTC)
        return now_local.astimezone(UTC)

    def _mark_destination_outcome(self, destination: ServiceDestination, statuses: set[str], *, count_toward_breaker: bool) -> None:
        if not count_toward_breaker:
            return
        if "success" in statuses:
            destination.consecutive_permanent_failures = 0
            destination.auto_disabled_at = None
            destination.disable_reason = None
            return
        if statuses == {"permanent_failure"}:
            destination.consecutive_permanent_failures += 1
            if destination.consecutive_permanent_failures >= 5:
                destination.enabled = False
                destination.auto_disabled_at = time_to_datetime()
                destination.disable_reason = "Automatically disabled after 5 consecutive permanent failures."

    def run(
        self,
        *,
        trigger: RunTrigger,
        initiated_by: str | None = None,
        initiated_by_user_id: int | None = None,
        acting_as_admin: bool = False,
        destination_id: int | None = None,
        scheduled_slot: datetime | None = None,
    ) -> dict[str, Any]:
        """Execute one full or filtered delivery run."""
        if not self._begin_run():
            return {"status": "failed", "error_message": "Another delivery run is already in progress or shutdown is in progress.", "run_id": None, "summary": {}}
        try:
            with session_scope(self._session_factory) as session:
                settings = get_settings(session, self._config)
                if trigger == RunTrigger.SCHEDULED and scheduled_slot is None:
                    scheduled_slot = self._compute_scheduled_slot(settings)
                if trigger == RunTrigger.SCHEDULED and self._config.ha_enabled:
                    lock_key = f"wednesday-frog:schedule:{scheduled_slot.isoformat()}"
                    acquired = self._coordinator.acquire(lock_key, 900)
                    self._metrics.record_lock_outcome("acquired" if acquired else "skipped")
                    if not acquired:
                        return {"run_id": None, "status": "skipped", "error_message": "Another node already owns this scheduled slot.", "summary": {}}

                run = DeliveryRun(
                    trigger_kind=trigger.value,
                    initiated_by=initiated_by,
                    initiated_by_user_id=initiated_by_user_id,
                    scheduled_slot=scheduled_slot if trigger == RunTrigger.SCHEDULED else None,
                    status="running",
                    summary_json={},
                )
                session.add(run)
                try:
                    session.flush()
                except IntegrityError:
                    if trigger == RunTrigger.SCHEDULED:
                        return {"run_id": None, "status": "skipped", "error_message": "This scheduled slot has already been recorded.", "summary": {}}
                    raise

                settings, asset, fallback_active, fallback_warning = self._prepare_asset(session)
                query = select(ServiceDestination).order_by(ServiceDestination.id.asc())
                if destination_id is not None:
                    query = query.where(ServiceDestination.id == destination_id)
                else:
                    query = query.where(ServiceDestination.enabled.is_(True))
                if initiated_by_user_id is not None and not acting_as_admin:
                    query = query.where(ServiceDestination.owner_user_id == initiated_by_user_id)
                destinations = list(session.scalars(query))
                if not destinations:
                    run.status = "failed"
                    if destination_id is not None:
                        run.error_message = _truncate_text("No accessible destination matched the requested id.", limit=MAX_ERROR_MESSAGE_LENGTH)
                    else:
                        run.error_message = _truncate_text("No matching destinations are configured.", limit=MAX_ERROR_MESSAGE_LENGTH)
                    run.finished_at = run.started_at
                    increment_metric_counter(session, metric_name=RUN_COUNTER_METRIC, label_primary=run.status)
                    return {"run_id": run.id, "status": run.status, "error_message": run.error_message, "summary": {}}
                counts: Counter[str] = Counter()
                attempt_metric_counts: Counter[tuple[str, str]] = Counter()
                if fallback_warning:
                    counts["fallback_asset"] += 1
                for destination in destinations:
                    plugin = self._plugin_manager.get(destination.plugin_id)
                    channels = [channel for channel in destination.channels if channel.enabled or destination_id is not None]
                    destination_statuses: set[str] = set()
                    if plugin is None:
                        attempts = _channel_attempts_for_validation(run.id, destination, channels, f"Plugin '{destination.plugin_id}' is unavailable.")
                        for attempt in attempts:
                            session.add(attempt)
                            counts[attempt.status] += 1
                            attempt_metric_counts[(destination.plugin_id, attempt.status)] += 1
                            destination_statuses.add(attempt.status)
                        self._mark_destination_outcome(destination, destination_statuses, count_toward_breaker=trigger != RunTrigger.TEST and destination_id is None)
                        continue
                    validation_context = PluginValidationContext(
                        session=session,
                        destination=destination,
                        secret_manager=self._secret_manager,
                        asset=asset,
                    )
                    try:
                        issues = [issue.message for issue in plugin.connector.validate_config(validation_context)]
                    except Exception as exc:  # noqa: BLE001
                        LOGGER.warning("Plugin validation crashed for plugin_id=%s destination_id=%s: %s", destination.plugin_id, destination.id, exc)
                        issues = [f"Plugin validation crashed: {exc}"]
                    if issues:
                        attempts = _channel_attempts_for_validation(run.id, destination, channels, "; ".join(issues))
                        for attempt in attempts:
                            session.add(attempt)
                            counts[attempt.status] += 1
                            attempt_metric_counts[(destination.plugin_id, attempt.status)] += 1
                            destination_statuses.add(attempt.status)
                        self._mark_destination_outcome(destination, destination_statuses, count_toward_breaker=trigger != RunTrigger.TEST and destination_id is None)
                        continue
                    for channel in channels:
                        attempt = DeliveryAttempt(
                            run_id=run.id,
                            destination_id=destination.id,
                            channel_id=channel.id,
                            plugin_id=destination.plugin_id,
                            status="running",
                        )
                        session.add(attempt)
                        session.flush()

                        send_context = PluginSendContext(
                            session=session,
                            destination=destination,
                            channel=channel,
                            asset=asset,
                            caption=settings.caption_text,
                            secret_manager=self._secret_manager,
                            http_client=self._http_client,
                        )
                        error_context = PluginErrorContext(destination=destination, channel=channel, operation="send_payload")
                        attempt_index, result = self._send_with_retry(
                            lambda context=send_context, connector=plugin.connector: connector.send_payload(context),
                            lambda exc, connector=plugin.connector, context=error_context: connector.handle_error(context, exc),
                        )
                        attempt.attempt_index = attempt_index
                        attempt.status = result["status"]
                        attempt.response_excerpt = result["response_excerpt"]
                        attempt.error_message = result["error_message"]
                        attempt.finished_at = time_to_datetime()
                        counts[attempt.status] += 1
                        attempt_metric_counts[(destination.plugin_id, attempt.status)] += 1
                        destination_statuses.add(attempt.status)
                    self._mark_destination_outcome(destination, destination_statuses, count_toward_breaker=trigger != RunTrigger.TEST and destination_id is None)

                success_count = counts["success"]
                failure_count = counts["permanent_failure"] + counts["retryable_failure"]
                if success_count and not failure_count:
                    run.status = "succeeded"
                elif success_count and failure_count:
                    run.status = "partial_success"
                else:
                    run.status = "failed"
                run.summary_json = {
                    "success": counts["success"],
                    "retryable_failure": counts["retryable_failure"],
                    "permanent_failure": counts["permanent_failure"],
                    "total_attempts": success_count + failure_count,
                    "fallback_asset": counts["fallback_asset"],
                }
                run.finished_at = time_to_datetime()
                increment_metric_counter(session, metric_name=RUN_COUNTER_METRIC, label_primary=run.status)
                for (plugin_id, status), count in attempt_metric_counts.items():
                    increment_metric_counter(
                        session,
                        metric_name=ATTEMPT_COUNTER_METRIC,
                        label_primary=plugin_id,
                        label_secondary=status,
                        amount=count,
                    )
                return {"run_id": run.id, "status": run.status, "error_message": run.error_message, "summary": run.summary_json}
        finally:
            self._end_run()


def list_recent_runs(session: Session, *, limit: int = 20, user: AdminUser | None = None) -> list[DeliveryRun]:
    """Return the most recent runs visible to the supplied user."""
    query = (
        select(DeliveryRun)
        .options(
            load_only(
                DeliveryRun.id,
                DeliveryRun.trigger_kind,
                DeliveryRun.status,
                DeliveryRun.initiated_by,
                DeliveryRun.initiated_by_user_id,
                DeliveryRun.summary_json,
                DeliveryRun.error_message,
                DeliveryRun.started_at,
                DeliveryRun.finished_at,
            )
        )
        .order_by(DeliveryRun.id.desc())
        .limit(limit)
    )
    if user is None or is_admin_user(user):
        return list(session.scalars(query))
    owned_destination_ids = select(ServiceDestination.id).where(ServiceDestination.owner_user_id == user.id)
    query = query.where(
        or_(
            DeliveryRun.initiated_by_user_id == user.id,
            DeliveryRun.attempts.any(DeliveryAttempt.destination_id.in_(owned_destination_ids)),
        )
    )
    return list(session.scalars(query))


def list_attempts_for_runs(
    session: Session,
    runs: list[DeliveryRun],
    *,
    user: AdminUser | None = None,
) -> list[DeliveryAttempt]:
    """Return attempts for the supplied runs, scoped to the supplied user when needed."""
    run_ids = {run.id for run in runs}
    if not run_ids:
        return []
    query = (
        select(DeliveryAttempt)
        .options(
            load_only(
                DeliveryAttempt.id,
                DeliveryAttempt.run_id,
                DeliveryAttempt.destination_id,
                DeliveryAttempt.channel_id,
                DeliveryAttempt.plugin_id,
                DeliveryAttempt.status,
                DeliveryAttempt.attempt_index,
                DeliveryAttempt.response_excerpt,
                DeliveryAttempt.error_message,
                DeliveryAttempt.started_at,
                DeliveryAttempt.finished_at,
            )
        )
        .where(DeliveryAttempt.run_id.in_(run_ids))
        .order_by(DeliveryAttempt.id.desc())
    )
    if user is None or is_admin_user(user):
        return list(session.scalars(query))
    owned_destination_ids = select(ServiceDestination.id).where(ServiceDestination.owner_user_id == user.id)
    query = query.where(
        or_(
            DeliveryAttempt.destination_id.in_(owned_destination_ids),
            DeliveryAttempt.run.has(DeliveryRun.initiated_by_user_id == user.id),
        )
    )
    return list(session.scalars(query))


def prune_history(session: Session, *, days: int, batch_size: int = 500) -> dict[str, int]:
    """Delete delivery history older than the supplied number of days."""
    cutoff = time_to_datetime() - timedelta(days=days)
    deleted_runs = 0
    deleted_attempts = 0
    while True:
        run_ids = list(
            session.scalars(
                select(DeliveryRun.id)
                .where(
                    or_(
                        DeliveryRun.finished_at < cutoff,
                        DeliveryRun.finished_at.is_(None) & (DeliveryRun.started_at < cutoff),
                    )
                )
                .order_by(DeliveryRun.id.asc())
                .limit(batch_size)
            )
        )
        if not run_ids:
            break
        deleted_attempts += session.execute(delete(DeliveryAttempt).where(DeliveryAttempt.run_id.in_(run_ids))).rowcount or 0
        deleted_runs += session.execute(delete(DeliveryRun).where(DeliveryRun.id.in_(run_ids))).rowcount or 0
        session.flush()
    return {"runs_deleted": deleted_runs, "attempts_deleted": deleted_attempts}
