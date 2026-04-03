"""Database models for the Wednesday Frog application."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    """Return an aware UTC timestamp for database defaults."""
    return datetime.now(UTC)


class Base(DeclarativeBase):
    """Shared SQLAlchemy declarative base."""


class RunTrigger(str, Enum):
    """Supported delivery triggers."""

    MANUAL = "manual"
    SCHEDULED = "scheduled"
    TEST = "test"


class UserRole(str, Enum):
    """Supported local user roles."""

    ADMIN = "admin"
    STANDARD = "standard"


class AdminUser(Base):
    """Local user account."""

    __tablename__ = "admin_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    role: Mapped[str] = mapped_column(String(32), default=UserRole.STANDARD.value, nullable=False)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    destinations: Mapped[list["ServiceDestination"]] = relationship(back_populates="owner", cascade="all, delete-orphan")
    initiated_runs: Mapped[list["DeliveryRun"]] = relationship(back_populates="initiated_by_user")

    @property
    def is_admin(self) -> bool:
        """Return whether the user has admin privileges."""
        return self.role == UserRole.ADMIN.value


class AssetRecord(Base):
    """Uploaded or bundled image metadata."""

    __tablename__ = "asset_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    original_filename: Mapped[str] = mapped_column(String(255), nullable=False)
    stored_filename: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    media_type: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    is_default: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    processing_status: Mapped[str] = mapped_column(String(32), default="ready", nullable=False)
    processing_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class AppSettings(Base):
    """Singleton application settings row."""

    __tablename__ = "app_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    timezone: Mapped[str] = mapped_column(String(128), default="UTC", nullable=False)
    schedule_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    schedule_cron: Mapped[str] = mapped_column(String(64), default="0 12 * * 3", nullable=False)
    caption_text: Mapped[str] = mapped_column(Text, default="", nullable=False)
    active_asset_id: Mapped[int | None] = mapped_column(ForeignKey("asset_records.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    active_asset: Mapped[AssetRecord | None] = relationship()


class ServiceDestination(Base):
    """Plugin-level configuration for one provider."""

    __tablename__ = "service_destinations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    owner_user_id: Mapped[int | None] = mapped_column(ForeignKey("admin_users.id"), nullable=True)
    plugin_id: Mapped[str] = mapped_column("service_type", String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    config_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    consecutive_permanent_failures: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    auto_disabled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    disable_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    channels: Mapped[list["DestinationChannel"]] = relationship(
        back_populates="destination",
        cascade="all, delete-orphan",
        order_by="DestinationChannel.id",
    )
    secrets: Mapped[list["EncryptedSecret"]] = relationship(back_populates="destination", cascade="all, delete-orphan")
    owner: Mapped[AdminUser | None] = relationship(back_populates="destinations")

    @property
    def service_type(self) -> str:
        """Compatibility alias for older templates and tests."""
        return self.plugin_id


class DestinationChannel(Base):
    """Channel or webhook target for a destination."""

    __tablename__ = "destination_channels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    destination_id: Mapped[int] = mapped_column(ForeignKey("service_destinations.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    config_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    destination: Mapped[ServiceDestination] = relationship(back_populates="channels")
    secrets: Mapped[list["EncryptedSecret"]] = relationship(back_populates="channel", cascade="all, delete-orphan")


class EncryptedSecret(Base):
    """Encrypted secret values for destinations and channels."""

    __tablename__ = "encrypted_secrets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    destination_id: Mapped[int | None] = mapped_column(ForeignKey("service_destinations.id"), nullable=True)
    channel_id: Mapped[int | None] = mapped_column(ForeignKey("destination_channels.id"), nullable=True)
    secret_key: Mapped[str] = mapped_column(String(128), nullable=False)
    label: Mapped[str] = mapped_column(String(255), nullable=False)
    ciphertext: Mapped[str] = mapped_column(Text, nullable=False)
    nonce: Mapped[str] = mapped_column(String(255), nullable=False)
    last_four: Mapped[str] = mapped_column(String(8), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    destination: Mapped[ServiceDestination | None] = relationship(back_populates="secrets")
    channel: Mapped[DestinationChannel | None] = relationship(back_populates="secrets")


class DeliveryRun(Base):
    """One delivery run across one or more destinations."""

    __tablename__ = "delivery_runs"
    __table_args__ = (UniqueConstraint("trigger_kind", "scheduled_slot", name="uq_delivery_runs_trigger_slot"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trigger_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    scheduled_slot: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="running")
    initiated_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    initiated_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("admin_users.id"), nullable=True)
    summary_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    attempts: Mapped[list["DeliveryAttempt"]] = relationship(
        back_populates="run",
        cascade="all, delete-orphan",
        order_by="DeliveryAttempt.id",
    )
    initiated_by_user: Mapped[AdminUser | None] = relationship(back_populates="initiated_runs")


class DeliveryAttempt(Base):
    """One delivery attempt for one destination and channel."""

    __tablename__ = "delivery_attempts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("delivery_runs.id"), nullable=False)
    destination_id: Mapped[int | None] = mapped_column(ForeignKey("service_destinations.id"), nullable=True)
    channel_id: Mapped[int | None] = mapped_column(ForeignKey("destination_channels.id"), nullable=True)
    plugin_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempt_index: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    response_excerpt: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    run: Mapped[DeliveryRun] = relationship(back_populates="attempts")
