"""Database engine and session helpers."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .config import AppConfig
from .models import Base


def _configure_sqlite(engine: Engine) -> None:
    """Apply SQLite pragmas tuned for local concurrent writes."""

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, connection_record):  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA busy_timeout=5000;")
        cursor.execute("PRAGMA synchronous=NORMAL;")
        cursor.execute("PRAGMA temp_store=FILE;")
        cursor.execute("PRAGMA cache_size=-8192;")
        cursor.execute("PRAGMA wal_autocheckpoint=1000;")
        cursor.execute("PRAGMA journal_size_limit=67108864;")
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()


def create_session_factory(config: AppConfig) -> sessionmaker[Session]:
    """Create a SQLAlchemy session factory for the configured database."""
    is_sqlite = config.database_url.startswith("sqlite")
    connect_args: dict[str, object] = {}
    if is_sqlite:
        connect_args["check_same_thread"] = False
    engine = create_engine(
        config.database_url,
        future=True,
        connect_args=connect_args,
        pool_pre_ping=not is_sqlite,
    )
    if is_sqlite:
        _configure_sqlite(engine)
    Base.metadata.create_all(engine)
    _migrate_legacy_schema(engine)
    _ensure_supporting_indexes(engine)
    return sessionmaker(engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _migrate_legacy_schema(engine: Engine) -> None:
    """Apply lightweight in-place upgrades for existing installs."""
    inspector = inspect(engine)
    with engine.begin() as connection:
        admin_columns = {column["name"] for column in inspector.get_columns("admin_users")}
        if "role" not in admin_columns:
            connection.execute(text("ALTER TABLE admin_users ADD COLUMN role VARCHAR(32) NOT NULL DEFAULT 'standard'"))
            connection.execute(text("UPDATE admin_users SET role = 'admin' WHERE id = (SELECT MIN(id) FROM admin_users)"))

        destination_columns = {column["name"] for column in inspector.get_columns("service_destinations")}
        if "owner_user_id" not in destination_columns:
            connection.execute(text("ALTER TABLE service_destinations ADD COLUMN owner_user_id INTEGER"))
            connection.execute(
                text(
                    "UPDATE service_destinations SET owner_user_id = "
                    "(SELECT MIN(id) FROM admin_users) WHERE owner_user_id IS NULL"
                )
            )

        run_columns = {column["name"] for column in inspector.get_columns("delivery_runs")}
        if "initiated_by_user_id" not in run_columns:
            connection.execute(text("ALTER TABLE delivery_runs ADD COLUMN initiated_by_user_id INTEGER"))


def _ensure_supporting_indexes(engine: Engine) -> None:
    """Create indexes for the app's hottest read paths when missing."""
    statements = (
        "CREATE INDEX IF NOT EXISTS idx_service_destinations_owner_user_id ON service_destinations(owner_user_id)",
        "CREATE INDEX IF NOT EXISTS idx_destination_channels_destination_id ON destination_channels(destination_id)",
        "CREATE INDEX IF NOT EXISTS idx_encrypted_secrets_destination_secret_key ON encrypted_secrets(destination_id, secret_key)",
        "CREATE INDEX IF NOT EXISTS idx_encrypted_secrets_channel_secret_key ON encrypted_secrets(channel_id, secret_key)",
        "CREATE INDEX IF NOT EXISTS idx_delivery_runs_initiated_by_user_id ON delivery_runs(initiated_by_user_id)",
        "CREATE INDEX IF NOT EXISTS idx_delivery_runs_started_at ON delivery_runs(started_at)",
        "CREATE INDEX IF NOT EXISTS idx_delivery_attempts_run_id ON delivery_attempts(run_id)",
        "CREATE INDEX IF NOT EXISTS idx_delivery_attempts_destination_id ON delivery_attempts(destination_id)",
    )
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


@contextmanager
def session_scope(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    """Yield a session and commit or roll back around the block."""
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
