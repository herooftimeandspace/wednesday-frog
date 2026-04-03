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
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()


def create_session_factory(config: AppConfig) -> sessionmaker[Session]:
    """Create a SQLAlchemy session factory for the configured database."""
    connect_args: dict[str, object] = {}
    if config.database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    engine = create_engine(
        config.database_url,
        future=True,
        connect_args=connect_args,
        pool_pre_ping=True,
    )
    if config.database_url.startswith("sqlite"):
        _configure_sqlite(engine)
    Base.metadata.create_all(engine)
    _migrate_legacy_schema(engine)
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
