"""Shared test helpers."""

from __future__ import annotations

from pathlib import Path
import re

import pytest
from fastapi.testclient import TestClient

from wednesday_frog.config import AppConfig
from wednesday_frog.db import create_session_factory
from wednesday_frog.web import create_app


def extract_csrf(html: str) -> str:
    """Extract a CSRF token from a rendered HTML form."""
    match = re.search(r'name="csrf_token" value="([^"]+)"', html)
    assert match, "CSRF token missing from page"
    return match.group(1)


@pytest.fixture()
def app_config(tmp_path: Path) -> AppConfig:
    """Build an isolated test configuration."""
    repo_root = Path(__file__).resolve().parents[1]
    data_dir = tmp_path / "data"
    return AppConfig(
        database_url=f"sqlite:///{tmp_path / 'wednesday_frog.db'}",
        master_key="test-master-key-which-is-definitely-32-chars",
        previous_master_key=None,
        session_secret="test-session-secret-which-is-definitely-32",
        setup_token="test-setup-token-which-is-definitely-32",
        metrics_token="metrics-token-which-is-definitely-32",
        timezone_env="UTC",
        scheduler_disabled=True,
        redis_url=None,
        outbound_allowlist=(),
        shutdown_grace_seconds=60,
        repo_root=repo_root,
        template_dir=repo_root / "templates",
        static_dir=repo_root / "static",
        data_dir=data_dir,
        assets_dir=data_dir / "assets",
        logs_dir=data_dir / "logs",
    )


@pytest.fixture()
def session_factory(app_config: AppConfig):
    """Create a database session factory for a test config."""
    app_config.ensure_runtime_dirs()
    return create_session_factory(app_config)


@pytest.fixture()
def client(app_config: AppConfig):
    """Create a TestClient with lifespan hooks enabled."""
    with TestClient(create_app(app_config)) as test_client:
        yield test_client


def bootstrap_admin(client: TestClient) -> None:
    """Create the first admin account through the setup flow."""
    response = client.get("/setup")
    csrf = extract_csrf(response.text)
    client.post(
        "/setup",
        data={
            "csrf_token": csrf,
            "setup_token": "test-setup-token-which-is-definitely-32",
            "username": "admin",
            "password": "secret-password",
        },
        follow_redirects=False,
    )


def login_user(client: TestClient, username: str, password: str) -> None:
    """Sign a user in through the login form."""
    response = client.get("/login")
    csrf = extract_csrf(response.text)
    client.post(
        "/login",
        data={
            "csrf_token": csrf,
            "username": username,
            "password": password,
        },
        follow_redirects=False,
    )
