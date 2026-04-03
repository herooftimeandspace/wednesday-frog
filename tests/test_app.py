"""Application tests for Wednesday Frog."""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import select

from wednesday_frog.assets import store_uploaded_asset
from wednesday_frog.config import AppConfig
from wednesday_frog.db import session_scope
from wednesday_frog.delivery.base import AdapterResult, ValidationIssue
from wednesday_frog.http_client import OutboundHttpClient
from wednesday_frog.metrics import MetricsCollector
from wednesday_frog.models import AppSettings, AssetRecord, RunTrigger, ServiceDestination, UserRole
from wednesday_frog.plugins import FrogConnector, LoadedPlugin, PluginErrorContext, PluginManager, PluginSendContext, PluginValidationContext
from wednesday_frog.security import PasswordManager, SecretManager
from wednesday_frog.services import (
    DeliveryManager,
    add_channel,
    build_plugin_manager,
    create_admin_user,
    create_destination,
    create_user,
    describe_secret_state,
    ensure_defaults,
    get_user_by_username,
    get_secret_value,
    resolve_active_asset,
    set_secret_value,
    validate_all_destinations,
)
from wednesday_frog.web import create_app, describe_cron_schedule, humanize_timezone_name, parse_schedule_time_input, timezone_options

from conftest import bootstrap_admin, extract_csrf, login_user


def seed_user(session, username: str = "owner", *, role: str = UserRole.ADMIN.value):
    if role == UserRole.ADMIN.value:
        return create_admin_user(session, username, "secret-password", PasswordManager())
    return create_user(session, username, "secret-password", PasswordManager(), role=role)


class PassingPlugin(FrogConnector):
    plugin_id = "slack"
    display_name = "Slack"

    def validate_config(self, context: PluginValidationContext):
        return []

    def send_payload(self, context: PluginSendContext):
        return AdapterResult(status="success", response_excerpt="ok")

    def destination_config_schema(self) -> dict:
        return {"type": "object", "properties": {}}

    def destination_secret_schema(self) -> dict:
        return {"type": "object", "properties": {}}

    def channel_config_schema(self) -> dict:
        return {"type": "object", "properties": {}}

    def channel_secret_schema(self) -> dict:
        return {"type": "object", "properties": {}}


class PermanentFailurePlugin(PassingPlugin):
    plugin_id = "discord"
    display_name = "Discord"

    def send_payload(self, context: PluginSendContext):
        return AdapterResult(status="permanent_failure", error_message="boom")

    def handle_error(self, context: PluginErrorContext, exc: Exception):
        return AdapterResult(status="permanent_failure", error_message=str(exc))


def test_defaults_seed_timezone_and_schedule(app_config, session_factory):
    with session_scope(session_factory) as session:
        settings = ensure_defaults(session, app_config)
        assert settings.timezone == "UTC"
        assert settings.schedule_enabled is True
        assert settings.schedule_cron == "0 12 * * 3"


def test_timezone_options_include_utc_and_common_regions():
    options = timezone_options()
    assert options[0] == "UTC"
    assert "America/Los_Angeles" in options
    assert "Europe/London" in options
    assert "Asia/Kolkata" in options
    assert len(options) > 100


def test_schedule_summary_humanizes_default_cron():
    assert describe_cron_schedule("0 12 * * 3", "America/Los_Angeles") == "Every Wednesday at 12:00 in America/Los Angeles"
    assert describe_cron_schedule("15 9 * * 1", "America/Los_Angeles") == "Every Wednesday at 09:15 in America/Los Angeles"
    assert humanize_timezone_name("America/Los_Angeles") == "America/Los Angeles"


def test_schedule_time_parser_accepts_12_and_24_hour_input():
    assert parse_schedule_time_input("9:05 AM") == (9, 5)
    assert parse_schedule_time_input("9:05 pm") == (21, 5)
    assert parse_schedule_time_input("21:05") == (21, 5)
    assert parse_schedule_time_input("25:05") is None


def test_from_env_prefers_working_directory_for_app_root(monkeypatch, app_config):
    monkeypatch.chdir(app_config.repo_root)
    monkeypatch.delenv("WEDNESDAY_FROG_APP_ROOT", raising=False)
    monkeypatch.setenv("WEDNESDAY_FROG_MASTER_KEY", "runtime-master-key-that-is-long-enough")
    monkeypatch.setenv("WEDNESDAY_FROG_SESSION_SECRET", "runtime-session-key-that-is-long-enough")
    monkeypatch.setenv("WEDNESDAY_FROG_SETUP_TOKEN", "runtime-setup-token-that-is-long-enough")
    config = AppConfig.from_env()
    assert config.repo_root == app_config.repo_root
    assert config.template_dir.is_dir()
    assert config.static_dir.is_dir()
    assert config.bundled_asset_path.is_file()


def test_secret_storage_round_trips_and_masks(app_config, session_factory):
    secret_manager = SecretManager(app_config.master_key)
    with session_scope(session_factory) as session:
        ensure_defaults(session, app_config)
        owner = seed_user(session)
        destination = create_destination(session, owner=owner, plugin_id="slack", name="Slack test")
        set_secret_value(
            session,
            secret_manager=secret_manager,
            destination=destination,
            secret_key="bot_token",
            label="Bot token",
            value="xoxb-example-token-1234",
        )
        assert get_secret_value(session, destination=destination, secret_key="bot_token", secret_manager=secret_manager) == "xoxb-example-token-1234"
        assert describe_secret_state(session, destination=destination, secret_key="bot_token")["label"].endswith("1234")


def test_plugin_manager_loads_builtins_and_isolates_broken_plugin(app_config, tmp_path: Path):
    plugins_root = tmp_path / "plugins"
    broken = plugins_root / "broken"
    broken.mkdir(parents=True)
    (broken / "plugin.py").write_text("broken = True\n", encoding="utf-8")
    (broken / "manifest.json").write_text('{"plugin_id":"broken","display_name":"Broken"}', encoding="utf-8")
    manager = PluginManager([plugins_root, app_config.package_plugins_dir])
    assert manager.get("slack") is not None
    assert any(item.plugin_id == "broken" for item in manager.failures())


def test_setup_flow_allows_login_and_dashboard(client: TestClient):
    bootstrap_admin(client)
    response = client.get("/", follow_redirects=True)
    assert response.status_code == 200
    assert "Destination readiness" in response.text


def test_dashboard_renders_active_asset_preview_and_asset_route(client: TestClient):
    bootstrap_admin(client)
    response = client.get("/")
    assert response.status_code == 200
    assert 'class="asset-preview-image"' in response.text
    assert 'src="/assets/' in response.text
    assert 'class="result-box dashboard-result-box"' in response.text
    asset_response = client.get("/assets/1")
    assert asset_response.status_code == 200
    assert asset_response.headers["content-type"].startswith("image/")


def test_every_page_footer_includes_attribution_and_kofi_widget(client: TestClient):
    response = client.get("/setup")
    assert response.status_code == 200
    assert "github.com/herooftimeandspace created this app." in response.text
    assert "https://storage.ko-fi.com/cdn/widget/Widget_2.js" in response.text
    assert "Support me on Ko-fi" in response.text
    assert "storage.ko-fi.com" in response.headers["content-security-policy"]


def test_standard_user_is_scoped_to_own_destinations_and_blocked_from_admin_pages(client: TestClient, session_factory):
    bootstrap_admin(client)
    users_page = client.get("/users/new")
    csrf = extract_csrf(users_page.text)
    client.post(
        "/users",
        data={
            "csrf_token": csrf,
            "username": "alice",
            "password": "secret-password",
        },
        follow_redirects=False,
    )
    with session_scope(session_factory) as session:
        admin = get_user_by_username(session, "admin")
        alice = get_user_by_username(session, "alice")
        assert admin is not None
        assert alice is not None
        admin_destination = create_destination(session, owner=admin, plugin_id="slack", name="Admin route")
        user_destination = create_destination(session, owner=alice, plugin_id="discord", name="Alice route")
        add_channel(session, admin_destination, name="admins", enabled=True, config_values={})
        add_channel(session, user_destination, name="alice", enabled=True, config_values={})

    logout_csrf = extract_csrf(client.get("/").text)
    client.post("/logout", data={"csrf_token": logout_csrf}, follow_redirects=False)
    login_user(client, "alice", "secret-password")

    destinations = client.get("/destinations")
    assert "Alice route" in destinations.text
    assert "Admin route" not in destinations.text

    settings = client.get("/settings", follow_redirects=True)
    assert "Admin access is required for that page." in settings.text

    users = client.get("/users", follow_redirects=True)
    assert "Admin access is required for that page." in users.text


def test_account_password_change_and_admin_user_edit_flow(client: TestClient, session_factory):
    bootstrap_admin(client)
    users_page = client.get("/users/new")
    csrf = extract_csrf(users_page.text)
    create_response = client.post(
        "/users",
        data={
            "csrf_token": csrf,
            "username": "bob",
            "password": "secret-password",
        },
        follow_redirects=False,
    )
    assert create_response.status_code == 303
    with session_scope(session_factory) as session:
        bob = get_user_by_username(session, "bob")
        assert bob is not None
        bob_id = bob.id

    edit_page = client.get(f"/users/{bob_id}")
    edit_csrf = extract_csrf(edit_page.text)
    client.post(
        f"/users/{bob_id}",
        data={
            "csrf_token": edit_csrf,
            "username": "robert",
            "role": "standard",
            "new_password": "",
        },
        follow_redirects=False,
    )

    logout_csrf = extract_csrf(client.get("/").text)
    client.post("/logout", data={"csrf_token": logout_csrf}, follow_redirects=False)
    login_user(client, "robert", "secret-password")

    account_page = client.get("/account")
    account_csrf = extract_csrf(account_page.text)
    password_response = client.post(
        "/account/password",
        data={
            "csrf_token": account_csrf,
            "current_password": "secret-password",
            "new_password": "new-secret-password",
            "confirm_password": "new-secret-password",
        },
        follow_redirects=False,
    )
    assert password_response.status_code == 303

    logout_csrf = extract_csrf(client.get("/account").text)
    client.post("/logout", data={"csrf_token": logout_csrf}, follow_redirects=False)
    login_user(client, "robert", "new-secret-password")
    assert client.get("/account").status_code == 200


def test_api_run_requires_csrf(client: TestClient):
    bootstrap_admin(client)
    response = client.post("/api/v1/runs")
    assert response.status_code == 403


def test_settings_page_renders_full_timezone_dropdown(client: TestClient):
    bootstrap_admin(client)
    response = client.get("/settings")
    assert response.status_code == 200
    assert '<select' in response.text
    assert 'name="timezone"' in response.text
    assert 'data-auto-submit-on-change="true"' in response.text
    assert "America/Los_Angeles" in response.text
    assert "Asia/Kolkata" in response.text
    assert "Current schedule is" in response.text
    assert "Every Wednesday at 12:00 in UTC" in response.text
    assert 'name="schedule_hour"' in response.text
    assert 'name="schedule_minute"' in response.text
    assert 'name="schedule_time_text"' in response.text
    assert 'name="schedule_enabled"' in response.text
    assert 'name="schedule_cron"' not in response.text
    assert "Every Wednesday" in response.text
    assert 'class="settings-asset-preview"' in response.text


def test_settings_rejects_unknown_timezone(client: TestClient):
    bootstrap_admin(client)
    settings_page = client.get("/settings")
    csrf = extract_csrf(settings_page.text)
    response = client.post(
        "/settings",
        data={
            "csrf_token": csrf,
            "timezone": "Mars/Olympus_Mons",
            "schedule_hour": "12",
            "schedule_minute": "0",
            "schedule_time_text": "12:00",
            "schedule_enabled": "on",
            "caption_text": "",
            "asset_id": "1",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    follow = client.get("/settings")
    assert "Choose a valid timezone from the list." in follow.text


def test_settings_upload_creates_pending_asset(client: TestClient, app_config, session_factory):
    bootstrap_admin(client)
    settings_page = client.get("/settings")
    csrf = extract_csrf(settings_page.text)
    payload = (app_config.repo_root / "wednesday-frog.png").read_bytes()
    response = client.post(
        "/settings",
        data={
            "csrf_token": csrf,
            "timezone": "UTC",
            "schedule_hour": "12",
            "schedule_minute": "0",
            "schedule_time_text": "12:00",
            "schedule_enabled": "on",
            "caption_text": "ribbit",
            "asset_id": "1",
        },
        files={"asset_file": ("frog-copy.png", payload, "image/png")},
        follow_redirects=False,
    )
    assert response.status_code == 303
    with session_scope(session_factory) as session:
        settings = session.get(AppSettings, 1)
        assets = list(session.query(AssetRecord).all())
        assert settings is not None
        assert len(assets) >= 2
        assert any(asset.processing_status in {"pending", "ready", "failed"} for asset in assets)


def test_settings_manual_time_normalizes_to_wednesday_schedule(client: TestClient, session_factory):
    bootstrap_admin(client)
    settings_page = client.get("/settings")
    csrf = extract_csrf(settings_page.text)
    response = client.post(
        "/settings",
        data={
            "csrf_token": csrf,
            "timezone": "America/Los_Angeles",
            "schedule_hour": "12",
            "schedule_minute": "0",
            "schedule_time_text": "9:05 PM",
            "schedule_enabled": "on",
            "caption_text": "",
            "asset_id": "1",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    with session_scope(session_factory) as session:
        settings = session.get(AppSettings, 1)
        assert settings is not None
        assert settings.schedule_cron == "5 21 * * 3"
        assert settings.timezone == "America/Los_Angeles"


def test_validate_config_reports_missing_destination_secret(app_config, session_factory):
    secret_manager = SecretManager(app_config.master_key)
    plugin_manager = build_plugin_manager(app_config)
    with session_scope(session_factory) as session:
        ensure_defaults(session, app_config)
        owner = seed_user(session)
        destination = create_destination(session, owner=owner, plugin_id="slack", name="Slack prod")
        add_channel(session, destination, name="general", enabled=True, config_values={"channel_id": "C123"})
        validation = validate_all_destinations(session, app_config, secret_manager, plugin_manager)
        slack_result = next(item for item in validation["destinations"] if item["id"] == destination.id)
        assert any("bot token" in issue.lower() for issue in slack_result["issues"])


def test_metrics_requires_token(client: TestClient):
    bootstrap_admin(client)
    unauth = client.get("/metrics")
    assert unauth.status_code == 401
    ok = client.get("/metrics", headers={"X-Metrics-Token": "metrics-token-which-is-definitely-32"})
    assert ok.status_code == 200
    assert "wednesday_frog_plugin_failures" in ok.text


def test_check_report_emits_plugin_env(app_config):
    report = build_plugin_manager(app_config).check_report(emit_plugin_env="slack")
    assert report["ok"] is True
    assert "emit_plugin_env" in report
    assert "WEDNESDAY_FROG_MASTER_KEY" in "\n".join(report["emit_plugin_env"]["env"])


def test_delivery_manager_records_partial_success_and_auto_disables_after_threshold(app_config, session_factory):
    secret_manager = SecretManager(app_config.master_key)
    plugin_manager = build_plugin_manager(app_config)
    slack_manifest = plugin_manager.get("slack").manifest  # type: ignore[union-attr]
    discord_manifest = plugin_manager.get("discord").manifest  # type: ignore[union-attr]
    plugin_manager._plugins["slack"] = LoadedPlugin(manifest=slack_manifest, connector=PassingPlugin())
    plugin_manager._plugins["discord"] = LoadedPlugin(manifest=discord_manifest, connector=PermanentFailurePlugin())

    with session_scope(session_factory) as session:
        ensure_defaults(session, app_config)
        owner = seed_user(session)
        slack = create_destination(session, owner=owner, plugin_id="slack", name="Slack ok")
        discord = create_destination(session, owner=owner, plugin_id="discord", name="Discord fail")
        add_channel(session, slack, name="general", enabled=True, config_values={"channel_id": "C123"})
        add_channel(session, discord, name="alerts", enabled=True, config_values={})

    http_client = OutboundHttpClient(app_config)
    try:
        manager = DeliveryManager(
            config=app_config,
            session_factory=session_factory,
            secret_manager=secret_manager,
            plugin_manager=plugin_manager,
            http_client=http_client,
            metrics=MetricsCollector(),
        )
        result = manager.run(trigger=RunTrigger.TEST, initiated_by="pytest")
        assert result["status"] == "partial_success"
        assert result["summary"]["success"] == 1
        assert result["summary"]["permanent_failure"] == 1
        for _ in range(5):
            manager.run(trigger=RunTrigger.MANUAL, initiated_by="pytest")
        with session_scope(session_factory) as session:
            disabled = session.scalar(select(ServiceDestination).where(ServiceDestination.name == "Discord fail"))  # type: ignore[name-defined]
            assert disabled is not None
            assert disabled.enabled is False
    finally:
        http_client.close()


def test_missing_asset_falls_back_to_bundled_frog(app_config, session_factory):
    with session_scope(session_factory) as session:
        ensure_defaults(session, app_config)
        custom = store_uploaded_asset(
            session,
            app_config,
            filename="custom.png",
            payload=app_config.bundled_asset_path.read_bytes(),
            media_type="image/png",
        )
        settings = session.get(AppSettings, 1)
        settings.active_asset_id = custom.id
        asset_path = app_config.assets_dir / custom.stored_filename
        asset_path.unlink()
        _, active_asset, fallback_active, warning = resolve_active_asset(session, app_config)
        assert fallback_active is True
        assert warning is not None
        assert active_asset.is_default is True


def test_governance_docs_reference_the_plan(app_config):
    readme = (app_config.repo_root / "README.md").read_text()
    contributing = (app_config.repo_root / "CONTRIBUTING.md").read_text()
    plan = (app_config.repo_root / "IMPLEMENTATION_PLAN.md").read_text()
    gitignore = (app_config.repo_root / ".gitignore").read_text()
    assert "IMPLEMENTATION_PLAN.md" in readme
    assert "IMPLEMENTATION_PLAN.md" in contributing
    assert "Wednesday Frog Plugin + Optional HA Execution Plan" in plan
    assert "*.db" in gitignore


def test_templates_do_not_use_inline_click_handlers(app_config):
    for path in (
        app_config.repo_root / "templates" / "dashboard.html",
        app_config.repo_root / "templates" / "destination_detail.html",
        app_config.repo_root / "templates" / "test.html",
    ):
        contents = path.read_text()
        assert "onclick=" not in contents
        assert "data-post-json-url=" in contents


def test_settings_template_uses_timezone_select_not_datalist(app_config):
    contents = (app_config.repo_root / "templates" / "settings.html").read_text()
    assert '<select' in contents
    assert 'name="timezone"' in contents
    assert '<datalist' not in contents
    assert 'data-auto-submit-on-change="true"' in contents
    assert 'id="schedule-summary-label"' in contents
    assert 'name="schedule_cron"' not in contents
    assert 'name="schedule_hour"' in contents
    assert 'name="schedule_minute"' in contents
    assert 'name="schedule_time_text"' in contents
    assert 'name="schedule_enabled"' in contents
    assert 'settings-inline-row' in contents
    assert 'settings-asset-preview' in contents


def test_dashboard_template_uses_validation_density_hooks(app_config):
    contents = (app_config.repo_root / "templates" / "dashboard.html").read_text()
    assert 'dashboard-validation-grid' in contents
    assert 'dashboard-validation-card' in contents


def test_test_template_uses_manual_run_header_layout(app_config):
    contents = (app_config.repo_root / "templates" / "test.html").read_text()
    assert 'class="section-header test-manual-header"' in contents
    assert 'data-post-json-target="test-run-result"' in contents
    assert 'class="section-header test-destination-header"' in contents


def test_history_template_hides_summary_json_under_details(app_config):
    contents = (app_config.repo_root / "templates" / "history.html").read_text()
    assert '<summary>See details</summary>' in contents
    assert 'class="history-details"' in contents
    assert 'class="run-card history-run-card"' in contents
    assert 'class="data-table compact history-attempt-table"' in contents
    assert 'run.trigger_kind' not in contents
    assert contents.index('data-table compact') < contents.index('<summary>See details</summary>')


def test_destinations_templates_use_compact_layout_hooks(app_config):
    list_contents = (app_config.repo_root / "templates" / "destinations.html").read_text()
    detail_contents = (app_config.repo_root / "templates" / "destination_detail.html").read_text()
    assert 'class="two-column destinations-shell"' in list_contents
    assert 'class="stack-form destination-create-form"' in list_contents
    assert 'class="destination-create-submit"' in list_contents
    assert 'class="badge destinations-total-badge"' in list_contents
    assert 'class="data-table compact destinations-table"' in list_contents
    assert '<th scope="col">Destination</th>' in list_contents
    assert '<th scope="col">Plugin</th>' in list_contents
    assert '<th scope="col">Owner</th>' in list_contents
    assert '<th scope="col">Action</th>' in list_contents
    assert 'class="button-link destinations-open-link"' in list_contents
    assert 'class="panel destination-detail-panel"' in detail_contents
    assert 'id="destination-edit-form"' in detail_contents
    assert 'class="destination-config-grid"' in detail_contents
    assert 'class="secret-grid destination-secret-grid"' in detail_contents
    assert 'class="section-header destination-channel-header"' in detail_contents
    assert 'class="section-header destination-section-header destination-add-channel-header"' in detail_contents


def test_account_and_users_templates_use_compact_layout_hooks(app_config):
    account_contents = (app_config.repo_root / "templates" / "account.html").read_text()
    users_contents = (app_config.repo_root / "templates" / "users.html").read_text()
    user_detail_contents = (app_config.repo_root / "templates" / "user_detail.html").read_text()
    styles = (app_config.repo_root / "static" / "style.css").read_text()
    assert 'class="panel narrow account-panel"' in account_contents
    assert 'class="stack-form account-password-form"' in account_contents
    assert 'class="account-password-grid account-password-stack"' in account_contents
    assert 'class="two-column users-shell"' in users_contents
    assert 'class="button-link users-create-link"' in users_contents
    assert 'class="data-table compact users-table"' in users_contents
    assert '<th scope="col">Username</th>' in users_contents
    assert '<th scope="col">Role</th>' in users_contents
    assert '<th scope="col">Action</th>' in users_contents
    assert 'class="button-link users-manage-link"' in users_contents
    assert 'class="panel narrow user-detail-panel"' in user_detail_contents
    assert 'id="user-detail-form"' in user_detail_contents
    assert 'class="user-detail-grid"' in user_detail_contents
    assert '.settings-timezone-field {' in styles
    assert 'flex: 1.1 1 190px;' in styles
    assert 'min-width: 0;' in styles
    assert '.settings-hour-field {' in styles
    assert 'flex: 0 0 164px;' in styles
    assert '.account-password-stack {' in styles


def test_login_and_setup_templates_use_auth_layout_hooks(app_config):
    login_contents = (app_config.repo_root / "templates" / "login.html").read_text()
    setup_contents = (app_config.repo_root / "templates" / "setup.html").read_text()
    assert 'class="panel narrow auth-panel"' in login_contents
    assert 'class="badge auth-badge"' in login_contents
    assert 'class="stack-form auth-form"' in login_contents
    assert 'class="button-row auth-actions"' in login_contents
    assert 'class="panel narrow auth-panel auth-setup-panel"' in setup_contents
    assert 'class="badge auth-badge"' in setup_contents
    assert 'class="stack-form auth-form"' in setup_contents
    assert 'class="button-row auth-actions"' in setup_contents
