"""CLI entrypoint for Wednesday Frog."""

from __future__ import annotations

import argparse
import json

import uvicorn

from .config import AppConfig
from .db import create_session_factory, session_scope
from .http_client import OutboundHttpClient
from .metrics import MetricsCollector
from .models import RunTrigger
from .security import SecretManager
from .services import DeliveryManager, build_plugin_manager, ensure_defaults, prune_history, rekey_all_secrets, validate_all_destinations
from .web import create_app


def _run_now() -> int:
    config = AppConfig.from_env()
    config.ensure_runtime_dirs()
    session_factory = create_session_factory(config)
    secret_manager = SecretManager(config.master_key, config.previous_master_key)
    plugin_manager = build_plugin_manager(config)
    http_client = OutboundHttpClient(config)
    try:
        with session_scope(session_factory) as session:
            ensure_defaults(session, config)
        manager = DeliveryManager(
            config=config,
            session_factory=session_factory,
            secret_manager=secret_manager,
            plugin_manager=plugin_manager,
            http_client=http_client,
            metrics=MetricsCollector(),
        )
        result = manager.run(trigger=RunTrigger.MANUAL, initiated_by="cli")
        print(json.dumps(result, indent=2))
        return 0 if result["status"] in {"succeeded", "partial_success"} else 1
    finally:
        http_client.close()


def _validate_config() -> int:
    config = AppConfig.from_env()
    config.ensure_runtime_dirs()
    session_factory = create_session_factory(config)
    secret_manager = SecretManager(config.master_key, config.previous_master_key)
    plugin_manager = build_plugin_manager(config)
    with session_scope(session_factory) as session:
        ensure_defaults(session, config)
        result = validate_all_destinations(session, config, secret_manager, plugin_manager)
    print(json.dumps(result, indent=2))
    return 0 if result["ok"] else 1


def _check(plugin_id: str | None) -> int:
    config = AppConfig.from_env()
    report = build_plugin_manager(config).check_report(emit_plugin_env=plugin_id)
    print(json.dumps(report, indent=2))
    return 0 if report["ok"] and "emit_plugin_env_error" not in report else 1


def _rekey() -> int:
    config = AppConfig.from_env()
    config.ensure_runtime_dirs()
    session_factory = create_session_factory(config)
    secret_manager = SecretManager(config.master_key, config.previous_master_key)
    with session_scope(session_factory) as session:
        count = rekey_all_secrets(session, secret_manager=secret_manager)
    print(json.dumps({"rekeyed": count}, indent=2))
    return 0


def _prune_history(days: int) -> int:
    if days < 1:
        raise SystemExit("--days must be at least 1")
    config = AppConfig.from_env()
    config.ensure_runtime_dirs()
    session_factory = create_session_factory(config)
    with session_scope(session_factory) as session:
        result = prune_history(session, days=days)
    print(json.dumps(result, indent=2))
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(prog="wednesday-frog")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve", help="Run the web application.")
    serve_parser.add_argument("--host", default="0.0.0.0")
    serve_parser.add_argument("--port", type=int, default=8000)

    subparsers.add_parser("run-now", help="Run a manual frog delivery immediately.")
    subparsers.add_parser("validate-config", help="Validate the saved application configuration.")
    check_parser = subparsers.add_parser("check", help="Validate bundled plugin manifests and schema support.")
    check_parser.add_argument("--emit-plugin-env", dest="emit_plugin_env", help="Print placeholder env and Compose hints for one plugin.")
    subparsers.add_parser("rekey-secrets", help="Re-encrypt stored secrets with the active master key.")
    prune_parser = subparsers.add_parser("prune-history", help="Delete old delivery history rows.")
    prune_parser.add_argument("--days", type=int, required=True, help="Delete runs older than this many days.")

    args = parser.parse_args()
    if args.command == "serve":
        uvicorn.run("wednesday_frog.web:create_app", factory=True, host=args.host, port=args.port)
        return
    if args.command == "run-now":
        raise SystemExit(_run_now())
    if args.command == "validate-config":
        raise SystemExit(_validate_config())
    if args.command == "check":
        raise SystemExit(_check(args.emit_plugin_env))
    if args.command == "rekey-secrets":
        raise SystemExit(_rekey())
    if args.command == "prune-history":
        raise SystemExit(_prune_history(args.days))


if __name__ == "__main__":
    main()
