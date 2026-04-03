"""Plugin discovery for bundled connectors."""

from __future__ import annotations

from dataclasses import dataclass
import importlib
import json
import logging
from pathlib import Path
from typing import Any

from .base import FrogConnector, LoadedPlugin, PluginManifest, render_schema_fields


LOGGER = logging.getLogger(__name__)
REQUIRED_MANIFEST_KEYS = {"plugin_id", "display_name", "version", "description", "entrypoint"}


@dataclass(frozen=True, slots=True)
class PluginFailure:
    """One plugin load failure."""

    plugin_id: str
    reason: str


class PluginManager:
    """Discover and load bundled plugins from known directories."""

    def __init__(self, search_dirs: list[Path]) -> None:
        self._search_dirs = search_dirs
        self._plugins: dict[str, LoadedPlugin] = {}
        self._failures: dict[str, PluginFailure] = {}
        self.reload()

    def reload(self) -> None:
        """Clear and rediscover every plugin."""
        self._plugins = {}
        self._failures = {}
        for base_dir in self._search_dirs:
            if not base_dir.is_dir():
                continue
            for candidate in sorted(path for path in base_dir.iterdir() if path.is_dir()):
                self._load_candidate(candidate)

    def _load_candidate(self, candidate: Path) -> None:
        manifest_path = candidate / "manifest.json"
        plugin_path = candidate / "plugin.py"
        if not manifest_path.is_file() or not plugin_path.is_file():
            return
        try:
            manifest = self._load_manifest(manifest_path)
            connector = self._load_connector(manifest)
            self._validate_connector(manifest, connector)
            self._plugins[manifest.plugin_id] = LoadedPlugin(manifest=manifest, connector=connector)
        except Exception as exc:
            plugin_id = candidate.name
            self._failures[plugin_id] = PluginFailure(plugin_id=plugin_id, reason=str(exc))
            LOGGER.warning("Plugin load failed for %s: %s", plugin_id, exc)

    def _load_manifest(self, path: Path) -> PluginManifest:
        raw = json.loads(path.read_text(encoding="utf-8"))
        missing = REQUIRED_MANIFEST_KEYS - set(raw)
        if missing:
            missing_text = ", ".join(sorted(missing))
            raise ValueError(f"manifest missing required keys: {missing_text}")
        return PluginManifest(
            plugin_id=str(raw["plugin_id"]),
            display_name=str(raw["display_name"]),
            version=str(raw["version"]),
            description=str(raw["description"]),
            entrypoint=str(raw["entrypoint"]),
        )

    def _load_connector(self, manifest: PluginManifest) -> FrogConnector:
        module_name, _, attribute = manifest.entrypoint.partition(":")
        if not module_name or not attribute:
            raise ValueError("entrypoint must use module:attribute form")
        module = importlib.import_module(module_name)
        target = getattr(module, attribute)
        connector = target() if isinstance(target, type) else target
        if not isinstance(connector, FrogConnector):
            raise TypeError("plugin entrypoint must resolve to a FrogConnector")
        return connector

    def _validate_connector(self, manifest: PluginManifest, connector: FrogConnector) -> None:
        if connector.plugin_id != manifest.plugin_id:
            raise ValueError("manifest plugin_id does not match connector plugin_id")
        render_schema_fields(connector.destination_config_schema())
        render_schema_fields(connector.destination_secret_schema())
        render_schema_fields(connector.channel_config_schema())
        render_schema_fields(connector.channel_secret_schema())

    def available_plugins(self) -> list[LoadedPlugin]:
        """Return every loaded plugin sorted by label."""
        return sorted(self._plugins.values(), key=lambda item: item.manifest.display_name.lower())

    def failures(self) -> list[PluginFailure]:
        """Return the recorded plugin load failures."""
        return sorted(self._failures.values(), key=lambda item: item.plugin_id)

    def get(self, plugin_id: str) -> LoadedPlugin | None:
        """Return one loaded plugin by id."""
        return self._plugins.get(plugin_id)

    def check_report(self, *, emit_plugin_env: str | None = None) -> dict[str, Any]:
        """Return a CLI-friendly validation report."""
        report: dict[str, Any] = {
            "ok": not self._failures,
            "plugins": [
                {
                    "plugin_id": plugin.manifest.plugin_id,
                    "display_name": plugin.manifest.display_name,
                    "version": plugin.manifest.version,
                    "description": plugin.manifest.description,
                }
                for plugin in self.available_plugins()
            ],
            "failures": [{"plugin_id": item.plugin_id, "reason": item.reason} for item in self.failures()],
        }
        if emit_plugin_env:
            loaded = self.get(emit_plugin_env)
            if loaded is None:
                report["emit_plugin_env_error"] = f"Plugin '{emit_plugin_env}' is not available."
            else:
                connector = loaded.connector
                report["emit_plugin_env"] = {
                    "env": [
                        "WEDNESDAY_FROG_MASTER_KEY=replace-with-32-plus-char-secret",
                        "WEDNESDAY_FROG_SESSION_SECRET=replace-with-32-plus-char-secret",
                        "WEDNESDAY_FROG_SETUP_TOKEN=replace-with-32-plus-char-secret",
                        "WEDNESDAY_FROG_DATABASE_URL=sqlite:////data/wednesday_frog.db",
                    ],
                    "compose": [
                        "services:",
                        "  wednesday-frog:",
                        "    build: .",
                        "    env_file:",
                        "      - .env",
                        "    volumes:",
                        "      - ./frog_data:/data",
                    ],
                    "plugin_inputs": {
                        "destination_fields": [field.name for field in render_schema_fields(connector.destination_config_schema())],
                        "destination_secrets": [field.name for field in render_schema_fields(connector.destination_secret_schema())],
                        "channel_fields": [field.name for field in render_schema_fields(connector.channel_config_schema())],
                        "channel_secrets": [field.name for field in render_schema_fields(connector.channel_secret_schema())],
                    },
                }
        return report
