"""Plugin datatypes and schema helpers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from ..delivery.base import AdapterResult, PreparedAsset, ValidationIssue
from ..http_client import OutboundHttpClient, OutboundTargetBlocked
from ..models import DestinationChannel, ServiceDestination
from ..security import SecretManager


SUPPORTED_PROPERTY_TYPES = {"string", "boolean", "integer"}
SUPPORTED_FORMATS = {"password", "uri", "textarea", ""}


@dataclass(frozen=True, slots=True)
class PluginManifest:
    """Manifest metadata for one discovered plugin."""

    plugin_id: str
    display_name: str
    version: str
    description: str
    entrypoint: str


@dataclass(frozen=True, slots=True)
class LoadedPlugin:
    """One loaded plugin bundle."""

    manifest: PluginManifest
    connector: "FrogConnector"


@dataclass(frozen=True, slots=True)
class PluginValidationContext:
    """Context passed to plugin validation."""

    session: Session
    destination: ServiceDestination
    secret_manager: SecretManager
    asset: PreparedAsset | None


@dataclass(frozen=True, slots=True)
class PluginSendContext:
    """Context passed to one plugin delivery."""

    session: Session
    destination: ServiceDestination
    channel: DestinationChannel
    asset: PreparedAsset
    caption: str
    secret_manager: SecretManager
    http_client: OutboundHttpClient


@dataclass(frozen=True, slots=True)
class PluginErrorContext:
    """Context passed to plugin error handling."""

    destination: ServiceDestination
    channel: DestinationChannel | None
    operation: str


@dataclass(frozen=True, slots=True)
class RenderedField:
    """One UI-ready field derived from JSON Schema."""

    name: str
    label: str
    field_type: str
    input_type: str
    help_text: str
    required: bool
    default: Any = None
    enum: tuple[Any, ...] = ()
    placeholder: str = ""


class FrogConnector(ABC):
    """Stable connector contract for bundled plugins."""

    plugin_id: str
    display_name: str
    requires_asset_for_validation = False

    @abstractmethod
    def validate_config(self, context: PluginValidationContext) -> list[ValidationIssue]:
        """Validate one destination and its channels."""

    @abstractmethod
    def send_payload(self, context: PluginSendContext) -> AdapterResult:
        """Send one payload to one configured channel."""

    def handle_error(self, context: PluginErrorContext, exc: Exception) -> AdapterResult:
        """Map an unexpected plugin error to a delivery result."""
        if isinstance(exc, OutboundTargetBlocked):
            return AdapterResult(status="permanent_failure", error_message=str(exc))
        return AdapterResult(status="retryable_failure", error_message=f"{context.operation} failed: {exc}")

    @abstractmethod
    def destination_config_schema(self) -> dict[str, Any]:
        """Return the destination-level plain-text config schema."""

    @abstractmethod
    def destination_secret_schema(self) -> dict[str, Any]:
        """Return the destination-level secret config schema."""

    @abstractmethod
    def channel_config_schema(self) -> dict[str, Any]:
        """Return the channel-level plain-text config schema."""

    @abstractmethod
    def channel_secret_schema(self) -> dict[str, Any]:
        """Return the channel-level secret config schema."""


def render_schema_fields(schema: dict[str, Any]) -> list[RenderedField]:
    """Convert the supported JSON Schema subset into template-ready fields."""
    if not schema:
        return []
    if schema.get("type") != "object":
        raise ValueError("Plugin schemas must declare type=object.")
    properties = schema.get("properties", {})
    if not isinstance(properties, dict):
        raise ValueError("Plugin schemas must use an object properties map.")
    required = set(schema.get("required", []))
    fields: list[RenderedField] = []
    for name, raw in properties.items():
        field_type = str(raw.get("type", "string"))
        if field_type not in SUPPORTED_PROPERTY_TYPES:
            raise ValueError(f"Unsupported field type '{field_type}' for '{name}'.")
        fmt = str(raw.get("format", "")) if raw.get("format") is not None else ""
        if fmt not in SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported field format '{fmt}' for '{name}'.")
        enum_values = raw.get("enum", [])
        if enum_values and not isinstance(enum_values, list):
            raise ValueError(f"Field '{name}' must declare enum as a list.")
        input_type = "text"
        if fmt == "password":
            input_type = "password"
        elif fmt == "textarea":
            input_type = "textarea"
        elif fmt == "uri":
            input_type = "url"
        elif field_type == "boolean":
            input_type = "checkbox"
        elif field_type == "integer":
            input_type = "number"
        elif enum_values:
            input_type = "select"
        fields.append(
            RenderedField(
                name=name,
                label=str(raw.get("title") or name.replace("_", " ").title()),
                field_type=field_type,
                input_type=input_type,
                help_text=str(raw.get("description", "")),
                required=name in required,
                default=raw.get("default"),
                enum=tuple(enum_values),
                placeholder=str(raw.get("placeholder", "")),
            )
        )
    return fields
