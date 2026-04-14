"""Teams plugin."""

from __future__ import annotations

from ...delivery.teams import TeamsAdapter
from ..base import FrogConnector, PluginSendContext, PluginValidationContext


class TeamsPlugin(FrogConnector):
    """Teams connector wrapper."""

    plugin_id = "teams"
    display_name = "Teams"
    requires_asset_for_validation = True

    def __init__(self) -> None:
        self._adapter = TeamsAdapter()

    def validate_config(self, context: PluginValidationContext):
        return self._adapter.validate(context.session, context.destination, context.secret_manager, context.asset)

    def send_payload(self, context: PluginSendContext):
        return self._adapter.send_image(
            context.session,
            context.destination,
            context.channel,
            context.asset,
            context.caption,
            context.secret_manager,
            context.http_client,
        )

    def destination_config_schema(self) -> dict:
        return {"type": "object", "properties": {}}

    def destination_secret_schema(self) -> dict:
        return {"type": "object", "properties": {}}

    def channel_config_schema(self) -> dict:
        return {"type": "object", "properties": {}}

    def channel_secret_schema(self) -> dict:
        return {
            "type": "object",
            "required": ["webhook_url"],
            "properties": {
                "webhook_url": {
                    "type": "string",
                    "title": "Webhook URL",
                    "description": "The per-channel Teams Incoming Webhook URL.",
                    "format": "password"
                }
            },
        }
