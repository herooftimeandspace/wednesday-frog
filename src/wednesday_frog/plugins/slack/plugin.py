"""Slack plugin."""

from __future__ import annotations

from ...delivery.slack import SlackAdapter
from ..base import FrogConnector, PluginSendContext, PluginValidationContext


class SlackPlugin(FrogConnector):
    """Slack connector wrapper."""

    plugin_id = "slack"
    display_name = "Slack"

    def __init__(self) -> None:
        self._adapter = SlackAdapter()

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
        return {
            "type": "object",
            "required": ["bot_token"],
            "properties": {
                "bot_token": {
                    "type": "string",
                    "title": "Bot token",
                    "description": "A Slack bot token with files:write and chat:write scopes.",
                    "format": "password"
                }
            },
        }

    def channel_config_schema(self) -> dict:
        return {
            "type": "object",
            "required": ["channel_id"],
            "properties": {
                "channel_id": {
                    "type": "string",
                    "title": "Channel ID",
                    "description": "The target Slack channel ID, such as C12345678."
                }
            },
        }

    def channel_secret_schema(self) -> dict:
        return {"type": "object", "properties": {}}
