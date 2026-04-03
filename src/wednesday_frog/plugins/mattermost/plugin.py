"""Mattermost plugin."""

from __future__ import annotations

from ...delivery.mattermost import MattermostAdapter
from ..base import FrogConnector, PluginSendContext, PluginValidationContext


class MattermostPlugin(FrogConnector):
    """Mattermost connector wrapper."""

    plugin_id = "mattermost"
    display_name = "Mattermost"

    def __init__(self) -> None:
        self._adapter = MattermostAdapter()

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
        return {
            "type": "object",
            "required": ["base_url"],
            "properties": {
                "base_url": {
                    "type": "string",
                    "title": "Base URL",
                    "description": "Your Mattermost server URL, such as https://chat.example.com.",
                    "format": "uri"
                }
            },
        }

    def destination_secret_schema(self) -> dict:
        return {
            "type": "object",
            "required": ["bot_token"],
            "properties": {
                "bot_token": {
                    "type": "string",
                    "title": "Bot or PAT token",
                    "description": "A Mattermost bot token or personal access token.",
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
                    "description": "The Mattermost channel ID to post into."
                }
            },
        }

    def channel_secret_schema(self) -> dict:
        return {"type": "object", "properties": {}}
