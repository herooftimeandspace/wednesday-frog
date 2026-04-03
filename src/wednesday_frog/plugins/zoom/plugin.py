"""Zoom plugin."""

from __future__ import annotations

from ...delivery.zoom import ZoomAdapter
from ..base import FrogConnector, PluginSendContext, PluginValidationContext


class ZoomPlugin(FrogConnector):
    """Zoom connector wrapper."""

    plugin_id = "zoom"
    display_name = "Zoom"

    def __init__(self) -> None:
        self._adapter = ZoomAdapter()

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
            "required": ["account_id", "client_id", "sender_user_id"],
            "properties": {
                "account_id": {
                    "type": "string",
                    "title": "Account ID",
                    "description": "The Zoom account ID for account-credentials OAuth."
                },
                "client_id": {
                    "type": "string",
                    "title": "Client ID",
                    "description": "The Zoom OAuth client ID."
                },
                "sender_user_id": {
                    "type": "string",
                    "title": "Sender user ID",
                    "description": "The Zoom sender user ID. Use me for compatible user-level apps.",
                    "default": "me"
                },
                "bot_jid": {
                    "type": "string",
                    "title": "Optional bot JID",
                    "description": "Optional chatbot JID used only for follow-up caption text."
                }
            },
        }

    def destination_secret_schema(self) -> dict:
        return {
            "type": "object",
            "required": ["client_secret"],
            "properties": {
                "client_secret": {
                    "type": "string",
                    "title": "Client secret",
                    "description": "The Zoom OAuth client secret.",
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
                    "description": "The Zoom Team Chat channel ID."
                }
            },
        }

    def channel_secret_schema(self) -> dict:
        return {"type": "object", "properties": {}}
