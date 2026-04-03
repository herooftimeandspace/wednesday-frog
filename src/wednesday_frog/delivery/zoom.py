"""Zoom Team Chat delivery adapter."""

from __future__ import annotations

import time

from sqlalchemy.orm import Session

from ..http_client import OutboundHttpClient
from ..models import DestinationChannel, ServiceDestination
from ..security import SecretManager
from ..services import get_secret_value
from .base import AdapterResult, DeliveryAdapter, PreparedAsset, ValidationIssue


class ZoomAdapter(DeliveryAdapter):
    """Send a chat file to Zoom Team Chat."""

    service_type = "zoom"

    def __init__(self) -> None:
        self._token_cache: dict[int, tuple[str, float]] = {}

    def validate(
        self,
        session: Session,
        destination: ServiceDestination,
        secret_manager: SecretManager,
        asset: PreparedAsset | None,
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        for field in ("account_id", "client_id", "sender_user_id"):
            if not destination.config_json.get(field):
                issues.append(ValidationIssue("error", f"Zoom destinations need '{field}' configured."))
        secret = get_secret_value(session, destination=destination, secret_key="client_secret", secret_manager=secret_manager)
        if not secret:
            issues.append(ValidationIssue("error", "Zoom destinations need a client secret."))
        enabled_channels = [channel for channel in destination.channels if channel.enabled]
        if not enabled_channels:
            issues.append(ValidationIssue("error", "Zoom destinations need at least one enabled channel."))
        for channel in enabled_channels:
            if not channel.config_json.get("channel_id"):
                issues.append(ValidationIssue("error", f"Zoom channel '{channel.name}' is missing a channel ID."))
        return issues

    def _get_access_token(
        self,
        session: Session,
        destination: ServiceDestination,
        secret_manager: SecretManager,
        http_client: OutboundHttpClient,
    ) -> str:
        cached = self._token_cache.get(destination.id)
        now = time.time()
        if cached and cached[1] > now + 30:
            return cached[0]
        account_id = destination.config_json.get("account_id", "")
        client_id = destination.config_json.get("client_id", "")
        client_secret = get_secret_value(session, destination=destination, secret_key="client_secret", secret_manager=secret_manager)
        token_response = http_client.post(
            "https://zoom.us/oauth/token",
            params={"grant_type": "account_credentials", "account_id": account_id},
            auth=(client_id, client_secret),
            timeout=30.0,
        )
        token_response.raise_for_status()
        payload = token_response.json()
        access_token = payload["access_token"]
        expires_at = now + int(payload.get("expires_in", 3600))
        self._token_cache[destination.id] = (access_token, expires_at)
        return access_token

    def _send_optional_caption(
        self,
        access_token: str,
        destination: ServiceDestination,
        channel: DestinationChannel,
        caption: str,
        http_client: OutboundHttpClient,
    ) -> None:
        robot_jid = destination.config_json.get("bot_jid")
        if not caption or not robot_jid:
            return
        http_client.post(
            "https://api.zoom.us/v2/im/chat/messages",
            headers={"Authorization": f"Bearer {access_token}"},
            json={
                "robot_jid": robot_jid,
                "to_channel": channel.config_json.get("channel_id"),
                "content": {
                    "head": {"text": caption},
                    "body": [{"type": "message", "text": caption}],
                },
            },
            timeout=30.0,
        )

    def send_image(
        self,
        session: Session,
        destination: ServiceDestination,
        channel: DestinationChannel,
        asset: PreparedAsset,
        caption: str,
        secret_manager: SecretManager,
        http_client: OutboundHttpClient,
    ) -> AdapterResult:
        try:
            access_token = self._get_access_token(session, destination, secret_manager, http_client)
        except Exception as exc:
            return AdapterResult(status="permanent_failure", error_message=f"Zoom token request failed: {exc}")
        sender_user_id = destination.config_json.get("sender_user_id", "me")
        response = http_client.post(
            f"https://file.zoom.us/v2/chat/users/{sender_user_id}/messages/files",
            headers={"Authorization": f"Bearer {access_token}"},
            data={"to_channel": channel.config_json.get("channel_id")},
            files={"files": (asset.filename, asset.payload, asset.media_type)},
            timeout=60.0,
        )
        if response.status_code >= 400:
            return AdapterResult(status="permanent_failure", error_message=f"Zoom file send failed: {response.text[:200]}")
        self._send_optional_caption(access_token, destination, channel, caption, http_client)
        body = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
        return AdapterResult(status="success", response_excerpt=f"Zoom message {body.get('id', 'sent')}.")
