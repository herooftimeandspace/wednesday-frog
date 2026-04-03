"""Slack delivery adapter."""

from __future__ import annotations

from sqlalchemy.orm import Session

from ..http_client import OutboundHttpClient
from ..models import DestinationChannel, ServiceDestination
from ..security import SecretManager
from ..services import get_secret_value
from .base import AdapterResult, DeliveryAdapter, PreparedAsset, ValidationIssue


class SlackAdapter(DeliveryAdapter):
    """Send a file to Slack using the external upload flow."""

    service_type = "slack"

    def validate(
        self,
        session: Session,
        destination: ServiceDestination,
        secret_manager: SecretManager,
        asset: PreparedAsset | None,
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        token = get_secret_value(session, destination=destination, secret_key="bot_token", secret_manager=secret_manager)
        if not token:
            issues.append(ValidationIssue("error", "Slack destinations need a saved bot token."))
        enabled_channels = [channel for channel in destination.channels if channel.enabled]
        if not enabled_channels:
            issues.append(ValidationIssue("error", "Slack destinations need at least one enabled channel."))
        for channel in enabled_channels:
            if not channel.config_json.get("channel_id"):
                issues.append(ValidationIssue("error", f"Slack channel '{channel.name}' is missing a channel ID."))
        return issues

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
        token = get_secret_value(session, destination=destination, secret_key="bot_token", secret_manager=secret_manager)
        if not token:
            return AdapterResult(status="permanent_failure", error_message="Missing Slack bot token.")
        channel_id = channel.config_json.get("channel_id")
        headers = {"Authorization": f"Bearer {token}"}
        begin = http_client.post(
            "https://slack.com/api/files.getUploadURLExternal",
            headers=headers,
            data={"filename": asset.filename, "length": str(len(asset.payload))},
            timeout=30.0,
        )
        begin.raise_for_status()
        payload = begin.json()
        if not payload.get("ok"):
            return AdapterResult(status="permanent_failure", error_message=payload.get("error", "Slack upload init failed."))
        upload_url = payload["upload_url"]
        file_id = payload["file_id"]
        upload = http_client.post(upload_url, content=asset.payload, headers={"Content-Type": asset.media_type}, timeout=60.0)
        upload.raise_for_status()
        complete_payload: dict[str, object] = {
            "files": [{"id": file_id, "title": asset.filename}],
            "channel_id": channel_id,
        }
        if caption:
            complete_payload["initial_comment"] = caption
        complete = http_client.post(
            "https://slack.com/api/files.completeUploadExternal",
            headers={**headers, "Content-Type": "application/json; charset=utf-8"},
            json=complete_payload,
            timeout=30.0,
        )
        complete.raise_for_status()
        result = complete.json()
        if not result.get("ok"):
            return AdapterResult(status="permanent_failure", error_message=result.get("error", "Slack upload completion failed."))
        return AdapterResult(status="success", response_excerpt=f"Uploaded Slack file {file_id}.")
