"""Mattermost delivery adapter."""

from __future__ import annotations

from sqlalchemy.orm import Session

from ..http_client import OutboundHttpClient
from ..models import DestinationChannel, ServiceDestination
from ..security import SecretManager
from ..services import get_secret_value
from .base import AdapterResult, DeliveryAdapter, PreparedAsset, ValidationIssue


class MattermostAdapter(DeliveryAdapter):
    """Send a file post to Mattermost."""

    service_type = "mattermost"

    def validate(
        self,
        session: Session,
        destination: ServiceDestination,
        secret_manager: SecretManager,
        asset: PreparedAsset | None,
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        base_url = str(destination.config_json.get("base_url", "")).strip()
        if not base_url:
            issues.append(ValidationIssue("error", "Mattermost destinations need a base URL."))
        token = get_secret_value(session, destination=destination, secret_key="bot_token", secret_manager=secret_manager)
        if not token:
            issues.append(ValidationIssue("error", "Mattermost destinations need a bot or personal access token."))
        enabled_channels = [channel for channel in destination.channels if channel.enabled]
        if not enabled_channels:
            issues.append(ValidationIssue("error", "Mattermost destinations need at least one enabled channel."))
        for channel in enabled_channels:
            if not channel.config_json.get("channel_id"):
                issues.append(ValidationIssue("error", f"Mattermost channel '{channel.name}' is missing a channel ID."))
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
        base_url = str(destination.config_json.get("base_url", "")).rstrip("/")
        token = get_secret_value(session, destination=destination, secret_key="bot_token", secret_manager=secret_manager)
        channel_id = channel.config_json.get("channel_id")
        if not base_url or not token:
            return AdapterResult(status="permanent_failure", error_message="Missing Mattermost connection details.")
        headers = {"Authorization": f"Bearer {token}"}
        upload = http_client.post(
            f"{base_url}/api/v4/files",
            headers=headers,
            data={"channel_id": channel_id},
            files={"files": (asset.filename, asset.payload, asset.media_type)},
            timeout=30.0,
        )
        if upload.status_code >= 400:
            return AdapterResult(status="permanent_failure", error_message=f"Mattermost upload failed: {upload.text[:200]}")
        upload_body = upload.json()
        file_infos = upload_body.get("file_infos", [])
        if not file_infos:
            return AdapterResult(status="permanent_failure", error_message="Mattermost upload did not return any file IDs.")
        file_id = file_infos[0]["id"]
        post = http_client.post(
            f"{base_url}/api/v4/posts",
            headers={**headers, "Content-Type": "application/json"},
            json={"channel_id": channel_id, "message": caption, "file_ids": [file_id]},
            timeout=30.0,
        )
        if post.status_code >= 400:
            return AdapterResult(status="permanent_failure", error_message=f"Mattermost post failed: {post.text[:200]}")
        post_body = post.json()
        return AdapterResult(status="success", response_excerpt=f"Mattermost post {post_body.get('id', 'created')}.")
