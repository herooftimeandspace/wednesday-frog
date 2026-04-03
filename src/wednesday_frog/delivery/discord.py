"""Discord delivery adapter."""

from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from sqlalchemy.orm import Session

from ..http_client import OutboundHttpClient
from ..models import DestinationChannel, ServiceDestination
from ..security import SecretManager
from ..services import get_secret_value
from .base import AdapterResult, DeliveryAdapter, PreparedAsset, ValidationIssue


def _with_wait(url: str) -> str:
    """Append the Discord wait query parameter."""
    parts = urlparse(url)
    query = dict(parse_qsl(parts.query))
    query["wait"] = "true"
    return urlunparse(parts._replace(query=urlencode(query)))


class DiscordAdapter(DeliveryAdapter):
    """Send a webhook file message to Discord."""

    service_type = "discord"

    def validate(
        self,
        session: Session,
        destination: ServiceDestination,
        secret_manager: SecretManager,
        asset: PreparedAsset | None,
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        enabled_channels = [channel for channel in destination.channels if channel.enabled]
        if not enabled_channels:
            issues.append(ValidationIssue("error", "Discord destinations need at least one enabled webhook channel."))
        for channel in enabled_channels:
            webhook_url = get_secret_value(session, channel=channel, secret_key="webhook_url", secret_manager=secret_manager)
            if not webhook_url:
                issues.append(ValidationIssue("error", f"Discord channel '{channel.name}' is missing a webhook URL."))
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
        webhook_url = get_secret_value(session, channel=channel, secret_key="webhook_url", secret_manager=secret_manager)
        if not webhook_url:
            return AdapterResult(status="permanent_failure", error_message="Missing Discord webhook URL.")
        response = http_client.post(
            _with_wait(webhook_url),
            data={"content": caption},
            files={"file0": (asset.filename, asset.payload, asset.media_type)},
            timeout=30.0,
        )
        if response.status_code >= 400:
            return AdapterResult(status="permanent_failure", error_message=f"Discord webhook failed: {response.text[:200]}")
        response_id = None
        if response.headers.get("content-type", "").startswith("application/json"):
            response_id = response.json().get("id")
        excerpt = f"Discord webhook sent{f' message {response_id}' if response_id else ''}."
        return AdapterResult(status="success", response_excerpt=excerpt)
