"""Microsoft Teams delivery adapter."""

from __future__ import annotations

from sqlalchemy.orm import Session

from ..assets import build_teams_data_uri
from ..http_client import OutboundHttpClient
from ..models import DestinationChannel, ServiceDestination
from ..security import SecretManager
from ..services import get_secret_value
from .base import AdapterResult, DeliveryAdapter, PreparedAsset, ValidationIssue


class TeamsAdapter(DeliveryAdapter):
    """Send Adaptive Card webhooks to Teams."""

    service_type = "teams"
    requires_asset_for_validation = True

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
            issues.append(ValidationIssue("error", "Teams destinations need at least one enabled webhook channel."))
        for channel in enabled_channels:
            webhook_url = get_secret_value(session, channel=channel, secret_key="webhook_url", secret_manager=secret_manager)
            if not webhook_url:
                issues.append(ValidationIssue("error", f"Teams channel '{channel.name}' is missing a webhook URL."))
        if asset is not None:
            try:
                build_teams_data_uri(asset)
            except ValueError as exc:
                issues.append(ValidationIssue("error", str(exc)))
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
            return AdapterResult(status="permanent_failure", error_message="Missing Teams webhook URL.")
        try:
            data_uri = build_teams_data_uri(asset)
        except ValueError as exc:
            return AdapterResult(status="permanent_failure", error_message=str(exc))
        card_text = caption or "Wednesday Frog"
        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.4",
                        "body": [
                            {"type": "TextBlock", "text": card_text, "wrap": True, "weight": "Bolder"},
                            {"type": "Image", "url": data_uri, "altText": "Wednesday Frog"},
                        ],
                    },
                }
            ],
        }
        response = http_client.post(webhook_url, json=payload, timeout=30.0)
        if response.status_code >= 400:
            return AdapterResult(
                status="permanent_failure",
                error_message=f"Teams webhook failed with status {response.status_code}: {response.text[:200]}",
            )
        return AdapterResult(status="success", response_excerpt=f"Teams webhook accepted with status {response.status_code}.")
