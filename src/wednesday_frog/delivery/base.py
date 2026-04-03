"""Common delivery datatypes and adapter interface."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from ..http_client import OutboundHttpClient
from ..models import DestinationChannel, ServiceDestination
from ..security import SecretManager


@dataclass(slots=True)
class PreparedAsset:
    """Asset bytes prepared for delivery."""

    filename: str
    media_type: str
    payload: bytes


@dataclass(slots=True)
class ValidationIssue:
    """One configuration validation issue."""

    level: str
    message: str


@dataclass(slots=True)
class AdapterResult:
    """Result of one provider send attempt."""

    status: str
    response_excerpt: str | None = None
    error_message: str | None = None


class DeliveryAdapter:
    """Base class for service-specific delivery adapters."""

    service_type: str

    def validate(
        self,
        session: Session,
        destination: ServiceDestination,
        secret_manager: SecretManager,
        asset: PreparedAsset | None,
    ) -> list[ValidationIssue]:
        """Validate configuration for a destination."""
        raise NotImplementedError

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
        """Send an image to the configured channel."""
        raise NotImplementedError
