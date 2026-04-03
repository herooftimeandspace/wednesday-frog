"""Centralized outbound HTTP client with SSRF protection."""

from __future__ import annotations

import ipaddress
from ipaddress import ip_address, ip_network
import socket
from typing import Any
from urllib.parse import urlparse

import httpx

from .config import AppConfig


class OutboundTargetBlocked(ValueError):
    """Raised when an outbound target resolves to a blocked address."""


def _is_blocked_ip(value: ipaddress._BaseAddress) -> bool:
    return any(
        (
            value.is_loopback,
            value.is_private,
            value.is_link_local,
            value.is_multicast,
            value.is_reserved,
            value.is_unspecified,
        )
    )


class OutboundHttpClient:
    """HTTP helper that blocks internal or reserved targets by default."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._client = httpx.Client(timeout=60.0)

    def close(self) -> None:
        """Close the underlying client."""
        self._client.close()

    def _allowlisted(self, host: str, address: str) -> bool:
        for item in self._config.outbound_allowlist:
            if item == host:
                return True
            try:
                if ip_address(address) in ip_network(item, strict=False):
                    return True
            except ValueError:
                continue
        return False

    def _check_url(self, url: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            raise OutboundTargetBlocked("Only http and https outbound URLs are allowed.")
        if not parsed.hostname:
            raise OutboundTargetBlocked("Outbound URL is missing a hostname.")
        try:
            resolved = socket.getaddrinfo(parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80), type=socket.SOCK_STREAM)
        except socket.gaierror as exc:
            raise OutboundTargetBlocked(f"Could not resolve outbound host '{parsed.hostname}': {exc}") from exc
        for _, _, _, _, sockaddr in resolved:
            address = sockaddr[0]
            parsed_ip = ip_address(address)
            if _is_blocked_ip(parsed_ip) and not self._allowlisted(parsed.hostname, address):
                raise OutboundTargetBlocked(f"Outbound host '{parsed.hostname}' resolves to blocked address {address}.")

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Validate the URL then send the request."""
        self._check_url(url)
        return self._client.request(method, url, **kwargs)

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("POST", url, **kwargs)
