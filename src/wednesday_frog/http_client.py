"""Centralized outbound HTTP client with SSRF protection."""

from __future__ import annotations

from dataclasses import dataclass
import ipaddress
from ipaddress import ip_address, ip_network
import socket
from typing import Any
from urllib.parse import urlparse, urlunparse

import httpx

from .config import AppConfig


class OutboundTargetBlocked(ValueError):
    """Raised when an outbound target resolves to a blocked address."""


@dataclass(frozen=True, slots=True)
class ResolvedOutboundTarget:
    """One validated outbound target resolution."""

    parsed_url: Any
    connect_address: str
    host_header: str
    server_hostname: str


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
        self._client = httpx.Client(
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=30.0, pool=5.0),
            limits=httpx.Limits(max_connections=8, max_keepalive_connections=4, keepalive_expiry=15.0),
            trust_env=False,
        )

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

    def _host_header(self, parsed) -> str:
        default_port = 443 if parsed.scheme == "https" else 80
        if parsed.port and parsed.port != default_port:
            return f"{parsed.hostname}:{parsed.port}"
        return parsed.hostname or ""

    def _connect_url(self, parsed, address: str) -> str:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        if ":" in address and not address.startswith("["):
            host = f"[{address}]"
        else:
            host = address
        netloc = f"{host}:{port}"
        return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))

    def _resolve_url(self, url: str) -> ResolvedOutboundTarget:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            raise OutboundTargetBlocked("Only http and https outbound URLs are allowed.")
        if not parsed.hostname:
            raise OutboundTargetBlocked("Outbound URL is missing a hostname.")
        try:
            resolved = socket.getaddrinfo(parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80), type=socket.SOCK_STREAM)
        except socket.gaierror as exc:
            raise OutboundTargetBlocked(f"Could not resolve outbound host '{parsed.hostname}': {exc}") from exc
        connect_address = None
        for _, _, _, _, sockaddr in resolved:
            address = sockaddr[0]
            parsed_ip = ip_address(address)
            if _is_blocked_ip(parsed_ip) and not self._allowlisted(parsed.hostname, address):
                raise OutboundTargetBlocked(f"Outbound host '{parsed.hostname}' resolves to blocked address {address}.")
            if connect_address is None:
                connect_address = address
        if connect_address is None:
            raise OutboundTargetBlocked(f"Outbound host '{parsed.hostname}' did not resolve to a usable address.")
        return ResolvedOutboundTarget(
            parsed_url=parsed,
            connect_address=connect_address,
            host_header=self._host_header(parsed),
            server_hostname=parsed.hostname,
        )

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Validate the URL then send the request."""
        resolved = self._resolve_url(url)
        headers = httpx.Headers(kwargs.pop("headers", None))
        headers["Host"] = resolved.host_header
        extensions = dict(kwargs.pop("extensions", {}) or {})
        extensions["sni_hostname"] = resolved.server_hostname
        request = self._client.build_request(
            method,
            self._connect_url(resolved.parsed_url, resolved.connect_address),
            headers=headers,
            extensions=extensions,
            **kwargs,
        )
        return self._client.send(request)

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("POST", url, **kwargs)
