"""Optional Redis coordination for scheduled runs."""

from __future__ import annotations

import os
import socket
from urllib.parse import urlparse


class RedisScheduleCoordinator:
    """Acquire simple Redis SET NX EX locks without a third-party client."""

    def __init__(self, redis_url: str | None) -> None:
        self._redis_url = redis_url

    def _encode(self, *parts: str) -> bytes:
        payload = [f"*{len(parts)}\r\n".encode("ascii")]
        for part in parts:
            encoded = part.encode("utf-8")
            payload.append(f"${len(encoded)}\r\n".encode("ascii"))
            payload.append(encoded + b"\r\n")
        return b"".join(payload)

    def _read_line(self, sock: socket.socket) -> bytes:
        data = bytearray()
        while not data.endswith(b"\r\n"):
            chunk = sock.recv(1)
            if not chunk:
                raise ConnectionError("Redis connection closed unexpectedly.")
            data.extend(chunk)
        return bytes(data[:-2])

    def _read_reply(self, sock: socket.socket) -> str | None:
        prefix = sock.recv(1)
        if not prefix:
            raise ConnectionError("Redis reply missing.")
        line = self._read_line(sock)
        if prefix == b"+":
            return line.decode("utf-8")
        if prefix == b"$":
            length = int(line.decode("ascii"))
            if length < 0:
                return None
            payload = bytearray()
            while len(payload) < length + 2:
                payload.extend(sock.recv(length + 2 - len(payload)))
            return bytes(payload[:-2]).decode("utf-8")
        if prefix == b"-":
            raise ConnectionError(line.decode("utf-8"))
        raise ConnectionError(f"Unsupported Redis reply type {prefix!r}")

    def acquire(self, key: str, ttl_seconds: int) -> bool:
        """Try to acquire one Redis-backed lock."""
        if not self._redis_url:
            return True
        parsed = urlparse(self._redis_url)
        if parsed.scheme != "redis":
            raise ValueError("Only redis:// URLs are supported for scheduled locking.")
        host = parsed.hostname or "localhost"
        port = parsed.port or 6379
        db = (parsed.path or "/0").lstrip("/") or "0"
        token = os.urandom(8).hex()
        with socket.create_connection((host, port), timeout=3.0) as sock:
            if parsed.password:
                sock.sendall(self._encode("AUTH", parsed.password))
                self._read_reply(sock)
            if db != "0":
                sock.sendall(self._encode("SELECT", db))
                self._read_reply(sock)
            sock.sendall(self._encode("SET", key, token, "NX", "EX", str(ttl_seconds)))
            reply = self._read_reply(sock)
        return reply == "OK"
