"""Password, session, and encryption helpers."""

from __future__ import annotations

import base64
import hashlib
import secrets

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class PasswordManager:
    """Small wrapper around Argon2 hashing."""

    def __init__(self) -> None:
        self._hasher = PasswordHasher(
            time_cost=3,
            memory_cost=65_536,
            parallelism=4,
            hash_len=32,
            salt_len=16,
        )

    def hash_password(self, password: str) -> str:
        """Hash a plaintext password."""
        return self._hasher.hash(password)

    def verify(self, password_hash: str, password: str) -> bool:
        """Return whether the password matches the stored hash."""
        try:
            return self._hasher.verify(password_hash, password)
        except VerifyMismatchError:
            return False


class SecretManager:
    """Encrypt and decrypt secret values for database storage."""

    def __init__(self, master_key: str, previous_master_key: str | None = None) -> None:
        self._active = AESGCM(hashlib.sha256(master_key.encode("utf-8")).digest())
        self._fallback = None
        if previous_master_key:
            self._fallback = AESGCM(hashlib.sha256(previous_master_key.encode("utf-8")).digest())

    def encrypt(self, value: str) -> tuple[str, str, str]:
        """Encrypt a secret and return ciphertext, nonce, and masked last-four."""
        nonce = secrets.token_bytes(12)
        ciphertext = self._active.encrypt(nonce, value.encode("utf-8"), None)
        last_four = value[-4:] if value else "empty"
        return (
            base64.urlsafe_b64encode(ciphertext).decode("ascii"),
            base64.urlsafe_b64encode(nonce).decode("ascii"),
            last_four,
        )

    def decrypt(self, ciphertext: str, nonce: str) -> str:
        """Decrypt a previously encrypted secret."""
        raw_ciphertext = base64.urlsafe_b64decode(ciphertext.encode("ascii"))
        raw_nonce = base64.urlsafe_b64decode(nonce.encode("ascii"))
        for candidate in (self._active, self._fallback):
            if candidate is None:
                continue
            try:
                return candidate.decrypt(raw_nonce, raw_ciphertext, None).decode("utf-8")
            except InvalidTag:
                continue
        raise InvalidTag("Secret could not be decrypted with the configured keys.")


def issue_csrf_token() -> str:
    """Create a new CSRF token."""
    return secrets.token_urlsafe(32)
