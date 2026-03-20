"""
FieldEncryptor: AES-256-GCM field-level encryption for PII in event payloads.

Problem
-------
Event payloads contain PII (applicant_name, applicant_id) stored as plaintext
JSONB. A database dump or unauthorized read access exposes all applicant data.

Design
------
- AES-256-GCM: authenticated encryption — provides both confidentiality and
  integrity. Tampering with ciphertext is detectable.
- Key: 32-byte key from environment variable FIELD_ENCRYPTION_KEY (base64).
  In production, use AWS KMS / HashiCorp Vault to supply the key.
- Each encrypted value is stored as: "enc:v1:<base64(nonce + ciphertext + tag)>"
  The "enc:v1:" prefix allows detection of encrypted vs plaintext values and
  supports future key rotation (v2, v3...).
- Nonce: 12 bytes, randomly generated per encryption. Never reused.
- encrypt_fields(payload, fields): returns new payload with specified fields encrypted.
- decrypt_fields(payload, fields): returns new payload with encrypted fields decrypted.
- is_encrypted(value): checks if a value is an encrypted blob.

Key rotation
------------
To rotate keys:
1. Set FIELD_ENCRYPTION_KEY_OLD = current key
2. Set FIELD_ENCRYPTION_KEY = new key
3. Run a migration that re-encrypts all events (decrypt with old, encrypt with new)
4. Remove FIELD_ENCRYPTION_KEY_OLD

This module supports a secondary key via the `old_key` parameter for rotation.

Usage
-----
    encryptor = FieldEncryptor.from_env()
    encrypted_payload = encryptor.encrypt_fields(
        payload, fields=["applicant_name", "applicant_id"]
    )
    decrypted_payload = encryptor.decrypt_fields(
        encrypted_payload, fields=["applicant_name", "applicant_id"]
    )
"""
from __future__ import annotations

import base64
import json
import logging
import os
import secrets
from typing import Any, Optional

logger = logging.getLogger(__name__)

ENCRYPTION_PREFIX = "enc:v1:"
NONCE_SIZE = 12  # bytes, GCM standard


class EncryptionError(Exception):
    pass


class FieldEncryptor:
    """
    AES-256-GCM field-level encryptor.

    Args:
        key: 32-byte encryption key.
        old_key: Optional previous key for rotation (decrypt-only).
    """

    def __init__(self, key: bytes, old_key: Optional[bytes] = None):
        if len(key) != 32:
            raise ValueError(f"Key must be 32 bytes, got {len(key)}")
        self._key = key
        self._old_key = old_key

    @classmethod
    def from_env(cls) -> "FieldEncryptor":
        """
        Load key from FIELD_ENCRYPTION_KEY environment variable (base64-encoded).
        Optionally loads FIELD_ENCRYPTION_KEY_OLD for rotation.
        """
        raw = os.environ.get("FIELD_ENCRYPTION_KEY")
        if not raw:
            raise EncryptionError(
                "FIELD_ENCRYPTION_KEY environment variable not set. "
                "Generate with: python -c \"import secrets,base64; print(base64.b64encode(secrets.token_bytes(32)).decode())\""
            )
        key = base64.b64decode(raw)

        old_key = None
        old_raw = os.environ.get("FIELD_ENCRYPTION_KEY_OLD")
        if old_raw:
            old_key = base64.b64decode(old_raw)

        return cls(key, old_key)

    @classmethod
    def generate_key(cls) -> str:
        """Generate a new random 32-byte key, base64-encoded. Use for setup."""
        return base64.b64encode(secrets.token_bytes(32)).decode()

    def encrypt_fields(self, payload: dict, fields: list[str]) -> dict:
        """
        Return a new payload dict with specified fields encrypted.
        Fields not present in payload are skipped.
        Already-encrypted fields are left unchanged.
        """
        result = dict(payload)
        for field in fields:
            if field in result and not self.is_encrypted(result[field]):
                result[field] = self._encrypt(str(result[field]))
        return result

    def decrypt_fields(self, payload: dict, fields: list[str]) -> dict:
        """
        Return a new payload dict with specified fields decrypted.
        Non-encrypted fields are left unchanged.
        """
        result = dict(payload)
        for field in fields:
            if field in result and self.is_encrypted(result[field]):
                result[field] = self._decrypt(result[field])
        return result

    @staticmethod
    def is_encrypted(value: Any) -> bool:
        """Check if a value is an encrypted blob."""
        return isinstance(value, str) and value.startswith(ENCRYPTION_PREFIX)

    def rotate_fields(self, payload: dict, fields: list[str]) -> dict:
        """
        Re-encrypt fields from old_key to current key.
        Used during key rotation. Requires old_key to be set.
        """
        if self._old_key is None:
            raise EncryptionError("old_key not set — cannot rotate")
        result = dict(payload)
        old_encryptor = FieldEncryptor(self._old_key)
        for field in fields:
            if field in result and self.is_encrypted(result[field]):
                plaintext = old_encryptor._decrypt(result[field])
                result[field] = self._encrypt(plaintext)
        return result

    # -------------------------------------------------------------------------
    # Internal
    # -------------------------------------------------------------------------

    def _encrypt(self, plaintext: str) -> str:
        """Encrypt a string value. Returns "enc:v1:<base64(nonce+ciphertext+tag)>"."""
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except ImportError:
            raise EncryptionError(
                "cryptography package required: pip install cryptography"
            )
        nonce = secrets.token_bytes(NONCE_SIZE)
        aesgcm = AESGCM(self._key)
        ciphertext_with_tag = aesgcm.encrypt(nonce, plaintext.encode(), None)
        blob = base64.b64encode(nonce + ciphertext_with_tag).decode()
        return f"{ENCRYPTION_PREFIX}{blob}"

    def _decrypt(self, encrypted: str) -> str:
        """Decrypt an "enc:v1:..." value. Tries current key then old_key."""
        if not self.is_encrypted(encrypted):
            return encrypted
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except ImportError:
            raise EncryptionError(
                "cryptography package required: pip install cryptography"
            )
        blob = base64.b64decode(encrypted[len(ENCRYPTION_PREFIX):])
        nonce = blob[:NONCE_SIZE]
        ciphertext_with_tag = blob[NONCE_SIZE:]

        for key in filter(None, [self._key, self._old_key]):
            try:
                aesgcm = AESGCM(key)
                plaintext = aesgcm.decrypt(nonce, ciphertext_with_tag, None)
                return plaintext.decode()
            except Exception:
                continue

        raise EncryptionError("Decryption failed with all available keys")
