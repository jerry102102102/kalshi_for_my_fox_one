from __future__ import annotations

import base64
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from kalshi_mlb_research.config import Settings, load_settings
from kalshi_mlb_research.exceptions import ExternalServiceError
from kalshi_mlb_research.time_utils import epoch_ms


class KalshiAuthError(ExternalServiceError):
    """Raised when Kalshi credentials cannot be used."""


@dataclass
class KalshiAuth:
    settings: Settings | None = None

    def __post_init__(self) -> None:
        self.settings = self.settings or load_settings()
        self._private_key = None

    def require_credentials(self) -> None:
        if not self.settings.kalshi_api_key_id:
            raise KalshiAuthError("Missing KALSHI_API_KEY_ID")
        if not self.settings.kalshi_private_key_path:
            raise KalshiAuthError("Missing KALSHI_PRIVATE_KEY_PATH")
        path = Path(self.settings.kalshi_private_key_path).expanduser()
        if not path.exists():
            raise KalshiAuthError(f"KALSHI_PRIVATE_KEY_PATH does not exist: {path}")

    def has_credentials(self) -> bool:
        return bool(self.settings.kalshi_api_key_id and self.settings.kalshi_private_key_path)

    def _load_private_key(self):
        self.require_credentials()
        if self._private_key is not None:
            return self._private_key
        path = Path(self.settings.kalshi_private_key_path).expanduser()
        try:
            key_bytes = path.read_bytes()
            self._private_key = serialization.load_pem_private_key(key_bytes, password=None)
        except ValueError as exc:
            raise KalshiAuthError("Invalid private key format") from exc
        except OSError as exc:
            raise KalshiAuthError(f"Could not read KALSHI_PRIVATE_KEY_PATH: {path}") from exc
        return self._private_key

    def sign(self, timestamp_ms: str, method: str, path: str) -> str:
        key = self._load_private_key()
        path_without_query = path.split("?", 1)[0]
        message = f"{timestamp_ms}{method.upper()}{path_without_query}".encode("utf-8")
        signature = key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def headers(self, method: str, path: str) -> dict[str, str]:
        self.require_credentials()
        timestamp = str(epoch_ms())
        signature = self.sign(timestamp, method, path)
        return {
            "KALSHI-ACCESS-KEY": self.settings.kalshi_api_key_id or "",
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": signature,
        }

    def headers_for_url(self, method: str, url: str) -> dict[str, str]:
        return self.headers(method, urlparse(url).path)

