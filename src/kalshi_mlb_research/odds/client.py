from __future__ import annotations

from dataclasses import dataclass

import httpx

from kalshi_mlb_research.config import Settings, load_settings
from kalshi_mlb_research.exceptions import ExternalServiceError


@dataclass
class OddsClient:
    settings: Settings | None = None
    timeout_seconds: float = 15.0

    def __post_init__(self) -> None:
        self.settings = self.settings or load_settings()
        self._client = httpx.Client(base_url=self.settings.odds_base_url, timeout=self.timeout_seconds)

    def close(self) -> None:
        self._client.close()

    def odds(
        self,
        sport: str = "baseball_mlb",
        regions: str | None = None,
        markets: str = "h2h,spreads,totals",
        odds_format: str = "american",
    ) -> list[dict]:
        if not self.settings.odds_api_key:
            raise ExternalServiceError("ODDS_API_KEY is required for live odds ingestion")
        params = {
            "regions": regions or self.settings.odds_region,
            "markets": markets,
            "oddsFormat": odds_format,
            "apiKey": self.settings.odds_api_key,
        }
        response = self._client.get(f"/sports/{sport}/odds", params=params)
        if response.status_code >= 400:
            raise ExternalServiceError(f"Odds API failed: {response.status_code} {response.text}")
        return response.json()

