from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import httpx

from kalshi_mlb_research.config import Settings, load_settings
from kalshi_mlb_research.exceptions import ExternalServiceError


@dataclass
class MLBClient:
    settings: Settings | None = None
    timeout_seconds: float = 15.0

    def __post_init__(self) -> None:
        self.settings = self.settings or load_settings()
        self._client = httpx.Client(base_url=self.settings.mlb_base_url, timeout=self.timeout_seconds)

    def close(self) -> None:
        self._client.close()

    def _get(self, path: str, params: dict | None = None) -> dict:
        response = self._client.get(path, params=params)
        if response.status_code >= 400:
            raise ExternalServiceError(f"MLB GET {path} failed: {response.status_code} {response.text}")
        return response.json()

    def schedule(self, target_date: date) -> list[dict]:
        data = self._get(
            "/schedule",
            params={"sportId": 1, "date": target_date.isoformat(), "hydrate": "team,linescore"},
        )
        games: list[dict] = []
        for day in data.get("dates", []):
            for game in day.get("games", []):
                teams = game.get("teams", {})
                games.append(
                    {
                        "game_pk": str(game.get("gamePk")),
                        "game_date": game.get("gameDate"),
                        "status": game.get("status", {}).get("detailedState"),
                        "home_team": teams.get("home", {}).get("team", {}).get("name"),
                        "away_team": teams.get("away", {}).get("team", {}).get("name"),
                        "raw": game,
                    }
                )
        return games

    def live_game(self, game_pk: str) -> dict:
        try:
            return self._get(f"/game/{game_pk}/feed/live")
        except ExternalServiceError:
            v11_url = str(self.settings.mlb_base_url).replace("/api/v1", "/api/v1.1")
            response = self._client.get(f"{v11_url}/game/{game_pk}/feed/live")
            if response.status_code >= 400:
                raise ExternalServiceError(f"MLB GET /game/{game_pk}/feed/live failed: {response.status_code} {response.text}")
            return response.json()
