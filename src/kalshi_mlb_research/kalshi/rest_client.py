from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import httpx

from kalshi_mlb_research.config import Settings, load_settings
from kalshi_mlb_research.exceptions import ExternalServiceError
from kalshi_mlb_research.kalshi.auth import KalshiAuth, KalshiAuthError


@dataclass
class KalshiRestClient:
    settings: Settings | None = None
    timeout_seconds: float = 15.0

    def __post_init__(self) -> None:
        self.settings = self.settings or load_settings()
        self._client = httpx.Client(base_url=self.settings.kalshi_base_url, timeout=self.timeout_seconds)
        self.auth = KalshiAuth(self.settings)

    def close(self) -> None:
        self._client.close()

    def _get(self, path: str, params: dict[str, Any] | None = None, authenticated: bool = False) -> dict:
        headers = self._auth_headers("GET", path) if authenticated else None
        response = self._client.get(path, params=params, headers=headers)
        if response.status_code >= 400:
            raise ExternalServiceError(f"Kalshi GET {path} failed: {response.status_code} {response.text}")
        return response.json()

    def _auth_headers(self, method: str, path: str) -> dict[str, str]:
        request = self._client.build_request(method, path)
        sign_path = urlparse(str(request.url)).path
        return self.auth.headers(method, sign_path)

    def list_markets(
        self,
        query: str | None = None,
        status: str | None = None,
        series_ticker: str | None = None,
        limit: int = 200,
    ) -> list[dict]:
        params: dict[str, Any] = {"limit": limit}
        if status:
            params["status"] = status
        if series_ticker:
            params["series_ticker"] = series_ticker
        if query:
            params["search"] = query
        data = self._get("/markets", params=params)
        markets = data.get("markets", [])
        if query:
            lowered = query.lower()
            markets = [
                market
                for market in markets
                if lowered in str(market.get("title", "")).lower()
                or lowered in str(market.get("ticker", "")).lower()
                or lowered in str(market.get("event_title", "")).lower()
            ]
        return markets

    def get_market(self, ticker: str) -> dict:
        data = self._get(f"/markets/{ticker}")
        return data.get("market", data)

    def get_orderbook(self, ticker: str, depth: int = 0) -> dict:
        return self._get(f"/markets/{ticker}/orderbook", params={"depth": depth})

    def get_trades(self, ticker: str, limit: int = 100) -> list[dict]:
        data = self._get("/markets/trades", params={"ticker": ticker, "limit": limit})
        return data.get("trades", [])

    def get_balance(self) -> dict:
        return self._get("/portfolio/balance", authenticated=True)

    def check_auth(self) -> dict:
        market_ok = False
        account_reachable = False
        reason = None
        try:
            self._get("/markets", params={"limit": 1})
            market_ok = True
        except Exception as exc:
            reason = f"market data endpoint failed: {exc}"
        try:
            self.get_balance()
            account_reachable = True
        except KalshiAuthError as exc:
            reason = str(exc)
        except ExternalServiceError as exc:
            reason = str(exc)
        except httpx.HTTPError as exc:
            reason = f"Network error: {exc}"
        return {
            "ok": market_ok and account_reachable,
            "environment": self.settings.kalshi_env,
            "account_endpoint_reachable": account_reachable,
            "market_data_endpoint_reachable": market_ok,
            "reason": reason,
        }
