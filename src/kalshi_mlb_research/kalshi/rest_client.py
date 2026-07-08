from __future__ import annotations

import time
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
        response = None
        for attempt in range(4):
            response = self._client.get(path, params=params, headers=headers)
            if response.status_code != 429 or attempt == 3:
                break
            retry_after = response.headers.get("retry-after")
            try:
                delay = float(retry_after) if retry_after else 1.5 * (attempt + 1)
            except ValueError:
                delay = 1.5 * (attempt + 1)
            time.sleep(min(delay, 10.0))
        assert response is not None
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

    def list_markets_page(
        self,
        *,
        cursor: str | None = None,
        limit: int = 1000,
        query: str | None = None,
        status: str | None = None,
    ) -> dict:
        params: dict[str, Any] = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        if query:
            params["search"] = query
        if status:
            params["status"] = status
        return self._get("/markets", params=params)

    def list_historical_markets_page(
        self,
        *,
        cursor: str | None = None,
        limit: int = 1000,
    ) -> dict:
        params: dict[str, Any] = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        return self._get("/historical/markets", params=params)

    def get_market(self, ticker: str) -> dict:
        data = self._get(f"/markets/{ticker}")
        return data.get("market", data)

    def get_orderbook(self, ticker: str, depth: int = 0) -> dict:
        return self._get(f"/markets/{ticker}/orderbook", params={"depth": depth})

    def get_market_candlesticks(
        self,
        series_ticker: str,
        ticker: str,
        start_ts: int,
        end_ts: int,
        period_interval: int = 1,
    ) -> dict:
        return self._get(
            f"/series/{series_ticker}/markets/{ticker}/candlesticks",
            params={"start_ts": start_ts, "end_ts": end_ts, "period_interval": period_interval},
        )

    def get_historical_market_candlesticks(
        self,
        ticker: str,
        start_ts: int,
        end_ts: int,
        period_interval: int = 1,
    ) -> dict:
        return self._get(
            f"/historical/markets/{ticker}/candlesticks",
            params={"start_ts": start_ts, "end_ts": end_ts, "period_interval": period_interval},
        )

    def get_trades(self, ticker: str, limit: int = 100) -> list[dict]:
        data = self._get("/markets/trades", params={"ticker": ticker, "limit": limit})
        return data.get("trades", [])

    def get_trades_page(
        self,
        ticker: str,
        *,
        cursor: str | None = None,
        limit: int = 1000,
        min_ts: int | None = None,
        max_ts: int | None = None,
        historical: bool = False,
    ) -> dict:
        params: dict[str, Any] = {"ticker": ticker, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        if min_ts is not None:
            params["min_ts"] = min_ts
        if max_ts is not None:
            params["max_ts"] = max_ts
        return self._get("/historical/trades" if historical else "/markets/trades", params=params)

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
