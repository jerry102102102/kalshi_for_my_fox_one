from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import websockets

from kalshi_mlb_research.config import Settings, load_settings
from kalshi_mlb_research.kalshi.auth import KalshiAuth

MessageHandler = Callable[[dict], Awaitable[None]]


@dataclass
class KalshiWebSocketClient:
    settings: Settings | None = None
    auth_headers: dict[str, str] | None = None

    def __post_init__(self) -> None:
        self.settings = self.settings or load_settings()
        self._message_id = 1
        self.auth = KalshiAuth(self.settings)

    async def stream(
        self,
        channels: list[str],
        market_tickers: list[str] | None,
        handler: MessageHandler,
    ) -> None:
        headers = self.auth_headers or self.auth.headers("GET", "/trade-api/ws/v2")
        async with websockets.connect(self.settings.kalshi_ws_url, additional_headers=headers) as websocket:
            params: dict[str, object] = {"channels": channels}
            if market_tickers:
                params["market_tickers"] = market_tickers
            await websocket.send(json.dumps({"id": self._next_id(), "cmd": "subscribe", "params": params}))
            async for message in websocket:
                await handler(json.loads(message))

    async def record_for(
        self,
        channels: list[str],
        market_tickers: list[str],
        duration_seconds: int,
        handler: MessageHandler,
    ) -> None:
        await asyncio.wait_for(self.stream(channels, market_tickers, handler), timeout=duration_seconds)

    def _next_id(self) -> int:
        value = self._message_id
        self._message_id += 1
        return value
