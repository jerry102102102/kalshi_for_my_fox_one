from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _decimal(name: str, default: str) -> Decimal:
    return Decimal(os.getenv(name, default))


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


@dataclass(frozen=True)
class Settings:
    enable_live_trading: bool = False
    no_video_capture: bool = True

    kalshi_env: str = "production"
    kalshi_base_url: str = "https://external-api.kalshi.com/trade-api/v2"
    kalshi_ws_url: str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
    kalshi_api_key_id: str | None = None
    kalshi_private_key_path: str | None = None

    mlb_base_url: str = "https://statsapi.mlb.com/api/v1"

    odds_api_key: str | None = None
    odds_base_url: str = "https://api.the-odds-api.com/v4"
    odds_region: str = "us"

    duckdb_path: Path = Path("data/kalshi_mlb.duckdb")
    parquet_dir: Path = Path("data/parquet")
    reports_dir: Path = Path("data/reports")

    safety_margin: Decimal = Decimal("0.03")
    slippage_buffer: Decimal = Decimal("0.01")
    max_spread: Decimal = Decimal("0.08")
    max_contracts_per_trade: int = 5
    max_position_per_market: int = 20
    max_open_markets: int = 3
    max_daily_loss_usd: Decimal = Decimal("25")
    min_book_depth: int = 5
    max_data_staleness_ms: int = 3000
    max_model_uncertainty_width: Decimal = Decimal("0.15")


def load_settings() -> Settings:
    kalshi_env = os.getenv("KALSHI_ENV", "production").strip().lower()
    default_base_url = (
        "https://external-api.demo.kalshi.co/trade-api/v2"
        if kalshi_env == "demo"
        else Settings.kalshi_base_url
    )
    default_ws_url = (
        "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"
        if kalshi_env == "demo"
        else Settings.kalshi_ws_url
    )
    return Settings(
        enable_live_trading=_bool("ENABLE_LIVE_TRADING", False),
        no_video_capture=_bool("NO_VIDEO_CAPTURE", True),
        kalshi_env=kalshi_env,
        kalshi_base_url=os.getenv("KALSHI_BASE_URL", default_base_url),
        kalshi_ws_url=os.getenv("KALSHI_WS_URL", default_ws_url),
        kalshi_api_key_id=os.getenv("KALSHI_API_KEY_ID") or None,
        kalshi_private_key_path=os.getenv("KALSHI_PRIVATE_KEY_PATH") or None,
        mlb_base_url=os.getenv("MLB_BASE_URL", Settings.mlb_base_url),
        odds_api_key=os.getenv("ODDS_API_KEY") or None,
        odds_base_url=os.getenv("ODDS_BASE_URL", Settings.odds_base_url),
        odds_region=os.getenv("ODDS_REGION", Settings.odds_region),
        duckdb_path=Path(os.getenv("DUCKDB_PATH", str(Settings.duckdb_path))),
        parquet_dir=Path(os.getenv("PARQUET_DIR", str(Settings.parquet_dir))),
        reports_dir=Path(os.getenv("REPORTS_DIR", str(Settings.reports_dir))),
        safety_margin=_decimal("SAFETY_MARGIN", "0.03"),
        slippage_buffer=_decimal("SLIPPAGE_BUFFER", "0.01"),
        max_spread=_decimal("MAX_SPREAD", "0.08"),
        max_contracts_per_trade=_int("MAX_CONTRACTS_PER_TRADE", 5),
        max_position_per_market=_int("MAX_POSITION_PER_MARKET", 20),
        max_open_markets=_int("MAX_OPEN_MARKETS", 3),
        max_daily_loss_usd=_decimal("MAX_DAILY_LOSS_USD", "25"),
        min_book_depth=_int("MIN_BOOK_DEPTH", 5),
        max_data_staleness_ms=_int("MAX_DATA_STALENESS_MS", 3000),
        max_model_uncertainty_width=_decimal("MAX_MODEL_UNCERTAINTY_WIDTH", "0.15"),
    )
