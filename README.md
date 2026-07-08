# Kalshi MLB Research

Research-only pipeline for MLB game state, Kalshi market data, executable-price edge analysis, replay, and paper trading.

This project does not place real-money orders and does not capture, scrape, OCR, archive, or analyze FOX One video/audio.

## 1. Setup

```bash
uv venv --python 3.11
source .venv/bin/activate
uv pip install ".[dev]"
uv run --no-editable pytest
uv run --no-editable ksr --help
```

Use `uv run --no-editable` for the `ksr` console script so it is installed as a package instead of relying on editable `src/` path handling.

## 2. Kalshi Credentials

Public market endpoints can work without credentials. WebSocket market data and account checks require Kalshi API credentials:

```bash
export KALSHI_ENV=production        # or demo
export KALSHI_API_KEY_ID="..."
export KALSHI_PRIVATE_KEY_PATH="/secure/path/kalshi.key"
```

Check auth:

```bash
uv run --no-editable ksr check-kalshi-auth
```

Expected failure without credentials:

```text
Kalshi auth: FAILED
reason: Missing KALSHI_API_KEY_ID
```

The private key is read from disk and never logged.

## 3. MLB Recording

Record live games for today:

```bash
uv run --no-editable ksr record-mlb --date today --duration 1800
uv run --no-editable ksr inspect-mlb-games --date today
```

If there are no live games, the command stores the schedule and prints `no live games currently` plus the next scheduled game time. To target one game:

```bash
uv run --no-editable ksr record-mlb --date today --game-pk <GAME_PK> --duration 1800
```

## 4. Kalshi Recording

Find candidate markets:

```bash
uv run --no-editable ksr discover-markets --query mlb --status open
```

Record a real ticker:

```bash
uv run --no-editable ksr record-kalshi --tickers <REAL_KALSHI_TICKER> --duration 1800 --book-snapshots-interval 30
uv run --no-editable ksr inspect-book --ticker <REAL_KALSHI_TICKER>
```

Recorder behavior:

- REST order book snapshot is stored first.
- If credentials exist, WebSocket is attempted.
- If WebSocket fails or credentials are missing, REST polling is used and `source_mode=polling` is recorded.
- Raw payloads and normalized executable book fields are stored.

## 5. Manual Mapping

Do not rely on fuzzy matching for first-pass research. Manually map a real MLB game to a real Kalshi ticker:

```bash
uv run --no-editable ksr map-market --game-pk <GAME_PK> --ticker <KALSHI_TICKER> --market-type GAME_WINNER --manual
uv run --no-editable ksr inspect-mapping --date today
```

If the ticker title does not appear to match the game teams, the mapping is still stored but marked with a warning.

## 6. Report Generation

Reports use stored DuckDB rows, not fixed text.

```bash
uv run --no-editable ksr record-odds --sport mlb --date today
uv run --no-editable ksr report-data-quality --date today
uv run --no-editable ksr report-latency --date today
uv run --no-editable ksr report-edge --date today
uv run --no-editable ksr report-backtest-readiness --date today
uv run --no-editable ksr compare-latency --date today
uv run --no-editable ksr report-market-replay-readiness --date today
uv run --no-editable ksr report-validation-summary --date today
uv run --no-editable ksr export-parquet
```

Outputs:

```text
data/reports/<date>/data_quality.md
data/reports/<date>/data_quality.csv
data/reports/<date>/latency_report.md
data/reports/<date>/latency_events.csv
data/reports/<date>/edge_report.md
data/reports/<date>/edge_samples.csv
data/reports/<date>/backtest_readiness.md
data/reports/<date>/backtest_readiness.csv
data/reports/<date>/latency_comparison.md
data/reports/<date>/latency_comparison.csv
data/reports/<date>/market_replay_readiness.md
data/reports/<date>/market_replay_readiness.csv
data/reports/<date>/first_real_validation_summary.md
data/reports/<date>/first_real_validation_summary.csv
data/parquet/*.parquet
```

If data is insufficient, reports say why, for example: no mapped markets, no MLB events, or no Kalshi snapshots around event windows.

## 7. Paper Trading

Paper trading uses stored MLB states, manual mappings, and Kalshi books. It does not place live orders.

```bash
uv run --no-editable ksr paper-trade --date today --duration 1800
uv run --no-editable ksr report-pnl --run-id <RUN_ID>
```

Written tables include `paper_orders`, `paper_fills`, `paper_positions`, `paper_equity`, and `skip_log`.

## 8. Replay

Replay applies a latency delay to stored MLB state timestamps and uses executable VWAP from the recorded Kalshi book.

```bash
uv run --no-editable ksr replay --date today --latency-ms 1000
```

If there are no edge samples, replay returns `INSUFFICIENT_BACKTEST_DATA` with no `run_id`; `0 trades / 0 fills` is not treated as a completed backtest.

## 9. Historical Backtest

Build a model-only replay dataset from completed MLB games, then run the coarse probability-model backtest:

```bash
uv run --no-editable ksr build-historical-replay-dataset --date <YYYY-MM-DD>
uv run --no-editable ksr backtest-model-only --date <YYYY-MM-DD>
uv run --no-editable ksr report-market-replay-readiness --date <YYYY-MM-DD>
```

Model-only backtest uses final game result as the label. Market replay still requires manual mapping plus overlapping Kalshi historical orderbook/trade data.

For season-to-date validation, build the MLB research database first, then run model and market feasibility reports:

```bash
uv run --no-editable ksr build-mlb-season-database --start-date 2026-03-01 --end-date 2026-07-07
uv run --no-editable ksr backtest-model-only --start-date 2026-03-01 --end-date 2026-07-07
uv run --no-editable ksr build-kalshi-historical-database --start-date 2026-03-01 --end-date 2026-07-07 --keywords baseball,MLB,Yankees,Rays,Cubs,Orioles,Dodgers,Mets,Astros,Giants,Phillies,Reds,Nationals,"Blue Jays"
uv run --no-editable ksr report-season-market-mapping --start-date 2026-03-01 --end-date 2026-07-07
uv run --no-editable ksr report-trading-backtest-feasibility --start-date 2026-03-01 --end-date 2026-07-07
uv run --no-editable ksr backtest-trading-candle-level --start-date 2026-03-01 --end-date 2026-07-07
uv run --no-editable ksr report-season-validation-summary
```

Season reports are written under `data/reports/season_to_date/`. Candle-level replay is a proxy only; full orderbook replay requires live-recorded orderbook snapshots over the game windows.

## 10. Known Limitations

- Kalshi WebSocket delta reconstruction is conservative: raw WS messages are stored, and normalized snapshots are refreshed through REST after WS updates.
- The MLB win probability model is a hand-built baseline, not trained/calibrated on historical MLB data.
- Edge and paper trading reports require real mapping plus overlapping MLB/Kalshi timestamps.
- Kalshi historical markets/candles/trades can support market availability checks, but historical full orderbook replay is not available unless orderbook snapshots were recorded live.
- No true live trading executor is implemented; the guard intentionally rejects live orders.
- If there is no live MLB game or no open Kalshi MLB market, latency/edge reports will be empty with blocking reasons.

## 11. Troubleshooting

- `Missing KALSHI_API_KEY_ID`: set `KALSHI_API_KEY_ID`.
- `Missing KALSHI_PRIVATE_KEY_PATH`: set a readable PEM private key path.
- `Invalid private key format`: verify the downloaded Kalshi private key file.
- `No stored Kalshi snapshots`: run `record-kalshi` with a real ticker first.
- `No live games currently`: choose a time with active MLB games or record a different date/game.
- `no mapped markets`: run `map-market --manual`.
- To confirm this is not demo placeholder output, inspect DuckDB/report rows for real tickers, real MLB `game_pk`, nonzero snapshot/state counts, and `source_mode` values.
