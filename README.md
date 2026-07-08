# Kalshi MLB Research

Research-only pipeline for MLB game state, Kalshi market data, executable-price edge analysis, replay, and paper trading.

This project does not place real-money orders and does not capture, scrape, OCR, archive, or analyze FOX One video/audio.

## 1. Setup

```bash
uv venv --python 3.11
source .venv/bin/activate
uv pip install ".[dev]"
uv run pytest
uv run ksr --help
```

Use non-editable install (`uv pip install ".[dev]"`) for the `ksr` console script.

## 2. Kalshi Credentials

Public market endpoints can work without credentials. WebSocket market data and account checks require Kalshi API credentials:

```bash
export KALSHI_ENV=production        # or demo
export KALSHI_API_KEY_ID="..."
export KALSHI_PRIVATE_KEY_PATH="/secure/path/kalshi.key"
```

Check auth:

```bash
uv run ksr check-kalshi-auth
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
uv run ksr record-mlb --date today --duration 1800
uv run ksr inspect-mlb-games --date today
```

If there are no live games, the command stores the schedule and prints `no live games currently` plus the next scheduled game time. To target one game:

```bash
uv run ksr record-mlb --date today --game-pk <GAME_PK> --duration 1800
```

## 4. Kalshi Recording

Find candidate markets:

```bash
uv run ksr discover-markets --query mlb --status open
```

Record a real ticker:

```bash
uv run ksr record-kalshi --tickers <REAL_KALSHI_TICKER> --duration 1800 --book-snapshots-interval 30
uv run ksr inspect-book --ticker <REAL_KALSHI_TICKER>
```

Recorder behavior:

- REST order book snapshot is stored first.
- If credentials exist, WebSocket is attempted.
- If WebSocket fails or credentials are missing, REST polling is used and `source_mode=polling` is recorded.
- Raw payloads and normalized executable book fields are stored.

## 5. Manual Mapping

Do not rely on fuzzy matching for first-pass research. Manually map a real MLB game to a real Kalshi ticker:

```bash
uv run ksr map-market --game-pk <GAME_PK> --ticker <KALSHI_TICKER> --market-type GAME_WINNER --manual
uv run ksr inspect-mapping --date today
```

If the ticker title does not appear to match the game teams, the mapping is still stored but marked with a warning.

## 6. Report Generation

Reports use stored DuckDB rows, not fixed text.

```bash
uv run ksr report-data-quality --date today
uv run ksr report-latency --date today
uv run ksr report-edge --date today
```

Outputs:

```text
data/reports/<date>/data_quality.md
data/reports/<date>/data_quality.csv
data/reports/<date>/latency_report.md
data/reports/<date>/latency_events.csv
data/reports/<date>/edge_report.md
data/reports/<date>/edge_samples.csv
```

If data is insufficient, reports say why, for example: no mapped markets, no MLB events, or no Kalshi snapshots around event windows.

## 7. Paper Trading

Paper trading uses stored MLB states, manual mappings, and Kalshi books. It does not place live orders.

```bash
uv run ksr paper-trade --date today --duration 1800
uv run ksr report-pnl --run-id <RUN_ID>
```

Written tables include `paper_orders`, `paper_fills`, `paper_positions`, `paper_equity`, and `skip_log`.

## 8. Replay

Replay applies a latency delay to stored MLB state timestamps and uses executable VWAP from the recorded Kalshi book.

```bash
uv run ksr replay --date today --latency-ms 1000
```

Output includes `run_id`, trade/fill/skip counts, fees, PnL fields, and drawdown placeholder.

## 9. Known Limitations

- Kalshi WebSocket delta reconstruction is conservative: raw WS messages are stored, and normalized snapshots are refreshed through REST after WS updates.
- The MLB win probability model is a hand-built baseline, not trained/calibrated on historical MLB data.
- Edge and paper trading reports require real mapping plus overlapping MLB/Kalshi timestamps.
- No true live trading executor is implemented; the guard intentionally rejects live orders.
- If there is no live MLB game or no open Kalshi MLB market, latency/edge reports will be empty with blocking reasons.

## 10. Troubleshooting

- `Missing KALSHI_API_KEY_ID`: set `KALSHI_API_KEY_ID`.
- `Missing KALSHI_PRIVATE_KEY_PATH`: set a readable PEM private key path.
- `Invalid private key format`: verify the downloaded Kalshi private key file.
- `No stored Kalshi snapshots`: run `record-kalshi` with a real ticker first.
- `No live games currently`: choose a time with active MLB games or record a different date/game.
- `no mapped markets`: run `map-market --manual`.
- To confirm this is not demo placeholder output, inspect DuckDB/report rows for real tickers, real MLB `game_pk`, nonzero snapshot/state counts, and `source_mode` values.

