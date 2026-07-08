# MLB + Kalshi Research Plan

This repo is a research pipeline, not a live trading bot.

## Execution Order

```bash
uv run --no-editable ksr check-kalshi-auth
uv run --no-editable ksr discover-markets --query baseball --status open --limit 500
uv run --no-editable ksr record-kalshi --tickers <TICKER> --duration 3600 --book-snapshots-interval 5
uv run --no-editable ksr record-mlb --date today --duration 3600
uv run --no-editable ksr record-odds --sport mlb --date today
uv run --no-editable ksr map-market --game-pk <GAME_PK> --ticker <TICKER> --market-type GAME_WINNER --manual
uv run --no-editable ksr report-data-quality --date today
uv run --no-editable ksr report-backtest-readiness --date today
uv run --no-editable ksr report-latency --date today
uv run --no-editable ksr report-edge --date today
uv run --no-editable ksr replay --date today --latency-ms 1000
uv run --no-editable ksr compare-latency --date today
uv run --no-editable ksr build-historical-replay-dataset --date <YYYY-MM-DD>
uv run --no-editable ksr backtest-model-only --date <YYYY-MM-DD>
uv run --no-editable ksr report-market-replay-readiness --date <YYYY-MM-DD>
uv run --no-editable ksr report-validation-summary --date today
uv run --no-editable ksr export-parquet
uv run --no-editable ksr report-pnl --run-id <RUN_ID>
```

Use `uv run --no-editable` so the console script is built as an installed package instead of a broken editable `src/` path.

## Season-To-Date Validation

Use this workflow when the goal is research validation rather than today's live run:

```bash
uv run --no-editable ksr build-mlb-season-database --start-date 2026-03-01 --end-date 2026-07-07
uv run --no-editable ksr backtest-model-only --start-date 2026-03-01 --end-date 2026-07-07
uv run --no-editable ksr build-kalshi-historical-database --start-date 2026-03-01 --end-date 2026-07-07 --keywords baseball,MLB,Yankees,Rays,Cubs,Orioles,Dodgers,Mets,Astros,Giants,Phillies,Reds,Nationals,"Blue Jays"
uv run --no-editable ksr report-season-market-mapping --start-date 2026-03-01 --end-date 2026-07-07
uv run --no-editable ksr report-trading-backtest-feasibility --start-date 2026-03-01 --end-date 2026-07-07
uv run --no-editable ksr backtest-trading-candle-level --start-date 2026-03-01 --end-date 2026-07-07
uv run --no-editable ksr report-season-validation-summary
```

Season outputs are under `data/reports/season_to_date/`. `backtest-trading-candle-level` must return `CANDLE_MARKET_REPLAY_NOT_AVAILABLE` instead of fake PnL when mapped Kalshi MLB markets, candles, or market/game overlap are missing.

## Required Outputs

- `data/parquet/*.parquet`
- `data/reports/<date>/data_quality.md/csv`
- `data/reports/<date>/backtest_readiness.md/csv`
- `data/reports/<date>/latency_report.md`
- `data/reports/<date>/latency_events.csv`
- `data/reports/<date>/edge_report.md`
- `data/reports/<date>/edge_samples.csv`
- `data/reports/<date>/latency_comparison.md/csv`
- `data/reports/<date>/model_only_backtest.md`
- `data/reports/<date>/model_only_predictions.csv`
- `data/reports/<date>/calibration_bins.csv`
- `data/reports/<date>/market_replay_readiness.md/csv`
- `data/reports/<date>/first_real_validation_summary.md/csv`
- `data/reports/<date>/pnl_<RUN_ID>.md/csv`
- `data/reports/season_to_date/model_only_backtest.md`
- `data/reports/season_to_date/model_only_predictions.csv`
- `data/reports/season_to_date/calibration_bins.csv`
- `data/reports/season_to_date/market_mapping_report.md`
- `data/reports/season_to_date/market_mapping_candidates.csv`
- `data/reports/season_to_date/trading_backtest_feasibility.md/csv`
- `data/reports/season_to_date/candle_trading_backtest.md`
- `data/reports/season_to_date/candle_trades.csv`
- `data/reports/season_to_date/season_validation_summary.md/csv`

## Go / No-Go Gates

| Gate | Go | No-Go |
|---|---:|---:|
| mapped MLB games | >= 100 | < 30 |
| high-impact events | >= 1000 | < 300 |
| state snapshots | >= 5000 | < 1500 |
| Kalshi stale ratio | < 20% | > 35% |
| median spread | <= 0.05 | > 0.08 |
| average spread | <= 0.08 | > 0.12 |
| median yes bid depth | >= 5 | < 3 |
| median yes ask depth | >= 5 | < 3 |

`report-validation-summary` writes `first_real_validation_summary.md/csv` with these gates.

## Rules That Must Not Drift

- No FOX One capture.
- No live money trading.
- No fake demo fallback in reports.
- No mid-price fills. Use executable VWAP.
- Missing live MLB games, mappings, or Kalshi markets must produce blocking reasons.
- Empty replay must produce `INSUFFICIENT_BACKTEST_DATA`, not fake PnL.
- Missing season market data must produce `CANDLE_MARKET_REPLAY_NOT_AVAILABLE`, not fake candle-level PnL.
- Historical full orderbook replay requires live-recorded orderbook snapshots over the game window.
- Taker-only replay comes first. Maker simulation waits until real fills/trades justify it.
