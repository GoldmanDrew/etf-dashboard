# Bucket 4 Backtest Dashboard Plan

## Product goal

Make Bucket 4 understandable at a glance: one page for the production book, one chart tab for the per-ticker production replay, and a clear escape hatch to the existing inverse ticker flow simulator.

## UX shape

- Keep the first screen focused on decisions: CAGR, drawdown, vol/Sharpe, active pairs, policy hash, and the equity path.
- Use plain labels: "Bucket 4 inverse decay", "Production weights", "Custom book", "B4 net edge", and "VCR/TR cadence".
- Avoid exposing old project names in user-facing copy. Use `ls-algo production method` or `inverse_decay_bucket4`.
- Treat custom weights as a what-if tool. The UI reblends precomputed unit returns; it does not rerun Kelly/QCQP or the ratchet solver.
- Keep the legacy inverse ticker flow simulator available on chart pages under Backtest Flow.

## Static-site implementation

Production stays fully static on GitHub Pages:

- Python builds `data/bucket4_backtest.json`.
- The JSON keeps the portfolio replay fields used by the old page.
- Schema v2 adds `default_weights`, `universes`, and compact per-pair daily paths under `pair_series`.
- The browser recomputes custom books from `pair_series` and stores local settings in `localStorage`.

## Delivered slice

- B4 artifact schema v2 with per-pair daily returns, h path, costs, drawdown, and rebalance logs for production-book pairs.
- B4 book lab controls: include/exclude, editable weights, production/equal/edge-weight presets, sorting, contribution, and persisted state.
- B4 chart tab uses the precomputed `inverse_decay_bucket4` path when available.
- Ticker headers show "Bucket 4 - Inverse decay" for `screener_bucket === "bucket_4"`.
- User-facing old project references were removed from the B4 surfaces touched here.

## Next expansion

- Emit one `data/bucket4_pairs/{ETF}.json` shard per screener Bucket 4 row, including gated-out names and gate reasons.
- Add a pair selector drawer with h, drawdown, leg/cost diagnostics, and rebalance log expansion.
- Add an ls-algo import parity test when CI has the checkout available.
- Add production/custom overlay and contribution stacked-area charts once the all-pair shard set is in place.
