# `data/nav_forecasts/` — multi-model NAV forecast pipeline

The forecaster (`scripts/forecast_nav.py`) emits **one row per (symbol, model)**
into `snapshots/<DATE>.jsonl` every 30 minutes during the US session, and the
scorer (`scripts/score_nav_forecasts.py`) settles them against the next-day
official NAV at 5 AM ET. The dashboard's Stats tab shows the **default
routed model** per symbol; every other applicable model is logged alongside
it so we can A/B compare them as the realized record grows.

## Models in flight

| Model tag | Math | Inputs | Notes |
|---|---|---|---|
| `delta_v1` | `nav_anchor · exp(β · log(spot/anchor)) · (1 − TER_d)` | β, anchor NAV, anchor + live underlying spot | Baseline closed-form for LETF / inverse / volatility / passive-β rows |
| `delta_v2_ito` | `delta_v1 · exp(−(β² − β)/2 · σ² · Δt)` | + `forecast_vol_underlying_annual`, business-day Δt since anchor | Adds path-dependent vol drag — pulls down LETFs (β > 1) and inverse (β < 0) |
| `delta_v3_swap_mark` | `(nav_anchor + Δmv / shares) · (1 − TER_d · Δt·252)` | `etf_holdings_latest.json` (per-leg shares + price + market_value) + spot lookups via `options_cache` | Δmv summed leg-by-leg from holdings (delta differencing — see "Why delta?" below). OPTION legs carry forward at zero delta. |
| `yieldboost_putspread_v1` | Same as v3 but OPTION legs are repriced from the matched contract's `mid` in `options_cache` | + parsed OCC components | Routed as default for `income_yieldboost` / `income_put_spread` rows |

### Default routing (used by `_latest.json[by_symbol]`)

```
income_yieldboost,        yieldboost_putspread_v1
income_put_spread      -> delta_v3_swap_mark
                       -> delta_v2_ito
                       -> delta_v1

letf, inverse,            delta_v3_swap_mark
volatility_etp,        -> delta_v2_ito
passive_low_beta,      -> delta_v1
other_structured,
(everything else)
```

A candidate is accepted only if its `nav_hat / nav_anchor` lands in
**[0.5, 2.0]**. If every applicable candidate violates that envelope the
dispatcher refuses to surface a default (`by_symbol[SYM]` is omitted) — the
full per-model rows still land in `by_symbol_models[SYM]` for offline review.

### Why delta differencing in v3 / yieldboost?

Issuer holdings files report **gross** long equity exposure: a 2× LETF on
AAPL shows ~2× AUM worth of swap notional priced at AAPL's level, but the
offsetting swap counterparty liability is *not* in the file (it is netted
into "cash" lines that don't represent free cash). Summing
`market_value` therefore equals gross exposure, not NAV. Per-leg delta
differencing (`Σ shares · (spot_now − spot_anchor)`) cancels the unobserved
counterparty leg under the assumption that it is constant between two
nearby snapshots — true to first order over a single trading day.

## File layout

| File | Producer | Consumer | Cadence |
|---|---|---|---|
| `_anchors.json` | `score_nav_forecasts.py` (5 AM ET) | `forecast_nav.py` | daily |
| `snapshots/<DATE>.jsonl` | `forecast_nav.py` | scoring; offline audits | every 30 min |
| `_latest.json` | `forecast_nav.py` | `index.html` Stats tab | every 30 min |
| `realized/<DATE>.jsonl` | `score_nav_forecasts.py` | rolling stats build | daily |
| `_metrics_daily.json` | `score_nav_forecasts.py` | `index.html` accuracy stat | daily |
| `_history_panel.json` | `score_nav_forecasts.py` | `index.html` NAV-vs-model chart | daily |

## Schemas

### `_anchors.json`

```json
{
  "as_of_date": "2026-04-27",
  "build_time": "2026-04-28T10:05:00Z",
  "n_total": 415,
  "n_with_und_close": 410,
  "n_with_shares": 415,
  "by_symbol": {
    "TSLL": {
      "as_of_date": "2026-04-27",
      "nav_close": 12.41,
      "etf_close": 12.42,
      "shares_outstanding": 5000000,
      "und_symbol": "TSLA",
      "und_close": 229.30,
      "beta": 2.0,
      "product_class": "letf",
      "is_yieldboost": false
    }
  }
}
```

### `snapshots/<DATE>.jsonl` — one record per `(symbol, model, snap)`

Common fields (every model):

```json
{
  "ts": "2026-04-28T13:35:00Z",
  "symbol": "TSLL",
  "model": "delta_v3_swap_mark",
  "is_default": true,
  "confidence": "high",
  "product_class": "letf",
  "und_symbol": "TSLA",
  "und_spot_t": 232.10,
  "und_spot_anchor": 229.30,
  "und_anchor_date": "2026-04-27",
  "und_spot_age_sec": 35.0,
  "beta": 2.0,
  "ter_daily": 0.000038889,
  "nav_anchor": 12.41,
  "nav_anchor_date": "2026-04-27",
  "nav_hat": 12.66,
  "etf_last": 12.65,
  "etf_last_ts": "2026-04-28T13:34:55Z",
  "premium_bp": -7.9,
  "notes": null
}
```

Model-specific extras:

* `delta_v2_ito` adds `sigma_annual`, `dt_years`, `vol_drag_logret`, `sigma_source`.
* `delta_v3_swap_mark` and `yieldboost_putspread_v1` add
  `legs_priced` / `legs_total`, `equity_legs_priced` / `equity_legs_total`,
  `option_legs_priced` / `option_legs_total`, `mv_total_now`,
  `shares_outstanding`, `holdings_skipped` (capped at 10 entries).

`confidence` ∈ {`high`, `medium`, `na`}:

* `high` — fresh inputs, full coverage.
* `medium` — at least one degraded input (stale spot, sub-50% leg coverage,
  fallback product class, etc.).
* `na` — model could not produce a number; row still emitted so the
  snapshot documents the attempt.

### `_latest.json`

```json
{
  "build_time": "2026-04-28T13:35:00Z",
  "models_run": ["delta_v1", "delta_v2_ito", "delta_v3_swap_mark", "yieldboost_putspread_v1"],
  "default_models_count": {"delta_v3_swap_mark": 80, "delta_v2_ito": 200, "delta_v1": 5, "yieldboost_putspread_v1": 50, "na": 85},
  "confidence_count": {"high": 200, "medium": 135, "na": 85},
  "anchor_date": "2026-04-27",
  "anchor_symbols": 415,
  "holdings_symbols": 220,
  "by_symbol": { "TSLL": { ...default-model row... } },
  "by_symbol_models": { "TSLL": { "delta_v1": {...}, "delta_v2_ito": {...}, "delta_v3_swap_mark": {...}, "yieldboost_putspread_v1": {...} } },
  "default_model_for_symbol": { "TSLL": "delta_v3_swap_mark" }
}
```

The dashboard reads only `by_symbol[SYM]`; A/B inspection happens against
`by_symbol_models[SYM][MODEL]` after enough realized days accumulate.

### `realized/<DATE>.jsonl` — one record per `(symbol, model, date)`

```json
{
  "date": "2026-04-28",
  "symbol": "TSLL",
  "model": "delta_v3_swap_mark",
  "is_default": true,
  "product_class": "letf",
  "nav_hat_close": 12.66,
  "nav_actual": 12.71,
  "close_actual": 12.69,
  "err_bp": -39.4,
  "abs_err_bp": 39.4,
  "premium_bp_at_snap": 21.5,
  "ts_snap": "2026-04-28T19:55:00Z"
}
```

### `_metrics_daily.json`

```json
{
  "build_time": "2026-04-29T10:10:00Z",
  "window_trading_days": 60,
  "by_symbol": { "TSLL": { ...default-model stats... } },
  "by_symbol_models": {
    "TSLL": {
      "delta_v1": {"model": "delta_v1", "n": 14, "median_abs_err_bp": 31.4, ...},
      "delta_v2_ito": {"model": "delta_v2_ito", "n": 14, "median_abs_err_bp": 18.9, ...},
      "delta_v3_swap_mark": {"model": "delta_v3_swap_mark", "n": 14, "median_abs_err_bp": 12.7, ...}
    }
  },
  "default_model_for_symbol": { "TSLL": "delta_v3_swap_mark" }
}
```

`by_symbol[SYM]` (UI compatibility) is the stats block for whichever model
the forecaster routed as default the last time it ran.

### `_history_panel.json`

```json
{
  "build_time": "2026-04-29T10:10:00Z",
  "window_trading_days": 60,
  "by_symbol": { "TSLL": [{"date": "2026-04-21", "nav_actual": 12.10, "nav_hat_close": 12.13, "close": 12.14, "err_bp": 24.8}, ...] },
  "by_symbol_models": { "TSLL": { "delta_v1": [...], "delta_v2_ito": [...], "delta_v3_swap_mark": [...] } }
}
```

## Disabling the yfinance fetch

Anchor builder calls `yfinance.download` once per scoring run. Set
`NAV_FORECAST_DISABLE_YFINANCE=1` to skip it (used by tests + offline
runs). Resulting anchors will simply have `und_close = null`; the
forecaster downgrades affected symbols to `confidence = "na"` for the
beta-based models, and `delta_v3_swap_mark` / `yieldboost_putspread_v1`
keep working since they don't depend on `und_close`.

## Adding a new model version

1. Add a `compute_<tag>` helper in `scripts/forecast_nav.py`.
2. Add `build_<tag>` that returns a `ForecastRecord` with the new model
   tag (use `_make_record` so the per-model diagnostics flow through).
3. Append it to the list in `build_forecasts_for_symbol`.
4. Slot the new tag into the appropriate `select_default_model` order if
   it should become a default candidate; otherwise it stays a parallel
   A/B model.
5. Push — `score_nav_forecasts.update_metrics_daily` automatically picks
   up the new tag in `by_symbol_models[SYM][NEW_TAG]` so the rolling
   accuracy comparison "just works".

Older snapshots keep their original `model` tag, so retrospective
accuracy comparisons stay honest.
