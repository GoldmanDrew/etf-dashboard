# ls-algo: split-aware gross decay port

**Status:** etf-dashboard implements the canonical TR path; ls-algo should mirror it.

## Problem

Forward splits such as **APLX 3-for-1** (2026-03-10) produce Yahoo panels where:

- `close` is continuous through the event
- `etf_adj_close` is back-adjusted pre-split, then switches to raw post-split

Using adj blindly creates a ~3× TR cliff and corrupts realized gross decay / Decay tab horizons.

## Canonical implementation (etf-dashboard)

| Module | Role |
|--------|------|
| `scripts/split_adjustments.py` | `yahoo_adj_looks_back_adjusted`, `detect_adj_basis_switch_splits`, `adj_basis_switch_tr_price` |
| `scripts/price_basis.py` | `resolve_split_context` → `adj_basis_switch`; `build_tr_series_from_metrics` |
| `scripts/realized_gross_decay.py` | `compute_gross_decay_annual` — mean daily log-drag × 252 |

## ls-algo port checklist

1. Copy or import the adj-basis-switch detection (do **not** mechanical-scale continuous close tickers like MTYY).
2. In `daily_screener.py` gross decay loop, build TR via split-aware prices:
   - Pre-split: `etf_adj_close` (back-adjusted)
   - Post-split (adj basis switch): `adj_basis_switch_tr_price(close, mult)`
3. Keep discrete close-jump reverse splits on the existing path (APLZ, SMUP).
4. Regression fixtures: **APLX** (forward 3-for-1), **APLZ** (reverse 1-for-5), **MTYY** (continuous).

## Formula

```
daily_drag_t = β · log(R_und_t) − log(R_etf_t)
gross_decay_annual = mean(daily_drag_t) · 252
```

TR prices must share a single basis across the split boundary.
