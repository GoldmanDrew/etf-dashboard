# Borrow Spike Evaluation — Operator Guide

## Artifacts (auto-refreshed on nightly / build-and-deploy)

| File | Purpose |
|------|---------|
| `data/borrow_spike_eval.json` | Replay metrics (L0 + L2), model comparison, findings |
| `data/borrow_spike_tracking.json` | **Milestone tracker** — what's done vs blocked |
| `data/borrow_forecast_latest.json` | Per-symbol 5-obs borrow drift estimate |
| `data/borrow_spike_replay_panel.parquet` | Walk-forward v1 L0 replay |
| `data/borrow_spike_replay_l2_panel.parquet` | Walk-forward v2 L2 calibrated replay |

## Two labels

- **L0 (trader headline):** Catastrophic jump — `future_max > max(1, 3×med60, p99)` and jump >25pp. Extremely rare.
- **L2 (model dev):** Relative stress — `future_max > p90_60` and jump >10pp. Used for **logistic_v2** training and CI gates.

## Models

- **logistic_v1_l0:** Legacy borrow+shares features, L0 label → `p_spike_5d`
- **logistic_v2_l2:** Adds supply/scale (`utilization_proxy`, `shares_drop3`, `log_aum`, …), L2 label → `p_spike_5d_l2_calibrated`
- **Isotonic calibration** on L2 holdout → alert tiers: watch ≥5%, elevated ≥12%, high ≥25%

## Best estimate of future borrow (simple)

1. **Level:** Current `borrow_current` from IBKR/git (best single predictor).
2. **Drift:** `borrow_forecast_delta_5d_p50` — pooled OLS on borrow momentum (~5% R²).
3. **Spike stress:** `p_spike_5d_l2_calibrated` — use **tier** (watch/elevated/high), not raw probability for L0.

## Pipeline

```bash
python scripts/borrow_spike_pipeline.py
```

Runs dual replay, forecast, eval JSON, and `borrow_spike_tracking.json`.

## Check progress

Open `data/borrow_spike_tracking.json` → `milestones` and `next_actions`.
