# Bucket 1 underlying views — plan

Simple discretionary overlay: express a bullish/bearish opinion on a **Bucket 1 underlying**, rank conviction on a 5-point scale, and map that to a hedge coverage `h` for every B1 pair on that name.

**Status:** local dashboard implemented (config + artifact + `#/views` tab). ls-algo EOD fetch + GTP apply landed behind `bucket1_views.enabled: false` (see [`bucket1_views_ls_algo_eod_wire_plan.md`](bucket1_views_ls_algo_eod_wire_plan.md)). Still **off** in production until ops enables the flag after a quiet dry-run.

---

## Object

- Opinion is on the **underlying stock**, not the ETF.
- All Bucket 1 pairs that share that underlying inherit the same score → `h`.
- Stays in force until changed (no horizon, no expiry).

---

## Score → hedge coverage

Score ∈ `{-2, -1, 0, 1, 2}`.

| Score | Meaning | `h` |
|------:|---------|----:|
| `-2` | Almost max short | `0.25` |
| `-1` | Lean short | `0.625` |
| `0` | No opinion (hard pile) | `1.0` |
| `+1` | Lean long | `1.25` |
| `+2` | Almost max long | `1.50` |

Bounds (clip / safety rail):

- `h_min = 0.25`
- `h_max = 1.75`

Score `+2` lands at `1.50`; `1.75` is headroom only if a future override needs it.

---

## What `h` means here

**`h` = long-underlying dollars per $1 of short ETF** (coverage ratio).

| `h` | Residual |
|----:|----------|
| `< 1` | Underhedged → residual **short** underlying |
| `= 1` | Matched notionals → **no opinion** default |
| `> 1` | Overhedged → residual **long** underlying |

Examples:

- `-2` → `h = 0.25` → mostly short ETF, barely long und
- `0` → `h = 1.0` → flat dollars, no view
- `+2` → `h = 1.50` → short ETF, overhedged long und

This is an **absolute** map from score → `h`. It does **not** multiply OLS `delta` and does **not** use `1/|δ|` as the neutral. Score `0` is always `h = 1`.

> When wiring later: Chart Backtest today uses `h_bt = |MV_ETF| / |MV_und|` (inverse of this coverage). Convert with `h_bt = 1 / h_view` (and clip), or adopt one definition everywhere. Do not mix them silently.

---

## Config shape (future)

One small YAML, keyed by underlying:

```yaml
schema: bucket1_underlying_views.v1

# Fixed ladder — generalizable, edit in one place
score_to_h:
  -2: 0.25
  -1: 0.625
  0: 1.0
  1: 1.25
  2: 1.50

h_min: 0.25
h_max: 1.75

views:
  NVDA:
    score: 2
    note: "optional free text"
    updated: "2026-07-24"
  TSLA:
    score: -1
    note: ""
    updated: "2026-07-24"
```

Missing underlying → treat as score `0` → `h = 1`.

---

## Dashboard (local)

- Tab: `#/views` (topbar **B1 Views**).
- Columns: underlying, B1 sleeves, score (± steppers), implied `h`, note, updated.
- Artifact: `data/bucket1_underlying_views.json`.
- **Save** (local FastAPI): `POST /api/bucket1-views` → writes `config/bucket1_underlying_views.yml` and rebuilds the JSON artifact. Requires `python run.py` (plain `http.server` has no write API). Hand-edit YAML + `python scripts/build_bucket1_underlying_views.py` still works.

### Ops for EOD (ls-algo)

After Save (or hand-edit), **commit + push** `config/bucket1_underlying_views.yml` to `etf-dashboard` main. The ls-algo EOD screener job fetches that file into `data/runs/<run_date>/bucket1_underlying_views.yml` (warn on miss) and `generate_trade_plan.py` reads **only** the run-dir copy when `bucket1_views.enabled` is true.

Production apply mode is **`absolute_nonzero`**: score `0` / missing leaves delta-matched B1 sizing alone; non-zero scores set absolute coverage `h = |und|/|ETF|` from the ladder.

See [`bucket1_views_ls_algo_eod_wire_plan.md`](bucket1_views_ls_algo_eod_wire_plan.md).

---

## Rules (keep minimal)

1. Score only: `-2…+2`. No separate direction field.
2. One view per underlying; all B1 sleeves inherit it.
3. No time horizon / expiry — edit or delete when the view changes.
4. Overlay only — do not rewrite screener `delta`.
5. Not production until explicitly wired; config + UI can land first as research/ops.

---

## Generalization

Same pattern works for any book that needs a discretionary coverage dial:

```
score ∈ discrete ladder  →  absolute h  →  apply to all pairs on that key
```

Swap the key (underlying today; later ETF, theme, etc.) or the ladder table without changing the idea. The ladder is the whole policy.
