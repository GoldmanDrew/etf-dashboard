# Data Health Sentinel — architecture & runbook

Deterministic (zero-token) validation layer that keeps corrupt, stale, or anomalous
market data out of committed artifacts and off the live dashboard, with a per-ticker
quarantine channel and an autonomous heal loop.

```
market-hours.yml (every 15 min)                district-health-sentinel.yml (3x/day)
┌──────────────────────────────┐               ┌──────────────────────────────────┐
│ ci_tick.py writes artifacts  │               │ sweep: full artifact surface     │
│           │                  │               │  + staleness (market-calendar)   │
│           ▼                  │               │  + universe coverage             │
│ data_sentinel.py gate        │               │  + provider drift (ledger)       │
│  parse/NaN · schema ·        │               │           │                      │
│  regression vs HEAD ·        │               │   pass ───┼── fail               │
│  per-ticker anomalies        │               │           ▼                      │
│     │            │           │               │ deterministic self-heal          │
│  kept files   blocked files  │               │  (spot/volume/corp re-fetch)     │
│     │         (dropped from  │               │           ▼                      │
│     ▼          commit list)  │               │ verify sweep → commit state      │
│ commit-data ──► deploy       │               │ → issue alert (auto-close)       │
└──────────────────────────────┘               │ → adapter-repair issue / LLM opt-in │
                                               └──────────────────────────────────┘
        both paths write: data/quarantine.json + data/sentinel_report.json
        sweep also writes: .github/data-memory/provider_health.json
```

## Components

| Piece | Path | Role |
| --- | --- | --- |
| Sentinel script | `scripts/data_sentinel.py` | `gate` (pre-commit file validation), `sweep` (full scan + lifecycle), `alert` (GitHub issue, auto-close on recovery) |
| Thresholds | `config/sentinel.json` | All tunables. JSON, not YAML — `requirements.txt` ships no PyYAML |
| Gate wiring | `.github/workflows/market-hours.yml` | Blocking step between tick and commit; commit uses the sentinel-filtered file list |
| Sweep workflow | `.github/workflows/data-health-sentinel.yml` | Cron 13:25 / 18:25 / 22:55 UTC weekdays + dispatch |
| Quarantine manifest | `data/quarantine.json` | Per-ticker/per-artifact holds; auto-ships with every Pages deploy |
| UI consumer | `assets/quarantine.js` + `index.html` | QN badge next to BL badge, Ops-page table; polled with the 5-min health effect |
| Memory | `.github/data-memory/` | `anti_patterns.md` (failure taxonomy), `provider_health.json` (coverage ledger) |
| Tests | `tests/test_data_sentinel.py` | Unit coverage for every check + gate end-to-end |

## Severity model

- **block** — artifact is not committable: unparseable / bare `NaN`/`Infinity` tokens
  (browser `JSON.parse` rejects them; failure mode #6), missing schema keys, record
  count or byte size collapse vs `HEAD` (>50%/60%), `build_time` moving backwards,
  NAV-confidence collapse. **Gate behavior:** the blocked file is dropped from the
  commit list; everything healthy still commits (a gate must never turn a quality
  signal into a data-loss event — the nightly hard-gate lesson). **Sweep behavior:**
  a heal output flagged block is restored from `HEAD` before the sentinel's own
  commit, so the sweep can never itself push a regressed artifact.
- **quarantine** — a ticker's data is suspect: `zombie_spot` (spot vs last metrics close
  > 2.5x with no declared split; NBIZ-class), `return_outlier` (>|20%| intraday move,
  MAD z > 6, no declared split, no leverage corroboration, no market-wide event),
  negative borrow. The ticker goes into `data/quarantine.json`; **records are never
  removed from `dashboard_data.json`** because cross-sectional percentiles are computed
  server-side over the full universe — removal would silently shift every other
  ticker's scores. Entries auto-clear after 2 consecutive clean sweeps.
- **warn** — recorded + alerted, never blocks: staleness beyond market-hour budgets on
  the four artifacts `freshness_diagnostics` doesn't cover (dashboard, nav, borrow
  history, spot), provider drift, expired-but-actionable VRP rows, dead volume feed
  during RTH, IBKR partial file, elevated NAV-na fraction, universe coverage below
  90% (WARN by design: the screener legitimately lists symbols the builder hasn't
  onboarded — healthy steady state sits near 95%; catastrophic drops are caught by
  the HEAD-relative record-count regression instead).

## False-positive guards (learned from repo history)

1. **Declared-split exemption** — every amplitude check consults
   `corporate_actions.json` first (reverse splits are routine in this universe).
2. **Market-event circuit breaker** — if >10% of the fleet moves >|20%|, that is the
   market, not an artifact; the outlier check suppresses itself for the run.
3. **Leverage corroboration** — a 2x ETF moving ~2x its underlying is exempt.
4. **Stale-baseline guard** — zombie-spot comparisons skip tickers whose metrics close
   is older than 7 calendar days.
5. **Market-age clocks** — staleness uses NYSE-session-aware age (Friday evening is not
   "60 hours stale" on Monday morning); intraday files are only age-checked during RTH.

## Operations

- **Observe-only mode:** set repo **variable** `SENTINEL_MODE=report` — the gate runs
  every check and files alerts but passes the tick's file list through unchanged.
  Default (`enforce`) drops blocked artifacts.
- **Sentinel crash = fail closed:** the gate step is deliberately *not*
  `continue-on-error`; if the sentinel itself dies, the tick doesn't commit and the
  next 15-min tick retries.
- **Alerts:** one rolling GitHub issue labeled `ops/data-sentinel` (label is created
  idempotently on first use). Updates comment on it, but identical finding sets are
  fingerprint-deduped so a persistent incident does not stack ~96 comments/day. A
  passing sweep auto-closes it — unless the per-tick gate reported block/quarantine
  within the last 6h (the committed artifacts a sweep validates may be healthy
  precisely *because* the gate keeps blocking a corrupt writer; the issue is held
  open with an explanatory comment instead).
- **Verify sweeps don't double-count recovery:** the post-heal re-check runs
  `sweep --verify`, which records new findings but does not advance quarantine
  `clean_streak` — recovery requires clean observations from *separate* runs.
- **Known limitation (accepted):** market-hours ticks and sentinel sweeps can race on
  `data/quarantine.json` (commit-data is snapshot-restore, last writer wins). Worst
  case a cleared entry is resurrected or a fresh one delayed until the next sweep
  (≤ ~5h); the manifest is re-derived from current data on every sweep, so the race
  self-corrects and never corrupts data artifacts themselves.
- **Manual un-quarantine:** delete the ticker's entry from `data/quarantine.json` and
  commit, or just wait — two clean sweeps clear it.
- **Threshold tuning:** edit `config/sentinel.json`; every threshold appears in the
  finding it produced (`observed` vs `threshold`) so tuning is evidence-driven.
- **LLM adapter repair (off by default):** adapter-class failures (parse/schema/provider
  drift) always file an `autofix/data-adapter-repair` issue with diagnostics. Setting
  repo variable `ENABLE_LLM_AUTOFIX=1` (plus `ANTHROPIC_API_KEY` secret) additionally
  launches a Claude patch job (Tier-2 routing, `claude-sonnet-5`) that must reproduce
  via the sentinel, patch the adapter, pass tests, and open a PR.

## Model-routing policy (cost discipline)

- **Tier 0 (default, $0):** everything in this document's detection/heal loop is
  deterministic Python — statistical checks, schema checks, failover re-fetch, issue
  templating. This intentionally covers ~everything.
- **Tier 2 (opt-in):** adapter code repair via `claude-sonnet-5` under
  `ENABLE_LLM_AUTOFIX=1`.
- **Tier 3 (human-in-the-loop):** architecture changes, threshold philosophy, new check
  classes — via normal development, seeded by `.github/data-memory/anti_patterns.md`.

## What the sentinel deliberately does NOT do

- No parquet-store validation — `etf_metrics_daily.parquet` already has a mature
  audit→repair→hard-gate chain in nightly.yml (gaps, decay coverage, pair-leg
  alignment, split-TR quality).
- No duplicate of `freshness_diagnostics.py` thresholds — the sentinel age-gates only
  the four high-traffic artifacts that pipeline doesn't cover, and tightens under it.
- No in-place data repair — repairs stay in the existing `repair_*.py` family; the
  sentinel only re-runs *fetchers* (failover) and holds suspect data. Git history is
  the recovery store of last resort and must stay intact (failure mode #14).
