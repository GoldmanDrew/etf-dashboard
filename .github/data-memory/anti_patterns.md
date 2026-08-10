# Data anti-pattern memory

Taxonomy of recurring data-failure modes in this repo, distilled from ~4,100 commits
(Feb–Aug 2026). Each entry records the failure shape, how it was detected, and the
codified defense — so new tooling extends the defenses instead of rediscovering the
incidents. The sentinel (`scripts/data_sentinel.py`) seeds its check catalog from this
file; when a new failure mode is healed, **add it here in the same commit as the fix**.

Companion ledger: `provider_health.json` (rolling per-provider coverage snapshots,
updated by every sentinel sweep).

---

## 1. Split-basis corruption of adjusted-close series *(largest family, ~20 commits)*

Yahoo restates `adj_close` inconsistently across split types/dates: basis switches at
forward splits (APLX 3:1 → ~3x TR cliffs), staggered reverse splits where adj switches
before raw close jumps (MTYY 1:6), blind scaling that FABRICATES cliffs when raw close
was already continuous (QBTZ 1:3 → 9x cliff, 29,131 cells rebuilt), double-scaling
(APLZ/MTYY pair returns in the hundreds of %%), and missing underlying split metadata
(KLAC 10:1 poisoning KLAG decay).

- **Detection:** human eye on Decay/Stats tabs → codified in `audit_split_tr_quality.py`
  (nightly hard gate) and `find_fabricated_adj_cliffs` (py + js parity).
- **Defense:** `split_adjustments.py` trusts declared ratio only when the observed close
  jump is within ~18% of the declared multiple; confirms via shares/NAV; skips scaling
  when close is continuous through the split.
- **Rule for new tools:** never flag or "fix" a price jump without first consulting
  `corporate_actions.json` declared splits.

## 2. Reverse-split distribution basis mismatch (income calibration)

Yahoo retroactively restates dividend amounts onto the post-split share basis while
issuer NAV stays pre-split → TSYY weekly yield inflated ~8x, modeled cash yield ~1109%/yr.
Full spec: `docs/reverse-split-distribution-fix-prompt.md`. **The fix was once reverted
by a merge and had to be re-applied** — fix-regression via merge is itself a failure mode.
Defense: split-boundary economic amounts, `MAX_INCOME_YIELD_ANNUAL=1.5` cap, 2.5x
capture-ratio cap within 120d of a split, `audit_distribution_split_basis.py` gate.

## 3. Shifted etf_adj_close via join-key asymmetry *(healed 2026-08-07, 89b01efb)*

`etf_adj_close` stored one session ahead of `close_price` → pair drag computed
`beta*r_und[t] - r_etf[t+1]` → confidently wrong 20d Realized decay; 2,119 cells across
163 tickers. Detection deliberately uses NO amplitude threshold: roughness-of-ratio scan
(`repair_shifted_etf_adj_close.py`) + lagged-vs-contemporaneous return correlation +
direction violations (`audit_pair_leg_alignment.py --fail-on-shifted`, nightly hard gate).

## 4. Stale/frozen issuer NAV tails

Issuer pages freeze or publish T+1 while the market moves → ~3,700 historical fake
premium/discount spikes. Defense: `stale_kind` taxonomy (issuer_lag / issuer_early /
carry_forward / market_backed_no_issuer_nav / anchor_lag / proshares_fallback),
carry-forward budget (5 bdays), CF→market_backed promotion, prem/disc eligibility gates.
**Over-strict staleness gates have repeatedly misfired** (blocked market_backed priors,
treated issuer_publish_lag as actionable) — reuse this taxonomy, never invent a parallel one.

## 5. Zombie/stale options-cache spots

`options_cache.json` spot for NBIZ was $1.89 vs issuer ~$34 (post-reverse-split zombie)
feeding Trade Lab and VRP. Defense: spot reconciliation to latest metrics close in
`build_data.py`, universe-wide Tradier quotes, per-symbol stale flags, and the sentinel's
`zombie_spot` check (spot vs metrics close ratio > 2.5x without a declared split).

## 6. NaN leakage into published JSON *(took the whole site down)*

Python's `json` serializes `NaN` as a bare token which `JSON.parse` rejects → the entire
dashboard blanked. Defense: `_sanitize_for_json` + `allow_nan=False` in build_data —
**except `options_cache.json`, which is still written without `allow_nan=False`** — and
the sentinel's strict parse (`parse_constant` rejection) on every gated commit.
Producer-side validity ≠ consumer-side parseability.

## 7. Provider routing fallthrough (issuer site drift)

Defiance redesigned its /etfs/ listing → `supports_ticker()` silently skipped the issuer
→ a whole cohort fell through to market-backed tails (~594k-line heal). No error was
raised anywhere. Defense: seeded `KNOWN_TICKERS`, `audit_defiance_issuer_routing.py`, and
the sentinel's `provider_drift` check (issuer coverage drop >50% day-over-day in
`etf_metrics_health.json` → alert). Watch all scrape issuers: REX, GraniteShares,
YieldMax, Roundhill, Tradr, Defiance.

## 8. Rate-limit starvation cascade

A degraded Polygon path amplified fallback request volume (~540 calls), exhausted quota,
and the 429-streak counter then aborted the *next* pipeline phase entirely. Defense:
capped truncation probes, counter resets between phases, per-workflow budget env vars.
Rule: fallbacks must have their own budget, never inherit "whatever is left".

## 9. Decimal-shifted shares_outstanding

Issuer/Yahoo feeds occasionally emit share counts off by powers of 10. Defense:
`repair_shares_vs_aum_nav` replaces shares when `shares/(aum/nav)` ratio > 80x or < 1/80.
Detection is automated but warn-only — invisible unless logs are read.

## 10. Calendar/session and dtype hygiene

Metrics rows landed on weekends/Juneteenth; mixed `datetime.date` vs `Timestamp` crashed
nightly ingest. Defense: `market_calendar.py` (computed NYSE calendar, **no half-day
support**), `filter_metrics_to_nyse_sessions`, dtype normalization at load/upsert.

## 11. Scheduler silent skip *(nightly ingest silently dead for ~10 days)*

nightly.yml gated on wall-clock ET hour, but GitHub delivers crons hours late → every
run self-skipped, zero alerts. Defense: DST-resolved dual-cron slot ownership
(resolve-slot job). Rule: schedule-time guards must fail LOUD, never fail-closed silently.

## 12. Publish/deploy artifact regression (hosting limits)

Cloudflare Pages silently dropped `etf_metrics_daily.json` for exceeding ~20MB → Decay
tabs went dark with green CI. Defense: ship `.json.gz`, `audit_pages_artifact_budget.py`
fail-closed on every deploy. Rule: the hosting layer has invisible limits; assert them in CI.

## 13. One-leg price-jump outliers poisoning realized decay

Orphan one-leg log jumps (backfill artifacts, pre-split prints) created absurd 20d decay
(RKLZ 168%/yr → ~1.5%). Defense: skip orphan jumps, `log1p(beta*Ru - Retf)` formulation,
skip calendar gaps > 5d.

## 14. Lost historical rows requiring git archaeology

Consecutive stale ingests producing identical (nav, aum, shares) triples let
`collapse_redundant_consecutive_rows` destroy sessions; `recover_rex_nav_from_git_history.py`
walks every commit of the metrics JSON to refill them. **The git history IS the recovery
store of last resort — no tool may rewrite/squash data-file history.** Quarantine is a
side manifest for exactly this reason.

## 15. Stale `prior_close` baseline in the intraday spot feed *(found by the sentinel, 2026-08-10)*

`data/underlying_intraday_spot.json` sources `prior_close` from the metrics store, but
for tickers whose metrics row has not advanced, the baseline can be days behind the
previous session — 166 of 824 symbols (20%), worst 10 sessions behind, on the day the
sentinel first ran. `return_d1_so_far` is then a multi-day return wearing a daily label,
which corrupts displayed intraday returns and the LETF rebalance-flow math that consumes
them. A separate variant of the same field: after a declared forward split, `prior_close`
stays on the pre-split basis (CRDU/GEVX/KORU/LABX/MUU/NEBX/SNXX/WDCX), producing
"returns" of +200% to +1900% that match the split multiple exactly.

- **Detection:** `data_sentinel.check_spot_anomalies` compares `prior_close_date` against
  `previous_nyse_session(file trading date)` (`stale_return_baseline`), and matches the
  observed ratio against declared split multiples (`split_basis_prior_close`).
- **Fix (staleness variant):** `refresh_underlying_spots` no longer takes "the latest
  metrics row" as the baseline — it pins the row to `previous_nyse_session` and publishes
  no `return_d1_so_far` when a symbol has no row for that session (`prior_close_stale`).
  Pinning also closed a *silent* second failure: during RTH the panel already carries a
  row for the open session, so 532 of 824 symbols were being priced against their own
  partial close and reading ~0 (NVDA showed −2.2% where the true move was −3.2%). The
  sentinel now separates a *withheld* return (`stale_return_baseline_suppressed`, WARN —
  the artifact is honest, the metrics tail is what's stalled) from one still published
  against a stale baseline (`stale_return_baseline`, quarantine).
- **Lesson for producers:** "most recent row available" is never the same statement as
  "the row for the session I mean". A row selector with no session assertion fails in
  both directions at once, and only the too-old direction is visible.
- **Split-basis variant root-caused and healed 2026-08-10.** Two independent defects had
  to line up. (a) `prior_close` read `etf_adj_close` in preference to raw `close_price`,
  a basis mismatch against a raw live quote regardless of split health — it also
  manufactured a fake +21.5% day for AAOZ (real: +0.5%) out of ordinary distribution
  adjustment, 82 tickers affected. Fixed by reading raw close, adj only as fallback.
  (b) `detect_adj_basis_switch_splits` proposed its `forward` remap for *every* declared
  forward split, including those where the provider had already restated raw close, so
  `normalize_adj_basis_switch_etf_adj_close` scaled the correctly back-adjusted
  post-split rows by the split factor a second time and put the entire history on the
  pre-split basis. Now gated on the close series actually being continuous through the
  split. Persisted history repaired by `repair_pre_split_basis_etf_adj_close`
  (`scripts/repair_etf_adj_split_basis.py`, also wired into the ingest).
- **Lesson for detectors:** never judge a "daily return" field with a daily threshold
  before confirming its baseline is actually the previous session. The first version of
  this check flagged seven tickers as "suspected bad quote" — a true symptom with the
  wrong cause, which would have sent someone hunting quote feeds instead of the
  metrics join.
- **Lesson — levels need their own invariant.** A uniform basis error is invisible to
  every return-based detector: a constant factor cancels in `adj[t]/adj[t-1]`, so the
  cliff and correlation scans stayed silent for weeks while `prior_close` was off by 20x.
  The invariant that catches it is a *level* one: after the last close-jump-verified
  split, `etf_adj_close` must equal raw `close_price` (a back-adjusted series is
  normalized to the newest basis). Assert that, not just return smoothness. Corollary:
  a field differenced against a live quote must be on the raw traded basis — pick the
  column by basis, never by "the more processed one is better".

## 16. Library upgrade breakage

pandas 2.2 changed `groupby.apply` semantics → holdings CSV lost its key column → the
whole YieldBOOST tick aborted before options refresh. Rule: one persist failure must not
abort a multi-stage tick; pin/upgrade deliberately.

---

## Detection patterns that work here (steal these)

The repo's most robust detectors avoid raw amplitude thresholds: roughness-of-ratio
(shifted adj), lagged-return correlation (join asymmetry), direction violations on
well-tracked pairs, close-jump-within-18%-of-declared-mult (split confirmation),
`shares/(aum/nav)` > 80x (decimal shift), |close/NAV−1| > 10% (fake prem/disc).
Amplitude checks that DO exist are always paired with an exemption source
(declared splits, market-event breaker, leverage corroboration).
