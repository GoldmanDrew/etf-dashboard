# YieldBOOST Options Strategy Playbook

This document explains how to use the dashboard's **kernel-aware model-edge**
metrics, calibrated-kernel greeks, and event-decomposed volatility to build a
repeatable trading process around weekly put-spread selling on YieldBOOST
sleeves. It is **not** financial advice -- it is a written-down version of the
analytical framework the dashboard implements, with the explicit knobs and
caveats so the reader can decide which trades to size up, fade, or skip.

The audience is someone who already understands:

- What a short put-spread is (credit, max-loss, breakeven).
- That YieldBOOST ETFs *systematically* sell weekly put-spreads on a 2x
  underlying sleeve (or the underlying itself for sleeves without a listed
  2x ETF, e.g. `CRWV`).
- That earnings events are *jumps*, not diffusion, and that Black-Scholes
  cannot price them; we use Bates jump-diffusion for single-stock sleeves
  with earnings risk, Heston for index sleeves, and the AZ rescale when the
  sleeve chain is missing.

---

## 1. The model-edge framework

The dashboard ranks every YB sleeve by a single primary **price-space**
metric:

```
edge_pp_of_max_loss  =  (spread_mid - model_fair) / max_loss * 100
```

where `model_fair` is the fair value of the held front spread produced by the
**chosen pricing kernel** for that row -- Bates > Heston > AZ in priority --
and `max_loss = K_short - K_long`. Black-Scholes is not the production kernel
for LETF put-spreads (BS outputs are kept under `debug["bs"]` for diagnostics).

### Why not Black-Scholes?

BS cannot price the three things that dominate LETF spread P&L:

1. **Variance drag on the sleeve.** A 2x daily-rebalanced LETF compounds with
   an `exp(-sigma^2 * T)` drag for `x=2`, `exp(-3*sigma^2 * T)` for `x=-2`.
   BS prices the sleeve as a single-period log-normal; it misses the drag.
2. **Sleeve skew and term structure.** Heston (stochastic-vol) calibrates a
   smile on the underlying chain and propagates it to the sleeve via
   Ahn-Haugh-Jain (AHJ); BS uses a flat sigma.
3. **Jump risk around earnings.** Bates adds a jump-diffusion component
   calibrated to pre-event smile widening; BS attributes the jump variance
   to the diffusive term and inflates IV across the entire surface.

### Why price-space (% of max-loss), not sigma-space?

Spreads at different sleeves trade at different absolute prices and have
different max-losses. (`spread_mid - model_fair) / max_loss` normalizes the
edge to "fraction of the worst-case", which is what a sizing rule actually
cares about: a +30pp edge means the market pays you 30% of max-loss above
the model fair; a +10pp edge means 10%.

| Edge bucket | Signal | Meaning |
|--|--|--|
| >= +30pp | **STRONG SELL** | Market pays >= 30% of max-loss over model fair. Even a deep tail won't structurally beat you over the held week. |
| >= +10pp | sell | Clean kernel-aware edge; size to greeks (see Section 3). |
| >= 0pp | lean sell | Positive but small -- only take if greeks are favorable, grade is A/B, and there's no earnings in window. |
| >= -10pp | lean fade | Slightly negative; only structural reasons (flow, mandate, recycling) justify selling. |
| < -10pp | **fade** | Model says the spread is rich vs market -- the YieldBOOST issuer is at this strike because it *must* roll (mechanical mandate), not because the trade has edge. |

### Data grade gates the signal

Every row carries a server-side `data_grade` (A/B/C/D) that collapses sleeve
quote age x underlying quote age x holdings age x IV source x expiry skew
into a single PM-grade letter. **A/B are tradeable today, C is monitor-only,
D is hidden by default.** Rows with positive edge but grade C/D are flipped
to "AVOID DATA" so traders never act on a stale or imputed signal.

### Where the model fair comes from in the code

`scripts/letf_options_models.py::compute_letf_model_extras` is the
single dispatcher. It tries each kernel in priority order:

- **Bates** (`bates_put_spread_fair`) - single-stock sleeves with sufficient
  chain points; uses jump-diffusion COS pricing with AHJ propagation.
- **Heston** (`heston_put_spread_fair`) - index sleeves; uses stochastic-vol
  COS pricing with AHJ propagation.
- **AZ rescale** (`az_put_spread_fair`) - thin or missing sleeve chain;
  rescales held strikes to underlying-equivalent strikes and prices in
  underlying IV.

The row's `model_name` tells you which one produced the fair value.
`model_disagreement_pp_of_max_loss` reports max-min across kernels in
% of max-loss; >20pp = model uncertainty, size at half band or skip.

---

## 2. Reading the dashboard

The **YB Edge** topbar button (`#/yb-edge`) opens a single sortable table for
**shorting the YieldBOOST ETF**. It is ranked by the structural short edge, not
the put-spread vol edge, and is organized into three tiers (? and ? collapse;
nothing is removed):

> **Two distinct edges — do not conflate them.** `net_edge_p50_annual` (tier ?)
> is the *annualized, comparable* edge of **shorting the ETF** (decay net of
> borrow/carry; `edge_sign_convention = short_favorable_positive`). It is the
> ranker. `edge_pp_of_max_loss` (tier ?) is the kernel-aware richness of the
> fund's **front put spread**, in % of that spread's one-week max-loss — a
> tactical vol overlay on the fund's hedge, in units that are *not* comparable
> across names. The fund is **short** that spread, so a **rich** spread is a mild
> **headwind** to shorting the fund; the `Align` chip surfaces the sign-corrected
> value (`= ?edge_pp_of_max_loss`). Never rank shorts by `edge_pp_of_max_loss`.

**? Short thesis (primary):**

| Column | Source field | Use |
|--|--|--|
| Grade | `data_grade` | A full size, B half size, C monitor, D blocked. |
| Short signal | `short_signal` (from `net_edge_p50_annual`, `net_edge_p05_annual`) | Headline. STRONG SHORT needs p50 ? +15% **and** p05 > 0. Default sort. |
| YB / Underlying | `yb_etf` / `underlying` | Click YB to jump to per-ETF Vol/VRP tab. |
| Net edge (ann) | `net_edge_p50_annual` | Median annual edge of **shorting the ETF**, net of borrow/carry. Higher = better short. Comparable across names. |
| p05…p95 | `net_edge_p05_annual` / `net_edge_p95_annual` | Confidence band. Red when p05 < 0 (edge not robust to the downside tail). |
| Sync | `quote_sync.sync_ok` | Cross-dataset timestamp sync (sleeve quote vs underlying quote vs holdings vs screener). NOT SYNCED = inputs describe different moments (the XEY case). |
| Short? | `short_directly_shortable` | ? = purgatory / no borrowable shares / borrow spike / blacklisted. Flagged, not hidden. |

**? Carry decomposition** (`expected_gross_decay_p50_annual` ? `borrow_fee_annual` ? `income_distributions_annual`): the gross decay (the short's fuel) net of the borrow and distribution carry that `net_edge_p50_annual` already accounts for.

**? Vol overlay (second-order, front put spread):**

| Column | Source field | Use |
|--|--|--|
| Spread vol signal | derived from `edge_pp_of_max_loss` + `data_grade` | Sell/fade signal for trading the **spread itself**, not the short thesis. |
| Edge (pp) | `edge_pp_of_max_loss` | Price-space edge in % of the front spread's one-week max-loss. |
| Align | `short_thesis_alignment` (`= ?edge_pp_of_max_loss`) | Sign-corrected: rich hedge = headwind to shorting; cheap = tailwind. |
| Model | `model_name` | Which kernel actually fired (bates / heston / az). |
| AZ cone | `az_cone_residual_iv` | Sleeve IV minus AZ-implied sleeve IV. Cross-surface arbitrage signal. |
| Fair | `model_fair` | Model fair value of the spread (hover for AZ/Heston/Bates side-by-side). |
| Mid | `spread_mid_market` | What you'd receive (or pay) to short the spread today. |
| IV | `iv_full_proxy` | Average IV of long+short legs at the held expiry (chain input, not a fair value). |
| $-gamma /1%U | `dollar_gamma_per_1pct_underlying` | $ P&L per 1% underlying move (short = negative). Drives sizing in Section 3. |
| theta/day | `theta_per_day` | Theta per calendar day. Positive = collecting. |
| Carry/wk | `expected_weekly_carry_usd` | model_fair x 100 = $ carry per OCC contract per week. |
| DTE | `days_to_exp` | Days to held expiry. |
| Event | `days_to_earnings` + `event_move_richness_pct` | Days to next earnings and richness vs historical move. Red badge = inside held window. |

---

## 3. Sizing from greeks

`dollar_gamma_per_1pct_underlying` and `theta_per_day` are central finite-diff
on the chosen kernel's `model_fair` -- the row's `greeks_kernel` field reports
which one (`heston` / `bates` / `az`). On skewed single-stock sleeves Heston
and Bates kernel-?? differ from BS-proxy ?? by 5-30%; the calibrated number
is the one you should size off, not the BS-at-sleeve-IV proxy. When no kernel
converges (`greeks_kernel == "bs_proxy"`), the fallback BS finite-diff at
sleeve IV is used and the row should be sized conservatively or skipped.

For a sleeve flagged **sell** or **STRONG SELL**, decide how much to put on by
working backwards from a tolerable P&L per unit of risk:

```
$-loss per 1% underlying gap-down  ?  $-?  (1) / 2  (gamma scaled)
$-loss per 5% underlying gap-down  ?  $-?  (5) / 2
```

Quick sizing recipe:

1. **Pick a tolerable gap-down loss** per spread. Example: $100 per OCC
   contract on a 5% adverse gap.
2. **Read `bs_dollar_gamma_per_1pct_underlying`** for the row. If it's
   `?0.50` per spread structure, then $-loss at 5% = `0.5  25 / 2 ? 6.25`
   dollars per *single* structure. To cap loss at $100, you can hold
   `100 / 6.25 ? 16` structures.
3. **Cross-check against theta**: `?/day  DTE` is the diffusion-only
   carry you expect. If carry is much less than 1 your gap-down loss,
   you're being paid too little for the tail.
4. **Mix in the ?-edge as a multiplier**: if `IV ? BE? ? +20pp`, you can
   2 the base size because the market is over-paying expected loss; if
   the edge is `+5pp` you should at most 1 size and only when the rest
   of the row is clean (no earnings, no skew, IV source = `exact_expiry`).

For inverse positions (long the spread / buy the move) the same logic
applies with signs flipped  use the **AVOID** rows where `Rich%` is very
negative as long-vol entry candidates around the event.

---

## 4. Earnings-week decomposition

Black-Scholes prices diffusion variance; earnings are *jumps*. When earnings
fall **inside** the held expiry window, the same physical IV is doing two
things at once:

1. Pricing the baseline diffusive ? for non-event days.
2. Loading up extra "implied jump" volatility for the event day.

`scripts/event_vol.py::strip_iv_to_base` solves this by subtracting the
event-day variance contribution out of the option-implied variance, leaving
a cleaner *base* ? that's comparable across event and non-event weeks.

Three derived metrics make the trade thesis concrete:

| Field | Formula | Signal |
|--|--|--|
| `event_implied_move_pct_underlying` | ?(var_full ? var_base) / ? | The 1? jump the option chain is pricing for the event day. |
| `event_historical_move_pct_underlying` | MAD of \|return\| on N prior earnings days | What the stock has actually done on event days. |
| `event_move_richness_pct` | implied/historical ? 1 | Rich/cheap. ?+30% sell the move; ??20% buy the move. |
| `event_jump_share_of_variance` | (var_full ? var_base) / var_full | If ?40%, BS diffusion fair is structurally wrong  use *event-aware* VRP. |

### Playing earnings

**Case A  Rich event, market over-paying.** `Rich% ? +30%`, sleeve flagged
`sell` or `STRONG SELL`, event inside window. The market is paying you a
premium for the jump that history says is too large. Options:

- Sell the *full* spread; you collect the rich event premium plus the
  diffusion edge.
- More conservative: sell a *straddle/strangle* on the underlying (?=1) of
  equivalent vega to bypass the YB-spread strike risk; close ahead of the
  event-day open.

**Case B  Cheap event, market under-paying.** `Rich% ? ?20%`. The chain is
under-pricing the jump vs history.

- Buy the spread *long* (be the customer YieldBOOST is selling to).
- Or buy an underlying strangle for a clean ?=1 long-vol position.
- Sizing: `vega/structure  ?_expected_event` gives the P&L per vol-point
  realized.

**Case C  Earnings inside window, BS fair invalid.** When
`event_jump_share_of_variance ? 0.40`, do **not** size off
`bs_put_spread_fair_diffusion`  use `put_spread_fair_event_aware` and
`put_spread_vrp_event_aware` from the event-aware leg of the pipeline (the
"E.A." stats on the per-ETF Vol/VRP tab).

---

## 5. Entry & exit rules

### Entry checklist

1. **?-edge ? +10pp** (sort descending; only consider top rows).
2. **No earnings inside held window** unless `event_move_richness_pct ? +30%`
   (you want to be the one selling the event premium, not buying it).
3. **Skew ? 7d** (`exact_expiry` or near-exact). 821d means the IV came
   from a different chain expiry; treat as regime indicator only.
4. **IV source is `holdings_exact_expiry` or `holdings_nearest_expiry` with skew ? 14d**.
5. **Carry-to-tail ratio ? 0.5**: `?  DTE` ? `0.5  |$-?  25/2|` for a
   5% gap reference.

If all five pass, size to the gap-down tolerance per 3 and enter at mid
(or use `spread_mid_market_unavailable` flag  if true, do **not** enter,
re-fetch the chain).

### Exit checklist

- **Time-stop**: close at `DTE == 1` regardless of P&L. The gamma curve
  blows up on the final day.
- **Profit-stop**: 50% of max profit (typical short-spread heuristic).
- **?-edge collapse**: if `IV ? BE?` drops below 0 mid-trade, the structural
  thesis is broken  close even if P&L is flat.
- **Event-aware override**: if an earnings event is *added* to the calendar
  inside the held window after you entered (e.g. Nasdaq publishes a new
  date), re-check Rich% before holding.

---

## 6. Risk caveats

### Modeling limitations

- **?=2 inversion is approximate.** When the underlying trades >+/? 5%
  intraday, sleeve return ? 2 underlying return because of the daily
  rebalance path. The `sleeve_diffusion_drag_annual` term captures the
  *expected* drag but doesn't price the path dependence.
- **BS diffusion is wrong for jumps.** The dashboard explicitly flags this
  (`event_in_held_horizon`, `event_jump_share_of_variance`); but if the
  underlying announces a *non-earnings* event (M&A, secondary, FDA outcome)
  the calendar won't catch it.
- **YieldBOOST chains can have OTC strikes** that aren't on OPRA. The chain
  fetch falls back to nearest listed expiry; the IV-skew column reflects
  how far the interpolation reached.

### Data freshness caveats

- `iv_coverage_front_pct` in the headline strip is the live pipeline IV
  coverage. **Below 50%** means the dashboard is starved  don't trust
  the ranking until coverage recovers (it usually does within 12 ticks).
- `IV source = holdings_missing_chain` rows have **no IV data at all**;
  ?-edge is ``. Ignore for sizing.
- The event calendar uses a tiered fallback (Nasdaq ? Yahoo ? seed ?
  quarterly projection). `confirmation: "projected"` rows are educated
  guesses from the prior quarterly pattern; treat them as 3 trading-day
  uncertain.

### Position-level risk

- **Path risk in the YieldBOOST ETF itself**: the *fund* sells a new
  spread every week, so the ETF NAV path is the cumulative sum of all
  past spread P&Ls. If you trade the spread directly you have *one*
  realization; the ETF averages many.
- **Borrow cost when shorting the sleeve as a hedge**: the screener tab
  carries the IBKR FTP borrow rate. For names like `MSTU`, `NUGT`, `BITX`
  borrow can swamp the option carry.
- **Liquidity at exit**: rows with `iv_expiry_skew_days > 21` are usually
  also wide-spread; mark fills generously inside the bid-ask.

---

## 7. Quick reference: which fields drive which decision

```
Pick a trade ?  Sort YB Edge by IV ? BE? desc.
Skip if      ?  Rich% < +30% AND earnings inside window
                (event premium not high enough to compensate jump risk).
Size by      ?  $-? /1%U  ?  pick max contracts s.t. 5% gap-down ? tolerance.
Verify with  ?  ?  DTE ? 0.5  |$-?  25/2|   (carry-to-tail rule of thumb).
Exit at      ?  DTE == 1, OR 50% profit, OR ?-edge crosses 0.
```

The ?-edge framework distills a four-dimensional surface (price, IV, strike,
time) into a single sortable number per sleeve. Every other metric in the
dashboard is a confirmation/risk overlay on top of that primary signal.
