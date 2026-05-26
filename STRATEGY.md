# YieldBOOST Options Strategy Playbook

This document explains how to use the dashboard's ?-space edge metrics, BS
greeks, and event-decomposed volatility to build a repeatable trading process
around weekly put-spread selling on YieldBOOST sleeves. It is **not** financial
advice — it is a written-down version of the analytical framework the dashboard
implements, with the explicit knobs and caveats so the reader can decide which
trades to size up, fade, or skip.

The audience is someone who already understands:

- What a short put-spread is (credit, max-loss, breakeven).
- That YieldBOOST ETFs *systematically* sell weekly put-spreads on a 2x
  underlying sleeve (or the underlying itself for sleeves without a listed
  2x ETF, e.g. `CRWV`).
- That earnings events are *jumps*, not diffusion — Black-Scholes pricing
  attributes the jump variance to the underlying ?, which inflates IV around
  the event and collapses it after.

---

## 1. The ?-space edge framework

The dashboard ranks every YB sleeve by a single primary metric:

```
?-edge  =  IV (held front spread)  ?  ?_BE
```

where `?_BE` ("breakeven sigma") is the volatility that would make the
Black-Scholes diffusion fair value of the spread exactly equal to the live
spread mid-market price. Equivalently, ?_BE is what realized vol *over the
held horizon* must come in at for the carry trade (collect credit, let theta
work) to break even.

| ?-edge bucket | Signal | Meaning |
|--|--|--|
| ? +30pp | **STRONG SELL** | Market is paying you ?30 vol-points more than expected loss. Even a fat tail tomorrow won't structurally beat you. |
| ? +10pp | sell | Clean short-vol edge; size to greeks (see §3). |
| ? 0pp | lean sell | Positive but small — only take if greeks are favorable and there's no earnings in window. |
| ? ?10pp | lean avoid | Slightly negative; rebated by theta only if you have a flow/structural reason. |
| < ?10pp | **AVOID** | Selling carries a structural deficit. The YieldBOOST ETF is at this strike because it *must* roll (mechanical issuer mandate), not because the trade has edge. |

### Why ?-space, not $-space?

A 5-cent fair-value miss on a 50-cent spread is +10% richness; the same 5
cents on a 5-dollar far-ITM spread is +1%. Putting the comparison in ?-space
normalizes across strike spacing, time to expiry, and spot level, so the
ranking is comparable across sleeves with very different absolute prices.

### Where ?_BE comes from in the code

`scripts/event_vol.py::implied_sigma_for_spread_mid` — Brent root-find on
`fair_put_spread_mid_from_iv(spot, K_long, K_short, ?, T) == mid`. Robust to
ITM/OTM and to spreads whose mid lies below intrinsic (in which case ?_BE
returns `None` and the row is flagged `IV ? BE?: —`).

---

## 2. Reading the dashboard

The **YB Edge** topbar button (`#/yb-edge`) opens a single sortable table
ranked by ?-edge. The columns are:

| Column | Source field | Use |
|--|--|--|
| Signal | derived from `iv_minus_breakeven_sigma` | Headline. Sort descending. |
| YB / Underlying | `yb_etf` / `underlying` | Click YB to jump to per-ETF Vol/VRP tab. |
| IV ? BE? | `iv_minus_breakeven_sigma` | The ?-edge. Bigger = better short-vol candidate. |
| IV (sleeve) | `iv_full_proxy` | Average IV of long+short legs at the held expiry. |
| BE ? | `spread_breakeven_sigma_annual` | The sigma the market is implicitly pricing the spread at. |
| Mid | `spread_mid_market` | What you'd receive (or pay) to short the spread today. |
| BS fair | `bs_put_spread_fair_diffusion` | Diffusion-only BS fair at the chosen ?. **Invalid if earnings in window.** |
| Wk loss % | `expected_weekly_loss_pct_of_spot` | Expected weekly NAV drag at current ?. |
| $-? /1%U | `bs_dollar_gamma_per_1pct_underlying` | $ P&L per 1% underlying move (short = negative). Drives sizing in §3. |
| ?/day | `bs_theta_spread_per_day` | Theta per calendar day. Positive = collecting. |
| DTE | `days_to_exp` | Days to held expiry. |
| DT-Earn | `days_to_earnings` | Days to next known earnings. **Red badge = inside held window.** |
| Impl. evt% | `event_implied_move_pct_underlying` | Implied event-day 1? move from variance split. |
| Hist. evt% | `event_historical_move_pct_underlying` | MAD of |return| on prior earnings days. |
| Rich% | `event_move_richness_pct` | (implied/historical) ? 1. ?+30% green, ??20% red. |
| Skew | `iv_expiry_skew_days` | Held-vs-chain expiry gap. ?7d clean, 8–21d amber, >21d red. |
| IV src | `iv_source` | `holdings_exact_expiry` clean; `_nearest_expiry` interpolated; `_missing_chain` data gap. |

---

## 3. Sizing from greeks

For a sleeve flagged **sell** or **STRONG SELL**, decide how much to put on by
working backwards from a tolerable P&L per unit of risk:

```
$-loss per 1% underlying gap-down  ?  $-? × (1)² / 2  (gamma scaled)
$-loss per 5% underlying gap-down  ?  $-? × (5)² / 2
```

Quick sizing recipe:

1. **Pick a tolerable gap-down loss** per spread. Example: $100 per OCC
   contract on a 5% adverse gap.
2. **Read `bs_dollar_gamma_per_1pct_underlying`** for the row. If it's
   `?0.50` per spread structure, then $-loss at 5% = `0.5 × 25 / 2 ? 6.25`
   dollars per *single* structure. To cap loss at $100, you can hold
   `100 / 6.25 ? 16` structures.
3. **Cross-check against theta**: `?/day × DTE` is the diffusion-only
   carry you expect. If carry is much less than 1× your gap-down loss,
   you're being paid too little for the tail.
4. **Mix in the ?-edge as a multiplier**: if `IV ? BE? ? +20pp`, you can
   2× the base size because the market is over-paying expected loss; if
   the edge is `+5pp` you should at most 1× size and only when the rest
   of the row is clean (no earnings, no skew, IV source = `exact_expiry`).

For inverse positions (long the spread / buy the move) the same logic
applies with signs flipped — use the **AVOID** rows where `Rich%` is very
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
| `event_jump_share_of_variance` | (var_full ? var_base) / var_full | If ?40%, BS diffusion fair is structurally wrong — use *event-aware* VRP. |

### Playing earnings

**Case A — Rich event, market over-paying.** `Rich% ? +30%`, sleeve flagged
`sell` or `STRONG SELL`, event inside window. The market is paying you a
premium for the jump that history says is too large. Options:

- Sell the *full* spread; you collect the rich event premium plus the
  diffusion edge.
- More conservative: sell a *straddle/strangle* on the underlying (?=1) of
  equivalent vega to bypass the YB-spread strike risk; close ahead of the
  event-day open.

**Case B — Cheap event, market under-paying.** `Rich% ? ?20%`. The chain is
under-pricing the jump vs history.

- Buy the spread *long* (be the customer YieldBOOST is selling to).
- Or buy an underlying strangle for a clean ?=1 long-vol position.
- Sizing: `vega/structure × ?_expected_event` gives the P&L per vol-point
  realized.

**Case C — Earnings inside window, BS fair invalid.** When
`event_jump_share_of_variance ? 0.40`, do **not** size off
`bs_put_spread_fair_diffusion` — use `put_spread_fair_event_aware` and
`put_spread_vrp_event_aware` from the event-aware leg of the pipeline (the
"E.A." stats on the per-ETF Vol/VRP tab).

---

## 5. Entry & exit rules

### Entry checklist

1. **?-edge ? +10pp** (sort descending; only consider top rows).
2. **No earnings inside held window** unless `event_move_richness_pct ? +30%`
   (you want to be the one selling the event premium, not buying it).
3. **Skew ? 7d** (`exact_expiry` or near-exact). 8–21d means the IV came
   from a different chain expiry; treat as regime indicator only.
4. **IV source is `holdings_exact_expiry` or `holdings_nearest_expiry` with skew ? 14d**.
5. **Carry-to-tail ratio ? 0.5×**: `? × DTE` ? `0.5 × |$-? × 25/2|` for a
   5% gap reference.

If all five pass, size to the gap-down tolerance per §3 and enter at mid
(or use `spread_mid_market_unavailable` flag — if true, do **not** enter,
re-fetch the chain).

### Exit checklist

- **Time-stop**: close at `DTE == 1` regardless of P&L. The gamma curve
  blows up on the final day.
- **Profit-stop**: 50% of max profit (typical short-spread heuristic).
- **?-edge collapse**: if `IV ? BE?` drops below 0 mid-trade, the structural
  thesis is broken — close even if P&L is flat.
- **Event-aware override**: if an earnings event is *added* to the calendar
  inside the held window after you entered (e.g. Nasdaq publishes a new
  date), re-check Rich% before holding.

---

## 6. Risk caveats

### Modeling limitations

- **?=2 inversion is approximate.** When the underlying trades >+/? 5%
  intraday, sleeve return ? 2× underlying return because of the daily
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
  coverage. **Below 50%** means the dashboard is starved — don't trust
  the ranking until coverage recovers (it usually does within 1–2 ticks).
- `IV source = holdings_missing_chain` rows have **no IV data at all**;
  ?-edge is `—`. Ignore for sizing.
- The event calendar uses a tiered fallback (Nasdaq ? Yahoo ? seed ?
  quarterly projection). `confirmation: "projected"` rows are educated
  guesses from the prior quarterly pattern; treat them as ±3 trading-day
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
Verify with  ?  ? × DTE ? 0.5 × |$-? × 25/2|   (carry-to-tail rule of thumb).
Exit at      ?  DTE == 1, OR 50% profit, OR ?-edge crosses 0.
```

The ?-edge framework distills a four-dimensional surface (price, IV, strike,
time) into a single sortable number per sleeve. Every other metric in the
dashboard is a confirmation/risk overlay on top of that primary signal.
