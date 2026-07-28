# Plan: Shrink crash size \(C\) in realized drawdowns

Research overlay for cash-residual sizing (`scripts/bucket4/cash_residual_path.py`).
Does **not** change production B4 policy until explicitly wired.

---

## 1. Problem

Today:

\[
\begin{aligned}
u_t &= \max\!\left(0,\ \frac{P_t}{\mathrm{median}(P_{t-W:t})} - 1\right)
  && \text{(runup vs \(W\)-day median)} \\
R_t &= \theta\,\frac{u_t}{1+u_t}
  && \text{(retrace component)} \\
T_t &= \underbrace{\max_i\big(-\,r^{(H)}_{t-i}\big)}_{\text{worst \(H\)-day return}}
       + \lambda\,\sigma_t^{-}
  && \text{(historical tail)} \\
C_t &= \max(T_t,\ R_t).
\end{aligned}
\]

When the name has **already** sold off hard:

- \(T_t\) stays large because that selloff *is* the worst \(H\)-day print in the lookback.
- \(u_t = 0\) (price below median) → \(R_t = 0\) and **h-first is gated** (`runup < runup_min`).
- Net: \(C_t\) and \(L_t\) stay elevated → **gross is cut after the crash**, opposite of “less remaining downside from here.”

Economic intent of the fix:

> \(C\) should measure **remaining** adverse move capacity from the current level, not recycle an already-realized drawdown as if it were still fully ahead.

---

## 2. Design principles

1. **Residual, not recycled.** A crash already taken out of the peak should reduce forward \(C\), not inflate it.
2. **Keep the historical envelope.** Do not invent a smaller crash than “what’s left” of the calibrated worst-case path from the peak.
3. **Monotone in drawdown depth.** Deeper into a drawdown (all else equal) ⇒ weakly smaller \(C\).
4. **No free lunch at lows.** Floor \(C\) so a washed-out name cannot size as if crash risk is zero (second-leg / gap risk).
5. **Local + portable.** Same math for every underlying; only depends on the price path and existing params.
6. **Diagnosable.** Emit the pieces (\(T\), drawdown depth, residual factor, \(C_{\mathrm{raw}}\), \(C\)) on the path telemetry.

---

## 3. State: peak, drawdown, residual capacity

### 3.1 Peak reference

Over the same window used for the tail lookback (or a dedicated peak window \(W_p\)):

\[
P^{\star}_t = \max_{s \in [t-W_p,\ t]}\ P_s.
\]

Default: \(W_p =\) `tail_lookback` calendar of sessions used for worst \(H\)-day (currently 756), or `anchor_window` (252) if we want a tighter “recent regime” peak. **Recommendation:** \(W_p = \max(W,\ H\cdot k)\) with \(k \approx 3\) so the peak that generated the worst \(H\)-day is usually in view; start with \(W_p =\) `tail_lookback` sessions.

### 3.2 Realized drawdown depth

\[
D_t = \max\!\left(0,\ 1 - \frac{P_t}{P^{\star}_t}\right) \in [0,1).
\]

- \(D_t = 0\): at peak (no shrink).
- \(D_t \to 1\): price → 0 (maximum shrink toward floor).

Optional robust variant (less sensitive to a one-day spike high):

\[
P^{\star}_t = \mathrm{percentile}_{q}\big(P_{t-W_p:t}\big),\quad q = 0.95,
\]

then the same \(D_t\). Keep \(q=1\) (true max) for v1 unless IPO spikes force it.

### 3.3 Historical crash envelope from the peak

Interpret \(T_t\) as the **calibrated full crash size from an extended/peak-like state** (status quo meaning of the tail).

If the und were at the peak and then realized a move of size \(T_t\) (fractional decline), the post-crash level would be:

\[
P_t^{\mathrm{floor}} = P^{\star}_t\,(1 - T_t)_+.
\]

From the **current** price, the remaining fractional decline to that same floor is:

\[
C^{\mathrm{resid}}_t
  = \max\!\left(0,\ 1 - \frac{P_t^{\mathrm{floor}}}{P_t}\right)
  = \max\!\left(0,\ 1 - \frac{P^{\star}_t}{P_t}\,(1-T_t)_+\right).
\]

Algebra identity (when \(P_t \le P^{\star}_t\) and \(T_t < 1\)):

\[
C^{\mathrm{resid}}_t
  = \max\!\left(0,\ \frac{T_t - D_t}{1 - D_t}\right).
\]

So:

- At peak (\(D=0\)): \(C^{\mathrm{resid}} = T\) (unchanged).
- After realizing depth \(D\): remaining crash is \((T-D)/(1-D)\).
- If \(D \ge T\): already through the historical envelope → residual **0** before floors.

This is the core generalization: **same envelope, recentered at current spot.**

---

## 4. Composing the shrunk \(C\)

### 4.1 Raw residual tail

\[
T^{\mathrm{shrunk}}_t = C^{\mathrm{resid}}_t = \max\!\left(0,\ \frac{T_t - D_t}{1 - D_t}\right).
\]

### 4.2 Soft floor (second-leg / gap risk)

Never let residual tail go fully to zero:

\[
T^{\mathrm{floor}}_t = \max\!\big(T_{\min},\ \alpha\,T_t\big),
\]

with defaults (v1):

| Symbol | Meaning | Suggested default |
|--------|---------|-------------------|
| \(T_{\min}\) | Absolute floor on crash size | `l_floor`-adjacent, e.g. `0.08`–`0.12` |
| \(\alpha\) | Fraction of raw historical tail always kept | `0.25`–`0.35` |

\[
\widetilde{T}_t = \max\!\big(T^{\mathrm{shrunk}}_t,\ T^{\mathrm{floor}}_t\big).
\]

### 4.3 Retrace term stays “from extension only”

Keep

\[
R_t = \theta\,\frac{u_t}{1+u_t}
\]

unchanged: it only fires when **above** the median. No double-counting of drawdowns into \(R\).

### 4.4 Final crash size

\[
\boxed{
C_t = \max\big(\widetilde{T}_t,\ R_t\big).
}
\]

Status quo is the special case \(D_t \equiv 0\) (or skip shrink): \(C_t = \max(T_t, R_t)\).

### 4.5 Optional smooth taper (if hard kink at \(D=T\) is disliked)

\[
T^{\mathrm{shrunk}}_t
  = T_t \cdot \left(\frac{1-D_t}{1 - \rho_D D_t}\right)_+
  \quad\text{with}\quad
  \rho_D \in [0,1),
\]

or stick to the exact residual formula in §3.3 (preferred — clearer economics).

---

## 5. Interaction with \(L\), cap, and h-first

Unchanged:

\[
L = (1-h)\frac{|\beta|}{1+h|\beta|}\,C\,(1+\varphi C),
\qquad
\mathrm{cap} = \frac{\rho\cdot B}{\max(L,\ L_{\mathrm{floor}})}.
\]

Shrinking \(C\) ⇒ shrinking \(L\) ⇒ **higher** applied gross when the residual envelope says so.

### 5.1 Runup gate (separate but related)

Today h-first requires \(u_t \ge u_{\min}\). In deep drawdowns \(u_t=0\), so even with smaller \(C\) you may still be stuck at policy \(h_0\) — but **gross can still rise** because \(\mathrm{cap}(C)\) rose.

Two policy layers (do not conflate in v1):

| Layer | Question | v1 choice |
|-------|----------|-----------|
| **C shrink** | How big is the crash from *here*? | Implement (§4) |
| **h-first gate** | May we raise \(h\) before cutting gross? | **Leave as-is** for v1 |

Optional v2 gate (only if C-shrink alone is not enough):

\[
\text{allow h-first if }
  u_t \ge u_{\min}
  \ \textbf{or}\
  D_t \ge D_{\min}
  \ \textbf{or}\
  T^{\mathrm{shrunk}}_t \le \kappa\,T_t.
\]

**Status (2026-07):** implemented in `CashResidualParams` (`h_first_on_drawdown=True`,
`dd_h_first_min=0.25`, `shrink_h_first_frac=0.70`). Reasons:
`h_first_solve` / `h_first_solve_dd` / `h_first_solve_shrink`. Intent: stop
emergency-cutting gross into und selloffs when runup is cold but drawdown is real.

Document separately; do not bundle into the first merge.

---

## 6. Worked intuition

Suppose \(T = 0.60\) (60% historical envelope), peak \(P^\star = 100\):

| Spot \(P\) | \(D\) | \(C^{\mathrm{resid}}=(T-D)/(1-D)\) | Notes |
|------------|-------|--------------------------------------|-------|
| 100 | 0.00 | 0.60 | At peak — full \(C\) |
| 80 | 0.20 | 0.50 | Mild DD — modest shrink |
| 55 | 0.45 | 0.27 | Deep DD — clear unlock |
| 40 | 0.60 | 0.00 → floored | Through envelope — floor only |
| 30 | 0.70 | 0 → floored | Same |

CBRS-style case: large realized \(D\) after IPO → \(C\) falls from “full recycled tail” toward residual/floor → cap and applied gross rise **without** pretending crash risk is gone.

---

## 7. Parameterization (v1)

Add to `CashResidualParams` (names illustrative):

```text
drawdown_shrink_enabled: bool = True
peak_window: int = 756          # Wp sessions for P*
tail_horizon: int = 20          # existing H
dd_floor_abs: float = 0.10      # T_min
dd_floor_frac: float = 0.30     # alpha
# unused in v1:
# dd_hfirst_unlock: bool = False
# dd_hfirst_D_min: float = 0.25
```

Telemetry extras per day:

```text
peak, D, T_raw, T_shrunk, T_floor, C
```

---

## 8. Invariants & guards

1. \(0 \le D_t < 1\) (if \(P^\star \le 0\), skip shrink, fall back to status quo).
2. \(0 \le T^{\mathrm{shrunk}}_t \le T_t\) whenever \(D_t \ge 0\) and \(T_t < 1\).
3. \(C_t \ge T^{\mathrm{floor}}_t\) when shrink enabled (unless retrace \(R_t\) dominates).
4. At \(D_t = 0\): \(C_t = \max(T_t, R_t)\) bit-identical to today (regression lock).
5. IPO / short history: if peak window is thin, require \(\ge\) `anchor_min_obs` points in \(W_p\); else disable shrink for that day (`dd_shrink_reason=insufficient_peak_history`).
6. **Proxy left-tail (SPY returns splice):** \(P^\star\) and \(D\) must be computed on the **same** series fed to crash stats. After the SPY return-splice fix, levels are continuous — OK. Never compute \(D\) on a level-stitched SPY path.

---

## 9. Generalization map

The same pattern applies anywhere we have “scenario severity from a reference state”:

\[
\begin{aligned}
\text{severity severity } S &\quad\text{(today: } T_t\text{)} \\
\text{realized progress } D &\quad\text{(fraction of path already consumed)} \\
\text{residual severity } S' &= g(S, D)
  \quad\text{with}\quad
  g(S,0)=S,\ 
  g(S,D)\downarrow\text{ in }D,\ 
  g(S,D)\ge S_{\mathrm{floor}}.
\end{aligned}
\]

For multiplicative crashes, the natural \(g\) is the **remaining fractional move to the same floor**:

\[
g(S,D) = \max\!\left(0,\ \frac{S-D}{1-D}\right).
\]

Other books can swap:

- \(S =\) vol shock, rate shock, borrow spike intensity, etc.
- \(D =\) fraction of that scenario already printed in spot/IV/spread space.

Cash-residual is just \(S = T_t\), \(D =\) peak drawdown.

---

## 10. Implementation sketch (when building)

1. Extend `conditional_crash_stats` to compute \(P^\star, D, T^{\mathrm{shrunk}}, \widetilde{T}, C\).
2. Thread new fields through `size_day` / `build_cash_residual_pins` telemetry.
3. Unit tests:
   - \(D=0\) ⇒ identical \(C\) to legacy.
   - Monotone: larger \(D\) ⇒ smaller or equal \(\widetilde{T}\).
   - \(D \ge T\) ⇒ \(\widetilde{T} = T^{\mathrm{floor}}\).
   - Synthetic path: peak 100 → 40 with \(T=0.6\) ⇒ residual 0 before floor.
4. Rebuild cash-residual fleet; diff top inverse names (CBRZ should unlock vs pre-shrink; BEZ/SNDQ at highs should be ~unchanged).
5. Flag on UI / path disclaimer: “C residual to peak-envelope floor.”

---

## 11. Acceptance (research)

| Check | Pass |
|-------|------|
| At-peak names (high runup, \(D\approx 0\)) | \(C\) within tol of legacy |
| Deep DD names (CBRS-like) | \(C\) down, end applied gross **up** vs pre-change |
| No path with \(C < T^{\mathrm{floor}}\) when shrink on | yes |
| SPY splice still cliff-free | max und print ≈ real IPO/high, not SPY~$700 |
| h-first gate behavior | unchanged in v1 |

---

## 12. Non-goals (v1)

- Changing \(\rho\), \(\varphi\), or production GTP export.
- Automatically unlocking h-first on drawdown (§5.1 v2). **Done** — see §5.1 status note.
- Replacing historical \(T_t\) with a forward vol model.
- Per-ticker calibrated floors (keep global \(\alpha, T_{\min}\)).

---

## 13. Summary formula (v1)

\[
\boxed{
\begin{aligned}
D_t &= 1 - P_t/P^\star_t, \\
T^{\mathrm{shrunk}}_t &= \max\big(0,\ (T_t - D_t)/(1 - D_t)\big), \\
\widetilde{T}_t &= \max\big(T^{\mathrm{shrunk}}_t,\ \max(T_{\min},\ \alpha T_t)\big), \\
C_t &= \max\big(\widetilde{T}_t,\ R_t\big).
\end{aligned}
}
\]

Status quo recovered at \(D_t = 0\) or with shrink disabled.

---

## 14. Reporting

Counterfactual PDF (hedge \(h\) + gross target/applied over time for **inverse** names with Net edge \(>50\%\), as if Magis implemented these rules):

```bash
python scripts/plot_cr_proposed_trades_pdf.py
# → data/runs/cr_proposed_trades_h_gross.pdf
```

Cover page states Magis production still uses `scale_to_budget: true`; charts read `data/bucket4_cash_residual_path/`. Default filter: `product_class=inverse` (or β<0) and `net_edge_p50_annual > 0.50`.
