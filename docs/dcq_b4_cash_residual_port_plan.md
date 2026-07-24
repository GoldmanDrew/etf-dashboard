# Plan: Port Magis B4 crash-budget + cadence-gated apply → Diamond-Creek-Quant (`scale_to_budget: false`)

Date: 2026-07-23  
Status: **implemented in DCQ** (2026-07-23; scale_to_budget=false)

## Intentional fork

| Repo | `crash_budget.scale_to_budget` | Role |
|------|-------------------------------|------|
| **ls-algo** (Magis) | **`true`** | Production refill: after ρ·budget/L trim, pro-rata scale so sleeve hits `target_weight` |
| **Diamond-Creek-Quant** | **`false`** | Cash residual: trim is real de-risk; room to add when L/run-up ease on cadence-DUE |
| **etf-dashboard** research YAML | **`true`** | Magis production mirror for Book/Pair Report parity tests |

Shared (both books once DCQ is wired):

- Conditional crash `L` (run-up retrace × historical tail) with asymmetric L-EMA  
- `apply_on_cadence_due_only` + `emergency_cut_rel` (compute L daily; change applied weights on DUE / emergency)

---

## Current state (gap analysis)

### Already in Magis `ls-algo` (source of truth for formulas)

- `scripts/b4_crash_budget.py` — `compute_crash_caps`, `cap_pair_weights`, `gate_crash_weights_to_cadence`, L-EMA  
- `generate_trade_plan.py` — crash-budget + cadence gate after opt2 weights  
- `scripts/bucket4_cadence_gate.py` — TR/VCR due/defer  
- Config: `config/strategy_config.yml` → `inverse_decay_bucket4.rules.bucket4_weekly_opt2.crash_budget`  
  - Magis: `scale_to_budget: true`  
  - Also: `apply_on_cadence_due_only: true`, `emergency_cut_rel: 0.25`

### Already in DCQ (partial)

- `config/strategy_config.yml` — B4 opt2 / hedge_cadence / ratchet YAML (largely Magis-shaped) but **no `crash_budget` block**  
- `config/sizing.yaml` — live DCQ GTP path: `sizing_v2` + `bucket_4_dynamic_hedge` + `bucket_4_ratchet`  
- `dcq/sizing/b4_ratchet_overlay.py`, `b4_dynamic_hedge.py`  
- `dcq/policy/cadence.py` — **simpler** VCR stretch gate (not Magis TR/VCR `interval_days` due gate)  
- `scripts/generate_trade_plan.py` — **E_full_stack / Kelly**, does **not** call Magis opt2 → crash-budget stack  

### Missing in DCQ

- Entire `b4_crash_budget` module  
- Cadence-gated freeze of applied crash weights  
- Wiring after B4 sleeve weights in the **actual** DCQ GTP path (`sizing.yaml` / `sizing_v2`)  
- Telemetry CSVs (`b4_crash_budget.csv`, `b4_crash_cadence_gate.csv`) under `data/runs/<date>/`  
- Unit tests for cash-residual + gate behavior  

---

## Target architecture (DCQ)

```text
DCQ GTP (bucket_4 sleeve)
  1) sizing_v2 / sleeve scores → pair weights (existing)
  2) NEW: compute_crash_caps (L daily, L-EMA state)
  3) NEW: cap_pair_weights(..., scale_to_budget=False)  → cash residual
  4) NEW: gate_crash_weights_to_cadence (DUE / freeze / emergency)
  5) existing: weight hysteresis / gross caps (if any)
  6) existing: h_eff + ratchet overlay → legs
  7) exec: only trade when cadence says DUE (align with Magis operator gate)
```

**Do not** turn on Magis `scale_to_budget: true` in DCQ. That is the whole point of the fork.

---

## Workstreams

### WS0 — Config fork (small, first)

**DCQ files**

- Add under `config/sizing.yaml` (preferred; this is what GTP reads) *or*  
  `strategy.sleeves.inverse_decay_bucket4.rules.bucket4_weekly_opt2.crash_budget`:

```yaml
crash_budget:
  enabled: true
  rho: 0.0075
  scale_to_budget: false          # DCQ fork — Magis stays true
  theta: 0.5
  phi: 0.5
  l_floor: 0.02
  missing_policy: book_quantile
  missing_l_quantile: 0.75
  l_ema_alpha: 0.4
  l_state_json: data/b4_crash_l_state.json
  apply_on_cadence_due_only: true
  emergency_cut_rel: 0.25
  applied_weights_json: data/b4_crash_applied_weights.json
```

- Document the Magis vs DCQ fork in `docs/b4_engine_notes.md` (one paragraph + table).

**Acceptance:** config loads; Magis YAML unchanged at `scale_to_budget: true`.

---

### WS1 — Port crash-budget math (no GTP wire yet)

**Copy / adapt from ls-algo → DCQ**

| Source (ls-algo) | Dest (DCQ) |
|------------------|------------|
| `scripts/b4_crash_budget.py` | `dcq/sizing/b4_crash_budget.py` (package import style) |
| `tests/test_b4_crash_budget.py` | `tests/test_b4_crash_budget.py` |

Keep APIs identical: `CrashBudgetParams`, `compute_crash_caps`, `cap_pair_weights`, `gate_crash_weights_to_cadence`, `clamp_sized_to_crash_budget`.

**Acceptance:** DCQ pytest for crash budget + cadence gate (cash mode) green; `scale_to_budget=False` path asserts `budget_eff < budget` when any name is capped.

---

### WS2 — Cadence due-keys for DCQ

**Choice (recommend A):**

- **A.** Port Magis `bucket4_cadence_gate.evaluate_pair_due` / `evaluate_cadence_gate` into `dcq/policy/b4_cadence_gate.py` (or extend `dcq/policy/cadence.py`) using the same TR/VCR `interval_days` formula already in DCQ `strategy_config.yml` hedge_cadence_policy.  
- **B.** Reuse simplified `dcq/policy/cadence.py` only — weaker alignment with Magis; not preferred for this port.

State: `data/b4_cadence_state.json` (pair keys `ETF|UND`) for Magis parity, or map from DCQ `rebalance_cadence_state.json` with an adapter.

**Acceptance:** Given fixed TR/VCR fixtures, due/defer matches Magis gate unit tests (± seed).

---

### WS3 — Wire into DCQ `generate_trade_plan.py`

Insert after B4 sleeve weights are known and **before** ratchet / leg split:

1. Load crash_budget config (`scale_to_budget` must be false).  
2. Build closes / hedge map from DCQ price panel (metrics / pair cache — reuse whatever GTP already loads for `h_eff`).  
3. `compute_crash_caps` → update L state JSON.  
4. `cap_pair_weights(..., scale_to_budget=False)`.  
5. Resolve due keys (WS2) → `gate_crash_weights_to_cadence`.  
6. Persist applied weights.  
7. Write run telemetry under `data/runs/<date>/`.  
8. Feed gated weights into existing gross → `h_eff` → ratchet path.

**Sleeve budget:** use DCQ B4 sleeve budget USD (from `sizing.yaml` `target_weight` × book gross), not Magis `b4_core_cash` name — same ρ math, different capital source.

**Acceptance:** Dry-run GTP on a recent date shows:

- Some pairs with `crash_budget_mult < 1`  
- `scale_to_budget: false` in telemetry  
- Non-zero cash residual when caps bind  
- Deferred pairs `apply_reason=frozen_defer`; DUE pairs update; emergency cut fires in a synthetic unit test  

---

### WS4 — Exec alignment ✅ implemented (2026-07-23)

Ensure Clear Street / DCQ rebalance only **adds back** size when the pair is cadence-eligible (same DUE notion as the plan gate). Otherwise cash residual cannot be redeployed on the intended clock.

**Landed in DCQ:**
- GTP → `data/b4_exec_cadence.json` via `dcq.policy.b4_exec_cadence`
- `scripts/rebalance.py` skips additive B4 redeploys while deferred; covers/emergency pass
- Config: `strategy.yaml` → `b4_exec_cadence`; `sizing.yaml` → `bucket_4_crash_budget.exec_cadence`

---

### WS5 — Research / dashboard (optional, later)

- etf-dashboard research-legacy stays Magis-mirrored (`scale_to_budget: true`).  
- Optional: DCQ risk dashboard panel for cash residual % and top crash-capped names.  
- Optional: A/B harness in DCQ comparing Kelly-only vs Kelly+crash(cash) on the same window (reuse Magis G2 spirit).

---

## Explicitly out of scope (for this port)

- Enabling Magis `scale_to_budget: false` (rejected — Magis stays refill)  
- G3 h-tilt-before-w-cut (follow-up)  
- Replacing DCQ `sizing_v2` with Magis opt2 wholesale (crash-budget is an **overlay** on DCQ weights)  
- Changing Magis production export / dashboard Book ledger  

---

## Suggested implementation order

1. WS0 config + docs note in DCQ  
2. WS1 module + tests (no live behavior change)  
3. WS2 cadence due-keys + tests  
4. WS3 GTP wire behind `crash_budget.enabled`  
5. WS4 exec due alignment  
6. Paper / dry-run week → enable live  

## Sync note

Cloud agents cannot push to DCQ. After implementation locally:

```bash
# from Diamond-Creek-Quant clone
python -m pytest tests/test_b4_crash_budget.py -q
python scripts/generate_trade_plan.py --help   # or dry-run flag you use
git add -A && git commit -m "feat(b4): crash-budget cash residual + cadence-gated apply"
git push origin master
```

Magis `ls-algo` remains the formula SoT; DCQ copies modules and **forks only** `scale_to_budget`.
