"""Per-pair Bucket 4 backtest engine (dynamic hedge path)."""

from __future__ import annotations

import numpy as np
import pandas as pd

V6_OPT2_H_BASE = 0.75


def realized_hedge_ratio(
    etf_notional: float,
    und_notional: float,
    *,
    beta_abs: float = 2.0,
) -> float:
    """Production-compatible realized h: |und| / (|β| · |etf|)."""
    e = abs(float(etf_notional))
    u = abs(float(und_notional))
    b = abs(float(beta_abs))
    if e <= 1e-12 or b <= 1e-12:
        return float("nan")
    return u / (b * e)


def run_bucket4_backtest_dynamic_h(
    prices: pd.DataFrame,
    h_daily: pd.Series,
    rebal_dates: pd.DatetimeIndex,
    *,
    initial_capital: float = 100_000.0,
    gross_multiplier: float = 1.0,
    beta_a: float = -2.0,
    beta_b: float = 1.0,
    borrow_a_annual: float = 0.0,
    borrow_b_annual: float = 0.0,
    borrow_a_series: pd.Series | None = None,
    borrow_b_series: pd.Series | None = None,
    short_proceeds_annual: float = 0.0,
    fee_bps: float = 0.0,
    slippage_bps: float = 0.0,
    opt2_h_base: float | None = None,
    drift_threshold_share_of_gross: float | None = None,
    force_rebalance_after_days: int | None = None,
    membership_start: str | pd.Timestamp | None = None,
    membership_end: str | pd.Timestamp | None = None,
    hard_exit: bool = False,
    force_rebal_dates: pd.DatetimeIndex | list | None = None,
    target_gross_by_date: pd.Series | dict | None = None,
    h_target_by_date: pd.Series | dict | None = None,
    capital_mode: str = "unit_equity",
) -> pd.DataFrame:
    """Mark-to-market two-leg short book with dynamic hedge *h* on rebalance days.

    ``capital_mode``:
      - ``unit_equity``: target gross = gross_multiplier * equity (research default)
      - ``sleeve_dollars``: target gross from ``target_gross_by_date`` on rebalance
        days (Layer A / production-parity). Between rebalances, shares are held.

    ``force_rebal_dates``: dates that always execute (no drift skip) — used when
    pinning production cadence.

    ``h_target_by_date``: optional override of policy h on specific sessions
    (typically production h_used on enter/cadence days).
    """
    h_base = float(opt2_h_base if opt2_h_base is not None else V6_OPT2_H_BASE)
    sleeve_mode = str(capital_mode or "unit_equity").lower() == "sleeve_dollars"
    bt = prices.copy()
    if bt.empty:
        return pd.DataFrame()

    mem_start = pd.Timestamp(membership_start) if membership_start is not None else None
    mem_end = pd.Timestamp(membership_end) if membership_end is not None else None
    if mem_start is not None or mem_end is not None:
        lo = mem_start if mem_start is not None else bt.index.min()
        hi = mem_end if mem_end is not None else bt.index.max()
        bt = bt.loc[(bt.index >= lo) & (bt.index <= hi)].copy()
        if bt.empty:
            return pd.DataFrame()

    h_aligned = h_daily.reindex(bt.index).ffill().fillna(h_base)
    if h_target_by_date is not None:
        h_pin = pd.Series(h_target_by_date)
        h_pin.index = pd.DatetimeIndex(h_pin.index)
        for d, v in h_pin.items():
            if d in h_aligned.index and v is not None and np.isfinite(float(v)):
                h_aligned.loc[d] = float(v)

    gross_pin = None
    if target_gross_by_date is not None:
        gross_pin = pd.Series(target_gross_by_date)
        gross_pin.index = pd.DatetimeIndex(gross_pin.index)

    rb_set = {pd.Timestamp(d) for d in pd.DatetimeIndex(rebal_dates)}
    _force_src = force_rebal_dates if force_rebal_dates is not None else []
    force_set = {pd.Timestamp(d) for d in pd.DatetimeIndex(_force_src)}
    rb_set |= force_set
    if mem_start is not None:
        rb_set = {d for d in rb_set if d >= mem_start}
        force_set = {d for d in force_set if d >= mem_start}
    if mem_end is not None:
        if hard_exit:
            rb_set = {d for d in rb_set if d < mem_end}
            force_set = {d for d in force_set if d < mem_end}
        else:
            rb_set = {d for d in rb_set if d <= mem_end}
            force_set = {d for d in force_set if d <= mem_end}

    bt["rebalance"] = bt.index.map(lambda d: pd.Timestamp(d) in rb_set)
    bt.iloc[0, bt.columns.get_loc("rebalance")] = True
    force_set.add(pd.Timestamp(bt.index[0]))

    a_sh, b_sh = 0.0, 0.0
    cash = float(initial_capital)
    fee_rate = fee_bps / 10_000.0
    slip_rate = float(slippage_bps) / 10_000.0
    borrow_a_const = float(borrow_a_annual) / 252.0
    borrow_b_const = float(borrow_b_annual) / 252.0
    if borrow_a_series is not None:
        ba_ann = (
            pd.Series(borrow_a_series)
            .reindex(bt.index)
            .ffill()
            .fillna(float(borrow_a_annual))
            .clip(lower=0.0)
        )
        ba_daily = ba_ann / 252.0
    else:
        ba_daily = None
    if borrow_b_series is not None:
        bb_ann = (
            pd.Series(borrow_b_series)
            .reindex(bt.index)
            .ffill()
            .fillna(float(borrow_b_annual))
            .clip(lower=0.0)
        )
        bb_daily = bb_ann / 252.0
    else:
        bb_daily = None
    short_proceeds_daily = float(short_proceeds_annual) / 252.0
    beta_inv_abs = abs(float(beta_a))

    rows: list[dict] = []
    first_row = True
    entered = False
    exited = False
    drift_thr = (
        float(drift_threshold_share_of_gross)
        if drift_threshold_share_of_gross is not None
        else None
    )
    clock_floor = int(force_rebalance_after_days) if force_rebalance_after_days else None
    days_since_rebal = 0
    last_target_gross = float(initial_capital) if sleeve_mode else None

    for dt, row in bt.iterrows():
        ap = float(row["a_px"])
        bp = float(row["b_px"])
        ts = pd.Timestamp(dt)
        h_target = float(h_aligned.loc[dt])
        a_pos_notional = a_sh * ap
        b_pos_notional = b_sh * bp
        borrow_a_daily = float(ba_daily.loc[dt]) if ba_daily is not None else borrow_a_const
        borrow_b_daily = float(bb_daily.loc[dt]) if bb_daily is not None else borrow_b_const
        borrow_cost = 0.0
        short_proceeds_credit = 0.0
        rebalance_fee = 0.0
        slippage_cost = 0.0
        rebalance_commission = 0.0
        rebalance_reason = ""

        is_exit_day = bool(hard_exit and mem_end is not None and ts == mem_end)
        forced_today = ts in force_set

        if a_pos_notional < 0:
            borrow_cost += abs(a_pos_notional) * borrow_a_daily
            short_proceeds_credit += abs(a_pos_notional) * short_proceeds_daily
        if b_pos_notional < 0:
            borrow_cost += abs(b_pos_notional) * borrow_b_daily
            short_proceeds_credit += abs(b_pos_notional) * short_proceeds_daily
        financing_pnl = short_proceeds_credit - borrow_cost
        cash += financing_pnl
        equity = cash + a_pos_notional + b_pos_notional

        scheduled_today = bool(row["rebalance"]) and not exited and not is_exit_day
        actually_rebal = scheduled_today
        drift_share = float("nan")
        if scheduled_today and drift_thr is not None and not first_row and entered and not forced_today:
            denom_target = 1.0 + h_target * beta_inv_abs
            target_a_share = 1.0 / denom_target if denom_target > 1e-12 else 0.5
            cur_gross = abs(a_pos_notional) + abs(b_pos_notional)
            if cur_gross <= 1e-9:
                drift_share = 1.0
                actually_rebal = True
            else:
                cur_a_share = abs(a_pos_notional) / cur_gross
                drift_share = abs(cur_a_share - target_a_share)
                actually_rebal = drift_share > drift_thr
                if not actually_rebal and clock_floor is not None and days_since_rebal >= clock_floor:
                    actually_rebal = True
        elif scheduled_today and forced_today:
            actually_rebal = True

        if is_exit_day and not exited:
            # Flatten with the same cash accounting as a retarget to zero
            # (cover shorts / sell longs) — do NOT add |short| notionals to cash.
            target_a_pos, target_b_pos = 0.0, 0.0
            delta_a, delta_b = target_a_pos - a_pos_notional, target_b_pos - b_pos_notional
            traded = abs(delta_a) + abs(delta_b)
            fee = traded * fee_rate
            slip = traded * slip_rate
            rebalance_commission = float(fee)
            rebalance_fee = float(fee + slip)
            slippage_cost = float(slip)
            cash -= delta_a + delta_b + fee + slip
            a_sh, b_sh = 0.0, 0.0
            a_pos_notional, b_pos_notional = 0.0, 0.0
            equity = cash
            actually_rebal = True
            scheduled_today = True
            rebalance_reason = "hard_exit"
            exited = True
            entered = False
        elif actually_rebal and not exited:
            if sleeve_mode:
                pin = None
                if gross_pin is not None and ts in gross_pin.index:
                    try:
                        pin = float(gross_pin.loc[ts])
                    except Exception:  # noqa: BLE001
                        pin = None
                if pin is not None and np.isfinite(pin) and pin > 0:
                    target_gross = pin
                elif last_target_gross is not None and last_target_gross > 0:
                    target_gross = float(last_target_gross)
                else:
                    target_gross = max(0.0, float(gross_multiplier) * abs(float(initial_capital)))
                last_target_gross = target_gross
            else:
                target_gross = max(0.0, float(gross_multiplier) * equity)

            denom = 1.0 + h_target * beta_inv_abs
            n_a = target_gross / denom if denom > 1e-12 else 0.5 * target_gross
            n_b = max(0.0, target_gross - n_a)
            target_a_pos, target_b_pos = -n_a, -n_b
            delta_a, delta_b = target_a_pos - a_pos_notional, target_b_pos - b_pos_notional
            traded = abs(delta_a) + abs(delta_b)
            fee = traded * fee_rate
            slip = traded * slip_rate
            rebalance_commission = float(fee)
            rebalance_fee = float(fee + slip)
            slippage_cost = float(slip)
            cash -= delta_a + delta_b + fee + slip
            a_sh = target_a_pos / ap if ap > 0 else 0.0
            b_sh = target_b_pos / bp if bp > 0 else 0.0
            a_pos_notional, b_pos_notional = a_sh * ap, b_sh * bp
            equity = cash + a_pos_notional + b_pos_notional
            if not entered:
                rebalance_reason = "enter_membership"
                entered = True
            else:
                rebalance_reason = "cadence_resize"

        first_row = False
        days_since_rebal = 0 if actually_rebal else days_since_rebal + 1

        h_realized = realized_hedge_ratio(a_pos_notional, b_pos_notional, beta_abs=beta_inv_abs)
        beta_notional = (
            (-1.0) * float(beta_a) * abs(a_pos_notional) + (-1.0) * float(beta_b) * abs(b_pos_notional)
        )
        rows.append(
            {
                "date": dt,
                "a_px": ap,
                "b_px": bp,
                "cash": cash,
                "a_shares": a_sh,
                "b_shares": b_sh,
                "equity": equity,
                "h_used": h_target,
                "h_target": h_target,
                "h_realized": h_realized,
                "rebalance": bool(actually_rebal),
                "rebalance_scheduled": bool(scheduled_today) or bool(is_exit_day and rebalance_reason == "hard_exit"),
                "rebalance_skipped_below_drift": bool(
                    scheduled_today and not actually_rebal and not is_exit_day and not forced_today
                ),
                "rebalance_reason": rebalance_reason,
                "drift_share_of_gross": float(drift_share),
                "beta_notional": beta_notional,
                "borrow_cost": borrow_cost,
                "short_proceeds_credit": short_proceeds_credit,
                "financing_pnl": financing_pnl,
                "rebalance_fee": rebalance_fee,
                "rebalance_commission": rebalance_commission,
                "slippage_cost": slippage_cost,
                "gross_exposure": abs(a_pos_notional) + abs(b_pos_notional),
            }
        )
    out = pd.DataFrame(rows).set_index("date")
    out["ret"] = out["equity"].pct_change().fillna(0.0)
    out["drawdown"] = out["equity"].div(out["equity"].cummax()).sub(1.0)
    out["beta_exposure_frac"] = np.where(
        out["equity"].abs() > 1e-9, out["beta_notional"] / out["equity"], np.nan
    )
    return out
