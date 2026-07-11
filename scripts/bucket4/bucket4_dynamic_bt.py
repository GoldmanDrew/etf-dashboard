"""Per-pair Bucket 4 backtest engine (dynamic hedge path)."""

from __future__ import annotations

import numpy as np
import pandas as pd

V6_OPT2_H_BASE = 0.75


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
) -> pd.DataFrame:
    """Mark-to-market two-leg short book with dynamic hedge *h* on rebalance days.

    Optional ``borrow_a_series`` / ``borrow_b_series`` supply annualized borrow
    per calendar day (point-in-time); otherwise constant ``borrow_*_annual``.
    """
    h_base = float(opt2_h_base if opt2_h_base is not None else V6_OPT2_H_BASE)
    bt = prices.copy()
    h_aligned = h_daily.reindex(bt.index).ffill().fillna(h_base)
    bt["rebalance"] = bt.index.isin(rebal_dates)
    bt.iloc[0, bt.columns.get_loc("rebalance")] = True

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
    drift_thr = (
        float(drift_threshold_share_of_gross)
        if drift_threshold_share_of_gross is not None
        else None
    )
    clock_floor = int(force_rebalance_after_days) if force_rebalance_after_days else None
    days_since_rebal = 0
    for dt, row in bt.iterrows():
        ap = float(row["a_px"])
        bp = float(row["b_px"])
        h = float(h_aligned.loc[dt])
        a_pos_notional = a_sh * ap
        b_pos_notional = b_sh * bp
        borrow_a_daily = float(ba_daily.loc[dt]) if ba_daily is not None else borrow_a_const
        borrow_b_daily = float(bb_daily.loc[dt]) if bb_daily is not None else borrow_b_const
        borrow_cost = 0.0
        short_proceeds_credit = 0.0
        rebalance_fee = 0.0
        slippage_cost = 0.0
        rebalance_commission = 0.0
        if a_pos_notional < 0:
            borrow_cost += abs(a_pos_notional) * borrow_a_daily
            short_proceeds_credit += abs(a_pos_notional) * short_proceeds_daily
        if b_pos_notional < 0:
            borrow_cost += abs(b_pos_notional) * borrow_b_daily
            short_proceeds_credit += abs(b_pos_notional) * short_proceeds_daily
        financing_pnl = short_proceeds_credit - borrow_cost
        cash += financing_pnl
        equity = cash + a_pos_notional + b_pos_notional

        scheduled_today = bool(row["rebalance"])
        actually_rebal = scheduled_today
        drift_share = float("nan")
        if scheduled_today and drift_thr is not None and not first_row:
            denom_target = 1.0 + h * beta_inv_abs
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

        if actually_rebal:
            target_gross = max(0.0, float(gross_multiplier) * equity)
            denom = 1.0 + h * beta_inv_abs
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
        first_row = False
        days_since_rebal = 0 if actually_rebal else days_since_rebal + 1

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
                "h_used": h,
                "rebalance": bool(actually_rebal),
                "rebalance_scheduled": bool(scheduled_today),
                "rebalance_skipped_below_drift": bool(scheduled_today and not actually_rebal),
                "drift_share_of_gross": float(drift_share),
                "beta_notional": beta_notional,
                "borrow_cost": borrow_cost,
                "short_proceeds_credit": short_proceeds_credit,
                "financing_pnl": financing_pnl,
                "rebalance_fee": rebalance_fee,
                "rebalance_commission": rebalance_commission,
                "slippage_cost": slippage_cost,
            }
        )
    out = pd.DataFrame(rows).set_index("date")
    out["ret"] = out["equity"].pct_change().fillna(0.0)
    out["drawdown"] = out["equity"].div(out["equity"].cummax()).sub(1.0)
    out["beta_exposure_frac"] = np.where(
        out["equity"].abs() > 1e-9, out["beta_notional"] / out["equity"], np.nan
    )
    return out
