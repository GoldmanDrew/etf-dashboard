"""Unit tests for cash-residual Optimized path math."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from bucket4.cash_residual_path import (  # noqa: E402
    CashResidualParams,
    build_cash_residual_pins,
    conditional_crash_stats,
    pair_loss,
    residual_crash_from_drawdown,
    size_day,
)


def _synth_close(n: int = 400, end: float = 100.0, runup: float = 0.6) -> pd.Series:
    """Price path with a late run-up so crash stats bind."""
    idx = pd.bdate_range("2024-01-02", periods=n)
    base = np.linspace(end / (1 + runup), end, n)
    noise = np.sin(np.linspace(0, 12, n)) * 0.5
    return pd.Series(base + noise, index=idx, name="close")


def test_pair_loss_decreases_with_h():
    assert pair_loss(0.3, 0.2, 2.0, 0.5) > pair_loss(0.3, 0.8, 2.0, 0.5)


def test_residual_crash_identity():
    assert residual_crash_from_drawdown(0.60, 0.0) == pytest.approx(0.60)
    assert residual_crash_from_drawdown(0.60, 0.20) == pytest.approx(0.50)
    assert residual_crash_from_drawdown(0.60, 0.45) == pytest.approx(0.272727, rel=1e-4)
    assert residual_crash_from_drawdown(0.60, 0.60) == pytest.approx(0.0)
    assert residual_crash_from_drawdown(0.60, 0.70) == pytest.approx(0.0)


def test_drawdown_shrink_at_peak_matches_legacy():
    # Rising path ending at peak → D≈0 → C ≈ legacy max(T, R)
    idx = pd.bdate_range("2023-01-02", periods=400)
    close = pd.Series(np.linspace(50.0, 100.0, len(idx)), index=idx)
    p_on = CashResidualParams(drawdown_shrink_enabled=True, peak_min_obs=40)
    p_off = CashResidualParams(drawdown_shrink_enabled=False)
    a = conditional_crash_stats(close, p_on)
    b = conditional_crash_stats(close, p_off)
    assert a is not None and b is not None
    assert a["drawdown"] == pytest.approx(0.0, abs=1e-9)
    assert a["C"] == pytest.approx(b["C"], rel=1e-9)


def test_drawdown_shrink_reduces_C_after_crash():
    idx = pd.bdate_range("2023-01-02", periods=400)
    # Long quiet rise, then 50% crash into the last 20 sessions
    px = np.linspace(80.0, 100.0, len(idx))
    px[-20:] = np.linspace(100.0, 50.0, 20)
    close = pd.Series(px, index=idx)
    p_off = CashResidualParams(drawdown_shrink_enabled=False, runup_min=0.0)
    p_on = CashResidualParams(drawdown_shrink_enabled=True, runup_min=0.0, dd_floor_abs=0.10, dd_floor_frac=0.30)
    raw = conditional_crash_stats(close, p_off)
    shr = conditional_crash_stats(close, p_on)
    assert raw is not None and shr is not None
    assert shr["drawdown"] > 0.4
    assert shr["C"] < raw["C"] - 1e-6
    assert shr["C"] >= max(p_on.dd_floor_abs, p_on.dd_floor_frac * shr["tail"]) - 1e-9


def test_size_day_leaves_cash_when_capped():
    close = _synth_close()
    p = CashResidualParams(rho=0.0075, h_first_enabled=False, runup_min=0.0)
    out = size_day(
        und_close=close,
        h0=0.45,
        beta=2.0,
        sleeve_budget_usd=168_000.0,
        pair_target_usd=63_000.0,
        edge_annual=0.5,
        params=p,
        l_ema_prev=None,
    )
    assert out["gross_applied_usd"] <= out["gross_target_usd"] + 1e-6
    # With elevated run-up + low h, expect a real trim on this synthetic path
    assert out["crash_mult"] < 1.0 + 1e-9
    assert out["gross_applied_usd"] < out["gross_target_usd"] - 1.0


def test_h_first_raises_h_when_binding():
    close = _synth_close(runup=0.8)
    p = CashResidualParams(rho=0.0075, h_first_enabled=True, runup_min=0.1, edge_floor=0.0)
    out = size_day(
        und_close=close,
        h0=0.45,
        beta=2.0,
        sleeve_budget_usd=168_000.0,
        pair_target_usd=63_000.0,
        edge_annual=0.5,
        params=p,
        l_ema_prev=None,
    )
    if out["h_first_reason"] in ("h_first_solve", "h_max_then_cut"):
        assert out["h1"] >= out["h0"] - 1e-12


def test_h_first_allowed_in_drawdown_without_runup():
    """v2 gate: deep DD (runup=0) should still allow h-first so we don't cut into selloffs."""
    idx = pd.bdate_range("2023-01-02", periods=400)
    px = np.linspace(80.0, 100.0, len(idx))
    px[-40:] = np.linspace(100.0, 55.0, 40)  # ~45% crash, ends below median → runup≈0
    close = pd.Series(px, index=idx)
    p_off = CashResidualParams(
        rho=0.0075,
        h_first_enabled=True,
        runup_min=0.25,
        h_first_on_drawdown=False,
        edge_floor=0.0,
        drawdown_shrink_enabled=True,
    )
    p_on = CashResidualParams(
        rho=0.0075,
        h_first_enabled=True,
        runup_min=0.25,
        h_first_on_drawdown=True,
        dd_h_first_min=0.25,
        edge_floor=0.0,
        drawdown_shrink_enabled=True,
    )
    off = size_day(
        und_close=close,
        h0=0.45,
        beta=2.0,
        sleeve_budget_usd=168_000.0,
        pair_target_usd=42_000.0,
        edge_annual=0.5,
        params=p_off,
        l_ema_prev=None,
    )
    on = size_day(
        und_close=close,
        h0=0.45,
        beta=2.0,
        sleeve_budget_usd=168_000.0,
        pair_target_usd=42_000.0,
        edge_annual=0.5,
        params=p_on,
        l_ema_prev=None,
    )
    assert float(off["runup"] or 0.0) < 0.25
    assert float(off["drawdown"] or 0.0) >= 0.25
    assert off["h_first_reason"] == "runup_gate"
    assert on["h_first_reason"] in ("h_first_solve_dd", "h_first_solve_shrink", "h_max_then_cut")
    assert on["h1"] >= on["h0"] - 1e-12
    assert on["gross_applied_usd"] >= off["gross_applied_usd"] - 1e-6


def test_h_first_dd_blocked_when_C_pathological():
    idx = pd.bdate_range("2023-01-02", periods=400)
    px = np.linspace(100.0, 100.0, len(idx))
    px[-5:] = np.linspace(100.0, 0.5, 5)  # ~99.5% wipe → huge D and absurd residual C
    close = pd.Series(px, index=idx)
    p = CashResidualParams(
        rho=0.0075,
        h_first_enabled=True,
        runup_min=0.25,
        h_first_on_drawdown=True,
        dd_h_first_min=0.25,
        c_h_first_max=1.0,
        dd_h_first_max=0.95,
        edge_floor=0.0,
        drawdown_shrink_enabled=True,
    )
    out = size_day(
        und_close=close,
        h0=0.45,
        beta=2.0,
        sleeve_budget_usd=168_000.0,
        pair_target_usd=42_000.0,
        edge_annual=0.5,
        params=p,
        l_ema_prev=None,
    )
    assert float(out["drawdown"] or 0.0) > 0.95 or float(out["C"] or 0.0) > 1.0
    assert out["h_first_reason"] in ("patho_gate", "runup_gate")
    # Must not unlock to full target via h→1; residual crash-cap may still leave a small book.
    assert out["h1"] <= out["h0"] + 1e-9
    assert out["gross_applied_usd"] < out["gross_target_usd"] * 0.5


def test_resolve_budget_weight_ignores_zero_shard_weight(tmp_path, monkeypatch):
    from build_b4_cash_residual_path import _resolve_budget_weight

    etf = "IREZ"
    shard_dir = tmp_path / "data" / "bucket4_pairs"
    shard_dir.mkdir(parents=True)
    (shard_dir / f"{etf}.json").write_text(
        json.dumps({"summary": {"effective_weight": 0.0}, "effective_weight": 0.0}),
        encoding="utf-8",
    )
    monkeypatch.setattr("build_b4_cash_residual_path.REPO", tmp_path)
    monkeypatch.setattr("build_b4_cash_residual_path.BOOK_VIEW", tmp_path / "missing.json")
    args = argparse.Namespace(budget=None, weight=None)
    budget, weight, src = _resolve_budget_weight(etf, args)
    assert budget == 168_000.0
    assert weight == 0.25
    assert "default_weight" in src


def test_build_pins_freeze_and_delta():
    close = _synth_close()
    dates = [d.strftime("%Y-%m-%d") for d in close.index[-60:]]
    rebal = [1 if i % 7 == 0 else 0 for i in range(len(dates))]
    rebal[0] = 1
    h = {d: 0.5 for d in dates}
    pins = build_cash_residual_pins(
        dates=dates,
        rebalance=rebal,
        h_series=h,
        und_close=close,
        beta=2.0,
        sleeve_budget_usd=168_000.0,
        pair_weight=0.375,
        edge_annual=0.5,
        params=CashResidualParams(h_first_enabled=True, runup_min=0.1),
    )
    tel = pins["telemetry"]
    assert len(tel["gross_applied_usd"]) == len(dates)
    assert pins["summary"]["scale_to_budget"] is False
    # Cadence freeze days should keep prior applied (no nan holes)
    assert all(v is not None for v in tel["gross_applied_usd"])
    # At least one rebalance day recorded
    assert sum(tel["cadence_due"]) >= 2


def test_pins_use_prelisting_und_history_on_day_one():
    """Joint panel starts late; crash L must still fire on first trade day."""
    full = _synth_close(n=400, runup=0.7)
    # Simulate ETF listing on the last 40 sessions only.
    trade_idx = full.index[-40:]
    dates = [d.strftime("%Y-%m-%d") for d in trade_idx]
    rebal = [1 if i % 10 == 0 else 0 for i in range(len(dates))]
    rebal[0] = 1
    h = {d: 0.45 for d in dates}
    pins = build_cash_residual_pins(
        dates=dates,
        rebalance=rebal,
        h_series=h,
        und_close=full,  # pre-listing history included
        beta=2.0,
        sleeve_budget_usd=168_000.0,
        pair_weight=0.25,
        edge_annual=0.5,
        params=CashResidualParams(h_first_enabled=True, runup_min=0.0),
    )
    tel = pins["telemetry"]
    assert tel["L"][0] is not None
    assert tel["reason"][0] != "no_crash_stats"


def test_load_underlying_series_merges_sibling_etfs(tmp_path):
    from bucket4.bucket4_price_loading import load_underlying_adj_close_series

    md = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2025-09-01", "2025-09-02", "2026-01-22", "2026-01-23"] * 2
            ),
            "ticker": ["APLX", "APLX", "APLX", "APLX", "APLZ", "APLZ", "APLZ", "APLZ"],
            "underlying_adj_close": [10.0, 10.5, 12.0, 12.2, np.nan, np.nan, 12.0, 12.2],
        }
    )
    mapping = {"APLX": "APLD", "APLZ": "APLD"}
    s = load_underlying_adj_close_series(
        "APLD",
        metrics=md,
        etf_to_und=mapping,
        yahoo_fallback=False,
        asof="2026-01-23",
        entry_date="2026-01-22",
    )
    assert len(s) >= 4
    assert s.loc[pd.Timestamp("2025-09-01")] == pytest.approx(10.0)


def test_yahoo_extend_triggers_on_thin_pre_entry_history(monkeypatch):
    """Long joint panel must not suppress Yahoo left-tail extend."""
    from bucket4 import bucket4_price_loading as bpl

    idx = pd.bdate_range("2022-07-14", periods=400)
    joint = pd.Series(np.linspace(10, 20, len(idx)), index=idx)
    md = pd.DataFrame(
        {
            "date": idx,
            "ticker": ["NVDS"] * len(idx),
            "underlying_adj_close": joint.to_numpy(),
        }
    )
    y_idx = pd.bdate_range("2021-01-04", periods=600)
    y = pd.Series(np.linspace(5, 15, len(y_idx)), index=y_idx, name="NVDA")

    monkeypatch.setattr(bpl, "_yahoo_adj_close", lambda *a, **k: y)
    s = bpl.load_underlying_adj_close_series(
        "NVDA",
        metrics=md,
        etf_to_und={"NVDS": "NVDA"},
        panel_fallback=joint,
        asof=idx.max(),
        entry_date=idx.min(),
        min_obs_before_entry=252,
        yahoo_fallback=True,
    )
    assert int(s.loc[s.index < idx.min()].shape[0]) >= 200


def test_spy_fallback_splices_returns_not_levels(monkeypatch):
    """IPO und must not inherit SPY dollar prices (CBRS ~$740 cliff bug)."""
    from bucket4 import bucket4_price_loading as bpl

    # Short real IPO panel ~$300
    ipo = pd.bdate_range("2026-05-14", periods=40)
    real = pd.Series(np.linspace(311.0, 210.0, len(ipo)), index=ipo, name="CBRS")
    md = pd.DataFrame(
        {
            "date": ipo,
            "ticker": ["CBRZ"] * len(ipo),
            "underlying_adj_close": real.to_numpy(),
        }
    )
    # SPY-like levels ~700 with mild returns — must NOT appear as CBRS prints
    spy_idx = pd.bdate_range("2024-01-02", periods=400)
    spy = pd.Series(np.linspace(480.0, 720.0, len(spy_idx)), index=spy_idx, name="SPY")

    def _yf(sym, *a, **k):
        if str(sym).upper() == "SPY":
            return spy
        # Thin/no Yahoo for the IPO ticker itself
        return real

    monkeypatch.setattr(bpl, "_yahoo_adj_close", _yf)
    s = bpl.load_underlying_adj_close_series(
        "CBRS",
        metrics=md,
        etf_to_und={"CBRZ": "CBRS"},
        asof=ipo.max(),
        entry_date=ipo.min(),
        min_obs_before_entry=252,
        yahoo_fallback=True,
    )
    pre = s.loc[s.index < ipo.min()]
    assert len(pre) >= 200
    # Scaled to first real print — never SPY's ~$700 absolute level
    assert float(pre.max()) < 400.0
    assert float(pre.iloc[-1]) == pytest.approx(float(real.iloc[0]), rel=1e-6)
    # No fabricated cliff into IPO day
    stitch = s.loc[ipo.min() - pd.Timedelta(days=10) : ipo.min() + pd.Timedelta(days=1)]
    rets = stitch.pct_change().dropna()
    assert float(rets.min()) > -0.35


def test_prepend_proxy_returns_preserves_proxy_rets():
    from bucket4.bucket4_price_loading import _prepend_proxy_returns

    real_idx = pd.bdate_range("2026-05-14", periods=5)
    real = pd.Series([100.0, 101.0, 102.0, 103.0, 104.0], index=real_idx)
    proxy_idx = pd.bdate_range("2026-04-01", periods=30)
    proxy = pd.Series(np.linspace(400.0, 500.0, len(proxy_idx)), index=proxy_idx)
    out = _prepend_proxy_returns(real, proxy)
    pre = out.loc[out.index < real_idx.min()]
    # Proxy returns preserved in left tail
    assert pre.pct_change().dropna().iloc[-1] == pytest.approx(
        proxy.loc[: pre.index[-1]].pct_change().dropna().iloc[-1], rel=1e-9
    )
