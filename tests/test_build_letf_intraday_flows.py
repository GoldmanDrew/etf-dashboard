"""Unit tests for the intraday LETF rebalance-flow estimator."""
from __future__ import annotations

import json
import math
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import build_letf_intraday_flows as intraday  # noqa: E402


def _universe() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "ticker": "UPRO",
            "underlying": "SPY",
            "leverage": 3.0,
            "product_class": "letf",
            "included_in_universe": True,
            "universe_exclusion_reason": None,
        },
        {
            "ticker": "SPXU",
            "underlying": "SPY",
            "leverage": -3.0,
            "product_class": "letf",
            "included_in_universe": True,
            "universe_exclusion_reason": None,
        },
        {
            # Excluded fund: should land in by_fund with quality_flag set,
            # but never count toward the per-underlying aggregate.
            "ticker": "TSLZ",
            "underlying": "TSLA",
            "leverage": -2.0,
            "product_class": "income_overlay",
            "included_in_universe": False,
            "universe_exclusion_reason": "income_overlay",
        },
    ])


def _latest_metrics() -> pd.DataFrame:
    return pd.DataFrame([
        {"ticker": "UPRO", "date": pd.Timestamp("2026-05-19"), "aum": 1_000_000_000.0, "nav": 70.0},
        {"ticker": "SPXU", "date": pd.Timestamp("2026-05-19"), "aum": 500_000_000.0, "nav": 9.0},
        {"ticker": "TSLZ", "date": pd.Timestamp("2026-05-19"), "aum": 50_000_000.0, "nav": 12.0},
    ])


def _spots() -> dict:
    return {
        "SPY": {
            "last": 707.07,
            "prior_close": 700.0,
            "return_d1_so_far": 0.01,
            "volume_so_far": 12_000_000.0,
            "as_of": "2026-05-20T19:55:00Z",
            "source": "polygon_snapshot",
            "stale": False,
        },
        "TSLA": {
            "last": 600.0,
            "prior_close": 600.0,
            "return_d1_so_far": 0.0,
            "as_of": "2026-05-20T19:55:00Z",
            "source": "polygon_snapshot",
            "stale": False,
        },
    }


def test_compute_fund_intraday_signs_match_avellaneda_zhang():
    fund_df = intraday.compute_fund_intraday(_universe(), _latest_metrics(), _spots())
    upro = fund_df[fund_df["ticker"].eq("UPRO")].iloc[0]
    spxu = fund_df[fund_df["ticker"].eq("SPXU")].iloc[0]
    # 3*(3-1)*1B*+1% = +$60M
    assert upro["estimated_close_rebalance_dollars"] == pytest.approx(60_000_000.0, rel=1e-9)
    # Inverse fund mechanics: -3 * (-3 - 1) * 500M * +1% = (+12) * 500M * 0.01 = +$60M.
    # Inverse LETFs buy the underlying on up days; the sign here matches the
    # Avellaneda-Zhang identity: L*(L-1) is +12 even for L = -3.
    assert spxu["estimated_close_rebalance_dollars"] == pytest.approx(60_000_000.0, rel=1e-9)


def test_excluded_fund_marked_but_not_aggregated():
    fund_df = intraday.compute_fund_intraday(_universe(), _latest_metrics(), _spots())
    excluded = fund_df[fund_df["ticker"].eq("TSLZ")].iloc[0]
    assert excluded["quality_flag"] == "income_overlay"
    assert excluded["included_in_aggregate"] == False  # noqa: E712 -- intentional bool comparison

    agg = intraday.aggregate_underlying(fund_df, adv_latest={}, bias_map={})
    # TSLA had no included funds, so it must be absent from the aggregate.
    assert "TSLA" not in agg["underlying"].tolist()


def test_aggregate_with_adv_and_bias_adjustment():
    fund_df = intraday.compute_fund_intraday(_universe(), _latest_metrics(), _spots())
    adv_latest = {
        "SPY": {"underlying_dollar_adv_20d": 30_000_000_000.0, "as_of_date": "2026-05-19"},
    }
    bias_map = {
        # Suppose historical signed error is +5 % (estimate consistently 5 %
        # over realised); adjusted = (1 - 0.05) � estimate = 95 % � est.
        "SPY": {"mean_signed_error_pct": 0.05, "n": 12},
    }
    agg = intraday.aggregate_underlying(fund_df, adv_latest=adv_latest, bias_map=bias_map)
    spy = agg[agg["underlying"].eq("SPY")].iloc[0]
    # Both UPRO and SPXU push +$60M each on a +1 % day ? net +$120M.
    assert spy["estimated_net_close_rebalance_dollars"] == pytest.approx(120_000_000.0, rel=1e-9)
    assert spy["estimated_close_rebalance_pct_adv_20d"] == pytest.approx(120_000_000.0 / 30_000_000_000.0, rel=1e-9)
    assert spy["bias_signed_error_pct"] == pytest.approx(0.05, rel=1e-9)
    # 120_000_000 � (1 ? 0.05) = 114_000_000
    assert spy["estimated_close_rebalance_dollars_bias_adj"] == pytest.approx(114_000_000.0, rel=1e-9)


def test_stale_spot_drops_from_aggregate():
    spots = _spots()
    spots["SPY"] = {
        **spots["SPY"],
        "return_d1_so_far": None,  # stale path: refresh script suppresses ret
        "stale": True,
    }
    fund_df = intraday.compute_fund_intraday(_universe(), _latest_metrics(), spots)
    upro = fund_df[fund_df["ticker"].eq("UPRO")].iloc[0]
    assert upro["quality_flag"] == "stale_spot"
    assert upro["included_in_aggregate"] == False  # noqa: E712


def test_today_history_tail(tmp_path):
    snap_dir = tmp_path / "snaps"
    snap_dir.mkdir()
    p = snap_dir / "2026-05-20.jsonl"
    rows = [
        {"as_of": "2026-05-20T18:00:00Z", "minutes_to_close": 120, "by_underlying": {"SPY": {"estimated_net_close_rebalance_dollars": 100.0, "estimated_net_close_rebalance_dollars_spot": 90.0, "estimated_net_close_rebalance_dollars_nav": 110.0, "estimated_net_close_rebalance_dollars_blend": 100.0}}},
        {"as_of": "2026-05-20T19:00:00Z", "minutes_to_close": 60, "by_underlying": {"SPY": {"estimated_net_close_rebalance_dollars": 200.0, "estimated_net_close_rebalance_dollars_spot": 180.0, "estimated_net_close_rebalance_dollars_nav": 220.0, "estimated_net_close_rebalance_dollars_blend": 200.0}}},
        {"as_of": "2026-05-20T19:55:00Z", "minutes_to_close": 5, "by_underlying": {"SPY": {"estimated_net_close_rebalance_dollars": 250.0, "estimated_net_close_rebalance_dollars_spot": 240.0, "estimated_net_close_rebalance_dollars_nav": 260.0, "estimated_net_close_rebalance_dollars_blend": 250.0}}},
    ]
    with p.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    hist = intraday.read_today_history(snap_dir, "2026-05-20", n=2)
    assert len(hist) == 2
    assert hist[-1]["minutes_to_close"] == 5
    assert hist[-1]["net_by_underlying"]["SPY"] == pytest.approx(250.0)
    assert hist[-1]["net_by_underlying_spot"]["SPY"] == pytest.approx(240.0)
    assert hist[-1]["net_by_underlying_nav"]["SPY"] == pytest.approx(260.0)
    assert hist[-1]["net_by_underlying_blend"]["SPY"] == pytest.approx(250.0)


# ── NAV-implied return helper ────────────────────────────────────────────


def test_implied_underlying_return_inverts_delta_v1():
    """For v1 the inversion is exact ignoring the TER drag (~ 1e-4)."""
    nav_anchor = 38.30
    delta = 2.0
    r_und = 0.012
    nav_hat = nav_anchor * math.exp(delta * r_und)
    r_back = intraday.implied_underlying_return_from_nav(
        nav_hat=nav_hat, nav_anchor=nav_anchor, delta=delta,
    )
    assert r_back == pytest.approx(r_und, abs=1e-9)


def test_implied_underlying_return_handles_negative_delta():
    nav_anchor = 12.0
    delta = -3.0
    r_und = -0.018
    nav_hat = nav_anchor * math.exp(delta * r_und)
    r_back = intraday.implied_underlying_return_from_nav(
        nav_hat=nav_hat, nav_anchor=nav_anchor, delta=delta,
    )
    assert r_back == pytest.approx(r_und, abs=1e-9)


def test_implied_underlying_return_rejects_invalid_inputs():
    assert intraday.implied_underlying_return_from_nav(nav_hat=None, nav_anchor=10, delta=2) is None
    assert intraday.implied_underlying_return_from_nav(nav_hat=10, nav_anchor=None, delta=2) is None
    assert intraday.implied_underlying_return_from_nav(nav_hat=10, nav_anchor=10, delta=0) is None
    assert intraday.implied_underlying_return_from_nav(nav_hat=-1, nav_anchor=10, delta=2) is None
    assert intraday.implied_underlying_return_from_nav(nav_hat=10, nav_anchor=10, delta=float("nan")) is None


# ── Blend weight policy ──────────────────────────────────────────────────


def test_default_blend_weight_high_confidence_progression():
    """delta_v3 > delta_v2 > delta_v1 at high confidence, all clipped."""
    w_v1 = intraday._default_blend_weight("delta_v1", "high")
    w_v2 = intraday._default_blend_weight("delta_v2_ito", "high")
    w_v3 = intraday._default_blend_weight("delta_v3_swap_mark", "high")
    w_yb = intraday._default_blend_weight("yieldboost_putspread_v1", "high")
    assert 0 < w_v1 < w_v2 < w_v3 == w_yb
    assert w_v3 <= intraday.NAV_BLEND_WEIGHT_CEIL


def test_default_blend_weight_medium_confidence_halved():
    high = intraday._default_blend_weight("delta_v2_ito", "high")
    medium = intraday._default_blend_weight("delta_v2_ito", "medium")
    assert medium == pytest.approx(high * intraday.NAV_BLEND_MEDIUM_CONFIDENCE_SCALE)


def test_default_blend_weight_na_or_unknown_model_zero():
    assert intraday._default_blend_weight(None, "high") == 0.0
    assert intraday._default_blend_weight("delta_v2_ito", "na") == 0.0
    assert intraday._default_blend_weight("delta_v2_ito", None) == 0.0
    assert intraday._default_blend_weight("unknown_model", "high") == 0.0


# ── NAV forecast loader staleness ────────────────────────────────────────


def _write_nav_latest(path: Path, *, sym: str, ts: str, **fields: object) -> None:
    payload = {
        "build_time": ts,
        "anchor_date": "2026-05-19",
        "default_models_count": {"delta_v2_ito": 1},
        "by_symbol": {
            sym: {
                "symbol": sym,
                "model": fields.get("model", "delta_v2_ito"),
                "confidence": fields.get("confidence", "high"),
                "ts": ts,
                "delta": fields.get("delta", 2.0),
                "nav_hat": fields.get("nav_hat", 38.30),
                "nav_anchor": fields.get("nav_anchor", 38.0),
                "und_spot_t": 302.25,
                "und_spot_anchor": 300.0,
                "und_symbol": fields.get("und_symbol", "AAPL"),
            }
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_load_nav_forecast_latest_marks_stale_rows(tmp_path):
    nav_path = tmp_path / "_latest.json"
    _write_nav_latest(nav_path, sym="AAPU", ts="2026-05-20T13:30:00Z")
    now = datetime(2026, 5, 20, 14, 0, tzinfo=UTC)  # 30 min later → fresh
    rows, meta = intraday.load_nav_forecast_latest(nav_path, now=now, max_age_sec=35 * 60)
    assert "AAPU" in rows
    assert not rows["AAPU"]["stale"]
    assert meta["n_total"] == 1 and meta["n_stale"] == 0

    # Past the freshness gate.
    later = datetime(2026, 5, 20, 14, 10, tzinfo=UTC)  # 40 min later → stale
    rows2, meta2 = intraday.load_nav_forecast_latest(nav_path, now=later, max_age_sec=35 * 60)
    assert rows2["AAPU"]["stale"] is True
    assert meta2["n_stale"] == 1


def test_load_nav_forecast_latest_skips_zero_delta_or_negative_nav(tmp_path):
    nav_path = tmp_path / "_latest.json"
    _write_nav_latest(nav_path, sym="ZZZZ", ts="2026-05-20T13:30:00Z", delta=0.0)
    rows, _ = intraday.load_nav_forecast_latest(nav_path, now=datetime(2026, 5, 20, 13, 35, tzinfo=UTC))
    assert rows["ZZZZ"]["stale"] is True


# ── Three-way per-fund blend ─────────────────────────────────────────────


def _spy_universe_with_v2_nav() -> tuple[pd.DataFrame, pd.DataFrame, dict, dict]:
    universe = pd.DataFrame([
        {"ticker": "UPRO", "underlying": "SPY", "leverage": 3.0,
         "product_class": "letf", "included_in_universe": True, "universe_exclusion_reason": None},
    ])
    metrics = pd.DataFrame([
        {"ticker": "UPRO", "date": pd.Timestamp("2026-05-19"), "aum": 1_000_000_000.0, "nav": 70.0},
    ])
    spots = {
        "SPY": {
            "last": 707.0, "prior_close": 700.0, "return_d1_so_far": 0.01,
            "as_of": "2026-05-20T19:55:00Z", "source": "polygon_snapshot", "stale": False,
        },
    }
    nav_forecasts = {
        "UPRO": {
            "model": "delta_v2_ito",
            "confidence": "high",
            "ts": "2026-05-20T19:50:00Z",
            "age_sec": 300.0,
            "delta": 3.0,
            # r_nav implied by these = log(1.018/1) / 3 ≈ 0.5950 / 100 = 0.005949 → smaller than 0.01 (vol drag).
            "nav_hat": 70.0 * math.exp(3.0 * 0.005949),
            "nav_anchor": 70.0,
            "und_symbol": "SPY",
            "stale": False,
        },
    }
    return universe, metrics, spots, nav_forecasts


def test_blend_uses_default_weight_when_no_override():
    u, m, s, nav = _spy_universe_with_v2_nav()
    fund_df = intraday.compute_fund_intraday(u, m, s, nav_forecasts=nav)
    row = fund_df[fund_df["ticker"].eq("UPRO")].iloc[0]
    # Spot-only and NAV-only dollar estimates.
    assert row["rebalance_spot"] == pytest.approx(3.0 * 2.0 * 1_000_000_000.0 * 0.01)
    expected_r_nav = math.log(nav["UPRO"]["nav_hat"] / nav["UPRO"]["nav_anchor"]) / 3.0
    assert row["r_nav"] == pytest.approx(expected_r_nav, rel=1e-9)
    expected_nav_dollars = 3.0 * 2.0 * 1_000_000_000.0 * expected_r_nav
    assert row["rebalance_nav"] == pytest.approx(expected_nav_dollars, rel=1e-9)
    # Default weight for delta_v2_ito high = 0.5
    assert row["blend_weight_nav"] == pytest.approx(0.5)
    expected_blend = 0.5 * 0.01 + 0.5 * expected_r_nav
    assert row["r_blend"] == pytest.approx(expected_blend, rel=1e-9)
    assert row["rebalance_blend"] == pytest.approx(3.0 * 2.0 * 1_000_000_000.0 * expected_blend, rel=1e-9)
    # Surfaced default = blend.
    assert row["estimated_close_rebalance_dollars"] == pytest.approx(row["rebalance_blend"], rel=1e-9)


def test_blend_per_ticker_override_wins():
    u, m, s, nav = _spy_universe_with_v2_nav()
    fund_df = intraday.compute_fund_intraday(
        u, m, s, nav_forecasts=nav,
        blend_weight_overrides={"UPRO": 0.8},
    )
    row = fund_df[fund_df["ticker"].eq("UPRO")].iloc[0]
    assert row["blend_source"] == "nav_override"
    assert row["blend_weight_nav"] == pytest.approx(0.8)
    expected_r_nav = math.log(nav["UPRO"]["nav_hat"] / nav["UPRO"]["nav_anchor"]) / 3.0
    expected_blend = 0.2 * 0.01 + 0.8 * expected_r_nav
    assert row["r_blend"] == pytest.approx(expected_blend, rel=1e-9)


def test_blend_falls_back_to_spot_when_nav_stale():
    u, m, s, nav = _spy_universe_with_v2_nav()
    nav["UPRO"]["stale"] = True
    fund_df = intraday.compute_fund_intraday(u, m, s, nav_forecasts=nav)
    row = fund_df[fund_df["ticker"].eq("UPRO")].iloc[0]
    assert row["blend_weight_nav"] == 0.0
    assert row["blend_source"] == "spot_only"
    assert pd.isna(row["rebalance_nav"])
    assert row["r_blend"] == pytest.approx(0.01)
    assert row["estimated_close_rebalance_dollars"] == pytest.approx(row["rebalance_spot"], rel=1e-9)


def test_blend_uses_nav_when_spot_missing_but_nav_fresh():
    """Per-fund quality flag ``ok_nav_only`` -- still aggregable."""
    u, m, s, nav = _spy_universe_with_v2_nav()
    s["SPY"] = {**s["SPY"], "return_d1_so_far": None, "stale": True}
    fund_df = intraday.compute_fund_intraday(u, m, s, nav_forecasts=nav)
    row = fund_df[fund_df["ticker"].eq("UPRO")].iloc[0]
    assert row["quality_flag"] == "ok_nav_only"
    assert pd.isna(row["rebalance_spot"])
    assert row["rebalance_nav"] is not None and not pd.isna(row["rebalance_nav"])
    assert row["r_blend"] == pytest.approx(row["r_nav"], rel=1e-9)
    assert bool(row["included_in_aggregate"]) is True


def test_aggregate_rolls_per_source_dollars_and_fund_counts():
    u, m, s, nav = _spy_universe_with_v2_nav()
    fund_df = intraday.compute_fund_intraday(u, m, s, nav_forecasts=nav)
    agg = intraday.aggregate_underlying(fund_df, adv_latest={}, bias_map={})
    spy = agg[agg["underlying"].eq("SPY")].iloc[0]
    assert spy["estimated_net_close_rebalance_dollars_spot"] == pytest.approx(fund_df.iloc[0]["rebalance_spot"])
    assert spy["estimated_net_close_rebalance_dollars_nav"] == pytest.approx(fund_df.iloc[0]["rebalance_nav"])
    assert spy["estimated_net_close_rebalance_dollars_blend"] == pytest.approx(fund_df.iloc[0]["rebalance_blend"])
    assert int(spy["n_funds_priced_spot"]) == 1
    assert int(spy["n_funds_priced_nav"]) == 1
    assert spy["mean_blend_weight_nav"] == pytest.approx(0.5)
