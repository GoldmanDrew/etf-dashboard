"""Tests for bucket assignment and data freshness logic."""
import datetime as dt
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.universe import load_universe, load_inverse_etfs, assign_buckets, norm_sym
from backend.models import Bucket, ETFRecord
from backend.ibkr_fetcher import fetch_mock, BorrowSnapshot
from backend.decay import estimate_mock_decay, compute_mock_decay_for_universe


# ══════════════════════════════════════════════
# Symbol Normalization
# ══════════════════════════════════════════════
class TestNormSym:
    def test_basic(self):
        assert norm_sym("aapl") == "AAPL"
        assert norm_sym("BRK.B") == "BRK-B"
        assert norm_sym("  sqqq ") == "SQQQ"

    def test_already_normalized(self):
        assert norm_sym("SPY") == "SPY"
        assert norm_sym("BRK-B") == "BRK-B"


# ══════════════════════════════════════════════
# Bucket Assignment
# ══════════════════════════════════════════════
class TestBucketAssignment:

    @pytest.fixture
    def sample_universe(self):
        return pd.DataFrame({
            "ETF": ["TQQQ", "SSO", "SQQQ", "QLD", "LABD"],
            "Underlying": ["QQQ", "SPY", "QQQ", "QQQ", "XBI"],
            "Leverage": [3.0, 2.0, -3.0, 2.0, -3.0],
            "Beta": [2.8, 1.3, -2.9, 1.4, -2.7],
            "symbol": ["TQQQ", "SSO", "SQQQ", "QLD", "LABD"],
            "underlying": ["QQQ", "SPY", "QQQ", "QQQ", "XBI"],
        })

    @pytest.fixture
    def inverse_set(self):
        return {"SQQQ", "LABD", "SDS", "FAZ"}

    def test_inverse_etfs_go_to_bucket_3(self, sample_universe, inverse_set):
        df = assign_buckets(sample_universe, inverse_set, high_beta_threshold=1.5)
        sqqq = df[df["symbol"] == "SQQQ"].iloc[0]
        labd = df[df["symbol"] == "LABD"].iloc[0]
        assert sqqq["bucket"] == Bucket.INVERSE.value
        assert labd["bucket"] == Bucket.INVERSE.value

    def test_high_beta_goes_to_bucket_1(self, sample_universe, inverse_set):
        df = assign_buckets(sample_universe, inverse_set, high_beta_threshold=1.5)
        tqqq = df[df["symbol"] == "TQQQ"].iloc[0]
        assert tqqq["bucket"] == Bucket.HIGH_BETA.value

    def test_low_beta_goes_to_bucket_2(self, sample_universe, inverse_set):
        df = assign_buckets(sample_universe, inverse_set, high_beta_threshold=1.5)
        sso = df[df["symbol"] == "SSO"].iloc[0]
        qld = df[df["symbol"] == "QLD"].iloc[0]
        assert sso["bucket"] == Bucket.LOW_BETA.value
        assert qld["bucket"] == Bucket.LOW_BETA.value

    def test_inverse_overrides_beta(self, sample_universe, inverse_set):
        """Even if an inverse ETF has high absolute beta, it goes to Bucket 3."""
        df = assign_buckets(sample_universe, inverse_set, high_beta_threshold=1.5)
        # SQQQ has beta=-2.9 (absolute > 1.5) but should still be Bucket 3
        sqqq = df[df["symbol"] == "SQQQ"].iloc[0]
        assert sqqq["bucket"] == Bucket.INVERSE.value

    def test_blacklist_removes_symbols(self, sample_universe, inverse_set):
        df = assign_buckets(sample_universe, inverse_set, blacklist=["TQQQ", "SSO"])
        assert "TQQQ" not in df["symbol"].values
        assert "SSO" not in df["symbol"].values
        assert len(df) == 3

    def test_nan_beta_goes_to_bucket_2(self, inverse_set):
        df = pd.DataFrame({
            "ETF": ["MYSTERY"],
            "Underlying": ["SPY"],
            "Leverage": [2.0],
            "Beta": [np.nan],
            "symbol": ["MYSTERY"],
            "underlying": ["SPY"],
        })
        result = assign_buckets(df, inverse_set, high_beta_threshold=1.5)
        assert result.iloc[0]["bucket"] == Bucket.LOW_BETA.value

    def test_threshold_boundary(self, inverse_set):
        """Beta exactly at threshold → Bucket 2 (not >)."""
        df = pd.DataFrame({
            "ETF": ["EXACT"],
            "Underlying": ["SPY"],
            "Leverage": [2.0],
            "Beta": [1.5],
            "symbol": ["EXACT"],
            "underlying": ["SPY"],
        })
        result = assign_buckets(df, inverse_set, high_beta_threshold=1.5)
        assert result.iloc[0]["bucket"] == Bucket.LOW_BETA.value


# ══════════════════════════════════════════════
# Data Freshness / Stale Detection
# ══════════════════════════════════════════════
class TestFreshness:

    def test_fresh_record(self):
        rec = ETFRecord(
            symbol="TQQQ", underlying="QQQ", leverage=3.0,
            bucket=Bucket.HIGH_BETA.value,
            last_updated=dt.datetime.utcnow(),
            is_stale=False,
        )
        assert not rec.is_stale

    def test_stale_record(self):
        old = dt.datetime.utcnow() - dt.timedelta(minutes=10)
        rec = ETFRecord(
            symbol="TQQQ", underlying="QQQ", leverage=3.0,
            bucket=Bucket.HIGH_BETA.value,
            last_updated=old,
            is_stale=True,
        )
        assert rec.is_stale

    def test_missing_borrow_flagged(self):
        rec = ETFRecord(
            symbol="TQQQ", underlying="QQQ", leverage=3.0,
            bucket=Bucket.HIGH_BETA.value,
            borrow_missing=True,
        )
        assert rec.borrow_missing

    def test_stale_detection_logic(self):
        """Simulate the staleness check from refresh_borrow."""
        stale_threshold = 300  # 5 min
        last_updated = dt.datetime.utcnow() - dt.timedelta(seconds=400)
        age = (dt.datetime.utcnow() - last_updated).total_seconds()
        assert age > stale_threshold

        recent = dt.datetime.utcnow() - dt.timedelta(seconds=60)
        age2 = (dt.datetime.utcnow() - recent).total_seconds()
        assert age2 < stale_threshold


# ══════════════════════════════════════════════
# Mock Fetcher
# ══════════════════════════════════════════════
class TestMockFetcher:

    @pytest.fixture
    def small_universe(self):
        return pd.DataFrame({
            "symbol": ["TQQQ", "SSO", "SQQQ"],
            "ETF": ["TQQQ", "SSO", "SQQQ"],
            "Underlying": ["QQQ", "SPY", "QQQ"],
            "Leverage": [3.0, 2.0, -3.0],
            "Beta": [2.8, 1.3, -2.9],
            "borrow_fee_annual": [0.05, 0.03, 0.08],
            "borrow_rebate_annual": [-0.01, 0.01, -0.02],
            "shares_available": [100000, 500000, 50000],
        })

    def test_mock_returns_all_symbols(self, small_universe):
        snap = fetch_mock(small_universe, seed_from_csv=True)
        assert snap.success
        assert "TQQQ" in snap.borrow_map
        assert "SSO" in snap.borrow_map
        assert "SQQQ" in snap.borrow_map

    def test_mock_borrow_is_nonnegative(self, small_universe):
        snap = fetch_mock(small_universe, seed_from_csv=True)
        for sym, rate in snap.borrow_map.items():
            assert rate >= 0, f"{sym} has negative borrow: {rate}"

    def test_mock_has_timestamp(self, small_universe):
        snap = fetch_mock(small_universe, seed_from_csv=True)
        assert snap.timestamp is not None
        assert isinstance(snap.timestamp, dt.datetime)


# ══════════════════════════════════════════════
# Decay Estimation
# ══════════════════════════════════════════════
class TestDecay:

    def test_mock_decay_positive(self):
        """Decay should always be positive for leveraged products."""
        d = estimate_mock_decay(leverage=3.0, beta=2.8, underlying_vol=0.20)
        assert d > 0

    def test_higher_leverage_more_decay(self):
        """3x should decay faster than 2x, on average."""
        np.random.seed(42)
        d2 = estimate_mock_decay(leverage=2.0, beta=1.9, underlying_vol=0.20)
        np.random.seed(42)
        d3 = estimate_mock_decay(leverage=3.0, beta=2.8, underlying_vol=0.20)
        assert d3 > d2

    def test_spread_computation(self):
        np.random.seed(42)
        universe = pd.DataFrame({
            "symbol": ["TQQQ"],
            "Leverage": [3.0],
            "Beta": [2.8],
        })
        borrow_map = {"TQQQ": 0.05}
        result = compute_mock_decay_for_universe(universe, borrow_map)
        assert "TQQQ" in result
        r = result["TQQQ"]
        assert abs(r["spread"] - (r["gross_decay_annual"] - 0.05)) < 1e-6


# ══════════════════════════════════════════════
# Load Real Data (integration test)
# ══════════════════════════════════════════════
class TestLoadRealData:

    @pytest.fixture
    def data_dir(self):
        return Path(__file__).parent.parent / "data"

    def test_load_universe_csv(self, data_dir):
        csv = data_dir / "etf_screened_today.csv"
        if not csv.exists():
            pytest.skip("Universe CSV not present")
        df = load_universe(csv)
        assert len(df) > 100
        assert "symbol" in df.columns
        assert "underlying" in df.columns
        assert "Beta" in df.columns

    def test_full_bucketing_pipeline(self, data_dir):
        csv = data_dir / "etf_screened_today.csv"
        inv_csv = Path(__file__).parent.parent / "config" / "inverse_etfs.csv"
        if not csv.exists() or not inv_csv.exists():
            pytest.skip("Data files not present")

        df = load_universe(csv)
        inv_set = load_inverse_etfs(inv_csv)
        bucketed = assign_buckets(df, inv_set, high_beta_threshold=1.5)

        b1 = bucketed[bucketed["bucket"] == Bucket.HIGH_BETA.value]
        b2 = bucketed[bucketed["bucket"] == Bucket.LOW_BETA.value]
        b3 = bucketed[bucketed["bucket"] == Bucket.INVERSE.value]

        assert len(b1) > 0, "Bucket 1 should have entries"
        assert len(b2) > 0, "Bucket 2 should have entries"
        # Bucket 3 depends on overlap between universe and inverse list
        assert len(b1) + len(b2) + len(b3) == len(bucketed)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
