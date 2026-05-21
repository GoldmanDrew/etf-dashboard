"""Tests for refresh_underlying_spots merge / fetch logic."""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import refresh_underlying_spots as refresh  # noqa: E402


def test_tradier_takes_priority_over_polygon_and_options_cache():
    underlyings = ["SPY", "TSLA"]
    tradier = {
        "SPY": {"last": 708.0, "as_of": None, "stale": False, "source": "tradier_spot"},
    }
    polygon = {
        "SPY": {"last": 707.0, "as_of": "T", "stale": False, "source": "polygon_last_trade"},
        "TSLA": {"last": 601.0, "as_of": "T", "stale": False, "source": "polygon_last_trade"},
    }
    options_cache = {
        "SPY": {"last": 706.5, "as_of": "U", "stale": False, "source": "options_cache"},
        "TSLA": {"last": 600.0, "as_of": "V", "stale": False, "source": "options_cache"},
    }
    metrics_priors = {
        "SPY": {"prior_close": 700.0, "prior_close_date": "2026-05-19"},
        "TSLA": {"prior_close": 590.0, "prior_close_date": "2026-05-19"},
    }
    out = refresh.merge_sources(
        underlyings,
        tradier=tradier,
        polygon=polygon,
        options_cache=options_cache,
        metrics_priors=metrics_priors,
    )
    assert out["SPY"]["source"] == "tradier_spot"
    assert out["SPY"]["return_d1_so_far"] == pytest.approx(708.0 / 700.0 - 1.0, rel=1e-9)
    assert out["TSLA"]["source"] == "polygon_last_trade"
    assert out["TSLA"]["return_d1_so_far"] == pytest.approx(601.0 / 590.0 - 1.0, rel=1e-9)


def test_polygon_takes_priority_over_options_cache():
    underlyings = ["NVDA"]
    out = refresh.merge_sources(
        underlyings,
        tradier={},
        polygon={"NVDA": {"last": 131.0, "as_of": "T", "stale": False, "source": "polygon_last_trade"}},
        options_cache={"NVDA": {"last": 130.0, "as_of": "U", "stale": False, "source": "options_cache"}},
        metrics_priors={"NVDA": {"prior_close": 128.0, "prior_close_date": "2026-05-19"}},
    )
    assert out["NVDA"]["source"] == "polygon_last_trade"


def test_stale_options_cache_rows_are_omitted_from_merge():
    out = refresh.merge_sources(
        ["AAPL"],
        tradier={},
        polygon={},
        options_cache={},
        metrics_priors={"AAPL": {"prior_close": 230.0, "prior_close_date": "2026-05-19"}},
    )
    assert "AAPL" not in out


def test_missing_prior_close_yields_no_return():
    out = refresh.merge_sources(
        ["NVDA"],
        tradier={},
        polygon={"NVDA": {"last": 130.0, "as_of": "T", "stale": False, "source": "polygon_last_trade"}},
        options_cache={},
        metrics_priors={},
    )
    assert out["NVDA"]["return_d1_so_far"] is None


def test_polygon_sym_dot_conversion():
    assert refresh._polygon_sym("BRK-B") == "BRK.B"
    assert refresh._norm_sym("brk.b") == "BRK-B"


def test_build_payload_counts_sources():
    by_und = {
        "SPY": {"source": "tradier_spot", "stale": False, "return_d1_so_far": 0.01, "volume_so_far": None},
        "AAPL": {"source": "polygon_last_trade", "stale": False, "return_d1_so_far": 0.02, "volume_so_far": None},
        "BRK-B": {"source": "yfinance_fast_info", "stale": False, "return_d1_so_far": 0.0, "volume_so_far": None},
    }
    payload = refresh.build_payload(["SPY", "AAPL", "BRK-B", "FOO"], by_und)
    assert payload["n_underlyings_universe"] == 4
    assert payload["n_underlyings_priced"] == 3
    assert payload["sources"]["tradier_spot"] == 1
    assert payload["sources"]["polygon_last_trade"] == 1
    assert payload["sources"]["yfinance_fast_info"] == 1
    assert payload["n_with_return"] == 3
    assert payload["n_stale"] == 0


def test_fetch_tradier_spots_parses_batch_response():
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.json.return_value = {
        "quotes": {
            "quote": [
                {"symbol": "SPY", "last": 707.5},
                {"symbol": "BRK.B", "last": 450.0},
            ]
        }
    }
    mock_session = MagicMock()
    mock_session.post.return_value = mock_resp

    with patch("requests.Session", return_value=mock_session):
        out = refresh.fetch_tradier_spots(["SPY", "BRK-B"], token="test-token")

    assert out["SPY"]["last"] == pytest.approx(707.5)
    assert out["BRK-B"]["last"] == pytest.approx(450.0)
    assert out["SPY"]["source"] == "tradier_spot"


def test_fetch_polygon_last_spots_parses_trade():
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"results": {"p": 123.45, "t": 1_700_000_000_000_000_000}}

    limiter = refresh._PolygonRateLimiter()
    limiter.get = MagicMock(return_value=(mock_resp, None))  # type: ignore[method-assign]
    mock_session = MagicMock()

    out = refresh.fetch_polygon_last_spots(["SMU"], "test-key", limiter=limiter)
    assert out["SMU"]["last"] == pytest.approx(123.45)
    assert out["SMU"]["source"] == "polygon_last_trade"


def test_load_options_cache_skips_stale_rows(tmp_path: Path):
    cache_path = tmp_path / "options_cache.json"
    cache_path.write_text(
        '{"symbols":{"AAPL":{"spot":200,"stale":true},"MSFT":{"spot":400,"stale":false,"updated_at":"T"}}}',
        encoding="utf-8",
    )
    out = refresh.load_options_cache_spots(cache_path)
    assert "AAPL" not in out
    assert out["MSFT"]["last"] == pytest.approx(400.0)


def test_min_with_return_guard(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    universe = tmp_path / "universe.csv"
    universe.write_text("ETF,Underlying\nAAA,SPY\n", encoding="utf-8")
    out_json = tmp_path / "spot.json"

    monkeypatch.setattr(
        refresh,
        "load_prior_closes_from_metrics",
        lambda *a, **k: {"SPY": {"prior_close": 700.0, "prior_close_date": "2026-05-19"}},
    )
    monkeypatch.setattr(refresh, "load_options_cache_spots", lambda *a, **k: {})
    monkeypatch.setattr(refresh, "fetch_tradier_spots", lambda tickers, **k: {})
    monkeypatch.setattr(refresh, "fetch_polygon_last_spots", lambda tickers, key, **k: {})

    with patch.object(sys, "argv", [
        "refresh_underlying_spots.py",
        "--universe", str(universe),
        "--output", str(out_json),
        "--max-yfinance", "0",
        "--skip-tradier",
        "--skip-polygon",
        "--min-with-return", "1",
    ]):
        rc = refresh.main()
    assert rc == 2
