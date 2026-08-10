"""Tests for scripts/data_sentinel.py — deterministic data-health sentinel."""
from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import data_sentinel as ds  # noqa: E402

NOW = datetime(2026, 8, 5, 18, 0, tzinfo=UTC)  # Wednesday, mid-RTH


@pytest.fixture()
def repo(tmp_path, monkeypatch):
    """Point the sentinel at a throwaway repo root with a data/ dir."""
    (tmp_path / "data").mkdir()
    monkeypatch.setattr(ds, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(ds, "UNIVERSE_CSV", tmp_path / "data" / "etf_screened_today.csv")
    return tmp_path


def _write(root: Path, rel: str, payload) -> Path:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, str):
        p.write_text(payload, encoding="utf-8")
    else:
        p.write_text(json.dumps(payload), encoding="utf-8")
    return p


def _cfg(**overrides):
    cfg = json.loads((Path(__file__).resolve().parents[1] / "config" / "sentinel.json")
                     .read_text(encoding="utf-8"))
    cfg.update(overrides)
    return cfg


# ---------------------------------------------------------------------------
# Integrity


def test_nan_token_blocks(repo):
    _write(repo, "data/options_cache.json", '{"build_time": "2026-08-05T17:00:00Z", "symbols": {"A": NaN}}')
    findings, payload = ds.check_file_integrity("data/options_cache.json", _cfg(), baseline_bytes=None)
    assert payload is None
    assert any(f["code"] == "parse_error" and f["severity"] == ds.BLOCK for f in findings)


def test_valid_json_passes(repo):
    _write(repo, "data/options_cache.json", {"build_time": "2026-08-05T17:00:00Z", "symbols": {"A": {}}})
    findings, payload = ds.check_file_integrity("data/options_cache.json", _cfg(), baseline_bytes=None)
    assert findings == []
    assert payload["symbols"] == {"A": {}}


def test_missing_required_key_blocks(repo):
    _write(repo, "data/vrp_live.json", {"build_time": "x", "rows": []})  # row_count missing
    findings, _ = ds.check_file_integrity("data/vrp_live.json", _cfg(), baseline_bytes=None)
    assert any(f["code"] == "schema_missing_keys" and f["severity"] == ds.BLOCK for f in findings)


def test_missing_and_empty_artifact_block(repo):
    findings, _ = ds.check_file_integrity("data/vrp_live.json", _cfg(), baseline_bytes=None)
    assert findings[0]["code"] == "missing_artifact"
    _write(repo, "data/vrp_live.json", "")
    findings, _ = ds.check_file_integrity("data/vrp_live.json", _cfg(), baseline_bytes=None)
    assert findings[0]["code"] == "empty_artifact"


def test_directory_paths_pass_through(repo, monkeypatch):
    # config/ci.yaml's nav task commits data/nav_forecasts/snapshots (a DIRECTORY).
    # Blocking it would re-break the realized-accuracy starvation of 2026-05-22.
    (repo / "data" / "nav_forecasts" / "snapshots").mkdir(parents=True)
    _write(repo, "data/nav_forecasts/_latest.json",
           {"anchor_date": "2026-08-05", "build_time": "2026-08-05T17:00:00Z",
            "by_symbol": {}, "confidence_count": {}})
    findings, payload = ds.check_file_integrity("data/nav_forecasts/snapshots", _cfg(),
                                                baseline_bytes=None)
    assert findings == [] and payload is None

    out_file = repo / "gh_output.txt"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out_file))
    monkeypatch.setenv("SENTINEL_MODE", "enforce")
    assert ds.main([
        "gate", "--task", "nav",
        "--files", "data/nav_forecasts/_latest.json data/nav_forecasts/snapshots",
        "--report-out", str(repo / "data" / "sentinel_report.json"),
        "--quarantine", str(repo / "data" / "quarantine.json"),
    ]) == 0
    outputs = dict(line.split("=", 1) for line in out_file.read_text().splitlines())
    assert outputs["verdict"] == "pass" and outputs["blocked"] == ""
    assert "data/nav_forecasts/snapshots" in outputs["files"].split()


def test_record_count_regression_blocks(repo):
    old = {"symbols": {f"S{i}": [] for i in range(100)}, "meta": {}}
    new = {"symbols": {f"S{i}": [] for i in range(20)}, "meta": {}}
    _write(repo, "data/borrow_history.json", new)
    baseline = json.dumps(old).encode() + b" " * 5000  # >4KB so the byte check is armed too
    findings, _ = ds.check_file_integrity("data/borrow_history.json", _cfg(), baseline_bytes=baseline)
    assert any(f["code"] == "record_count_regression" for f in findings)


def test_small_record_drop_is_tolerated(repo):
    old = {"symbols": {f"S{i}": [] for i in range(100)}, "meta": {}}
    new = {"symbols": {f"S{i}": [] for i in range(80)}, "meta": {}}
    _write(repo, "data/borrow_history.json", new)
    findings, _ = ds.check_file_integrity("data/borrow_history.json", _cfg(),
                                          baseline_bytes=json.dumps(old).encode())
    assert not any(f["severity"] == ds.BLOCK for f in findings)


def test_build_time_regression_blocks(repo):
    old = {"build_time": "2026-08-05T17:00:00Z", "symbols": {}}
    new = {"build_time": "2026-08-04T17:00:00Z", "symbols": {}}
    _write(repo, "data/options_cache.json", new)
    findings, _ = ds.check_file_integrity("data/options_cache.json", _cfg(),
                                          baseline_bytes=json.dumps(old).encode())
    assert any(f["code"] == "build_time_regression" for f in findings)


# ---------------------------------------------------------------------------
# Spot anomalies


def _spot_payload(by_symbol, by_underlying=None):
    return {
        "build_time": "2026-08-05T17:55:00Z",
        "by_symbol": by_symbol,
        "by_underlying": by_underlying or {},
        "n_symbols_priced": len(by_symbol),
        "n_symbols_universe": len(by_symbol),
    }


def _bulk_symbols(n=40, ret=0.004):
    return {f"OK{i}": {"last": 10.0, "return_d1_so_far": ret * ((-1) ** i)} for i in range(n)}


def test_return_outlier_quarantined_without_split():
    ctx = {"splits": {}, "metrics_close": {}, "metrics_date": {}, "delta": {}, "universe_count": None}
    syms = _bulk_symbols()
    syms["BAD"] = {"last": 5.0, "return_d1_so_far": 0.42}
    findings = ds.check_spot_anomalies(_spot_payload(syms), ctx, _cfg(), now=NOW)
    hits = [f for f in findings if f["code"] == "return_outlier"]
    assert len(hits) == 1 and hits[0]["ticker"] == "BAD" and hits[0]["severity"] == ds.QUARANTINE


def test_return_outlier_exempt_with_declared_split():
    ctx = {"splits": {"BAD": [{"date": "2026-08-04", "mult": 6.0}]}, "metrics_close": {},
           "metrics_date": {}, "delta": {}, "universe_count": None}
    syms = _bulk_symbols()
    syms["BAD"] = {"last": 5.0, "return_d1_so_far": 0.42}
    findings = ds.check_spot_anomalies(_spot_payload(syms), ctx, _cfg(), now=NOW)
    assert not [f for f in findings if f["code"] == "return_outlier"]


def test_split_exemption_uses_file_date_not_sweep_date():
    # Sweep three weeks later over a stale file must still honor a split that was
    # recent relative to the file's own trading day.
    ctx = {"splits": {"BAD": [{"date": "2026-08-04", "mult": 6.0}]}, "metrics_close": {},
           "metrics_date": {}, "delta": {}, "universe_count": None}
    syms = _bulk_symbols()
    syms["BAD"] = {"last": 5.0, "return_d1_so_far": 0.42}
    late = datetime(2026, 8, 26, 18, 0, tzinfo=UTC)
    findings = ds.check_spot_anomalies(_spot_payload(syms), ctx, _cfg(), now=late)
    assert not [f for f in findings if f["ticker"] == "BAD"]


def test_split_basis_prior_close_diagnosed():
    # KORU-class: +1868%% "return" because 1+r matches the declared 20x multiple
    # of a split executed weeks before the file's trading day.
    ctx = {"splits": {"KORU": [{"date": "2026-07-15", "mult": 20.0}]}, "metrics_close": {},
           "metrics_date": {}, "delta": {}, "universe_count": None}
    syms = _bulk_symbols()
    syms["KORU"] = {"last": 18.82, "return_d1_so_far": 18.686}
    findings = ds.check_spot_anomalies(_spot_payload(syms), ctx, _cfg(), now=NOW)
    hits = [f for f in findings if f.get("ticker") == "KORU"]
    assert len(hits) == 1 and hits[0]["code"] == "split_basis_prior_close"
    assert hits[0]["severity"] == ds.QUARANTINE


def test_underlying_move_corroborated_by_levered_fund():
    # Earnings-day stock move confirmed by its 2x fund: neither is flagged.
    ctx = {"splits": {}, "metrics_close": {}, "metrics_date": {},
           "delta": {"TDL": (2.0, "TDC")}, "universe_count": None}
    syms = _bulk_symbols()
    syms["TDC"] = {"last": 27.47, "return_d1_so_far": -0.201}
    syms["TDL"] = {"last": 10.0, "return_d1_so_far": -0.400}
    findings = ds.check_spot_anomalies(_spot_payload(syms), ctx, _cfg(), now=NOW)
    assert not [f for f in findings if f["code"] == "return_outlier"]


def test_matching_split_multiple_both_directions():
    ctx = {"splits": {"X": [{"date": "2026-07-01", "mult": 20.0}]}}
    assert ds.matching_split_multiple(ctx, "X", 19.7) is not None
    assert ds.matching_split_multiple(ctx, "X", 1 / 19.7) is not None
    assert ds.matching_split_multiple(ctx, "X", 3.0) is None
    assert ds.matching_split_multiple(ctx, "X", -1.0) is None


def test_return_outlier_exempt_with_leverage_corroboration():
    # 2x ETF moving +24% while underlying moved +12% is a real leveraged move.
    ctx = {"splits": {}, "metrics_close": {}, "metrics_date": {},
           "delta": {"LEV2": (2.0, "UND")}, "universe_count": None}
    syms = _bulk_symbols()
    syms["LEV2"] = {"last": 5.0, "return_d1_so_far": 0.24}
    findings = ds.check_spot_anomalies(
        _spot_payload(syms, by_underlying={"UND": {"return_d1_so_far": 0.12}}),
        ctx, _cfg(), now=NOW)
    assert not [f for f in findings if f["code"] == "return_outlier"]


def test_market_event_breaker_suppresses_outliers():
    ctx = {"splits": {}, "metrics_close": {}, "metrics_date": {}, "delta": {}, "universe_count": None}
    syms = {f"S{i}": {"last": 10.0, "return_d1_so_far": -0.30} for i in range(20)}
    syms.update(_bulk_symbols(20))
    findings = ds.check_spot_anomalies(_spot_payload(syms), ctx, _cfg(), now=NOW)
    assert [f for f in findings if f["code"] == "market_event_breaker"]
    assert not [f for f in findings if f["code"] == "return_outlier"]


def _ctx_empty():
    return {"splits": {}, "metrics_close": {}, "metrics_date": {}, "delta": {},
            "universe_count": None}


def test_stale_return_baseline_detected_and_reclassifies_outlier():
    # NOW is Wed 2026-08-05, so the expected prior session is Tue 2026-08-04.
    # A symbol priced against 2026-07-28 is showing a multi-day return labelled
    # daily: it must be named as such, not as a "suspected bad quote".
    syms = _bulk_symbols()
    for e in syms.values():
        e["prior_close_date"] = "2026-08-04"
    syms["MSFO"] = {"last": 13.0, "return_d1_so_far": 0.226,
                    "prior_close_date": "2026-07-28"}
    findings = ds.check_spot_anomalies(_spot_payload(syms), _ctx_empty(), _cfg(), now=NOW)
    codes = {f["code"] for f in findings}
    assert "return_outlier" not in codes
    per_ticker = [f for f in findings if f.get("ticker") == "MSFO"]
    assert len(per_ticker) == 1 and per_ticker[0]["code"] == "stale_return_baseline"
    fleet = [f for f in findings if f["code"] == "stale_return_baseline" and not f.get("ticker")]
    assert len(fleet) == 1 and "1/41" in fleet[0]["detail"]


def test_stale_baseline_fleet_severity_scales_with_fraction():
    cfg = _cfg()
    # 1 of 41 (2.4%) is under the 5% fleet threshold -> WARN.
    syms = _bulk_symbols()
    for e in syms.values():
        e["prior_close_date"] = "2026-08-04"
    syms["ONE"] = {"last": 1.0, "return_d1_so_far": 0.01, "prior_close_date": "2026-07-28"}
    fleet = [f for f in ds.check_spot_anomalies(_spot_payload(syms), _ctx_empty(), cfg, now=NOW)
             if f["code"] == "stale_return_baseline" and not f.get("ticker")]
    assert fleet[0]["severity"] == ds.WARN
    # Half the fleet stale -> QUARANTINE.
    syms2 = _bulk_symbols()
    for i, (s, e) in enumerate(syms2.items()):
        e["prior_close_date"] = "2026-07-28" if i % 2 else "2026-08-04"
    fleet2 = [f for f in ds.check_spot_anomalies(_spot_payload(syms2), _ctx_empty(), cfg, now=NOW)
              if f["code"] == "stale_return_baseline" and not f.get("ticker")]
    assert fleet2[0]["severity"] == ds.QUARANTINE


def test_suppressed_stale_baseline_warns_instead_of_quarantining():
    # refresh_underlying_spots now withholds return_d1_so_far when the baseline
    # is not the previous session. Nothing wrong is being served, so half a
    # fleet of withheld returns must not quarantine the artifact -- but the
    # stalled metrics tail behind it still has to be reported.
    syms = _bulk_symbols()
    for i, (s, e) in enumerate(syms.items()):
        if i % 2:
            e["prior_close_date"] = "2026-07-28"
            e["return_d1_so_far"] = None
        else:
            e["prior_close_date"] = "2026-08-04"
    findings = ds.check_spot_anomalies(_spot_payload(syms), _ctx_empty(), _cfg(), now=NOW)
    assert not any(f["code"] == "stale_return_baseline" for f in findings)
    suppressed = [f for f in findings if f["code"] == "stale_return_baseline_suppressed"]
    assert len(suppressed) == 1
    assert suppressed[0]["severity"] == ds.WARN
    assert suppressed[0]["observed"] == 20


def test_correct_baseline_still_flags_genuine_outlier():
    # Regression guard: the baseline check must not swallow real bad quotes.
    syms = _bulk_symbols()
    for e in syms.values():
        e["prior_close_date"] = "2026-08-04"
    syms["BAD"] = {"last": 5.0, "return_d1_so_far": 0.42, "prior_close_date": "2026-08-04"}
    findings = ds.check_spot_anomalies(_spot_payload(syms), _ctx_empty(), _cfg(), now=NOW)
    hits = [f for f in findings if f.get("ticker") == "BAD"]
    assert len(hits) == 1 and hits[0]["code"] == "return_outlier"


def test_baseline_check_skipped_on_non_session_file_date():
    syms = _bulk_symbols()
    for e in syms.values():
        e["prior_close_date"] = "2026-07-28"
    payload = _spot_payload(syms)
    payload["build_time"] = "2026-08-08T17:55:00Z"  # Saturday
    saturday = datetime(2026, 8, 8, 17, 55, tzinfo=UTC)
    findings = ds.check_spot_anomalies(payload, _ctx_empty(), _cfg(), now=saturday)
    assert not [f for f in findings if f["code"] == "stale_return_baseline"]


def test_zombie_spot_quarantined():
    # NBIZ-class: spot $1.89 vs metrics close $34 with no declared split.
    ctx = {"splits": {}, "metrics_close": {"NBIZ": 34.0}, "metrics_date": {"NBIZ": "2026-08-04"},
           "delta": {}, "universe_count": None}
    syms = _bulk_symbols()
    syms["NBIZ"] = {"last": 1.89, "return_d1_so_far": 0.001}
    findings = ds.check_spot_anomalies(_spot_payload(syms), ctx, _cfg(), now=NOW)
    hits = [f for f in findings if f["code"] == "zombie_spot"]
    assert len(hits) == 1 and hits[0]["ticker"] == "NBIZ"


def test_zombie_spot_exempt_after_split_or_stale_metrics():
    cfg = _cfg()
    syms = _bulk_symbols()
    syms["NBIZ"] = {"last": 1.89, "return_d1_so_far": 0.001}
    ctx = {"splits": {"NBIZ": [{"date": "2026-08-03", "mult": None}]},
           "metrics_close": {"NBIZ": 34.0}, "metrics_date": {"NBIZ": "2026-08-04"},
           "delta": {}, "universe_count": None}
    assert not [f for f in ds.check_spot_anomalies(_spot_payload(syms), ctx, cfg, now=NOW)
                if f["code"] == "zombie_spot"]
    ctx = {"splits": {}, "metrics_close": {"NBIZ": 34.0}, "metrics_date": {"NBIZ": "2026-07-01"},
           "delta": {}, "universe_count": None}
    assert not [f for f in ds.check_spot_anomalies(_spot_payload(syms), ctx, cfg, now=NOW)
                if f["code"] == "zombie_spot"]


def test_zombie_reclassified_as_split_basis_when_multiple_matches():
    # Metrics close still pre-split: 34 -> spot 1.89 is ~1/18 = a declared 18x split.
    syms = _bulk_symbols()
    syms["NBIZ"] = {"last": 1.89, "return_d1_so_far": 0.001}
    ctx = {"splits": {"NBIZ": [{"date": "2026-06-01", "mult": 18.0}]},
           "metrics_close": {"NBIZ": 34.0}, "metrics_date": {"NBIZ": "2026-08-04"},
           "delta": {}, "universe_count": None}
    findings = ds.check_spot_anomalies(_spot_payload(syms), ctx, _cfg(), now=NOW)
    hits = [f for f in findings if f.get("ticker") == "NBIZ"]
    assert len(hits) == 1 and hits[0]["code"] == "split_basis_metrics_close"


# ---------------------------------------------------------------------------
# Coverage / dashboard / nav / vrp


def test_dashboard_universe_coverage_warns_not_blocks():
    # WARN by design: healthy steady state sits near 94.8% (528/557 observed on
    # main), so a BLOCK here would freeze dashboard commits on healthy data.
    ctx = {"splits": {}, "metrics_close": {}, "metrics_date": {}, "delta": {}, "universe_count": 564}
    payload = {"summary": {}, "records": [{"symbol": f"S{i}"} for i in range(400)]}
    findings = ds.check_dashboard(payload, ctx, _cfg())
    hits = [f for f in findings if f["code"] == "universe_coverage_drop"]
    assert len(hits) == 1 and hits[0]["severity"] == ds.WARN
    # 528/557 (the real steady state) must produce no coverage finding at all.
    ctx["universe_count"] = 557
    payload["records"] = [{"symbol": f"S{i}"} for i in range(528)]
    assert not ds.check_dashboard(payload, ctx, _cfg())


def test_nav_na_fraction_tiers():
    cfg = _cfg()
    warn = {"confidence_count": {"high": 30, "medium": 5, "na": 65}}
    block = {"confidence_count": {"high": 5, "medium": 5, "na": 90}}
    ok = {"confidence_count": {"high": 60, "medium": 20, "na": 20}}
    assert any(f["code"] == "nav_na_elevated" for f in ds.check_nav(warn, cfg))
    assert any(f["code"] == "nav_na_collapse" and f["severity"] == ds.BLOCK
               for f in ds.check_nav(block, cfg))
    assert not ds.check_nav(ok, cfg)


def test_vrp_expired_actionable_warns():
    payload = {"rows": [
        {"yb_etf": "AMYY", "expiry": "2026-08-03", "actionable": True},
        {"yb_etf": "TSYY", "expiry": "2026-08-03", "actionable": False},
        {"yb_etf": "NVYY", "expiry": "2026-09-04", "actionable": True},
    ], "row_count": 3}
    findings = ds.check_vrp(payload, _cfg(), now=NOW)
    hits = [f for f in findings if f["code"] == "vrp_expired_actionable"]
    assert len(hits) == 1 and "AMYY" in hits[0]["detail"] and "TSYY" not in hits[0]["detail"]


def test_spot_coverage_severity_depends_on_rth():
    payload = {"n_symbols_priced": 100, "n_symbols_universe": 800}
    cfg = _cfg()
    rth = ds.check_spot_coverage(payload, cfg, rth=True)
    off = ds.check_spot_coverage(payload, cfg, rth=False)
    assert rth[0]["severity"] == ds.QUARANTINE
    assert off[0]["severity"] == ds.WARN


# ---------------------------------------------------------------------------
# Market-age / staleness


def test_market_age_skips_weekend():
    friday_close = datetime(2026, 7, 31, 21, 0, tzinfo=UTC)   # Friday
    monday_open = datetime(2026, 8, 3, 14, 0, tzinfo=UTC)     # Monday
    age = ds.market_age_hours(friday_close, monday_open)
    assert age < 24.0  # 65 raw hours minus Sat+Sun

    tue = datetime(2026, 8, 4, 21, 0, tzinfo=UTC)
    wed = datetime(2026, 8, 5, 21, 0, tzinfo=UTC)
    assert ds.market_age_hours(tue, wed) == pytest.approx(24.0)


def test_staleness_flags_old_dashboard(repo):
    old = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
    _write(repo, "data/dashboard_data.json",
           {"build_time": old.isoformat().replace("+00:00", "Z")})
    findings = ds.check_staleness(_cfg(), now=NOW)
    assert any(f["code"] == "artifact_stale" and f["artifact"] == "data/dashboard_data.json"
               for f in findings)


def test_staleness_spot_skipped_early_in_session(repo):
    # The 13:25 UTC pre-open sweep sees a spot file last written at the prior
    # session's close (~15h). That is not staleness — the feed only runs during
    # RTH — so it must not WARN until the session has run past the 2h budget.
    _write(repo, "data/underlying_intraday_spot.json",
           {"build_time": "2026-08-04T22:45:00Z"})
    pre_open = datetime(2026, 8, 5, 13, 25, tzinfo=UTC)
    assert not [f for f in ds.check_staleness(_cfg(), now=pre_open)
                if f["artifact"] == "data/underlying_intraday_spot.json"]
    # Well into the session with the same stale file, it is a genuine finding.
    midday = datetime(2026, 8, 5, 18, 25, tzinfo=UTC)
    assert [f for f in ds.check_staleness(_cfg(), now=midday)
            if f["artifact"] == "data/underlying_intraday_spot.json"]


def test_staleness_spot_skipped_off_hours(repo):
    _write(repo, "data/underlying_intraday_spot.json",
           {"build_time": "2026-07-30T12:00:00Z"})
    weekend = datetime(2026, 8, 8, 18, 0, tzinfo=UTC)  # Saturday
    findings = ds.check_staleness(_cfg(), now=weekend)
    assert not any(f["artifact"] == "data/underlying_intraday_spot.json" for f in findings)


# ---------------------------------------------------------------------------
# Quarantine lifecycle


def test_quarantine_flag_then_recover():
    cfg = _cfg()
    manifest = {"schema_v": 1, "tickers": {}, "artifacts": {}}
    hit = [ds.finding(ds.QUARANTINE, "zombie_spot", "data/underlying_intraday_spot.json",
                      "bad", ticker="NBIZ")]
    manifest = ds.apply_quarantine(manifest, hit, cfg, now=NOW, full_sweep=True)
    assert "NBIZ" in manifest["tickers"]
    assert manifest["tickers"]["NBIZ"]["clean_streak"] == 0

    manifest = ds.apply_quarantine(manifest, [], cfg, now=NOW, full_sweep=True)
    assert manifest["tickers"]["NBIZ"]["clean_streak"] == 1
    manifest = ds.apply_quarantine(manifest, [], cfg, now=NOW, full_sweep=True)
    assert "NBIZ" not in manifest["tickers"]


def test_quarantine_gate_does_not_advance_recovery():
    cfg = _cfg()
    manifest = {"schema_v": 1, "tickers": {}, "artifacts": {}}
    hit = [ds.finding(ds.QUARANTINE, "zombie_spot", "x", "bad", ticker="NBIZ")]
    manifest = ds.apply_quarantine(manifest, hit, cfg, now=NOW, full_sweep=True)
    for _ in range(5):
        manifest = ds.apply_quarantine(manifest, [], cfg, now=NOW, full_sweep=False)
    assert "NBIZ" in manifest["tickers"]  # gate runs never clear entries


def test_block_finding_holds_artifact_in_sweep():
    cfg = _cfg()
    manifest = {"schema_v": 1, "tickers": {}, "artifacts": {}}
    hit = [ds.finding(ds.BLOCK, "parse_error", "data/options_cache.json", "NaN token")]
    manifest = ds.apply_quarantine(manifest, hit, cfg, now=NOW, full_sweep=True)
    assert "data/options_cache.json" in manifest["artifacts"]


# ---------------------------------------------------------------------------
# Gate end-to-end


def test_gate_drops_blocked_file_keeps_rest(repo, monkeypatch, capsys):
    _write(repo, "data/options_cache.json", '{"build_time": "t", "symbols": {"A": NaN}}')
    _write(repo, "data/vrp_health.json", {"build_time": "2026-08-05T17:00:00Z"})
    _write(repo, "data/ci_state.json", {"last_options_utc": "2026-08-05T17:00:00Z"})
    out_file = repo / "gh_output.txt"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out_file))
    monkeypatch.setenv("SENTINEL_MODE", "enforce")

    rc = ds.main([
        "gate", "--task", "options",
        "--files", "data/options_cache.json data/vrp_health.json data/ci_state.json",
        "--report-out", str(repo / "data" / "sentinel_report.json"),
        "--quarantine", str(repo / "data" / "quarantine.json"),
    ])
    assert rc == 0
    outputs = dict(line.split("=", 1) for line in out_file.read_text().splitlines())
    assert outputs["verdict"] == "block"
    kept = outputs["files"].split()
    assert "data/options_cache.json" not in kept
    assert "data/vrp_health.json" in kept and "data/ci_state.json" in kept
    assert "data/quarantine.json" in kept and "data/sentinel_report.json" in kept
    assert outputs["blocked"] == "data/options_cache.json"
    report = json.loads((repo / "data" / "sentinel_report.json").read_text())
    assert report["verdict"] == "block" and report["mode"] == "gate"


def test_gate_report_mode_keeps_blocked_files(repo, monkeypatch):
    _write(repo, "data/options_cache.json", '{"build_time": "t", "symbols": {"A": NaN}}')
    out_file = repo / "gh_output.txt"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out_file))
    monkeypatch.setenv("SENTINEL_MODE", "report")
    rc = ds.main([
        "gate", "--files", "data/options_cache.json",
        "--report-out", str(repo / "data" / "sentinel_report.json"),
        "--quarantine", str(repo / "data" / "quarantine.json"),
    ])
    assert rc == 0
    outputs = dict(line.split("=", 1) for line in out_file.read_text().splitlines())
    assert outputs["verdict"] == "block"
    assert "data/options_cache.json" in outputs["files"].split()


def test_gate_clean_pass(repo, monkeypatch):
    _write(repo, "data/vrp_health.json", {"build_time": "2026-08-05T17:00:00Z"})
    out_file = repo / "gh_output.txt"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out_file))
    monkeypatch.setenv("SENTINEL_MODE", "enforce")
    rc = ds.main([
        "gate", "--files", "data/vrp_health.json",
        "--report-out", str(repo / "data" / "sentinel_report.json"),
        "--quarantine", str(repo / "data" / "quarantine.json"),
    ])
    assert rc == 0
    outputs = dict(line.split("=", 1) for line in out_file.read_text().splitlines())
    assert outputs["verdict"] == "pass"
    assert outputs["blocked"] == ""


# ---------------------------------------------------------------------------
# Provider ledger


def test_provider_drift_detected(repo):
    ledger_path = repo / ".github" / "data-memory" / "provider_health.json"
    ledger_path.parent.mkdir(parents=True)
    ledger_path.write_text(json.dumps({"schema_v": 1, "entries": [
        {"date": "2026-08-04", "ts": "2026-08-04T20:00:00Z",
         "metrics_providers": {"defiance": 40, "yfinance": 100}, "ibkr": {"success": True}},
    ]}), encoding="utf-8")
    snap = {"date": "2026-08-05", "ts": "2026-08-05T20:00:00Z",
            "metrics_providers": {"defiance": 4, "yfinance": 30}, "ibkr": {"success": True}}
    findings = ds.update_provider_ledger(ledger_path, snap, _cfg())
    drift = [f for f in findings if f["code"] == "provider_drift"]
    assert len(drift) == 1 and "defiance" in drift[0]["detail"]
    # yfinance is a market fallback, not an issuer feed — its churn must not alert.
    ledger = json.loads(ledger_path.read_text())
    assert len(ledger["entries"]) == 2


def test_provider_drift_survives_same_day_resweep(repo):
    # The verify sweep (and any later same-day sweep) must still compare against
    # the last PRIOR-date entry — otherwise the first sweep's own ledger write
    # erases the drift finding minutes after detecting it.
    ledger_path = repo / ".github" / "data-memory" / "provider_health.json"
    ledger_path.parent.mkdir(parents=True)
    ledger_path.write_text(json.dumps({"schema_v": 1, "entries": [
        {"date": "2026-08-04", "ts": "2026-08-04T20:00:00Z",
         "metrics_providers": {"defiance": 40}, "ibkr": {"success": True}},
    ]}), encoding="utf-8")
    snap = {"date": "2026-08-05", "ts": "2026-08-05T13:30:00Z",
            "metrics_providers": {"defiance": 4}, "ibkr": {"success": True}}
    first = ds.update_provider_ledger(ledger_path, snap, _cfg())
    snap2 = dict(snap, ts="2026-08-05T13:35:00Z")
    second = ds.update_provider_ledger(ledger_path, snap2, _cfg())
    assert any(f["code"] == "provider_drift" for f in first)
    assert any(f["code"] == "provider_drift" for f in second)


def test_provider_ledger_same_day_overwrites(repo):
    ledger_path = repo / ".github" / "data-memory" / "provider_health.json"
    ledger_path.parent.mkdir(parents=True)
    snap1 = {"date": "2026-08-05", "ts": "2026-08-05T13:00:00Z", "metrics_providers": {}}
    snap2 = {"date": "2026-08-05", "ts": "2026-08-05T20:00:00Z", "metrics_providers": {}}
    ds.update_provider_ledger(ledger_path, snap1, _cfg())
    ds.update_provider_ledger(ledger_path, snap2, _cfg())
    ledger = json.loads(ledger_path.read_text())
    assert len(ledger["entries"]) == 1
    assert ledger["entries"][0]["ts"] == "2026-08-05T20:00:00Z"


# ---------------------------------------------------------------------------
# Alert


def test_alert_dry_run_on_findings(repo, capsys):
    report = {"schema_v": 1, "build_time": "2026-08-05T18:00:00Z", "mode": "sweep",
              "verdict": "block", "findings": [
                  {"severity": "block", "code": "parse_error",
                   "artifact": "data/options_cache.json", "detail": "NaN token"}]}
    rp = _write(repo, "data/sentinel_report.json", report)
    rc = ds.main(["alert", "--report-out", str(rp), "--dry-run"])
    assert rc == 0
    assert "[dry-run] would alert" in capsys.readouterr().out


def test_alert_pass_no_issue(repo, capsys):
    rp = _write(repo, "data/sentinel_report.json",
                {"verdict": "pass", "findings": [], "build_time": "x", "mode": "sweep"})
    rc = ds.main(["alert", "--report-out", str(rp), "--dry-run"])
    assert rc == 0


def test_verify_sweep_does_not_advance_recovery(repo, monkeypatch):
    # sweep --verify re-checks after a heal in the same run; clean_streak must
    # only advance once per observation window, not once per invocation.
    for rel in ds.SWEEP_ARTIFACTS:
        if rel == "data/vrp_health.json":
            _write(repo, rel, {"build_time": "2026-08-05T17:00:00Z"})
    q = repo / "data" / "quarantine.json"
    _write(repo, "data/quarantine.json", {
        "schema_v": 1,
        "tickers": {"NBIZ": {"first_seen": "x", "last_seen": "x", "clean_streak": 0,
                             "reasons": []}},
        "artifacts": {}})
    out_file = repo / "gh_output.txt"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out_file))
    args = ["sweep", "--verify",
            "--report-out", str(repo / "data" / "sentinel_report.json"),
            "--quarantine", str(q),
            "--ledger", str(repo / ".github" / "data-memory" / "provider_health.json")]
    assert ds.main(args) == 0
    manifest = json.loads(q.read_text())
    assert manifest["tickers"]["NBIZ"]["clean_streak"] == 0  # unchanged


def test_findings_fingerprint_stable_and_order_insensitive():
    a = [ds.finding(ds.BLOCK, "parse_error", "data/x.json", "detail A"),
         ds.finding(ds.WARN, "artifact_stale", "data/y.json", "detail B")]
    b = list(reversed([dict(f, detail="different text same identity") for f in a]))
    assert ds._findings_fingerprint(a) == ds._findings_fingerprint(b)
    c = a + [ds.finding(ds.WARN, "artifact_stale", "data/z.json", "d")]
    assert ds._findings_fingerprint(a) != ds._findings_fingerprint(c)


def test_marker_roundtrip():
    mark = ds._marker("abc123def456", "gate", "block", "2026-08-05T18:00:00Z")
    m = ds._MARKER_RE.search(mark)
    assert m and m.group("fp") == "abc123def456"
    assert m.group("mode") == "gate" and m.group("verdict") == "block"
    assert ds.parse_ts(m.group("ts")) is not None


# ---------------------------------------------------------------------------
# Verdict ordering


def test_verdict_precedence():
    b = ds.finding(ds.BLOCK, "x", "a", "d")
    q = ds.finding(ds.QUARANTINE, "x", "a", "d")
    w = ds.finding(ds.WARN, "x", "a", "d")
    assert ds.verdict_of([w, q, b]) == "block"
    assert ds.verdict_of([w, q]) == "quarantine"
    assert ds.verdict_of([w]) == "warn"
    assert ds.verdict_of([]) == "pass"
