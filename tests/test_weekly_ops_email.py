"""Tests for weekly ops email builder."""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from weekly_ops_email import (  # noqa: E402
    EARNINGS_HORIZON_DAYS,
    build_email_payload,
    collect_recent_delistings,
    collect_recent_listings,
    collect_upcoming_earnings,
    format_subject,
    headline_looks_like_listing,
    parse_recipients,
    render_plain_text,
)


def test_parse_recipients_handles_mixed_formats():
    raw = "Alice <a@x.com>, b@y.com; c@z.com"
    assert parse_recipients(raw) == ["a@x.com", "b@y.com", "c@z.com"]


def test_default_earnings_horizon_is_seven_days():
    assert EARNINGS_HORIZON_DAYS == 7


def test_collect_upcoming_earnings_filters_window(tmp_path, monkeypatch):
    cal = tmp_path / "known.json"
    cal.write_text(json.dumps({
        "items": [
            {"underlying": "AMD", "event_type": "earnings", "event_date": "2026-07-10", "confirmation": "confirmed"},
            {"underlying": "NVDA", "event_type": "earnings", "event_date": "2026-08-30", "confirmation": "projected"},
            {"underlying": "SPY", "event_type": "earnings", "event_date": "2026-07-12", "confirmation": "projected"},
            {"underlying": "TSLA", "event_type": "earnings", "event_date": "2026-07-22", "confirmation": "confirmed"},
        ],
    }), encoding="utf-8")
    monkeypatch.setattr(
        "weekly_ops_email.load_bucket_underlying_etfs",
        lambda **kwargs: {"AMD": ["AMYY"], "NVDA": ["NVYY"], "SPY": [], "TSLA": ["TSYY"]},
    )
    rows = collect_upcoming_earnings(
        today=date(2026, 7, 8),
        horizon_days=7,
        calendar_path=cal,
        etf_map={"AMD": ["AMYY"], "NVDA": ["NVYY"], "TSLA": ["TSYY"]},
    )
    assert [r["underlying"] for r in rows] == ["AMD"]


def test_collect_upcoming_earnings_seven_day_excludes_day_14(tmp_path):
    cal = tmp_path / "known.json"
    cal.write_text(json.dumps({
        "items": [
            {"underlying": "TSM", "event_type": "earnings", "event_date": "2026-07-16", "confirmation": "confirmed"},
            {"underlying": "TSLA", "event_type": "earnings", "event_date": "2026-07-22", "confirmation": "confirmed"},
        ],
    }), encoding="utf-8")
    rows = collect_upcoming_earnings(
        today=date(2026, 7, 12),
        horizon_days=7,
        calendar_path=cal,
        etf_map={"TSM": ["TMYY"], "TSLA": ["TSYY"]},
    )
    assert [r["underlying"] for r in rows] == ["TSM"]


def test_collect_recent_delistings_lookback(tmp_path):
    corp = tmp_path / "corp.json"
    corp.write_text(json.dumps({
        "events": [
            {
                "type": "delisting",
                "ticker": "OLD1",
                "announcement_date": "2026-07-01",
                "status": "pending",
                "bucket": "bucket_2",
            },
            {
                "type": "delisting",
                "ticker": "OLD2",
                "announcement_date": "2026-06-01",
                "status": "executed",
                "bucket": "bucket_4_edge",
            },
        ],
    }), encoding="utf-8")
    rows = collect_recent_delistings(
        today=date(2026, 7, 8),
        lookback_days=14,
        corporate_actions_path=corp,
    )
    assert len(rows) == 1
    assert rows[0]["ticker"] == "OLD1"
    assert rows[0]["bucket"] == "B2"


def test_collect_recent_delistings_skips_launch_headlines(tmp_path):
    corp = tmp_path / "corp.json"
    corp.write_text(json.dumps({
        "events": [
            {
                "type": "delisting",
                "ticker": "PUL",
                "announcement_date": "2026-07-07",
                "status": "pending",
                "bucket": "bucket_1_high_delta",
                "headline": (
                    "GraniteShares Launches BBUL and PUL, 2x Long ETFs on "
                    "BlackBerry and Everpure - Yahoo Finance"
                ),
            },
            {
                "type": "delisting",
                "ticker": "PAAU",
                "announcement_date": "2026-06-29",
                "status": "pending",
                "bucket": "bucket_1_high_delta",
                "headline": "T-REX 2X LONG PAAS DAILY TARGET ETF (PAAU) to liquidate",
            },
        ],
    }), encoding="utf-8")
    rows = collect_recent_delistings(
        today=date(2026, 7, 12),
        lookback_days=14,
        corporate_actions_path=corp,
    )
    assert [r["ticker"] for r in rows] == ["PAAU"]


def test_collect_recent_listings(tmp_path):
    corp = tmp_path / "corp.json"
    corp.write_text(json.dumps({
        "events": [
            {
                "type": "listing",
                "ticker": "BBUL",
                "announcement_date": "2026-07-07",
                "status": "pending",
                "bucket": "bucket_1_high_delta",
                "headline": "GraniteShares Launches BBUL and PUL",
            },
            {
                "type": "delisting",
                "ticker": "PAAU",
                "announcement_date": "2026-06-29",
                "status": "pending",
                "headline": "PAAU to liquidate",
            },
            {
                "type": "listing",
                "ticker": "OLDX",
                "announcement_date": "2026-06-01",
                "status": "pending",
                "headline": "Issuer Launches OLDX",
            },
        ],
    }), encoding="utf-8")
    rows = collect_recent_listings(
        today=date(2026, 7, 12),
        lookback_days=14,
        corporate_actions_path=corp,
    )
    assert [r["ticker"] for r in rows] == ["BBUL"]


def test_headline_looks_like_listing():
    assert headline_looks_like_listing(
        "GraniteShares Launches BBUL and PUL, 2x Long ETFs"
    )
    assert not headline_looks_like_listing("PAAU to liquidate")
    assert not headline_looks_like_listing(None)


def test_build_email_payload_and_subject(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "weekly_ops_email.collect_upcoming_earnings",
        lambda **kwargs: [{"underlying": "AMD", "event_date": "2026-07-10", "linked_etfs": ["AMYY"], "confirmation": "confirmed", "source": "nasdaq_earnings"}],
    )
    monkeypatch.setattr(
        "weekly_ops_email.collect_recent_delistings",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        "weekly_ops_email.collect_recent_listings",
        lambda **kwargs: [{"ticker": "BBUL", "announcement_date": "2026-07-07", "status": "pending", "bucket": "B1", "headline": "Launches BBUL"}],
    )
    payload = build_email_payload(today=date(2026, 7, 8))
    assert payload["diagnostics"]["earnings_count"] == 1
    assert payload["diagnostics"]["listings_count"] == 1
    assert payload["earnings_horizon_days"] == 7
    subject = format_subject(payload)
    assert "1 earnings" in subject
    assert "1 listings" in subject
    body = render_plain_text(payload)
    assert "AMD" in body
    assert "AMYY" in body
    assert "New listings" in body
    assert "BBUL" in body


def test_send_email_requires_secrets(monkeypatch):
    from weekly_ops_email import send_email

    monkeypatch.delenv("SMTP_PASS", raising=False)
    with pytest.raises(ValueError, match="SMTP_PASS"):
        send_email(
            subject="test",
            plain_body="hi",
            html_body="<p>hi</p>",
            recipients=["a@b.com"],
        )
