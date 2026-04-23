import sys
from pathlib import Path
import importlib.util

import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

_MOD_PATH = Path(__file__).parent.parent / "scripts" / "ingest_corporate_actions.py"
_SPEC = importlib.util.spec_from_file_location("etf_dashboard_ingest_corporate_actions", _MOD_PATH)
assert _SPEC and _SPEC.loader
mod = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = mod
_SPEC.loader.exec_module(mod)


def _maps():
    bucket_map = {
        "TOPW": "bucket_2_low_beta",
        "BTFL": "bucket_1_high_beta",
        "XRPK": "bucket_1_high_beta",
        "SOLX": "bucket_1_high_beta",
        "NNEX": "bucket_1_high_beta",
    }
    underlying_map = {
        "TOPW": "TOP",
        "BTFL": "KEEL",
        "XRPK": "XRP",
        "SOLX": "SOL",
        "NNEX": "NNE",
    }
    return bucket_map, underlying_map


def test_google_news_drops_unrelated_symbol_change_spam(monkeypatch):
    bucket_map, underlying_map = _maps()
    monkeypatch.setattr(mod, "GOOGLE_NEWS_QUERIES", [('"changes its ticker"', "symbol_change")])
    monkeypatch.setattr(
        mod,
        "_fetch_google_news_rss",
        lambda _s, _q: [
            {
                "title": "Americas Uranium Corp. will Change its Ticker to NUCA from ASR - marketscreener.com",
                "description": "",
                "link": "https://example.com/bad",
                "pub_date": "Tue, 22 Apr 2026 15:40:32 GMT",
                "source": "marketscreener.com",
            }
        ],
    )

    events, news = mod.phase_5_google_news(
        requests.Session(),
        universe={"TOPW", "BTFL"},
        ever_known={"TOPW", "BTFL"},
        underlyings={"TOP", "KEEL"},
        alias_map={"BITF": {"new": "KEEL", "effective_date": "2026-04-06"}},
        bucket_map=bucket_map,
        underlying_map=underlying_map,
    )
    assert events == []
    assert news == []


def test_google_news_keel_bitf_chain_still_maps_to_btfl(monkeypatch):
    bucket_map, underlying_map = _maps()
    monkeypatch.setattr(mod, "GOOGLE_NEWS_QUERIES", [('"changes its ticker"', "symbol_change")])
    monkeypatch.setattr(
        mod,
        "_fetch_google_news_rss",
        lambda _s, _q: [
            {
                "title": "Keel Infrastructure Corp. will Change its Ticker to KEEL from BITF - marketscreener.com",
                "description": "",
                "link": "https://example.com/keel",
                "pub_date": "Mon, 06 Apr 2026 07:00:00 GMT",
                "source": "marketscreener.com",
            }
        ],
    )

    events, news = mod.phase_5_google_news(
        requests.Session(),
        universe={"BTFL"},
        ever_known={"BTFL"},
        underlyings={"KEEL"},
        alias_map={"BITF": {"new": "KEEL", "effective_date": "2026-04-06"}},
        bucket_map=bucket_map,
        underlying_map=underlying_map,
    )
    assert news and news[0].tickers == ["BTFL"]
    assert news[0].match_tier in {"high", "explicit"}
    assert any(ev.ticker == "BTFL" and ev.type == "symbol_change" for ev in events)


def test_google_news_trex_body_extracts_xrpk_and_solx(monkeypatch):
    bucket_map, underlying_map = _maps()
    monkeypatch.setattr(mod, "GOOGLE_NEWS_QUERIES", [('"ETF" "to liquidate"', "delisting")])
    monkeypatch.setattr(
        mod,
        "_fetch_google_news_rss",
        lambda _s, _q: [
            {
                "title": "T-REX 2X XRP Daily Target ETF and T-REX 2X SOL Daily Target ETF to Liquidate",
                "description": "",
                "link": "https://example.com/trex",
                "pub_date": "Mon, 21 Apr 2026 14:22:53 GMT",
                "source": "ACCESS Newswire",
            }
        ],
    )
    monkeypatch.setattr(
        mod,
        "_fetch_article_body",
        lambda _s, _u: (
            "Announcement: T-REX 2X XRP Daily Target ETF to liquidate. "
            "(Cboe BZX Exchange, Inc: XRPK) and (Cboe BZX Exchange, Inc: SOLX)."
        ),
    )

    events, news = mod.phase_5_google_news(
        requests.Session(),
        universe={"XRPK", "SOLX"},
        ever_known={"XRPK", "SOLX"},
        underlyings={"XRP", "SOL"},
        alias_map={},
        bucket_map=bucket_map,
        underlying_map=underlying_map,
    )
    assert news
    assert news[0].tickers == ["SOLX", "XRPK"] or news[0].tickers == ["XRPK", "SOLX"]
    assert {e.ticker for e in events if e.type == "delisting"} == {"XRPK", "SOLX"}


def test_phase2_delisting_fallback_captures_nnex(monkeypatch):
    # Simulate max-page truncation and no direct hit for NNEX in bulk payload.
    raw = [{"ticker": f"DUMMY{i}", "type": "CS"} for i in range(mod.DELISTINGS_MAX_PAGES * 1000)]
    monkeypatch.setattr(mod, "_bulk_paginate", lambda *args, **kwargs: raw)
    monkeypatch.setattr(mod, "_load_current_universe", lambda: {"NNEX"})
    monkeypatch.setattr(mod, "DELISTING_FALLBACK_MAX_CALLS", 5)
    monkeypatch.setattr(mod, "DELISTING_FALLBACK_SYMBOLS", {"NNEX"})

    def fake_polygon_get(_session, url, params=None):
        if url.endswith("/NNEX"):
            return {
                "results": {
                    "ticker": "NNEX",
                    "active": False,
                    "name": "Tradr 2x Long NNE Daily ETF",
                    "primary_exchange": "NYSE Arca",
                    "delisted_utc": "2026-04-20T00:00:00Z",
                    "type": "ETF",
                }
            }
        return None

    monkeypatch.setattr(mod, "_polygon_get", fake_polygon_get)
    bucket_map, underlying_map = _maps()
    events = mod.phase_2_delistings(
        requests.Session(),
        universe={"NNEX"},
        bucket_map=bucket_map,
        underlying_map=underlying_map,
    )
    assert any(e.ticker == "NNEX" and e.type == "delisting" for e in events)


def test_collapse_news_by_ticker_category_merges_sources():
    rows = [
        mod.NewsItem(
            id="a",
            tickers=["TOPW"],
            category="symbol_change",
            confidence=0.65,
            published_utc="2026-04-22T12:00:00+00:00",
            title="A",
            publisher="marketscreener.com",
            source_count=1,
            source_publishers=["marketscreener.com"],
            match_tier="inferred",
        ),
        mod.NewsItem(
            id="b",
            tickers=["TOPW"],
            category="symbol_change",
            confidence=0.91,
            published_utc="2026-04-22T13:00:00+00:00",
            title="B",
            publisher="Yahoo Finance",
            source_count=1,
            source_publishers=["Yahoo Finance"],
            match_tier="explicit",
        ),
        mod.NewsItem(
            id="c",
            tickers=["TOPW"],
            category="symbol_change",
            confidence=0.77,
            published_utc="2026-04-22T11:00:00+00:00",
            title="C",
            publisher="ACCESS Newswire",
            source_count=1,
            source_publishers=["ACCESS Newswire"],
            match_tier="high",
        ),
    ]
    out, dropped = mod.collapse_news_by_ticker_category(rows)
    assert dropped == 2
    assert len(out) == 1
    keep = out[0]
    assert keep.tickers == ["TOPW"]
    assert keep.source_count == 3
    assert set(keep.source_publishers) == {"marketscreener.com", "Yahoo Finance", "ACCESS Newswire"}
    assert keep.match_tier == "explicit"


def test_collapse_news_projects_multi_ticker_rows_per_ticker():
    rows = [
        mod.NewsItem(
            id="one",
            tickers=["TOPW"],
            category="delisting",
            confidence=0.82,
            published_utc="2026-03-31T07:00:00+00:00",
            title="BMAX to liquidate",
            publisher="Newswire.com",
            source_count=1,
            source_publishers=["Newswire.com"],
            match_tier="high",
        ),
        mod.NewsItem(
            id="two",
            tickers=["TOPW", "XYZG", "XYZY"],
            category="delisting",
            confidence=0.79,
            published_utc="2026-03-30T07:00:00+00:00",
            title="BMAX to liquidate (regional syndication)",
            publisher="The Norfolk Daily News",
            source_count=1,
            source_publishers=["The Norfolk Daily News"],
            match_tier="high",
        ),
    ]
    out, _dropped = mod.collapse_news_by_ticker_category(rows)
    topw_rows = [r for r in out if r.category == "delisting" and r.tickers == ["TOPW"]]
    xyzg_rows = [r for r in out if r.category == "delisting" and r.tickers == ["XYZG"]]
    assert len(topw_rows) == 1
    assert len(xyzg_rows) == 1
    assert topw_rows[0].source_count == 2


def test_classify_drops_motley_dimon_breaking_down_episode():
    cat, conf = mod.classify_text(
        "Breaking Down Jamie Dimon's Investing Letter — and whether to buy JEPQ on the dip. "
        "Mention of a Universal Music acquisition and merger talk."
    )
    assert cat is None
    assert conf == 0.0


def test_dedupe_news_merges_identical_title_across_ticker_mismatches():
    same_title = "REX Bitcoin Corporate Treasury Convertible Bond ETF (BMAX) to Liquidate - wire"
    a = mod.NewsItem(
        id="a",
        tickers=["WRONG1"],
        category="delisting",
        confidence=0.78,
        published_utc="2026-03-31T07:00:00+00:00",
        title=same_title,
        publisher="A",
        match_tier="inferred",
    )
    b = mod.NewsItem(
        id="b",
        tickers=["WRONG2"],
        category="delisting",
        confidence=0.78,
        # Same calendar day as `a` so dedupe key (title, day, category) matches.
        published_utc="2026-03-31T12:00:00+00:00",
        title=same_title,
        publisher="B",
        match_tier="inferred",
    )
    out = mod.dedupe_news([a, b])
    assert len(out) == 1
    assert out[0].source_count == 2
    # Same tier+confidence: both tickers are unioned; publisher list merged.
    assert "WRONG1" in out[0].tickers and "WRONG2" in out[0].tickers


def test_gnews_opinion_drops_cramer_ticker_suggestion(monkeypatch):
    bucket_map, underlying_map = _maps()
    monkeypatch.setattr(mod, "GOOGLE_NEWS_QUERIES", [('"ETF" "ticker change"', "symbol_change")])
    monkeypatch.setattr(
        mod,
        "_fetch_google_news_rss",
        lambda _s, _q: [
            {
                "title": "Block (XYZ) Shares Up Since Jim Cramer Said It Should Change Its Ticker To 'SELL' - Yahoo",
                "description": "<b>not</b> a filing",
                "link": "https://example.com/op",
                "pub_date": "Mon, 31 Mar 2026 07:00:00 GMT",
                "source": "Yahoo",
            }
        ],
    )
    _events, news = mod.phase_5_google_news(
        requests.Session(),
        universe={"TOPW", "BTFL", "XYZG", "XYZY", "XRPK", "SOLX"},
        ever_known=set(),
        underlyings={"TOP", "KEEL", "XRP", "SOL"},
        alias_map={},
        bucket_map=bucket_map,
        underlying_map=underlying_map,
    )
    assert news == []


def test_title_anchor_prefers_parens_bmax_in_universe(monkeypatch):
    """Headline (BMAX) in universe should emit BMAX, not a weak sidebar guess."""
    bucket_map, underlying_map = _maps()
    bmap = {**dict(bucket_map), "BMAX": "bucket_1_high_beta", "BTFL": "bucket_1_high_beta"}
    umap = {**dict(underlying_map), "BMAX": "BMXX"}
    monkeypatch.setattr(mod, "GOOGLE_NEWS_QUERIES", [('"to liquidate"', "delisting")])
    monkeypatch.setattr(
        mod,
        "_fetch_google_news_rss",
        lambda _s, _q: [
            {
                "title": "REX (BMAX) to Liquidate — placeholder",
                "description": "",
                "link": "https://example.com/bmax1",
                "pub_date": "Tue, 1 Apr 2026 07:00:00 GMT",
                "source": "wire",
            }
        ],
    )
    ev, news = mod.phase_5_google_news(
        requests.Session(),
        universe={"BMAX", "BTFL", "TOPW", "XRPK", "SOLX", "NNEX"},
        ever_known=set(),
        underlyings=set(),
        alias_map={},
        bucket_map=bmap,
        underlying_map=umap,
    )
    assert news
    assert news[0].tickers == ["BMAX"]
    assert "BMAX" in [e.ticker for e in ev if e.type == "delisting"]


def test_gnews_bmax_paren_drops_body_only_tickers(monkeypatch):
    """(BMAX) in title but BMAX not in screener; body must not tag XYZ* from HTML."""
    bucket_map, underlying_map = _maps()
    monkeypatch.setattr(mod, "GOOGLE_NEWS_QUERIES", [('"to liquidate"', "delisting")])
    monkeypatch.setattr(
        mod,
        "_fetch_google_news_rss",
        lambda _s, _q: [
            {
                "title": "REX Bitcoin Corporate Treasury Convertible Bond ETF (BMAX) to Liquidate - test",
                "description": "",
                "link": "https://example.com/bmax-paren",
                "pub_date": "Tue, 1 Apr 2026 07:00:00 GMT",
                "source": "wire",
            }
        ],
    )
    monkeypatch.setattr(
        mod,
        "_fetch_article_body",
        lambda _s, _u: (
            "Press release body with unrelated tickers "
            "XYZG XYZY SQ in a sidebar for testing."
        ),
    )
    _ev, news = mod.phase_5_google_news(
        requests.Session(),
        universe={"XYZG", "XYZY", "TOPW", "BTFL", "XRPK", "SOLX", "NNEX"},
        ever_known=set(),
        underlyings=set(),
        alias_map={},
        bucket_map=bucket_map,
        underlying_map=underlying_map,
    )
    assert news == []


def test_gnews_structured_event_id_symbol_change_ticker_only(monkeypatch):
    """Pinned strip ids must be stable per fund so the UI does not duplicate cards."""
    bucket_map, underlying_map = _maps()
    monkeypatch.setattr(mod, "GOOGLE_NEWS_QUERIES", [('"changes its ticker"', "symbol_change")])
    monkeypatch.setattr(
        mod,
        "_fetch_google_news_rss",
        lambda _s, _q: [
            {
                "title": "Keel Infrastructure Corp. will Change its Ticker to KEEL from BITF - marketscreener.com",
                "description": "",
                "link": "https://example.com/keel2",
                "pub_date": "Mon, 06 Apr 2026 07:00:00 GMT",
                "source": "marketscreener.com",
            }
        ],
    )
    ev, _news = mod.phase_5_google_news(
        requests.Session(),
        universe={"BTFL"},
        ever_known={"BTFL"},
        underlyings={"KEEL"},
        alias_map={"BITF": {"new": "KEEL", "effective_date": "2026-04-06"}},
        bucket_map=bucket_map,
        underlying_map=underlying_map,
    )
    sc = [e for e in ev if e.type == "symbol_change"]
    assert sc and all(e.id == "gnews_symbol_change:BTFL" for e in sc)
