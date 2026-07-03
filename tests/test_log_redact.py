"""Tests for log_redact helpers."""
from __future__ import annotations

import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from log_redact import redact_url, url_has_host, url_host_is_or_subdomain_of  # noqa: E402


def test_redact_url_strips_api_key():
    url = "https://api.polygon.io/v2/aggs?apiKey=secret123&adjusted=true"
    assert redact_url(url) == "https://api.polygon.io/v2/aggs?apiKey=***&adjusted=true"


def test_url_has_host_rejects_substring_bypass():
    assert not url_has_host("https://evil.com/raw.githubusercontent.com/x", "raw.githubusercontent.com")
    assert url_has_host("https://raw.githubusercontent.com/org/repo/main/x.csv", "raw.githubusercontent.com")


def test_url_host_is_or_subdomain_of_rex():
    assert url_host_is_or_subdomain_of("https://www.rexshares.com/SOLX/", "rexshares.com")
    assert not url_host_is_or_subdomain_of("https://evil.com/rexshares.com", "rexshares.com")
