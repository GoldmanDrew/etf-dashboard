"""Helpers for safe logging and URL host checks (CodeQL / ops hygiene)."""
from __future__ import annotations

import re
from urllib.parse import urlparse

_SENSITIVE_QUERY = re.compile(
    r"([?&](?:apiKey|api_key|token|access_token|password)=)[^&\s#]+",
    re.IGNORECASE,
)


def redact_url(url: str | None) -> str:
    """Strip credential-like query params before writing URLs to logs."""
    if not url:
        return ""
    return _SENSITIVE_QUERY.sub(r"\1***", str(url))


def url_host(url: str | None) -> str:
    """Lowercase netloc from *url*, or empty string when unparseable."""
    if not url:
        return ""
    try:
        return urlparse(str(url)).netloc.lower()
    except Exception:  # noqa: BLE001
        return ""


def url_has_host(url: str | None, host: str) -> bool:
    """True when *url*'s host equals *host* (case-insensitive)."""
    want = str(host or "").strip().lower()
    if not want:
        return False
    return url_host(url) == want


def url_host_is_or_subdomain_of(url: str | None, domain: str) -> bool:
    """True when *url* is served from *domain* or a subdomain thereof."""
    root = str(domain or "").strip().lower().lstrip(".")
    if not root:
        return False
    host = url_host(url)
    return host == root or host.endswith(f".{root}")
