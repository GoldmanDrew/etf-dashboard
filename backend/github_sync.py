"""
GitHub Universe Sync
====================
Pulls the latest etf_screened_today.csv (and optionally other files) from
the GoldmanDrew/ls-algo repo on GitHub.

Features:
  - SHA-based change detection (only reload when file actually changes)
  - Atomic writes (temp → rename) to prevent partial-read corruption
  - Dated backups of each version in data/universe_history/
  - Last-commit metadata (who ran the screener and when)
  - Optional GitHub token for private repos / higher rate limits
  - Graceful fallback to local file on any network error

Usage from main.py:
    from backend.github_sync import (
        sync_universe_from_github,
        get_last_commit_info,
        resolve_github_token,
    )
"""
from __future__ import annotations

import datetime as dt
import hashlib
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Optional
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import json

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# Token resolution
# ──────────────────────────────────────────────
def resolve_github_token(config: dict) -> Optional[str]:
    """
    Resolve GitHub token from (in priority order):
      1. config.yaml → github.token
      2. Environment variable GITHUB_TOKEN
      3. None (public repo, no auth — subject to 60 req/hr rate limit)
    """
    gh_cfg = config.get("github", {})

    # From config
    token = gh_cfg.get("token")
    if token and token != "null":
        return token

    # From env
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        return token

    return None


# ──────────────────────────────────────────────
# Core sync function
# ──────────────────────────────────────────────
def sync_universe_from_github(
    *,
    repo: str = "GoldmanDrew/ls-algo",
    branch: str = "main",
    remote_path: str = "data/etf_screened_today.csv",
    local_path: str = "data/etf_screened_today.csv",
    backup_dir: str = "data/universe_history",
    github_token: Optional[str] = None,
    timeout: int = 30,
) -> dict:
    """
    Download the latest version of a file from GitHub if it has changed.

    Returns dict:
      {
        "success": bool,
        "updated": bool,         # True if local file was replaced
        "error": str | None,
        "remote_sha": str,       # SHA256 of downloaded content
        "local_sha": str,        # SHA256 of existing local file
        "size_bytes": int,
        "timestamp": str,        # ISO timestamp
        "backup_path": str|None, # path to dated backup if updated
      }
    """
    result = {
        "success": False,
        "updated": False,
        "error": None,
        "remote_sha": None,
        "local_sha": None,
        "size_bytes": 0,
        "timestamp": dt.datetime.utcnow().isoformat(),
        "backup_path": None,
    }

    local = Path(local_path)

    # Hash existing local file
    if local.exists():
        result["local_sha"] = _sha256_file(local)

    # Build raw URL
    raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{remote_path}"

    try:
        # Download from GitHub
        req = Request(raw_url)
        req.add_header("User-Agent", "etf-borrow-dashboard/1.0")
        if github_token:
            req.add_header("Authorization", f"token {github_token}")

        resp = urlopen(req, timeout=timeout)
        content = resp.read()

        remote_sha = hashlib.sha256(content).hexdigest()
        result["remote_sha"] = remote_sha
        result["size_bytes"] = len(content)

        # Compare hashes — skip write if unchanged
        if result["local_sha"] == remote_sha:
            result["success"] = True
            result["updated"] = False
            logger.info(
                f"GitHub sync: {remote_path} unchanged "
                f"(SHA: {remote_sha[:12]}, {len(content):,} bytes)"
            )
            return result

        # File has changed — create dated backup of the old version
        if local.exists():
            bk_dir = Path(backup_dir)
            bk_dir.mkdir(parents=True, exist_ok=True)
            date_str = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            backup_name = f"{local.stem}_{date_str}{local.suffix}"
            backup_path = bk_dir / backup_name
            shutil.copy2(str(local), str(backup_path))
            result["backup_path"] = str(backup_path)
            logger.info(f"GitHub sync: backed up old universe to {backup_path}")

        # Atomic write: temp file → rename
        local.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            dir=str(local.parent),
            suffix=".tmp",
            prefix=f".{local.stem}_",
        )
        try:
            with open(fd, "wb") as f:
                f.write(content)
            shutil.move(tmp_path, str(local))
        except Exception:
            Path(tmp_path).unlink(missing_ok=True)
            raise

        result["success"] = True
        result["updated"] = True
        logger.info(
            f"GitHub sync: updated {local_path} "
            f"({len(content):,} bytes, SHA: {remote_sha[:12]})"
        )

    except HTTPError as e:
        result["error"] = f"HTTP {e.code}: {e.reason}"
        logger.warning(f"GitHub sync failed: {result['error']}")

    except URLError as e:
        result["error"] = f"Network error: {e.reason}"
        logger.warning(f"GitHub sync failed: {result['error']}")

    except Exception as e:
        result["error"] = str(e)
        logger.exception(f"GitHub sync failed")

    # On any error, we still have the local file as fallback
    if result["error"] and local.exists():
        result["success"] = True  # degraded but usable
        logger.info("GitHub sync: falling back to existing local file")

    return result


# ──────────────────────────────────────────────
# Commit metadata
# ──────────────────────────────────────────────
def get_last_commit_info(
    *,
    repo: str = "GoldmanDrew/ls-algo",
    file_path: str = "data/etf_screened_today.csv",
    github_token: Optional[str] = None,
    timeout: int = 15,
) -> dict:
    """
    Fetch the last commit that touched the file via GitHub API.

    Returns:
      {
        "sha": "abc123...",
        "message": "daily screener run",
        "author": "GoldmanDrew",
        "date": "2026-02-20T14:30:00Z",
        "error": None
      }
    """
    result = {"sha": None, "message": None, "author": None, "date": None, "error": None}

    api_url = (
        f"https://api.github.com/repos/{repo}/commits"
        f"?path={file_path}&per_page=1"
    )

    try:
        req = Request(api_url)
        req.add_header("User-Agent", "etf-borrow-dashboard/1.0")
        req.add_header("Accept", "application/vnd.github.v3+json")
        if github_token:
            req.add_header("Authorization", f"token {github_token}")

        resp = urlopen(req, timeout=timeout)
        data = json.loads(resp.read().decode("utf-8"))

        if data and len(data) > 0:
            commit = data[0]
            result["sha"] = commit.get("sha", "")[:12]
            result["message"] = commit.get("commit", {}).get("message", "")
            result["author"] = (
                commit.get("commit", {}).get("author", {}).get("name")
                or commit.get("author", {}).get("login", "")
            )
            result["date"] = commit.get("commit", {}).get("author", {}).get("date")

    except HTTPError as e:
        result["error"] = f"HTTP {e.code}"
        # Don't log as warning — this is a nice-to-have, not critical
        logger.debug(f"Could not fetch commit info: {result['error']}")

    except Exception as e:
        result["error"] = str(e)
        logger.debug(f"Could not fetch commit info: {e}")

    return result


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _sha256_file(path: Path) -> str:
    """Compute SHA256 of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()
