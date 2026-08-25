#!/usr/bin/env python3
"""Fetch ls-algo Bucket 6 watch JSON for deploy / local dev (never committed).

The artifact carries per-underlying book exposure, so it stays gitignored on this
public repo. Cloudflare Pages deploy fetches it at build time with LS_ALGO_TOKEN.

Quality gate: refuse degraded ny4 runs where ``barrier_sourced`` is 0 and every
gate fell back to proxy basis (no real barrier levels from Granite holdings).
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import sys
from pathlib import Path
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

from product_taxonomy import autocallable_single_stock_pairs  # noqa: E402

DEFAULT_REPO = os.environ.get("UNIVERSE_REPO", "GoldmanDrew/ls-algo")
DEFAULT_BRANCH = os.environ.get("UNIVERSE_BRANCH", "main")
REMOTE_PATH = "data/bucket6_watch.json"
LOCAL_PATH = REPO_ROOT / "data" / "bucket6_watch.json"
SCHEMA = "b6-watch-1"


def resolve_github_token() -> str | None:
    for key in ("LS_ALGO_TOKEN", "GITHUB_TOKEN"):
        val = os.environ.get(key, "").strip()
        if val:
            return val
    return None


def _github_headers(token: str | None) -> dict[str, str]:
    headers = {"User-Agent": "etf-dashboard-bucket6-fetch/1.0"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def fetch_remote_text(
    *,
    repo: str,
    branch: str,
    remote_path: str,
    token: str | None,
    timeout: int = 30,
) -> tuple[str, str]:
    raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{remote_path}"
    headers = _github_headers(token)
    resp = requests.get(raw_url, headers=headers, timeout=timeout)
    if resp.ok:
        return resp.text, raw_url

    raw_error = f"HTTP {resp.status_code}"
    if resp.text:
        raw_error += f": {resp.text[:200].strip()}"

    api_url = f"https://api.github.com/repos/{repo}/contents/{remote_path}"
    api_resp = requests.get(api_url, headers=headers, params={"ref": branch}, timeout=timeout)
    api_resp.raise_for_status()
    payload = api_resp.json()
    content = payload.get("content")
    if isinstance(content, str):
        encoding = str(payload.get("encoding") or "").lower()
        if encoding == "base64":
            return base64.b64decode(content).decode("utf-8"), api_url
        if encoding in {"", "utf-8", "utf8"}:
            return content, api_url

    download_url = payload.get("download_url")
    if download_url:
        dl_resp = requests.get(download_url, headers=headers, timeout=timeout)
        dl_resp.raise_for_status()
        return dl_resp.text, download_url

    raise RuntimeError(
        f"GitHub Contents API response for {repo}/{remote_path}@{branch} "
        "did not include readable file content."
    )


def validate_artifact(
    data: dict[str, Any],
    *,
    min_funds: int,
    min_barrier_sourced: int,
) -> list[str]:
    errors: list[str] = []
    if data.get("schema") != SCHEMA:
        errors.append(f"schema must be {SCHEMA!r}, got {data.get('schema')!r}")

    counts = data.get("counts") if isinstance(data.get("counts"), dict) else {}
    funds = int(counts.get("funds") or 0)
    barrier = int(counts.get("barrier_sourced") or 0)

    if funds < min_funds:
        errors.append(f"counts.funds={funds} < expected {min_funds}")
    if barrier < min_barrier_sourced:
        errors.append(
            f"counts.barrier_sourced={barrier} < required {min_barrier_sourced} "
            "(degraded artifact: holdings scrape likely failed — proxy gates only)"
        )
    if not isinstance(data.get("funds"), list) or not data["funds"]:
        errors.append("funds[] missing or empty")
    return errors


def fetch_bucket6_watch(
    *,
    repo: str = DEFAULT_REPO,
    branch: str = DEFAULT_BRANCH,
    local_path: Path = LOCAL_PATH,
    local_source: Path | None = None,
    min_funds: int | None = None,
    min_barrier_sourced: int | None = None,
    fail_soft: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    expected = len(autocallable_single_stock_pairs())
    min_funds = min_funds if min_funds is not None else expected
    min_barrier_sourced = (
        min_barrier_sourced if min_barrier_sourced is not None else expected
    )

    if local_source is not None:
        print(f"Reading local source {local_source} ...")
        text = local_source.read_text(encoding="utf-8")
        source_url = str(local_source.resolve())
    else:
        token = resolve_github_token()
        print(f"Fetching {repo}/{branch}/{REMOTE_PATH} ...")
        text, source_url = fetch_remote_text(
            repo=repo,
            branch=branch,
            remote_path=REMOTE_PATH,
            token=token,
        )
    print(f"  -> {len(text):,} bytes from {source_url}")

    data = json.loads(text)
    errors = validate_artifact(
        data,
        min_funds=min_funds,
        min_barrier_sourced=min_barrier_sourced,
    )
    if errors:
        msg = "Bucket 6 watch artifact failed quality gate:\n  - " + "\n  - ".join(errors)
        if fail_soft:
            print(f"warning: {msg}")
            print("  Skipping write; Info-page panel will stay hidden on this deploy.")
            return {"ok": False, "errors": errors, "written": False}
        raise SystemExit(msg)

    counts = data.get("counts") or {}
    print(
        f"  Gate OK: funds={counts.get('funds')} barrier_sourced={counts.get('barrier_sourced')} "
        f"autocall_imminent={counts.get('autocall_imminent')}"
    )

    if dry_run:
        print(f"  dry-run: would write {local_path}")
        return {"ok": True, "written": False, "counts": counts}

    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    print(f"  -> wrote {local_path}")
    return {"ok": True, "written": True, "counts": counts, "path": str(local_path)}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--repo", default=DEFAULT_REPO)
    ap.add_argument("--branch", default=DEFAULT_BRANCH)
    ap.add_argument("--local-path", type=Path, default=LOCAL_PATH)
    ap.add_argument(
        "--local-source",
        type=Path,
        default=None,
        help="read artifact from a local file (dev) instead of GitHub",
    )
    ap.add_argument(
        "--min-funds",
        type=int,
        default=None,
        help="minimum counts.funds (default: autocallable pair count from taxonomy)",
    )
    ap.add_argument(
        "--min-barrier-sourced",
        type=int,
        default=None,
        help="minimum counts.barrier_sourced (default: same as --min-funds)",
    )
    ap.add_argument(
        "--fail-soft",
        action="store_true",
        help="on fetch/gate failure: warn and skip write (exit 0)",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    try:
        result = fetch_bucket6_watch(
            repo=args.repo,
            branch=args.branch,
            local_path=args.local_path,
            local_source=args.local_source,
            min_funds=args.min_funds,
            min_barrier_sourced=args.min_barrier_sourced,
            fail_soft=args.fail_soft,
            dry_run=args.dry_run,
        )
    except (requests.RequestException, json.JSONDecodeError, RuntimeError) as exc:
        if args.fail_soft:
            print(f"warning: bucket6 watch fetch failed ({exc}); panel will be hidden.")
            return 0
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return 0 if result.get("ok") else (0 if args.fail_soft else 1)


if __name__ == "__main__":
    raise SystemExit(main())
