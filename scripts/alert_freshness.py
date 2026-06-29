#!/usr/bin/env python3
"""Alert operators when freshness_summary.ok is false (GitHub issue or comment)."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SUMMARY = REPO_ROOT / "data" / "freshness_summary.json"
ISSUE_LABEL = "ops/stale-data"


def _run(cmd: list[str], *, check: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=check,
    )


def _load_summary(path: Path) -> dict:
    if not path.exists():
        return {"ok": False, "violations": [f"missing {path.name}"]}
    try:
        return json.loads(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        return {"ok": False, "violations": [f"invalid JSON: {exc}"]}


def _find_open_issue() -> int | None:
    if os.environ.get("GITHUB_ACTIONS") != "true" and not os.environ.get("GH_TOKEN"):
        return None
    proc = _run(
        [
            "gh", "issue", "list",
            "--label", ISSUE_LABEL,
            "--state", "open",
            "--json", "number,title",
            "--limit", "5",
        ]
    )
    if proc.returncode != 0:
        print(proc.stderr or proc.stdout, file=sys.stderr)
        return None
    try:
        items = json.loads(proc.stdout or "[]")
    except json.JSONDecodeError:
        return None
    for item in items:
        num = item.get("number")
        if isinstance(num, int):
            return num
    return None


def _create_or_update_issue(summary: dict, *, dry_run: bool) -> int:
    violations = summary.get("violations") or []
    if summary.get("ok") or not violations:
        return 0

    build_time = summary.get("build_time") or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    title = f"Data freshness violations ({len(violations)}) — {build_time[:10]}"
    body_lines = [
        "## Freshness violations",
        "",
        f"Build time: `{build_time}`",
        "",
    ]
    for v in violations:
        body_lines.append(f"- {v}")
    body_lines.extend(
        [
            "",
            "---",
            "Auto-filed by `scripts/alert_freshness.py`. Close when `freshness_summary.ok` is true.",
            "",
            "<details><summary>Raw summary</summary>",
            "",
            "```json",
            json.dumps(summary, indent=2)[:12000],
            "```",
            "</details>",
        ]
    )
    body = "\n".join(body_lines)

    if dry_run:
        print(f"[dry-run] would alert: {title}")
        for v in violations:
            print(f"  - {v}")
        return 0

    existing = _find_open_issue()
    if existing is not None:
        comment = f"**Update {build_time}**\n\n" + "\n".join(f"- {v}" for v in violations)
        proc = _run(["gh", "issue", "comment", str(existing), "--body", comment])
        if proc.returncode != 0:
            print(proc.stderr or proc.stdout, file=sys.stderr)
            return proc.returncode
        print(f"Commented on issue #{existing}")
        return 0

    proc = _run(
        [
            "gh", "issue", "create",
            "--title", title,
            "--body", body,
            "--label", ISSUE_LABEL,
        ]
    )
    if proc.returncode != 0:
        print(proc.stderr or proc.stdout, file=sys.stderr)
        return proc.returncode
    print(proc.stdout.strip())
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Alert on freshness_summary violations.")
    parser.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    summary = _load_summary(args.summary)
    if summary.get("ok"):
        if not args.dry_run:
            print("freshness ok — no alert")
        return 0
    return _create_or_update_issue(summary, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
