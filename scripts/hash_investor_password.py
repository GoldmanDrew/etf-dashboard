#!/usr/bin/env python3
"""Create or update a row in data/investors.json for static-site login.

The dashboard (GitHub Pages) checks passwords with PBKDF2-HMAC-SHA256 in the
browser.  Run this locally, then commit **only** the updated JSON (never
commit plaintext passwords).

Examples::

    # Prefer env var so the password is not in shell history:
    set INVESTOR_PASSWORD=your-secret
    python scripts/hash_investor_password.py --id jordan --name "Jordan" --merge data/investors.json

    # Or one-shot print (merge manually):
    python scripts/hash_investor_password.py --id jordan --password "your-secret"

User ids are normalized to lower-case in the JSON ``id`` field.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import secrets
import sys
from pathlib import Path


def hash_entry(user_id: str, display_name: str, password: str, iterations: int = 250_000) -> dict:
    uid = user_id.strip().lower()
    if not uid:
        raise SystemExit("id must be non-empty")
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations, dklen=32)
    return {
        "id": uid,
        "name": (display_name or user_id).strip() or uid,
        "salt_b64": base64.b64encode(salt).decode("ascii"),
        "hash_b64": base64.b64encode(dk).decode("ascii"),
        "iterations": iterations,
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Add investor login row (PBKDF2 hash) to investors.json")
    p.add_argument("--id", required=True, help="Login id (e.g. email local-part or investor handle)")
    p.add_argument("--name", default="", help="Display name shown after login")
    p.add_argument("--password", default="", help="Plain password (prefer INVESTOR_PASSWORD env)")
    p.add_argument("--iterations", type=int, default=250_000, help="PBKDF2 iterations (default 250000)")
    p.add_argument(
        "--merge",
        type=Path,
        help="Read JSON, upsert this user by id, write back (pretty-printed)",
    )
    args = p.parse_args()

    pw = args.password or os.environ.get("INVESTOR_PASSWORD") or ""
    if not pw:
        print("Provide --password or set INVESTOR_PASSWORD", file=sys.stderr)
        raise SystemExit(2)

    entry = hash_entry(args.id, args.name, pw, iterations=args.iterations)

    if not args.merge:
        print(json.dumps({"version": 1, "users": [entry]}, indent=2))
        return

    path = args.merge
    if path.exists():
        doc = json.loads(path.read_text(encoding="utf-8"))
    else:
        doc = {"version": 1, "users": []}
    users = doc.get("users")
    if not isinstance(users, list):
        users = []
    replaced = False
    for i, u in enumerate(users):
        if isinstance(u, dict) and str(u.get("id", "")).lower() == entry["id"]:
            users[i] = entry
            replaced = True
            break
    if not replaced:
        users.append(entry)
    doc["version"] = int(doc.get("version") or 1)
    doc["users"] = users
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {path} ({'updated' if replaced else 'added'} user {entry['id']!r})")


if __name__ == "__main__":
    main()
