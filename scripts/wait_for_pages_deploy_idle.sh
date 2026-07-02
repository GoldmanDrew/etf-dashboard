#!/usr/bin/env bash
# Poll GitHub Pages until no deployment is queued or in progress for this repo.
set -euo pipefail

MAX_WAIT_SEC="${MAX_WAIT_SEC:-1200}"
POLL_SEC="${POLL_SEC:-30}"
REPO="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
TOKEN="${GITHUB_TOKEN:?GITHUB_TOKEN is required}"

deadline=$((SECONDS + MAX_WAIT_SEC))

is_active_status() {
  local status="${1,,}"
  case "$status" in
    queued|in_progress|pending|deployment_queued|deployment_in_progress|syncing|building)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

latest_pages_status() {
  curl -sS -f \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "https://api.github.com/repos/${REPO}/pages/deployments?per_page=5" \
    | python3 - <<'PY'
import json, sys
try:
    rows = json.load(sys.stdin)
except Exception:
    print("")
    sys.exit(0)
if not isinstance(rows, list) or not rows:
    print("")
    sys.exit(0)
for row in rows:
    if not isinstance(row, dict):
        continue
    status = str(row.get("status") or "").strip()
    if status:
        print(status)
        sys.exit(0)
print("")
PY
}

echo "Waiting for Pages deploy idle (repo=${REPO}, max=${MAX_WAIT_SEC}s)..."

while (( SECONDS < deadline )); do
  status="$(latest_pages_status || true)"
  if [ -z "$status" ]; then
    echo "No active Pages deployment status; proceeding."
    exit 0
  fi
  if ! is_active_status "$status"; then
    echo "Latest Pages deployment status='${status}' (terminal); proceeding."
    exit 0
  fi
  echo "Pages deployment status='${status}'; sleeping ${POLL_SEC}s..."
  sleep "$POLL_SEC"
done

echo "Timed out after ${MAX_WAIT_SEC}s waiting for Pages deploy idle (last status='${status}')."
exit 1
