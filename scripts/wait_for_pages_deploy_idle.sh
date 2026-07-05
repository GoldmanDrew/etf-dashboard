#!/usr/bin/env bash
# Wait until GitHub Pages is not mid-build before starting a new deployment.
#
# The legacy GET /pages/deployments REST endpoint returns 404 for workflow-
# sourced Pages sites, so we poll GET /pages (site status) instead.
set -euo pipefail

MAX_WAIT_SEC="${MAX_WAIT_SEC:-1200}"
POLL_SEC="${POLL_SEC:-30}"
REPO="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
TOKEN="${GITHUB_TOKEN:?GITHUB_TOKEN is required}"

deadline=$((SECONDS + MAX_WAIT_SEC))

pages_site_status() {
  local response http_code body
  response="$(
    curl -sS \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "Accept: application/vnd.github+json" \
      -H "X-GitHub-Api-Version: 2022-11-28" \
      -w $'\n%{http_code}' \
      "https://api.github.com/repos/${REPO}/pages" 2>/dev/null || true
  )"
  http_code="${response##*$'\n'}"
  body="${response%$'\n'*}"
  if [ "$http_code" = "404" ] || [ -z "$body" ]; then
    echo ""
    return 0
  fi
  if [ "$http_code" -lt 200 ] 2>/dev/null || [ "$http_code" -ge 300 ] 2>/dev/null; then
    echo ""
    return 0
  fi
  printf '%s' "$body" | python3 -c "import json,sys
try:
    payload = json.load(sys.stdin)
except Exception:
    print('')
    raise SystemExit
if not isinstance(payload, dict):
    print('')
    raise SystemExit
print(str(payload.get('status') or '').strip())"
}

is_active_status() {
  local status="${1,,}"
  case "$status" in
    building|queued|in_progress|pending|syncing|syncing_files)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

echo "Waiting for Pages deploy idle (repo=${REPO}, max=${MAX_WAIT_SEC}s)..."

while (( SECONDS < deadline )); do
  status="$(pages_site_status || true)"
  if [ -z "$status" ]; then
    echo "Pages site status unavailable; proceeding."
    exit 0
  fi
  if ! is_active_status "$status"; then
    echo "Pages site status='${status}' (idle); proceeding."
    exit 0
  fi
  echo "Pages site status='${status}'; sleeping ${POLL_SEC}s..."
  sleep "$POLL_SEC"
done

echo "Timed out after ${MAX_WAIT_SEC}s waiting for Pages deploy idle (last status='${status}')."
exit 1
