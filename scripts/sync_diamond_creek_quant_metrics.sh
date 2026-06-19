#!/usr/bin/env bash
# Sync ETF metrics / Stats-tab plumbing from magis-capital-partners/etf-dashboard main
# into a local Diamond-Creek-Quant clone (no push — you commit from your machine).
#
# Usage:
#   chmod +x scripts/sync_diamond_creek_quant_metrics.sh
#   ./scripts/sync_diamond_creek_quant_metrics.sh /path/to/Diamond-Creek-Quant
#
# Or from anywhere (downloads this script's siblings from raw GitHub; set
# GITHUB_TOKEN first because the upstream repo is private):
#   curl -H "Authorization: Bearer $GITHUB_TOKEN" -fsSL "https://raw.githubusercontent.com/magis-capital-partners/etf-dashboard/main/scripts/sync_diamond_creek_quant_metrics.sh" | bash -s -- /path/to/Diamond-Creek-Quant
#
set -euo pipefail

UPSTREAM="${ETF_DASHBOARD_UPSTREAM:-magis-capital-partners/etf-dashboard}"
BRANCH="${ETF_DASHBOARD_BRANCH:-main}"
RAW="https://raw.githubusercontent.com/${UPSTREAM}/${BRANCH}"
AUTH_ARGS=()
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
  AUTH_ARGS=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
fi

DCQ_ROOT="${1:-}"
if [[ -z "${DCQ_ROOT}" ]] || [[ ! -d "${DCQ_ROOT}/.git" ]]; then
  echo "usage: $0 /path/to/Diamond-Creek-Quant" >&2
  exit 1
fi

die() { echo "error: $*" >&2; exit 1; }

fetch() {
  local rel="$1"
  local dest="$2"
  mkdir -p "$(dirname "$dest")"
  curl "${AUTH_ARGS[@]}" -fsSL "${RAW}/${rel}" -o "$dest" || die "failed to fetch ${RAW}/${rel}"
}

echo "==> Fetching Python + test from ${UPSTREAM} (${BRANCH})"
fetch "scripts/ingest_etf_metrics.py" "${DCQ_ROOT}/scripts/ingest_etf_metrics.py"
fetch "scripts/backfill_underlying_adj_close.py" "${DCQ_ROOT}/scripts/backfill_underlying_adj_close.py"
fetch "scripts/bootstrap_metrics_yahoo_history.py" "${DCQ_ROOT}/scripts/bootstrap_metrics_yahoo_history.py"
fetch "scripts/recover_rex_nav_from_git_history.py" "${DCQ_ROOT}/scripts/recover_rex_nav_from_git_history.py"
fetch "tests/test_backfill_underlying_adj_close_script.py" "${DCQ_ROOT}/tests/test_backfill_underlying_adj_close_script.py"
fetch "tests/test_recover_rex_nav_from_git_history.py" "${DCQ_ROOT}/tests/test_recover_rex_nav_from_git_history.py"
fetch "tests/test_bootstrap_metrics_yahoo_history.py" "${DCQ_ROOT}/tests/test_bootstrap_metrics_yahoo_history.py"

WF="${DCQ_ROOT}/.github/workflows/update-etf-metrics.yml"
[[ -f "$WF" ]] || die "missing ${WF}"

if grep -q "scripts/backfill_underlying_adj_close.py" "$WF"; then
  echo "==> Workflow already has backfill step; skipping YAML edit"
else
  echo "==> Inserting Backfill underlying adj. close gaps step into update-etf-metrics.yml"
  python3 - <<'PY' "$WF"
import sys
from pathlib import Path
path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
needle = "      - name: Ingest ETF metrics\n        run: python scripts/ingest_etf_metrics.py --lookback-days 10 --polygon-lookback-days 5\n\n      - name: Backfill historical close prices"
insert = """      - name: Ingest ETF metrics
        run: python scripts/ingest_etf_metrics.py --lookback-days 10 --polygon-lookback-days 5

      # Runs even when ingest skipped (ETF_METRICS_SKIP_IF_RECENT_HOURS): that early-exit
      # used to skip gap-fill entirely, leaving historical underlying_adj_close null forever.
      - name: Backfill underlying adj. close gaps
        continue-on-error: true
        run: python scripts/backfill_underlying_adj_close.py

      - name: Backfill historical close prices"""
if needle not in text:
    raise SystemExit(f"unexpected workflow layout in {path}: anchor not found")
path.write_text(text.replace(needle, insert, 1), encoding="utf-8")
PY
fi

IDX="${DCQ_ROOT}/index.html"
[[ -f "$IDX" ]] || die "missing ${IDX}"

echo "==> Patching index.html cache bust (etf_metrics_daily + etf_distributions → 1-minute bucket)"
python3 - <<'PY' "$IDX"
import sys
from pathlib import Path
path = Path(sys.argv[1])
t = path.read_text(encoding="utf-8")
old = "const cacheBust = `?t=${Math.floor(Date.now() / 3600000)}`;\n        const res = await fetch(`${ETF_METRICS_DAILY_URL}${cacheBust}`"
new = "const cacheBust = `?t=${Math.floor(Date.now() / 60000)}`;\n        const res = await fetch(`${ETF_METRICS_DAILY_URL}${cacheBust}`"
if old in t:
    t = t.replace(old, new, 1)
old2 = "const cacheBust = `?t=${Math.floor(Date.now() / 3600000)}`;\n        const res = await fetch(`${ETF_DISTRIBUTIONS_URL}${cacheBust}`"
new2 = "const cacheBust = `?t=${Math.floor(Date.now() / 60000)}`;\n        const res = await fetch(`${ETF_DISTRIBUTIONS_URL}${cacheBust}`"
if old2 in t:
    t = t.replace(old2, new2, 1)
path.write_text(t, encoding="utf-8")
PY

AG="${DCQ_ROOT}/AGENTS.md"
if [[ -f "$AG" ]] && ! grep -q "backfill_underlying_adj_close.py" "$AG"; then
  echo "==> Appending short AGENTS.md note (manual one-liner)"
  cat >>"$AG" <<'NOTE'

**etf-dashboard parity (Stats / `underlying_adj_close`):** `scripts/ingest_etf_metrics.py` matches upstream gap backfill + skip-path behavior; `update-etf-metrics.yml` runs `scripts/backfill_underlying_adj_close.py` after ingest; `index.html` busts `etf_metrics_daily.json` / `etf_distributions.json` on a **1-minute** `?t=` bucket. Re-sync with `etf-dashboard` via `scripts/sync_diamond_creek_quant_metrics.sh`.
NOTE
fi

echo ""
echo "Done. Next in ${DCQ_ROOT}:"
echo "  git status"
echo "  python3 -m pytest tests/test_backfill_underlying_adj_close_script.py tests/test_bootstrap_metrics_yahoo_history.py tests/test_etf_metrics_shares_repair.py tests/test_recover_rex_nav_from_git_history.py -v"
echo "  git add -A && git commit -m 'sync: etf-dashboard ETF metrics + underlying adj close pipeline' && git push"
