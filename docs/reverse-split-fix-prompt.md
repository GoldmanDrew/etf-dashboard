# Reverse-split fix — Exp. ETF return (YieldBOOST)

**Status:** implemented 2026-06-05

Fixes inflated main-grid **Exp. ETF return** on post-reverse-split YieldBOOST rows (TSYY, COYY, MTYY) by making income calibration post-split aware and capping legacy yield fallbacks at 150% annual.

**Code:** `scripts/income_schedule.py`, `index.html`, `assets/income_scenario.js`, tests in `tests/test_income_schedule.py` and `tests/test_income_scenario.js`.

See git history / PR for full audit notes.
