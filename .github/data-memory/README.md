# data-memory

Repo-resident anomaly memory for the data-health sentinel.

| File | What it is | Who writes it |
| --- | --- | --- |
| `anti_patterns.md` | Curated taxonomy of every recurring data-failure mode, its detection method, and its codified defense | Humans/agents, in the same commit as each new fix |
| `provider_health.json` | Rolling daily ledger of provider coverage (issuer feeds, spot sources, IBKR, options), used for drift detection | `scripts/data_sentinel.py sweep` (automated) |

Protocol:

1. **After healing a new class of data corruption**, append the failure mode to
   `anti_patterns.md` (shape, detection, defense) so the next tool starts from the
   lesson instead of the incident.
2. **Never** store secrets or per-user data here; this directory is committed but not
   published (Pages only ships `data/`).
3. Live operational state stays in `data/` (`quarantine.json`, `sentinel_report.json`,
   `freshness_summary.json`) — this directory is the *memory*, not the *state*.

Runbook: `docs/data-health-sentinel.md`.
