"""Config-parity guards for the data sentinel.

These tests exist because two of the sentinel's structures are hand-maintained
and drift silently as the pipeline grows:

  * ``SPECS`` — schema keys per artifact. A new file in a ``config/ci.yaml``
    commit list silently falls back to parse-only checking.
  * ``staleness_market_hours_warn`` — per-file age budgets. These can quietly
    become tighter than the cadence of the task that produces the file.

Rather than relying on someone remembering, a failure here is the forcing
function: adding an artifact to ci.yaml fails CI until it has either a spec and
a budget, or an explicit waiver carrying a reason.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_SCRIPTS = _ROOT / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import ci_tick  # noqa: E402
import data_sentinel as ds  # noqa: E402

TASKS = ["borrow", "options", "yieldboost", "intraday", "nav"]


def _gated_paths() -> set[str]:
    """Every path any ci_tick task can stage — i.e. everything the gate sees.

    Read from ``commit_files`` directly rather than via ``ci_tick.commit_paths``:
    that helper existence-filters against the working tree, which would make
    these guards vacuous for an artifact declared in config but not yet built.
    Config intent is what we want to hold the sentinel to.
    """
    cfg = ci_tick.load_config()
    gated: set[str] = {"data/ci_state.json"}  # commit_paths always appends it
    for task in TASKS:
        for path in cfg.get("commit_files", {}).get(task, []) or []:
            gated.add(str(path).replace("\\", "/"))
    return gated


def _sentinel_cfg() -> dict:
    return ds.load_config(ds.DEFAULT_CONFIG)


def test_every_gated_json_artifact_has_a_spec_or_waiver():
    missing = sorted(
        p for p in _gated_paths()
        if p.endswith(".json") and p not in ds.SPECS and p not in ds.SPEC_WAIVERS
    )
    assert not missing, (
        "These artifacts are committed by ci_tick but have no schema spec:\n  "
        + "\n  ".join(missing)
        + "\n\nAdd them to SPECS in scripts/data_sentinel.py, or to SPEC_WAIVERS "
          "with the reason they cannot be schema-checked."
    )


def test_every_gated_artifact_has_a_staleness_budget_or_waiver():
    budgets = _sentinel_cfg()["staleness_market_hours_warn"]
    missing = sorted(
        p for p in _gated_paths()
        if p not in budgets and p not in ds.STALENESS_WAIVERS
    )
    assert not missing, (
        "These artifacts have no freshness budget and no waiver:\n  "
        + "\n  ".join(missing)
        + "\n\nAdd a budget to staleness_market_hours_warn in config/sentinel.json, "
          "or a reason to STALENESS_WAIVERS in scripts/data_sentinel.py."
    )


def test_waivers_do_not_rot():
    """A waiver for a path no ci_tick task stages anymore is dead weight."""
    gated = _gated_paths()
    for name, waivers in (("SPEC_WAIVERS", ds.SPEC_WAIVERS),
                          ("STALENESS_WAIVERS", ds.STALENESS_WAIVERS)):
        stale = sorted(p for p in waivers if p not in gated)
        assert not stale, (
            f"{name} waives paths that are no longer in any ci_tick commit list: "
            f"{stale}. Delete them so the waiver list stays meaningful."
        )


def test_waivers_carry_a_reason():
    for name, waivers in (("SPEC_WAIVERS", ds.SPEC_WAIVERS),
                          ("STALENESS_WAIVERS", ds.STALENESS_WAIVERS)):
        for path, reason in waivers.items():
            assert isinstance(reason, str) and len(reason) > 15, (
                f"{name}[{path}] needs a real explanation, got {reason!r}"
            )


@pytest.mark.parametrize("artifact,task", sorted(ds.ARTIFACT_PRODUCER_TASK.items()))
def test_staleness_budget_is_looser_than_producer_cadence(artifact, task):
    """A budget tighter than its producer's cadence is a guaranteed false positive.

    Guards the drift direction that actually bites: someone loosens a cadence in
    config/ci.yaml (say intraday 5m -> 4h) and the sentinel starts screaming
    because its budget was written against the old cadence.
    """
    budgets = _sentinel_cfg()["staleness_market_hours_warn"]
    if artifact not in budgets:
        pytest.skip(f"{artifact} has no age budget (waived)")
    ci_cfg = ci_tick.load_config()
    cadence_h = ci_tick._cadence_minutes(task, ci_cfg, rth=True) / 60.0
    budget_h = float(budgets[artifact])
    assert budget_h >= 4 * cadence_h, (
        f"{artifact}: budget {budget_h}h is under 4x the RTH cadence of its "
        f"producer '{task}' ({cadence_h:.2f}h). Either raise the budget in "
        "config/sentinel.json or lower the cadence in config/ci.yaml."
    )


def test_producer_map_points_at_real_tasks():
    for artifact, task in ds.ARTIFACT_PRODUCER_TASK.items():
        assert artifact in _gated_paths(), f"{artifact} is not staged by any ci_tick task"
        cadence = ci_tick._cadence_minutes(task, ci_tick.load_config(), rth=True)
        assert cadence > 0, f"unknown producer task {task!r} for {artifact}"


def test_sweep_artifacts_all_have_specs():
    """The sweep's own list must stay fully specified — it is ours to maintain."""
    missing = [p for p in ds.SWEEP_ARTIFACTS if p not in ds.SPECS]
    assert not missing, f"SWEEP_ARTIFACTS without a spec: {missing}"
