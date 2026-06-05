#!/usr/bin/env python3
"""CI refresh orchestrator for market-hours.yml.

During US RTH, runs stale NAV forecast and intraday flow on fast-lane cadences
(NAV first so intraday blends fresh ``_latest.json`` rows), then at most one
other rotation task (borrow / options / yieldboost). Off-hours: borrow only.

Updates ``data/ci_state.json`` and prints commit file paths for deploy steps.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config" / "ci.yaml"
STATE_PATH = ROOT / "data" / "ci_state.json"
BUILD_DATA = ROOT / "scripts" / "build_data.py"

DEFAULT_CONFIG: dict = {
    "cadence_minutes": {
        "borrow": 30,
        "options": 15,
        "yieldboost": 15,
        "intraday": 15,
        "intraday_rth": 5,
        "nav_forecast": 30,
        "nav_forecast_rth": 30,
    },
    "rth": {"utc_hours": list(range(13, 23)), "weekdays": [0, 1, 2, 3, 4]},
    "rotation_rth": ["borrow", "options", "yieldboost", "intraday"],
    "commit_files": {
        "borrow": ["data/dashboard_data.json", "data/borrow_history.json"],
        "options": [
            "data/options_cache.json",
            "data/vrp_live.json",
            "data/vrp_health.json",
            "data/yieldboost_put_spreads_latest.json",
            "data/yieldboost_options_target.json",
            "data/event_calendar_known.json",
            "data/event_calendar_inferred.json",
            "data/event_calendar_combined.json",
        ],
        "yieldboost": [
            "data/options_cache.json",
            "data/vrp_live.json",
            "data/vrp_health.json",
            "data/yieldboost_put_spreads_latest.json",
            "data/yieldboost_options_target.json",
            "data/event_calendar_known.json",
            "data/event_calendar_inferred.json",
            "data/event_calendar_combined.json",
        ],
        "intraday": [
            "data/underlying_intraday_spot.json",
            "data/underlying_intraday_volume.json",
            "data/letf_rebalance_flows_intraday_latest.json",
        ],
        "nav": ["data/nav_forecasts/_latest.json"],
    },
    "options_env": {},
    "yieldboost_env": {},
}


def _load_yaml(path: Path) -> dict:
    try:
        import yaml  # type: ignore
    except ImportError:
        return DEFAULT_CONFIG
    if not path.exists():
        return DEFAULT_CONFIG
    with open(path, encoding="utf-8") as f:
        loaded = yaml.safe_load(f) or {}
    merged = json.loads(json.dumps(DEFAULT_CONFIG))
    for key, val in loaded.items():
        if isinstance(val, dict) and isinstance(merged.get(key), dict):
            merged[key].update(val)
        else:
            merged[key] = val
    return merged


def _parse_simple_yaml(path: Path) -> dict:
    """Minimal YAML subset when PyYAML is unavailable."""
    if not path.exists():
        return DEFAULT_CONFIG
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    section: str | None = None
    sub: str | None = None
    with open(path, encoding="utf-8") as f:
        for raw in f:
            line = raw.rstrip()
            if not line or line.lstrip().startswith("#"):
                continue
            if not line.startswith(" ") and line.endswith(":"):
                section = line[:-1].strip()
                sub = None
                if section not in cfg:
                    cfg[section] = {}
                continue
            if section and line.startswith("  ") and not line.startswith("    ") and line.rstrip().endswith(":"):
                sub = line.strip()[:-1]
                if sub not in cfg[section]:
                    cfg[section][sub] = {} if section in ("commit_files",) else []
                continue
            if section == "cadence_minutes" and ":" in line:
                k, v = line.strip().split(":", 1)
                cfg["cadence_minutes"][k.strip()] = int(v.strip())
            elif section == "rotation_rth" and line.strip().startswith("- "):
                cfg.setdefault("rotation_rth", []).append(line.strip()[2:].strip())
            elif section == "commit_files" and sub and line.strip().startswith("- "):
                cfg["commit_files"].setdefault(sub, []).append(line.strip()[2:].strip())
            elif section in ("options_env", "yieldboost_env") and ":" in line:
                k, v = line.strip().split(":", 1)
                cfg[section][k.strip()] = v.strip().strip('"').strip("'")
    return cfg


def load_config() -> dict:
    try:
        import yaml  # noqa: F401
        return _load_yaml(CONFIG_PATH)
    except ImportError:
        return _parse_simple_yaml(CONFIG_PATH)


def load_state() -> dict:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _parse_ts(val: str | None) -> datetime | None:
    if not val:
        return None
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except Exception:
        return None


def minutes_since(state: dict, task: str, now: datetime) -> float:
    ts = _parse_ts(state.get(f"last_{task}_utc"))
    if ts is None:
        return float("inf")
    return (now - ts).total_seconds() / 60.0


def is_rth(now: datetime, config: dict) -> bool:
    rth = config.get("rth") or {}
    hours = set(rth.get("utc_hours") or range(13, 23))
    weekdays = set(rth.get("weekdays") or [0, 1, 2, 3, 4])
    return now.weekday() in weekdays and now.hour in hours


def _cadence_minutes(task: str, config: dict, *, rth: bool = False) -> float:
    cadence_map = config.get("cadence_minutes") or {}
    if task == "nav":
        if rth:
            return float(cadence_map.get("nav_forecast_rth", cadence_map.get("nav_forecast", 30)))
        return float(cadence_map.get("nav_forecast", 30))
    if task == "intraday" and rth:
        return float(cadence_map.get("intraday_rth", cadence_map.get("intraday", 15)))
    return float(cadence_map.get(task, 30))


def is_stale(task: str, state: dict, config: dict, now: datetime, *, rth: bool = False) -> bool:
    state_key = "nav" if task == "nav" else task
    return minutes_since(state, state_key, now) >= _cadence_minutes(task, config, rth=rth)


def _pick_rotation_secondary(state: dict, config: dict, now: datetime) -> str | None:
    """One stale borrow / options / yieldboost task from the RTH rotation.

    Prefers the **most stale** task (by ``minutes_since / cadence`` ratio) so
    no single task can sit idle for many hours while another always pre-empts
    it under fixed slot ordering. Falls back to rotation-slot order as the
    tiebreaker when multiple tasks have similar staleness.
    """
    rotation = list(config.get("rotation_rth") or ["borrow", "options", "yieldboost", "intraday"])
    candidates = [t for t in rotation if t not in {"intraday", "nav"}]
    if not candidates:
        return None
    slot = (int(now.timestamp()) // 900) % max(len(rotation), 1)

    scored: list[tuple[float, int, str]] = []
    for idx, task in enumerate(candidates):
        if not is_stale(task, state, config, now):
            continue
        cadence = max(_cadence_minutes(task, config), 1.0)
        since = minutes_since(state, task, now)
        ratio = since / cadence if since != float("inf") else float("inf")
        # Slot-distance tiebreaker: tasks closer to the current rotation slot win.
        try:
            rot_idx = rotation.index(task)
        except ValueError:
            rot_idx = idx
        slot_dist = (rot_idx - slot) % max(len(rotation), 1)
        scored.append((-ratio, slot_dist, task))
    if not scored:
        return None
    scored.sort()
    return scored[0][2]


def _append_overdue_rotation_task(
    tasks: list[str],
    state: dict,
    config: dict,
    now: datetime,
    *,
    overdue_multiplier: float = 2.0,
) -> list[str]:
    """Add one rotation task when it is multiples of its cadence overdue.

    Intraday runs every 5 minutes during RTH; without this, options/yieldboost
    could sit idle for days while the fast lane always wins.
    """
    secondary = _pick_rotation_secondary(state, config, now)
    if not secondary or secondary in tasks:
        return tasks
    cadence = max(_cadence_minutes(secondary, config), 1.0)
    since = minutes_since(state, secondary, now)
    if since >= overdue_multiplier * cadence:
        tasks.append(secondary)
    return tasks


def pick_auto_tasks(state: dict, config: dict, now: datetime) -> list[str]:
    """Return ordered task list for this tick (NAV + intraday fast-lanes first during RTH)."""
    if is_rth(now, config):
        tasks: list[str] = []
        # NAV before intraday: build_letf_intraday_flows reads nav_forecasts/_latest.json.
        if is_stale("nav", state, config, now, rth=True):
            tasks.append("nav")
        if is_stale("intraday", state, config, now, rth=True):
            tasks.append("intraday")
        if tasks:
            return _append_overdue_rotation_task(tasks, state, config, now)
        secondary = _pick_rotation_secondary(state, config, now)
        return [secondary] if secondary else []

    if is_stale("borrow", state, config, now):
        return ["borrow"]
    return []


def pick_auto_task(state: dict, config: dict, now: datetime) -> str | None:
    """Backward-compatible single-task picker (first auto task only)."""
    tasks = pick_auto_tasks(state, config, now)
    return tasks[0] if tasks else None


def commit_paths(config: dict, task: str) -> list[str]:
    files = list((config.get("commit_files") or {}).get(task, []))
    files.append("data/ci_state.json")
    seen: set[str] = set()
    out: list[str] = []
    for f in files:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return [f for f in out if (ROOT / f).exists()]


def apply_env(extra: dict) -> None:
    for k, v in (extra or {}).items():
        if v is not None:
            os.environ[str(k)] = str(v)


def run_subprocess(cmd: list[str], extra_env: dict | None = None) -> None:
    env = os.environ.copy()
    if extra_env:
        env.update({str(k): str(v) for k, v in extra_env.items()})
    subprocess.run(cmd, cwd=str(ROOT), env=env, check=True)


def execute_task(task: str, config: dict) -> None:
    py = sys.executable
    if task == "borrow":
        run_subprocess([py, str(BUILD_DATA), "--borrow-only"])
    elif task == "options":
        apply_env(config.get("options_env") or {})
        run_subprocess([py, str(BUILD_DATA), "--options-only"], config.get("options_env"))
    elif task == "yieldboost":
        apply_env(config.get("yieldboost_env") or {})
        run_subprocess([py, str(BUILD_DATA), "--yieldboost-vrp-only"], config.get("yieldboost_env"))
    elif task == "intraday":
        run_subprocess([py, str(ROOT / "scripts" / "refresh_underlying_spots.py"), "--max-yfinance", "0", "--min-with-return", "50"])
        try:
            run_subprocess([py, str(ROOT / "scripts" / "refresh_underlying_volume.py"), "--max-yfinance", "0"])
        except subprocess.CalledProcessError:
            print("[ci_tick] volume refresh failed; continuing with stale/missing volume")
        run_subprocess([py, str(ROOT / "scripts" / "build_letf_intraday_flows.py")])
    elif task == "nav":
        run_subprocess([py, str(ROOT / "scripts" / "forecast_nav.py")])
    else:
        raise ValueError(f"Unknown task: {task}")


def write_github_output(
    tasks: list[str],
    skipped: bool,
    files: list[str],
    *,
    failures: list[tuple[str, str]] | None = None,
) -> None:
    out_path = os.environ.get("GITHUB_OUTPUT")
    if not out_path:
        return
    primary = tasks[0] if tasks else "none"
    fail_str = ",".join(name for name, _ in (failures or []))
    with open(out_path, "a", encoding="utf-8") as f:
        f.write(f"task={primary}\n")
        f.write(f"tasks={' '.join(tasks) if tasks else 'none'}\n")
        f.write(f"skipped={'true' if skipped else 'false'}\n")
        f.write(f"files={' '.join(files)}\n")
        f.write(f"failures={fail_str}\n")


def _task_is_stale(task: str, state: dict, config: dict, now: datetime) -> bool:
    rth = is_rth(now, config)
    use_rth = rth and task in {"intraday", "nav"}
    return is_stale(task, state, config, now, rth=use_rth)


def _merge_commit_paths(config: dict, tasks: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for task in tasks:
        for path in commit_paths(config, task):
            if path not in seen:
                seen.add(path)
                out.append(path)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one CI refresh task with staleness gates")
    parser.add_argument(
        "--mode",
        default="auto",
        choices=["auto", "borrow", "options", "yieldboost", "intraday", "nav"],
    )
    parser.add_argument("--force", action="store_true", help="Ignore staleness gates")
    args = parser.parse_args()

    config = load_config()
    state = load_state()
    now = datetime.now(UTC)

    if args.mode == "auto":
        tasks = pick_auto_tasks(state, config, now)
    else:
        tasks = [args.mode]

    if not tasks:
        print("[ci_tick] No stale task; skipping")
        write_github_output([], True, [])
        return 0

    if not args.force and args.mode == "auto":
        tasks = [t for t in tasks if _task_is_stale(t, state, config, now)]
        if not tasks:
            print("[ci_tick] All auto tasks fresh; skipping")
            write_github_output([], True, [])
            return 0

    print(f"[ci_tick] Running task(s)={','.join(tasks)}")
    succeeded: list[str] = []
    failed: list[tuple[str, str]] = []
    now_iso = now.isoformat().replace("+00:00", "Z")
    for task in tasks:
        state_key = "nav" if task == "nav" else task
        try:
            execute_task(task, config)
        except subprocess.CalledProcessError as exc:
            err_msg = f"exit={exc.returncode}"
            failed.append((task, err_msg))
            # Advance the failure marker so we can see the last attempt time and
            # cool the retry briefly, but do NOT advance ``last_<task>_utc`` --
            # that should reflect a successful refresh.
            state[f"last_{state_key}_attempt_utc"] = now_iso
            state[f"last_{state_key}_error"] = err_msg
            print(f"[ci_tick] Task '{task}' failed ({err_msg}); continuing with remaining tasks.")
            save_state(state)
            continue
        except Exception as exc:  # pragma: no cover - belt & suspenders
            err_msg = repr(exc)[:160]
            failed.append((task, err_msg))
            state[f"last_{state_key}_attempt_utc"] = now_iso
            state[f"last_{state_key}_error"] = err_msg
            print(f"[ci_tick] Task '{task}' crashed ({err_msg}); continuing with remaining tasks.")
            save_state(state)
            continue
        state[f"last_{state_key}_utc"] = now_iso
        state.pop(f"last_{state_key}_error", None)
        succeeded.append(task)
        save_state(state)

    files = _merge_commit_paths(config, succeeded) if succeeded else []
    print(f"[ci_tick] Commit candidates: {' '.join(files) or '(none)'}")
    write_github_output(succeeded, False, files, failures=failed)
    if failed:
        for name, err in failed:
            # GitHub Actions annotation surfaces in the run summary without
            # red-flagging the whole workflow; the commit/deploy steps for the
            # tasks that did succeed are still allowed to run.
            print(f"::warning title=ci_tick task {name} failed::{err}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
