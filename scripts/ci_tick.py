#!/usr/bin/env python3
"""Single-task CI refresh orchestrator for market-hours.yml.

Picks at most one stale refresh per invocation, runs it, updates data/ci_state.json,
and prints commit file paths for the workflow commit/deploy steps.
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
        "nav_forecast": 30,
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
        ],
        "yieldboost": [
            "data/options_cache.json",
            "data/vrp_live.json",
            "data/vrp_health.json",
            "data/yieldboost_put_spreads_latest.json",
            "data/yieldboost_options_target.json",
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


def is_stale(task: str, state: dict, config: dict, now: datetime) -> bool:
    cadence_map = config.get("cadence_minutes") or {}
    if task == "nav":
        cadence = cadence_map.get("nav_forecast", 30)
        state_key = "nav"
    else:
        cadence = cadence_map.get(task, 30)
        state_key = task
    return minutes_since(state, state_key, now) >= float(cadence)


def pick_auto_task(state: dict, config: dict, now: datetime) -> str | None:
    rth = is_rth(now, config)
    rotation = list(config.get("rotation_rth") or ["borrow", "options", "yieldboost", "intraday"])
    slot = (int(now.timestamp()) // 900) % max(len(rotation), 1)

    if rth:
        # Nav forecast on alternating ticks when intraday slot and nav is stale.
        if slot == 3 and is_stale("nav", state, config, now):
            if int(now.timestamp()) // 1800 % 2 == 0:
                return "nav"
        order = [rotation[slot % len(rotation)]] + [t for i, t in enumerate(rotation) if i != slot % len(rotation)]
        order += ["nav"]
        for task in order:
            if is_stale(task, state, config, now):
                return task
        return None

    if is_stale("borrow", state, config, now):
        return "borrow"
    return None


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


def write_github_output(task: str, skipped: bool, files: list[str]) -> None:
    out_path = os.environ.get("GITHUB_OUTPUT")
    if not out_path:
        return
    with open(out_path, "a", encoding="utf-8") as f:
        f.write(f"task={task}\n")
        f.write(f"skipped={'true' if skipped else 'false'}\n")
        f.write(f"files={' '.join(files)}\n")


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
        task = pick_auto_task(state, config, now)
    else:
        task = args.mode

    if task is None:
        print("[ci_tick] No stale task; skipping")
        write_github_output("none", True, [])
        return 0

    if not args.force and args.mode == "auto":
        if not is_stale(task, state, config, now):
            print(f"[ci_tick] Task {task} not stale; skipping")
            write_github_output(task, True, [])
            return 0

    print(f"[ci_tick] Running task={task}")
    execute_task(task, config)

    state_key = "nav" if task == "nav" else task
    state[f"last_{state_key}_utc"] = now.isoformat().replace("+00:00", "Z")
    save_state(state)

    files = commit_paths(config, task)
    print(f"[ci_tick] Commit candidates: {' '.join(files) or '(none)'}")
    write_github_output(task, False, files)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
