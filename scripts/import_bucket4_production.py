"""Validate and publish the authoritative ls-algo Bucket 4 replay contract."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from datetime import date
from pathlib import Path
from typing import Any, Mapping

REPO = Path(__file__).resolve().parents[1]
OUT_JSON = REPO / "data" / "bucket4_backtest.json"
OUT_STATE = REPO / "data" / "bucket4_backtest_state.json"
OUT_HASH = REPO / "data" / "bucket4_backtest_policy_hash.txt"
OUT_PAIR_DIR = REPO / "data" / "bucket4_pairs"
CONTRACT_SCHEMA = "bucket4_production_replay.v1"
DASHBOARD_SCHEMA = "bucket4_backtest.v4"


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    return out if math.isfinite(out) else float(default)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def find_production_export(explicit: str | Path | None = None) -> Path | None:
    env = os.environ.get("B4_PRODUCTION_EXPORT", "").strip()
    ls_env = os.environ.get("LS_ALGO_ROOT", "").strip()
    candidates = [
        Path(explicit) if explicit else None,
        Path(env) if env else None,
        (Path(ls_env) / "risk_dashboard" / "data" / "bucket4_production_replay") if ls_env else None,
        REPO / "ls-algo" / "risk_dashboard" / "data" / "bucket4_production_replay",
        REPO.parent / "ls-algo" / "risk_dashboard" / "data" / "bucket4_production_replay",
        Path.home() / "Projects" / "quant" / "ls-algo" / "risk_dashboard" / "data" / "bucket4_production_replay",
    ]
    for candidate in candidates:
        if candidate and (candidate / "manifest.json").is_file():
            return candidate.resolve()
    return None


def validate_contract(
    root: Path,
    *,
    allow_dirty_source: bool = False,
    max_age_days: int | None = None,
) -> dict[str, Any]:
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Bucket 4 production manifest missing: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema") != CONTRACT_SCHEMA:
        raise ValueError(f"unsupported Bucket 4 contract schema: {manifest.get('schema')}")
    if manifest.get("authoritative") is not True:
        raise ValueError("Bucket 4 contract is not authoritative")
    source = manifest.get("source") or {}
    if source.get("dirty") is True:
        if not source.get("working_tree_hash"):
            raise ValueError("dirty source has no archived working-tree hash")
        if not allow_dirty_source:
            raise ValueError("dirty ls-algo source is not publishable; pass --allow-dirty-source only for local validation")
    for rel, expected in (manifest.get("output_hashes") or {}).items():
        path = root / str(rel)
        if not path.is_file() or _sha256_file(path) != str(expected):
            raise ValueError(f"Bucket 4 contract hash mismatch: {rel}")
    recon = ((manifest.get("reconciliation") or {}).get("pair_to_sleeve") or {})
    if abs(_finite(recon.get("max_abs_after_usd"))) > 0.01:
        raise ValueError(f"Bucket 4 pair reconciliation failed: {recon}")
    if abs(_finite((manifest.get("reconciliation") or {}).get("book_max_abs_residual_usd"))) > 0.01:
        raise ValueError("Bucket 4 source book reconciliation failed")
    if max_age_days is not None:
        end_raw = str((manifest.get("run") or {}).get("end") or "")
        try:
            age = (date.today() - date.fromisoformat(end_raw)).days
        except ValueError as exc:
            raise ValueError(f"Bucket 4 contract has invalid end date: {end_raw}") from exc
        if age < 0 or age > int(max_age_days):
            raise ValueError(f"Bucket 4 contract is stale: end={end_raw}, age={age}d, max={max_age_days}d")
    return manifest


def _latest_gross(pair: Mapping[str, Any]) -> float:
    daily = pair.get("daily") or {}
    vals = daily.get("gross_exposure_dollars") or []
    return _finite(vals[-1]) if vals else 0.0


def _truthy_daily_flag(value: Any) -> bool:
    if value is True or value == 1 or value == "1":
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"true", "yes", "y"}
    try:
        return bool(float(value))
    except (TypeError, ValueError):
        return False


def _rebalance_log_has_fields(log: Any) -> bool:
    if not isinstance(log, list) or not log:
        return False
    for ev in log:
        if not isinstance(ev, Mapping):
            continue
        if (
            ev.get("h") is not None
            or ev.get("executed") is True
            or ev.get("executed") is False
            or ev.get("rebalance_fee") is not None
            or ev.get("reason")
        ):
            return True
    return False


def _daily_has_rebalance_flags(daily: Mapping[str, Any] | None) -> bool:
    if not isinstance(daily, Mapping):
        return False
    flags = daily.get("rebalance") or []
    return any(_truthy_daily_flag(v) for v in flags)


def load_etf_inception_dates(data_dir: Path | None = None) -> dict[str, str]:
    """First metrics session per ticker (metadata only — not production PnL)."""
    root = data_dir or (REPO / "data")
    pq = root / "etf_metrics_daily.parquet"
    csv = root / "etf_metrics_daily.csv"
    try:
        import pandas as pd
    except ImportError:
        return {}
    frame = None
    if pq.is_file():
        try:
            frame = pd.read_parquet(pq, columns=["date", "ticker"])
        except Exception:
            frame = None
    if frame is None and csv.is_file():
        try:
            frame = pd.read_csv(csv, usecols=["date", "ticker"])
        except Exception:
            return {}
    if frame is None or frame.empty:
        return {}
    frame = frame.copy()
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    frame["date"] = frame["date"].astype(str).str.slice(0, 10)
    out: dict[str, str] = {}
    for ticker, group in frame.groupby("ticker", sort=False):
        dates = sorted(d for d in group["date"].tolist() if d and d != "nan")
        if dates:
            out[str(ticker).upper()] = dates[0]
    return out


def enrich_pair_shard(
    etf: str,
    shard: dict[str, Any],
    *,
    inception_by_etf: Mapping[str, str] | None = None,
    membership_row: Mapping[str, Any] | None = None,
) -> list[str]:
    """Stamp plan/inception metadata; return soft limitation warnings (fail-soft)."""
    warnings: list[str] = []
    summary = dict(shard.get("summary") or {})
    plan_entry = (
        summary.get("plan_entry_date")
        or (membership_row or {}).get("first_plan_date")
        or summary.get("entry_date")
    )
    plan_exit = (
        summary.get("plan_exit_date")
        or (membership_row or {}).get("last_plan_date")
        or summary.get("latest_date")
    )
    if plan_entry:
        summary["plan_entry_date"] = str(plan_entry)
        summary.setdefault("entry_date", str(plan_entry))
    if plan_exit:
        summary["plan_exit_date"] = str(plan_exit)
    inception = (
        summary.get("etf_inception_date")
        or shard.get("etf_inception_date")
        or (membership_row or {}).get("etf_inception_date")
        or (inception_by_etf or {}).get(etf)
    )
    if inception:
        summary["etf_inception_date"] = str(inception)
        shard["etf_inception_date"] = str(inception)
    summary.setdefault("history_basis", shard.get("history_basis") or "plan")
    shard["history_basis"] = summary["history_basis"]
    shard.setdefault("rebalance_log_basis", "production_execution_ledger")
    shard.setdefault("rebalance_log_fee_units", "fraction_of_sleeve_capital")
    daily = shard.get("daily") or {}
    log = shard.get("rebalance_log")
    if _daily_has_rebalance_flags(daily) and not _rebalance_log_has_fields(log):
        warnings.append(
            f"{etf}: daily.rebalance has flags but rebalance_log is empty/stub; "
            "UI will derive the log client-side until ls-algo exports a filled log"
        )
    inception_research = shard.get("inception_research")
    if isinstance(inception_research, Mapping):
        # Never treat research path as authoritative for book KPIs.
        ir = dict(inception_research)
        ir["authoritative"] = False
        ir.setdefault(
            "disclaimer",
            "Research path from ETF/underlying overlap. Not production-policy replay. Not used for book PnL.",
        )
        ir.setdefault("history_basis", "inception_research")
        shard["inception_research"] = ir
    shard["summary"] = summary
    return warnings


def build_dashboard_payload(root: Path, manifest: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    book = json.loads((root / "book.json").read_text(encoding="utf-8"))
    membership_path = root / "membership.json"
    if not membership_path.is_file():
        raise ValueError("Bucket 4 production contract has no membership artifact")
    membership = json.loads(membership_path.read_text(encoding="utf-8"))
    if not isinstance(membership, list):
        raise ValueError("Bucket 4 membership artifact is invalid")
    pair_dir = root / "pairs"
    pair_files = sorted(pair_dir.glob("*.json"))
    if not pair_files:
        raise ValueError("Bucket 4 production contract has no pair files")
    pair_shards = {p.stem.upper(): json.loads(p.read_text(encoding="utf-8")) for p in pair_files}
    membership_by_etf = {str(row.get("ETF") or row.get("etf") or "").upper(): dict(row) for row in membership}
    inception_by_etf = load_etf_inception_dates(REPO / "data")
    budget = _finite(book.get("initial_capital_usd"), 0.0)
    gross_by_etf = {etf: _latest_gross(pair) for etf, pair in pair_shards.items()}
    deployed_gross = sum(max(0.0, value) for value in gross_by_etf.values())
    default_weights = {
        etf: (max(0.0, gross) / budget if budget > 0 else 0.0)
        for etf, gross in gross_by_etf.items()
        if gross > 1e-12
    }
    pairs: list[dict[str, Any]] = []
    pair_series: dict[str, Any] = {}
    pair_manifest: list[dict[str, Any]] = []
    soft_limitations: list[str] = []
    for etf, shard in pair_shards.items():
        mem_row = membership_by_etf.get(etf)
        soft_limitations.extend(
            enrich_pair_shard(
                etf,
                shard,
                inception_by_etf=inception_by_etf,
                membership_row=mem_row,
            )
        )
        summary = dict(shard.get("summary") or {})
        weight = default_weights.get(etf, 0.0)
        summary.update({
            "etf": etf,
            "underlying": shard.get("underlying"),
            "in_production_book": True,
            "production_status": "production_policy_replay",
            "portfolio_weight": weight,
            "effective_weight": weight,
            "gate_reason": "production_ledger",
        })
        shard["summary"] = summary
        shard["in_production_book"] = True
        shard["production_status"] = "production_policy_replay"
        shard["policy_version"] = str(manifest.get("resolved_policy_hash", ""))[:16]
        pair_series[etf] = shard
        pairs.append(summary)
        pair_manifest.append({
            **summary,
            "shard_url": f"data/bucket4_pairs/{etf}.json",
            "has_daily": True,
        })
    pairs.sort(key=lambda row: (-_finite(row.get("portfolio_weight")), str(row.get("etf"))))
    pair_manifest.sort(key=lambda row: (-_finite(row.get("portfolio_weight")), str(row.get("etf"))))
    book_summary = book.get("summary") or {}
    policy = manifest.get("resolved_policy") or {}
    run = manifest.get("run") or {}
    lifecycle_rows = []
    for etf, row in sorted(membership_by_etf.items()):
        if not etf:
            continue
        inception = row.get("etf_inception_date") or inception_by_etf.get(etf)
        normalized_row = {
            ("etf" if key == "ETF" else "underlying" if key == "Underlying" else key): value
            for key, value in row.items()
        }
        item = {
            **normalized_row,
            "etf": etf,
            "underlying": str(row.get("Underlying") or row.get("underlying") or "").upper(),
            "has_daily": etf in pair_shards,
            "in_production_book": etf in pair_shards,
        }
        if inception:
            item["etf_inception_date"] = str(inception)
        if item["has_daily"]:
            item["shard_url"] = f"data/bucket4_pairs/{etf}.json"
            shard_sum = (pair_shards.get(etf) or {}).get("summary") or {}
            item.setdefault("first_plan_date", shard_sum.get("plan_entry_date") or shard_sum.get("entry_date"))
            item.setdefault("last_plan_date", shard_sum.get("plan_exit_date") or shard_sum.get("latest_date"))
        lifecycle_rows.append(item)
    open_members = [r["etf"] for r in lifecycle_rows if str(r.get("lifecycle_state", "")).lower() == "open"]
    pending_members = [r["etf"] for r in lifecycle_rows if str(r.get("lifecycle_state", "")).lower() == "pending_entry"]
    purgatory_members = [r["etf"] for r in lifecycle_rows if bool(r.get("latest_purgatory"))]
    blocked_members = [r["etf"] for r in lifecycle_rows if str(r.get("lifecycle_state", "")).lower() == "blocked"]
    payload = {
        "schema": DASHBOARD_SCHEMA,
        "generated_at_utc": manifest.get("generated_at_utc"),
        "authoritative": True,
        "mode": "production_policy_replay",
        "ledger_mode": "actual_dollar",
        "research_reblend_enabled": False,
        "policy_version": str(manifest.get("resolved_policy_hash", ""))[:16],
        "policy_hash": manifest.get("resolved_policy_hash"),
        "source_contract_schema": manifest.get("schema"),
        "source": manifest.get("source"),
        "source_run": run,
        "source_input_hashes": manifest.get("input_hashes"),
        "limitations": list(manifest.get("limitations") or []) + soft_limitations,
        "reconciliation": manifest.get("reconciliation"),
        "sizing_method": "full_generate_trade_plan_replay",
        "execution_method": policy.get("b4_execution", "cadence"),
        "sleeve_budget_usd": budget,
        "deployed_fraction": deployed_gross / budget if budget > 0 else 0.0,
        "cash_residual": max(0.0, 1.0 - deployed_gross / budget) if budget > 0 else 1.0,
        "window_start": (book.get("dates") or [run.get("start")])[0],
        "window_end": (book.get("dates") or [run.get("end")])[-1],
        "sim_dates": book.get("dates") or [],
        "port_daily_returns": book.get("returns") or [],
        "port_equity": book.get("equity") or [],
        "port_nav_usd": book.get("nav_usd") or [],
        "port_daily_pnl_usd": book.get("daily_pnl_usd") or [],
        "port_cumulative_pnl_usd": book.get("cumulative_pnl_usd") or [],
        "costs": {
            "borrow_cost_usd": book.get("borrow_cost_usd") or [],
            "short_credit_usd": book.get("short_credit_usd") or [],
            "margin_cost_usd": book.get("margin_cost_usd") or [],
            "txn_cost_usd": book.get("txn_cost_usd") or [],
        },
        "realized": {
            "cagr": book_summary.get("cagr"),
            "ann_vol": book_summary.get("ann_vol"),
            "sharpe": book_summary.get("sharpe"),
            "maxdd": book_summary.get("max_drawdown"),
            "net_pnl_usd": book_summary.get("net_pnl_usd"),
            "final_nav_usd": book_summary.get("final_nav_usd"),
        },
        "cadence": {
            "source": "ls-algo production ledger",
            "operator_check_days": policy.get("operator_check_days"),
            "membership_clock": policy.get("b4_membership_clock"),
            "execution": policy.get("b4_execution"),
        },
        "parity": {
            "authoritative_export": True,
            "full_gtp_timeline": True,
            "production_execution_ledger": True,
            "point_in_time_plans": True,
            "execution_lag_sessions": policy.get("execution_lag_sessions"),
            "purgatory_model_zero_policy": policy.get("purgatory_model_zero_policy"),
            "phase2b_resize_bands": policy.get("b4_apply_resize_bands"),
            "ratchet_execution_guard": policy.get("b4_ratchet_execution_guard"),
            "empty_plan_policy": policy.get("b4_empty_plan_policy"),
            "net_shared_underlyings": policy.get("net_shared_underlyings"),
            "turnover_pace_mode": policy.get("turnover_pace_mode"),
            "pair_reconciliation_max_abs_usd": (((manifest.get("reconciliation") or {}).get("pair_to_sleeve") or {}).get("max_abs_after_usd")),
            "research_reblend_enabled": False,
        },
        "default_weights": default_weights,
        "pairs": pairs,
        "pair_series": pair_series,
        "pair_manifest": pair_manifest,
        "pair_shard_base_url": "data/bucket4_pairs",
        "membership": lifecycle_rows,
        "n_pairs": len(pairs),
        "n_membership": len(lifecycle_rows),
        "n_obs": len(book.get("dates") or []),
        "universes": {
            "production_book": {"pairs": [p["etf"] for p in pairs], "count": len(pairs)},
            "historical_membership": {"pairs": [r["etf"] for r in lifecycle_rows], "count": len(lifecycle_rows), "note": "All point-in-time B4 plan members, including blocked and purgatory states."},
            "current_open": {"pairs": open_members, "count": len(open_members)},
            "pending_entry": {"pairs": pending_members, "count": len(pending_members)},
            "purgatory": {"pairs": purgatory_members, "count": len(purgatory_members)},
            "blocked": {"pairs": blocked_members, "count": len(blocked_members)},
        },
    }
    return payload, pair_shards


def import_contract(
    root: Path,
    *,
    allow_dirty_source: bool = False,
    remove_stale_pairs: bool = False,
    max_age_days: int | None = 10,
) -> dict[str, Any]:
    root = root.resolve()
    manifest = validate_contract(
        root,
        allow_dirty_source=allow_dirty_source,
        max_age_days=max_age_days,
    )
    payload, pair_shards = build_dashboard_payload(root, manifest)
    OUT_PAIR_DIR.mkdir(parents=True, exist_ok=True)
    keep = {f"{etf}.json" for etf in pair_shards}
    if remove_stale_pairs:
        resolved_pair_dir = OUT_PAIR_DIR.resolve()
        if resolved_pair_dir.parent != (REPO / "data").resolve():
            raise RuntimeError(f"refusing to clean unexpected pair directory: {resolved_pair_dir}")
        for existing in OUT_PAIR_DIR.glob("*.json"):
            if existing.name not in keep:
                existing.unlink()
    for etf, shard in pair_shards.items():
        _write_json(OUT_PAIR_DIR / f"{etf}.json", shard)
    _write_json(OUT_JSON, payload)
    state = {
        "schema": "bucket4_backtest_state.v2",
        "generated_at_utc": payload.get("generated_at_utc"),
        "authoritative": True,
        "mode": payload.get("mode"),
        "policy_version": payload.get("policy_version"),
        "source": payload.get("source"),
        "source_run": payload.get("source_run"),
        "reconciliation": payload.get("reconciliation"),
        "n_pairs": payload.get("n_pairs"),
        "n_membership": payload.get("n_membership"),
        "n_obs": payload.get("n_obs"),
    }
    _write_json(OUT_STATE, state)
    OUT_HASH.write_text(str(payload.get("policy_hash") or "") + "\n", encoding="utf-8")
    return payload


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source", type=Path, default=None)
    ap.add_argument("--allow-dirty-source", action="store_true")
    ap.add_argument("--remove-stale-pairs", action="store_true")
    ap.add_argument("--max-age-days", type=int, default=10)
    args = ap.parse_args(argv)
    source = find_production_export(args.source)
    if source is None:
        raise FileNotFoundError("authoritative Bucket 4 production export not found; set B4_PRODUCTION_EXPORT or LS_ALGO_ROOT")
    payload = import_contract(
        source,
        allow_dirty_source=bool(args.allow_dirty_source),
        remove_stale_pairs=bool(args.remove_stale_pairs),
        max_age_days=int(args.max_age_days),
    )
    print(json.dumps({
        "ok": True,
        "schema": payload["schema"],
        "source": str(source),
        "pairs": payload["n_pairs"],
        "days": payload["n_obs"],
        "policy_version": payload["policy_version"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
