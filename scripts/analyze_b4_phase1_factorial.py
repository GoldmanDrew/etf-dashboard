#!/usr/bin/env python3
"""Build reproducible robustness diagnostics from the isolated B4 factorial."""
from __future__ import annotations

import argparse
import json
import math
from math import comb
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
DEFAULT_RESULTS = REPO / "data" / "_phase1_experiments"
DEFAULT_DASHBOARD = REPO / "data" / "dashboard_data.json"
BASELINE = "current__current"


def _trimmed_mean(values: pd.Series, fraction: float = 0.10) -> float:
    vals = pd.to_numeric(values, errors="coerce").dropna().sort_values()
    cut = int(fraction * len(vals))
    use = vals.iloc[cut : len(vals) - cut] if cut and len(vals) > 2 * cut else vals
    return float(use.mean())


def _sign_test_pvalue(wins: int, n: int) -> float:
    if n <= 0:
        return float("nan")
    return float(sum(comb(n, k) for k in range(wins, n + 1)) / (2**n))


def _underlying_map(path: Path) -> dict[str, str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("records") or payload.get("rows") or []
    return {
        str(row.get("symbol") or "").strip().upper():
        str(row.get("underlying") or row.get("symbol") or "").strip().upper()
        for row in rows
    }


def build_robustness(summary: pd.DataFrame, pairs: pd.DataFrame, underlying: dict[str, str]) -> pd.DataFrame:
    out: list[dict] = []
    for borrow, group in pairs.groupby("underlying_borrow_annual"):
        base = group.loc[group["variant"].eq(BASELINE)].set_index("etf")["cumulative_return"]
        for variant, vg in group.groupby("variant"):
            alt = vg.set_index("etf")["cumulative_return"]
            delta = (alt - base).dropna()
            wins = int((delta > 0).sum())
            cluster_means: list[float] = []
            clusters = sorted({underlying.get(etf, etf) for etf in delta.index})
            for cluster in clusters:
                keep = [etf for etf in delta.index if underlying.get(etf, etf) != cluster]
                if keep:
                    cluster_means.append(float(delta.loc[keep].mean()))
            srow = summary.loc[
                summary["underlying_borrow_annual"].eq(borrow) & summary["variant"].eq(variant)
            ].iloc[0]
            portfolio_days = srow.get("n_days_x", srow.get("n_days"))
            record = {
                "underlying_borrow_annual": float(borrow),
                "variant": variant,
                "n_pairs": int(len(delta)),
                "wins": wins,
                "win_rate": float(wins / len(delta)) if len(delta) else None,
                "sign_test_one_sided_p": _sign_test_pvalue(wins, len(delta)),
                "median_pair_return_delta": float(delta.median()),
                "mean_pair_return_delta": float(delta.mean()),
                "trim10_mean_pair_return_delta": _trimmed_mean(delta),
                "worst_cluster_leaveout_mean_delta": min(cluster_means) if cluster_means else None,
                "positive_cluster_leaveout_count": sum(x > 0 for x in cluster_means),
                "cluster_leaveout_count": len(cluster_means),
                "largest_positive_driver": str(delta.idxmax()) if len(delta) else None,
                "largest_positive_driver_delta": float(delta.max()) if len(delta) else None,
                "largest_negative_driver": str(delta.idxmin()) if len(delta) else None,
                "largest_negative_driver_delta": float(delta.min()) if len(delta) else None,
                "portfolio_common_days": int(portfolio_days) if pd.notna(portfolio_days) else None,
                "portfolio_cumulative_return": float(srow["cumulative_return"]),
                "portfolio_relative_return_vs_current": float(srow["observed_relative_return"]),
                "portfolio_bootstrap_probability_outperforms": float(srow["probability_outperforms"]),
                "portfolio_max_drawdown": float(srow["max_drawdown"]),
                "portfolio_expected_shortfall_95_daily": float(srow["expected_shortfall_95_daily"]),
                "median_pair_cagr": float(srow["median_pair_cagr"]),
                "positive_pair_count": int(srow["positive_pair_count"]),
                "total_rebalances": int(srow["total_rebalances"]),
            }
            # A candidate must improve broadly and survive every single-underlying
            # exclusion; recent common-window portfolio return is a separate gate.
            record["breadth_robust"] = bool(
                variant != BASELINE
                and record["sign_test_one_sided_p"] <= 0.10
                and record["median_pair_return_delta"] > 0
                and record["trim10_mean_pair_return_delta"] > 0
                and (record["worst_cluster_leaveout_mean_delta"] or -math.inf) > 0
            )
            record["promotion_ready"] = bool(
                record["breadth_robust"]
                and record["portfolio_relative_return_vs_current"] >= 0
                and record["portfolio_bootstrap_probability_outperforms"] >= 0.60
            )
            out.append(record)
    return pd.DataFrame(out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS)
    ap.add_argument("--dashboard", type=Path, default=DEFAULT_DASHBOARD)
    args = ap.parse_args(argv)
    summary = pd.read_csv(args.results_dir / "factorial_summary.csv")
    pairs = pd.read_csv(args.results_dir / "factorial_by_pair.csv")
    robust = build_robustness(summary, pairs, _underlying_map(args.dashboard))
    robust.to_csv(args.results_dir / "robustness_summary.csv", index=False)
    payload = {
        "schema": "bucket4_phase1_factorial_robustness.v1",
        "authoritative": False,
        "baseline": BASELINE,
        "promotion_ready_count": int(robust["promotion_ready"].sum()),
        "breadth_robust_count": int(robust["breadth_robust"].sum()),
        "rows": robust.replace({np.nan: None}).to_dict(orient="records"),
    }
    (args.results_dir / "robustness_results.json").write_text(
        json.dumps(payload, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    print(json.dumps({
        "ok": True,
        "rows": len(robust),
        "breadth_robust": payload["breadth_robust_count"],
        "promotion_ready": payload["promotion_ready_count"],
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
