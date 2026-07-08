#!/usr/bin/env python3
"""Generate Borrow Research Summary PDF from tracking/eval artifacts."""
from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from fpdf import FPDF

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / "data"
OUT = REPO / "docs" / "Borrow_Research_Summary.pdf"


def _load(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


class BorrowResearchPDF(FPDF):
    def header(self) -> None:
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(90, 90, 90)
        self.cell(0, 8, "ETF Borrow Dashboard - Borrow Research Summary", align="L")
        self.ln(10)

    def footer(self) -> None:
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(120, 120, 120)
        self.cell(0, 10, f"Page {self.page_no()}", align="C")

    def section_title(self, title: str) -> None:
        self.ln(4)
        self.set_font("Helvetica", "B", 13)
        self.set_text_color(20, 40, 80)
        self.multi_cell(0, 8, title)
        self.ln(2)

    def subsection(self, title: str) -> None:
        self.ln(2)
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(40, 60, 100)
        self.multi_cell(0, 7, title)
        self.ln(1)

    def body(self, text: str) -> None:
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5.5, text)
        self.ln(2)

    def bullet(self, text: str) -> None:
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5.5, f"  - {text}")
        self.ln(1)


def build_pdf() -> Path:
    tracking = _load(DATA / "borrow_spike_tracking.json")
    eval_data = _load(DATA / "borrow_spike_eval.json")
    study = _load(DATA / "borrow_predictor_study_summary.json")
    findings = tracking.get("findings_summary") or eval_data.get("findings_summary") or {}
    milestones = tracking.get("milestones") or []
    replay_l0 = (eval_data.get("metrics") or {}).get("replay_l0") or (eval_data.get("metrics") or {}).get("replay") or {}
    replay_l2 = (eval_data.get("metrics") or {}).get("replay_l2") or {}

    pdf = BorrowResearchPDF()
    pdf.set_auto_page_break(auto=True, margin=18)
    pdf.add_page()

    pdf.set_font("Helvetica", "B", 18)
    pdf.set_text_color(15, 35, 75)
    pdf.multi_cell(0, 10, "Borrow Research Conclusions")
    pdf.ln(2)
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(80, 80, 80)
    as_of = tracking.get("as_of") or eval_data.get("as_of") or datetime.now(UTC).isoformat()
    pdf.multi_cell(0, 6, f"Generated: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}  |  Data as of: {str(as_of)[:19]}")
    pdf.ln(4)

    pdf.section_title("Executive Summary")
    pdf.body(
        findings.get(
            "best_estimate_borrow",
            "Combine (1) current borrow level, (2) L2 calibrated stress tier, and (3) delta_borrow "
            "5D OLS drift for directional context. Net edge uses historical borrow posterior unchanged.",
        )
    )
    pdf.body(
        "We cannot reliably predict catastrophic borrow spikes (L0) with calibrated probabilities today. "
        "We can rank relative borrow stress (L2 tiers) and give a weak directional drift hint (~5% R2)."
    )

    pdf.section_title("1. Three-Piece Borrow Outlook (Dashboard)")
    pdf.subsection("Piece 1: Level")
    pdf.body("Field: borrow_current. Spot annualized IBKR fee-only borrow. Strongest single anchor for future borrow.")
    pdf.subsection("Piece 2: Drift")
    pdf.body(
        "Field: borrow_forecast_delta_5d_p50. Expected change in annual borrow over the next 5 borrow "
        "observations (git/IBKR history rows, not always 5 calendar days). "
        "Forecast level = borrow_current + delta. Pooled OLS on borrow_current, borrow_slope5, "
        "borrow_vol10, borrow_z60. Use directionally only; low R2 (~5%)."
    )
    pdf.subsection("Piece 3: Stress")
    pdf.body(
        "Field: borrow_spike_alert_tier (watch / elevated / high). Based on L2 calibrated probability. "
        "Use tier names for monitoring, not literal L0 spike percentages."
    )

    pdf.section_title("2. Drift Forecast Definition")
    pdf.body("Target: delta_borrow_5 = borrow at t+5 observations minus borrow at t (decimal annual rate).")
    pdf.body(
        "Model: delta_borrow_5 ~ intercept + b1*borrow_current + b2*borrow_slope5 + "
        "+ b3*borrow_vol10 + b4*borrow_z60, fit pooled across all symbols in borrow_predictor_panel.parquet."
    )
    pdf.bullet("Positive delta: borrow likely rising; short carry may worsen vs spot.")
    pdf.bullet("Negative delta: borrow easing; spot may overstate long-run cost.")
    pdf.bullet("Does NOT change net_edge_p50 or spike tier logic.")

    pdf.section_title("3. L2 Calibrated Tier (Deep Summary)")
    pdf.subsection("L2 label (training target)")
    pdf.body(
        "spike_event = 1 if BOTH: (a) max borrow over next 5 observations > 90th percentile of last 60 obs, "
        "and (b) jump > 10 percentage points annual. ~841 positives in replay vs 1 for L0 catastrophic."
    )
    pdf.subsection("Model pipeline")
    pdf.bullet("logistic_v2: L2-regularized logistic regression on borrow dynamics + supply/scale features.")
    pdf.bullet("Isotonic calibration: bin raw scores, map to realized L2 rate, enforce monotonicity.")
    pdf.bullet("Alert tiers on calibrated p: watch >= 5%, elevated >= 12%, high >= 25%.")

    if replay_l2:
        pdf.subsection("L2 replay metrics")
        pdf.bullet(f"Rows: {replay_l2.get('n_rows', 'n/a')}  |  Positives: {replay_l2.get('positives', 'n/a')}")
        pdf.bullet(f"AUROC: {replay_l2.get('auroc', 'n/a')}  |  ECE: {replay_l2.get('ece', 'n/a')}")
        cal = replay_l2.get("calibration_by_band") or []
        for band in cal:
            pdf.bullet(
                f"{band.get('band', '?')}: avg pred {band.get('avg_pred', 0):.3f}, "
                f"realized {band.get('realized_rate', 0):.3f} (n={band.get('count', 0)})"
            )

    if replay_l0:
        pdf.subsection("L0 catastrophic (not for sizing)")
        pdf.bullet(
            f"Replay positives: {replay_l0.get('positives', 'n/a')} in {replay_l0.get('n_rows', 'n/a')} rows. "
            "Elevated/high L0 bands showed 0% realized catastrophic rate. Do not use P(spike) >= 30%."
        )

    pdf.section_title("4. Key Features Explained")
    pdf.subsection("borrow_vol10")
    pdf.body(
        "Std dev of day-over-day borrow changes over last 10 observations. Measures borrow choppiness, "
        "not level. Rank-corr ~0.12 with 5-obs delta borrow."
    )
    pdf.subsection("log_aum")
    pdf.body(
        "log(1 + AUM) from etf_metrics_daily (NAV x shares_outstanding fallback). Fund size proxy. "
        "Rank-corr ~0.11 with delta borrow at h=5."
    )
    pdf.subsection("shares_available")
    pdf.body(
        "IBKR-reported shares available to borrow. Drives shares_drop*, utilization_proxy, near_zero_shares. "
        "Rank-corr ~0.10 with delta borrow. Broker-specific and noisy."
    )

    pdf.section_title("5. Net Edge and Borrow (Unchanged)")
    pdf.body(
        "net_edge_p50_annual still comes from ls-algo: block-bootstrap gross drag, inverse-variance blend "
        "with forward Exp. edge, minus resampled borrow history (weighted_empirical, 90d halflife). "
        "Anchor: borrow_for_net_annual ~ borrow_posterior_annual."
    )
    pdf.body(
        "Borrow outlook is annotation only: drift and stress tiers do not modify net edge math. "
        "YieldBOOST borrow-once invariant preserved."
    )

    pdf.section_title("6. Research Programs")
    pdf.subsection("Program A - Spike predictor accuracy")
    pdf.body("Walk-forward replay, live scoring, borrow_spike_eval.json metrics. Dual labels L0 + L2.")
    pdf.subsection("Program B - Borrow predictor study")
    pdf.body(
        f"Panel: {study.get('panel_rows', 'n/a')} rows, {study.get('panel_symbols', 'n/a')} symbols. "
        f"Best ablation: {study.get('best_block_ablation', 'borrow_plus_supply')}. "
        "Supply helps slightly; peer basket hurts holdout (excluded from v2)."
    )
    if study.get("key_findings"):
        for kf in study["key_findings"]:
            pdf.bullet(str(kf))

    pdf.section_title("7. Milestone Status")
    for m in milestones:
        pdf.bullet(
            f"[{m.get('status', '?').upper()}] {m.get('description', m.get('id', ''))}: "
            f"{m.get('current')} / {m.get('target')}"
        )
    next_actions = tracking.get("next_actions") or []
    if next_actions:
        pdf.subsection("Next actions")
        for a in next_actions:
            pdf.bullet(str(a))

    pdf.section_title("8. Recommended Next Steps")
    pdf.bullet("Fix utilization coverage (42% -> 80%): improve shares_outstanding joins in metrics panel.")
    pdf.bullet("Keep daily L0 prediction archive; do not productize L0 probabilities until >= 20 replay positives.")
    pdf.bullet("Optional: net_edge_stress_p50 display column using max(posterior, forecast) for elevated tier.")
    pdf.bullet("Optional: live calibration monitor on elevated-tier hit rate over rolling 60d.")
    pdf.bullet("Do NOT: peer features in v2, replace ls-algo borrow with drift forecast, L0 prob sizing.")

    pdf.section_title("9. Artifacts and Commands")
    pdf.bullet("data/borrow_spike_tracking.json - milestone tracker")
    pdf.bullet("data/borrow_spike_eval.json - replay metrics L0 + L2")
    pdf.bullet("data/borrow_forecast_latest.json - per-symbol drift")
    pdf.bullet("data/borrow_predictor_study_summary.json - feature rankings")
    pdf.bullet("BORROW_SPIKE_EVAL.md - operator guide")
    pdf.body("Run: python scripts/borrow_spike_pipeline.py")
    pdf.body("Refresh dashboard rows: python scripts/build_data.py --borrow-only")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    pdf.output(str(OUT))
    return OUT


if __name__ == "__main__":
    path = build_pdf()
    print(f"[OK] Wrote {path}")
    sys.exit(0)
