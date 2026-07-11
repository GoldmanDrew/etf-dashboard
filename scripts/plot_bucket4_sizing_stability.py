#!/usr/bin/env python3
"""Plot per-pair B4 sizing stability: underlying price vs hedge ratio vs proposed gross.

Reads rebuilt ``data/bucket4_pairs/{ETF}.json`` shards (production book only)
and overlays underlying close from the dashboard price panel / Yahoo fallback.

Outputs a multi-page PDF + per-ticker PNGs under ``data/bucket4_sizing_stability/``.

    python scripts/plot_bucket4_sizing_stability.py
    python scripts/plot_bucket4_sizing_stability.py --etfs DAMD,CORD,LITZ
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
PAIR_DIR = REPO / "data" / "bucket4_pairs"
OUT_DIR = REPO / "data" / "bucket4_sizing_stability"
PANEL_CANDIDATES = [
    REPO / "data" / "_experiment_price_panel.parquet",
    REPO / "data" / "bucket4_price_panel.parquet",
    REPO / "data" / "price_panel.parquet",
]


def _load_pair(etf: str) -> dict | None:
    p = PAIR_DIR / f"{etf}.json"
    if not p.is_file():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def _production_etfs() -> list[str]:
    out: list[str] = []
    for p in sorted(PAIR_DIR.glob("*.json")):
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if d.get("in_production_book") or (d.get("summary") or {}).get("in_production_book"):
            out.append(str(d.get("etf") or p.stem).upper())
    return out


def _und_price_series(und: str, dates: pd.DatetimeIndex, etf: str | None = None) -> pd.Series:
    und = str(und).strip().upper()
    etf_u = str(etf or "").strip().upper()

    # Preferred: etf_metrics_daily underlying_adj_close for this pair.
    for metrics_path in (
        REPO / "data" / "etf_metrics_daily.parquet",
        REPO / "data" / "etf_metrics_daily.csv",
    ):
        if not metrics_path.is_file():
            continue
        try:
            md = pd.read_parquet(metrics_path) if metrics_path.suffix == ".parquet" else pd.read_csv(metrics_path)
        except Exception:
            continue
        if "underlying_adj_close" not in md.columns or "date" not in md.columns:
            continue
        tick_col = "ticker" if "ticker" in md.columns else None
        und_col = "underlying" if "underlying" in md.columns else None
        g = md
        if etf_u and tick_col:
            g = md[md[tick_col].astype(str).str.upper() == etf_u]
        elif und_col:
            g = md[md[und_col].astype(str).str.upper() == und]
        if g.empty:
            continue
        s = pd.Series(
            pd.to_numeric(g["underlying_adj_close"], errors="coerce").to_numpy(),
            index=pd.to_datetime(g["date"]),
        ).sort_index()
        s = s[~s.index.duplicated(keep="last")].reindex(dates).ffill()
        if s.notna().sum() >= max(5, len(dates) // 5):
            return s

    for panel_path in PANEL_CANDIDATES:
        if not panel_path.is_file():
            continue
        try:
            panel = pd.read_parquet(panel_path)
        except Exception:
            continue
        # Long panel: date / underlying / close
        cols_l = {str(c).lower(): c for c in panel.columns}
        if {"date", "underlying", "close"}.issubset(cols_l):
            sub = panel[panel[cols_l["underlying"]].astype(str).str.upper() == und]
            if sub.empty:
                continue
            s = pd.Series(
                pd.to_numeric(sub[cols_l["close"]], errors="coerce").to_numpy(),
                index=pd.to_datetime(sub[cols_l["date"]]),
            ).sort_index()
            s = s[~s.index.duplicated(keep="last")].reindex(dates).ffill()
            if s.notna().sum() >= max(5, len(dates) // 5):
                return s
            continue
        # Wide panel: one column per ticker
        cols = {str(c).upper(): c for c in panel.columns}
        if und not in cols:
            continue
        s = pd.to_numeric(panel[cols[und]], errors="coerce")
        if not isinstance(s.index, pd.DatetimeIndex):
            s.index = pd.to_datetime(s.index)
        s = s.reindex(dates).ffill()
        if s.notna().sum() >= max(5, len(dates) // 5):
            return s

    # Yahoo fallback (handle MultiIndex columns from recent yfinance)
    try:
        import yfinance as yf

        hist = yf.download(
            und,
            start=str(dates.min().date()),
            end=str((dates.max() + pd.Timedelta(days=5)).date()),
            progress=False,
            auto_adjust=True,
        )
        if hist is not None and not hist.empty:
            if isinstance(hist.columns, pd.MultiIndex):
                close = hist["Close"]
                if isinstance(close, pd.DataFrame):
                    close = close.iloc[:, 0]
            else:
                close = hist["Close"] if "Close" in hist.columns else hist.iloc[:, 0]
            close = pd.to_numeric(close, errors="coerce")
            if not isinstance(close.index, pd.DatetimeIndex):
                close.index = pd.to_datetime(close.index)
            return close.reindex(dates).ffill()
    except Exception:
        pass
    return pd.Series(np.nan, index=dates)


def _pair_frame(d: dict) -> pd.DataFrame:
    daily = d.get("daily") or {}
    dates = pd.to_datetime(daily.get("dates") or [])
    if len(dates) == 0:
        return pd.DataFrame()
    idx = pd.DatetimeIndex(dates)
    rebalance = daily.get("rebalance")
    def _col(key: str) -> pd.Series:
        return pd.to_numeric(pd.Series(daily.get(key), index=idx), errors="coerce")

    df = pd.DataFrame(
        {
            "h": _col("h_used"),
            # total_gross in shards is cumulative leg PnL — use gross_exposure for size.
            "gross": _col("gross_exposure"),
            "etf_gross": _col("etf_gross"),
            "und_gross": _col("underlying_gross"),
            "rebalance": (
                pd.Series(rebalance, index=idx).astype(bool)
                if rebalance is not None
                else pd.Series(False, index=idx)
            ),
        },
        index=idx,
    )
    # Unit-capital gross_exposure * portfolio weight * sleeve budget → book USD gross.
    summary = d.get("summary") or {}
    w = float(summary.get("effective_weight") or summary.get("portfolio_weight") or 0.0)
    sleeve = 100_000.0
    bt_path = REPO / "data" / "bucket4_backtest.json"
    if bt_path.is_file():
        try:
            bt = json.loads(bt_path.read_text(encoding="utf-8"))
            sleeve = float(bt.get("sleeve_budget_usd") or (bt.get("sizing_latest") or {}).get("budget_usd") or sleeve)
        except Exception:
            pass
    if w > 0 and df["gross"].notna().any():
        df["proposed_gross_usd"] = df["gross"] * w * sleeve
    else:
        df["proposed_gross_usd"] = df["gross"]
    return df


def plot_pair(etf: str, d: dict, out_dir: Path) -> Path | None:
    und = str(d.get("underlying") or (d.get("summary") or {}).get("underlying") or "").upper()
    df = _pair_frame(d)
    if df.empty or und == "":
        return None
    px = _und_price_series(und, df.index, etf=etf)
    df["und_px"] = px

    fig, axes = plt.subplots(3, 1, figsize=(11, 8.5), sharex=True, gridspec_kw={"hspace": 0.08})
    fig.suptitle(f"{etf}/{und} — B4 sizing stability (production path)", fontsize=12, y=0.98)

    ax = axes[0]
    ax.plot(df.index, df["und_px"], color="#1f4e79", lw=1.2, label=f"{und} close")
    ax.set_ylabel("Underlying price")
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(True, alpha=0.25)

    ax = axes[1]
    ax.plot(df.index, df["h"], color="#2e7d32", lw=1.2, label="hedge ratio h")
    ax.axhline(0.45, color="#2e7d32", ls="--", lw=0.7, alpha=0.5)
    rb = df.index[df["rebalance"].astype(bool)]
    if len(rb):
        for t in rb:
            ax.axvline(t, color="#9e9e9e", lw=0.5, alpha=0.5)
    ax.set_ylabel("Hedge ratio h")
    ax.set_ylim(0.2, 0.9)
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(True, alpha=0.25)

    ax = axes[2]
    ax.plot(df.index, df["proposed_gross_usd"], color="#c62828", lw=1.3, label="proposed gross (USD)")
    if len(rb):
        ax.scatter(rb, df.loc[rb, "proposed_gross_usd"], color="#c62828", s=18, zorder=3, label="rebalance")
    ax.set_ylabel("Proposed gross $")
    ax.set_xlabel("Date")
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(True, alpha=0.25)

    # Stability annotation
    g = df["proposed_gross_usd"].dropna()
    if len(g) >= 5:
        weekly = g.resample("W-FRI").last().dropna()
        if len(weekly) >= 3:
            chg = weekly.pct_change().dropna()
            med_abs = float(chg.abs().median()) if len(chg) else float("nan")
            ax.text(
                0.99,
                0.95,
                f"median |Δgross| week-to-week: {med_abs:.1%}" if np.isfinite(med_abs) else "",
                transform=ax.transAxes,
                ha="right",
                va="top",
                fontsize=8,
                color="#424242",
            )

    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{etf}_{und}_sizing_stability.png"
    fig.savefig(path, dpi=140, bbox_inches="tight")
    plt.close(fig)
    return path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--etfs", default="", help="Comma-separated ETF list (default: production book)")
    ap.add_argument("--out", default=str(OUT_DIR))
    args = ap.parse_args()
    out_dir = Path(args.out)
    etfs = [e.strip().upper() for e in args.etfs.split(",") if e.strip()] or _production_etfs()
    if not etfs:
        print("No production B4 pairs found in", PAIR_DIR, file=sys.stderr)
        return 1

    paths: list[Path] = []
    for etf in etfs:
        d = _load_pair(etf)
        if d is None:
            print(f"[WARN] missing shard {etf}")
            continue
        p = plot_pair(etf, d, out_dir)
        if p is not None:
            print(f"[OK] {p}")
            paths.append(p)

    # Combined PDF
    if paths:
        from matplotlib.backends.backend_pdf import PdfPages

        pdf_path = out_dir / "b4_sizing_stability.pdf"
        with PdfPages(pdf_path) as pdf:
            for p in paths:
                img = plt.imread(p)
                fig = plt.figure(figsize=(11, 8.5))
                ax = fig.add_axes([0, 0, 1, 1])
                ax.imshow(img)
                ax.axis("off")
                pdf.savefig(fig, dpi=140)
                plt.close(fig)
        print(f"[OK] {pdf_path} ({len(paths)} pages)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
