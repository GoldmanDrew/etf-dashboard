"""Bucket 4 hedge-ratio + rebalance-cadence engine."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .bucket4_hedge_v7 import V7_DEFAULT_H_MID, V7_GLOBAL_H_MAX, V7_GLOBAL_H_MIN


def _norm_sym(x: str) -> str:
    return str(x).strip().upper().replace(".", "-")


@dataclass(frozen=True)
class HedgeCadenceKnobs:
    h_mid: float = V7_DEFAULT_H_MID
    k_vcr: float = 1.0
    h_min: float = V7_GLOBAL_H_MIN
    h_max: float = V7_GLOBAL_H_MAX
    alpha: float = 0.25
    k_z: float = 0.0
    cadence_signal_col: str = "tr"
    base_days: float = 10.0
    k_tr: float = 2.25
    m_vcr: float = 2.5
    min_interval: int = 1
    max_interval: int = 21

    @classmethod
    def from_config(cls, block: Mapping[str, Any] | None) -> "HedgeCadenceKnobs":
        b = dict(block or {})
        d = cls()
        return cls(
            h_mid=float(b.get("h_mid", d.h_mid)),
            k_vcr=float(b.get("k_vcr", d.k_vcr)),
            h_min=float(b.get("h_min", d.h_min)),
            h_max=float(b.get("h_max", d.h_max)),
            alpha=float(b.get("alpha", d.alpha)),
            k_z=float(b.get("k_z", d.k_z)),
            cadence_signal_col=str(b.get("cadence_signal_col", d.cadence_signal_col) or d.cadence_signal_col),
            base_days=float(b.get("base_days", d.base_days)),
            k_tr=float(b.get("k_tr", d.k_tr)),
            m_vcr=float(b.get("m_vcr", d.m_vcr)),
            min_interval=int(b.get("min_interval", d.min_interval)),
            max_interval=int(b.get("max_interval", d.max_interval)),
        )


@dataclass(frozen=True)
class NameTilt:
    h_shift: float = 0.0
    h_mult: float = 1.0
    interval_mult: float = 1.0
    note: str = ""

    @classmethod
    def from_config(cls, block: Mapping[str, Any] | None) -> "NameTilt":
        b = dict(block or {})
        return cls(
            h_shift=float(b.get("h_shift", 0.0)),
            h_mult=float(b.get("h_mult", 1.0)),
            interval_mult=float(b.get("interval_mult", 1.0)),
            note=str(b.get("note", "")),
        )

    @property
    def is_identity(self) -> bool:
        return self.h_shift == 0.0 and self.h_mult == 1.0 and self.interval_mult == 1.0


def load_name_tilts(block: Mapping[str, Any] | None) -> dict[str, NameTilt]:
    out: dict[str, NameTilt] = {}
    for sym, spec in dict(block or {}).items():
        out[_norm_sym(sym)] = NameTilt.from_config(spec)
    return out


@dataclass
class PairPolicy:
    etf: str
    underlying: str
    tr: float
    vcr: float
    vcr_med: float
    signal_ok: bool
    h: float
    h_raw: float
    h_prev: float | None
    interval_days: int
    interval_raw: float
    denom: float
    cadence_signal_col: str = "tr"
    cadence_signal: float | None = None
    tilt_note: str = ""
    h_explain: str = ""
    interval_explain: str = ""

    def as_row(self) -> dict[str, Any]:
        return {
            "ETF": self.etf,
            "Underlying": self.underlying,
            "tr": round(self.tr, 4) if np.isfinite(self.tr) else np.nan,
            "cadence_signal_col": self.cadence_signal_col,
            "cadence_signal": (
                round(float(self.cadence_signal), 4)
                if self.cadence_signal is not None and np.isfinite(float(self.cadence_signal))
                else np.nan
            ),
            "vcr": round(self.vcr, 5) if np.isfinite(self.vcr) else np.nan,
            "vcr_med": round(self.vcr_med, 5) if np.isfinite(self.vcr_med) else np.nan,
            "hedge_ratio": round(self.h, 4),
            "hedge_ratio_raw": round(self.h_raw, 4),
            "interval_days": int(self.interval_days),
            "interval_raw": round(self.interval_raw, 3),
            "signal_ok": bool(self.signal_ok),
            "tilt_note": self.tilt_note,
            "h_explain": self.h_explain,
            "interval_explain": self.interval_explain,
        }


def _clip(x: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, x)))


def compute_pair_policy(
    tr: float,
    vcr: float,
    vcr_med: float,
    *,
    knobs: HedgeCadenceKnobs,
    name_tilt: NameTilt | None = None,
    prev_h: float | None = None,
    etf: str = "",
    underlying: str = "",
    xsec_z: float = float("nan"),
    cadence_signal_col: str = "tr",
) -> PairPolicy:
    tilt = name_tilt or NameTilt()
    t = float(tr) if tr is not None and np.isfinite(tr) else np.nan
    v = float(vcr) if vcr is not None and np.isfinite(vcr) else np.nan
    vm = float(vcr_med) if vcr_med is not None and np.isfinite(vcr_med) else np.nan
    signal_ok = np.isfinite(v) and np.isfinite(vm)

    if signal_ok:
        dvcr = v - vm
        h_raw = knobs.h_mid + knobs.k_vcr * dvcr
        h_src = (
            f"h_mid({knobs.h_mid:.3f}) + k_vcr({knobs.k_vcr:.2f})*"
            f"(VCR({v:.5f})-VCR_med({vm:.5f})={dvcr:+.5f}) = {h_raw:.4f}"
        )
    else:
        h_raw = knobs.h_mid
        h_src = f"signal missing -> neutral h_mid({knobs.h_mid:.3f})"

    zx = float(xsec_z) if xsec_z is not None and np.isfinite(xsec_z) else np.nan
    if knobs.k_z != 0.0 and np.isfinite(zx):
        h_raw = h_raw - knobs.k_z * zx
        h_src += f" - k_z({knobs.k_z:.2f})*z_xsec({zx:+.3f}) = {h_raw:.4f}"

    h_tilted = h_raw * tilt.h_mult + tilt.h_shift
    tilt_part = ""
    if not tilt.is_identity:
        tilt_part = f" -> tilt(x{tilt.h_mult:.2f}{tilt.h_shift:+.3f})={h_tilted:.4f}"
    h_clipped = _clip(h_tilted, knobs.h_min, knobs.h_max)
    clip_part = f" -> clip[{knobs.h_min:.2f},{knobs.h_max:.2f}]={h_clipped:.4f}"

    if prev_h is not None and np.isfinite(prev_h) and 0.0 < knobs.alpha < 1.0:
        h_final = (1.0 - knobs.alpha) * float(prev_h) + knobs.alpha * h_clipped
        h_final = _clip(h_final, knobs.h_min, knobs.h_max)
        ema_part = f" -> EMA(a={knobs.alpha:.2f}; prev={float(prev_h):.4f})={h_final:.4f}"
    else:
        h_final = h_clipped
        ema_part = ""

    h_explain = f"h={h_final:.4f}  |  {h_src}{tilt_part}{clip_part}{ema_part}"

    denom = 1.0
    denom_terms = ["1"]
    if np.isfinite(t):
        denom += knobs.k_tr * (t - 1.0)
        sig_label = str(cadence_signal_col or "tr")
        denom_terms.append(
            f"k_tr({knobs.k_tr:.2f})*({sig_label}({t:.3f})-1)={knobs.k_tr*(t-1.0):+.4f}"
        )
    if np.isfinite(v) and np.isfinite(vm):
        denom += knobs.m_vcr * (v - vm)
        denom_terms.append(f"m_vcr({knobs.m_vcr:.2f})*(VCR-VCR_med)={knobs.m_vcr*(v-vm):+.4f}")

    if denom > 1e-9:
        interval_raw = knobs.base_days / denom
    else:
        interval_raw = float(knobs.max_interval)
    interval_tilted = interval_raw * tilt.interval_mult
    interval_days = int(np.clip(round(interval_tilted), knobs.min_interval, knobs.max_interval))

    tilt_cad = ""
    if tilt.interval_mult != 1.0:
        tilt_cad = f" *tilt({tilt.interval_mult:.2f})={interval_tilted:.3f}"
    interval_explain = (
        f"interval={interval_days}d  |  denom=" + " + ".join(denom_terms) + f" = {denom:.4f}"
        f" -> base_days({knobs.base_days:.1f})/denom={interval_raw:.3f}{tilt_cad}"
        f" -> round->clip[{knobs.min_interval},{knobs.max_interval}]={interval_days}"
    )

    return PairPolicy(
        etf=_norm_sym(etf),
        underlying=_norm_sym(underlying),
        tr=t,
        vcr=v,
        vcr_med=vm,
        signal_ok=bool(signal_ok),
        h=float(h_final),
        h_raw=float(h_raw),
        h_prev=(float(prev_h) if prev_h is not None else None),
        interval_days=int(interval_days),
        interval_raw=float(interval_raw),
        denom=float(denom),
        cadence_signal_col=str(cadence_signal_col or "tr"),
        cadence_signal=float(t) if np.isfinite(t) else None,
        tilt_note=tilt.note,
        h_explain=h_explain,
        interval_explain=interval_explain,
    )


def build_h_series(
    signal: pd.DataFrame,
    calendar: pd.DatetimeIndex,
    *,
    knobs: HedgeCadenceKnobs,
    name_tilt: NameTilt | None = None,
) -> pd.Series:
    cal = pd.DatetimeIndex(calendar).sort_values()
    if len(cal) == 0:
        return pd.Series(dtype=float)
    cadence_col = str(getattr(knobs, "cadence_signal_col", "tr") or "tr")
    if signal is not None and cadence_col in signal:
        tr = signal.get(cadence_col)
    else:
        cadence_col = "tr"
        tr = signal.get("tr") if signal is not None else None
    vcr = signal.get("vcr") if signal is not None else None
    vm = signal.get("vcr_med") if signal is not None else None
    zx = signal.get("xsec_z") if signal is not None else None
    out = pd.Series(index=cal, dtype=float)
    prev_h: float | None = None
    for d in cal:
        pol = compute_pair_policy(
            float(tr.get(d, np.nan)) if tr is not None else np.nan,
            float(vcr.get(d, np.nan)) if vcr is not None else np.nan,
            float(vm.get(d, np.nan)) if vm is not None else np.nan,
            knobs=knobs,
            name_tilt=name_tilt,
            prev_h=prev_h,
            xsec_z=float(zx.get(d, np.nan)) if zx is not None else np.nan,
            cadence_signal_col=cadence_col,
        )
        out.loc[d] = pol.h
        prev_h = pol.h
    return out.astype(float)


def build_rebal_dates(
    signal: pd.DataFrame,
    calendar: pd.DatetimeIndex,
    *,
    knobs: HedgeCadenceKnobs,
    name_tilt: NameTilt | None = None,
    warmup_bdays: int = 0,
) -> tuple[pd.DatetimeIndex, pd.DataFrame]:
    cal = pd.DatetimeIndex(calendar).sort_values().unique()
    if warmup_bdays > 0:
        cal = cal[int(warmup_bdays):]
    if len(cal) == 0:
        return pd.DatetimeIndex([]), pd.DataFrame()
    cadence_col = str(getattr(knobs, "cadence_signal_col", "tr") or "tr")
    if signal is not None and cadence_col in signal:
        tr = signal.get(cadence_col)
    else:
        cadence_col = "tr"
        tr = signal.get("tr") if signal is not None else None
    vcr = signal.get("vcr") if signal is not None else None
    vm = signal.get("vcr_med") if signal is not None else None
    dates: list[pd.Timestamp] = []
    diag: list[dict[str, Any]] = []
    i, n = 0, len(cal)
    while i < n:
        d = pd.Timestamp(cal[i])
        dates.append(d)
        pol = compute_pair_policy(
            float(tr.get(d, np.nan)) if tr is not None else np.nan,
            float(vcr.get(d, np.nan)) if vcr is not None else np.nan,
            float(vm.get(d, np.nan)) if vm is not None else np.nan,
            knobs=knobs,
            name_tilt=name_tilt,
            cadence_signal_col=cadence_col,
        )
        diag.append({
            "date": d,
            "cadence_signal_col": cadence_col,
            "cadence_signal": pol.cadence_signal,
            "tr": float(signal.get("tr").get(d, np.nan)) if signal is not None and "tr" in signal else np.nan,
            "tr_est": float(signal.get("tr_est").get(d, np.nan)) if signal is not None and "tr_est" in signal else np.nan,
            "cadence_score": (
                float(signal.get("cadence_score").get(d, np.nan))
                if signal is not None and "cadence_score" in signal
                else np.nan
            ),
            "vcr": pol.vcr,
            "vcr_med": pol.vcr_med,
            "interval_days": pol.interval_days,
            "interval_explain": pol.interval_explain,
        })
        i += max(1, pol.interval_days)
    return pd.DatetimeIndex(dates), pd.DataFrame(diag)


def build_xsec_z_panel(closes: pd.DataFrame) -> pd.DataFrame:
    closes = closes.sort_index().astype(float)
    logret = np.log(closes / closes.shift(1))
    r10 = np.log(closes / closes.shift(10))
    rx = logret.rolling(5, min_periods=5).std(ddof=1) / logret.rolling(63, min_periods=30).std(ddof=1)

    def _robust_z_row(row: pd.Series) -> pd.Series:
        v = pd.to_numeric(row, errors="coerce")
        m = v.median(skipna=True)
        mad = (v - m).abs().median(skipna=True)
        scale = 1.4826 * float(mad) if pd.notna(mad) and mad > 0 else float(v.std(skipna=True) or 1.0)
        return (v - m) / scale if scale > 0 else v * 0.0

    z10 = r10.apply(_robust_z_row, axis=1)
    zrx = rx.apply(_robust_z_row, axis=1)
    return (0.5 * (-z10) + 0.5 * zrx).shift(1)


def load_policy_from_config(cfg: Mapping[str, Any] | None) -> tuple[HedgeCadenceKnobs, dict[str, NameTilt], str]:
    block: Mapping[str, Any] = {}
    try:
        rules = (
            (cfg or {})
            .get("inverse_decay_bucket4", {})
            .get("rules", {})
        )
        block = (
            rules.get("hedge_cadence_policy")
            or (rules.get("bucket4_weekly_opt2", {}) or {}).get("hedge_cadence_policy")
            or {}
        )
    except Exception:
        block = {}
    knobs = HedgeCadenceKnobs.from_config(block)
    tilts = load_name_tilts(block.get("name_tilt"))
    source = str(block.get("source", "tr_vcr")).strip().lower()
    return knobs, tilts, source


__all__ = [
    "HedgeCadenceKnobs",
    "NameTilt",
    "PairPolicy",
    "compute_pair_policy",
    "build_h_series",
    "build_rebal_dates",
    "build_xsec_z_panel",
    "load_policy_from_config",
    "load_name_tilts",
]
