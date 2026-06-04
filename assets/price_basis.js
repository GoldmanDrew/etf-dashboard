/**
 * Canonical split-aware total-return price series for ETF metrics rows.
 * Mirrors scripts/split_adjustments.py + build_data window logic.
 */
(function initPriceBasis(globalObj) {
  const INTEGER_SPLIT_FACTORS = [2, 3, 4, 5, 6, 10, 15, 20, 25, 50];
  const SPLIT_RATIOS = Array.from(
    new Set(INTEGER_SPLIT_FACTORS.concat(INTEGER_SPLIT_FACTORS.map((f) => 1 / f))),
  ).sort((a, b) => b - a);

  function toNum(v) {
    if (typeof v === "number") return Number.isFinite(v) ? v : NaN;
    if (typeof v === "string") {
      const s = v.trim();
      if (!s) return NaN;
      const n = Number(s);
      return Number.isFinite(n) ? n : NaN;
    }
    return NaN;
  }

  function parseDate(s) {
    const d = String(s || "").trim().slice(0, 10);
    return d.length === 10 ? d : "";
  }

  function nearestSplitRatio(observed, relTol = 0.075) {
    const x = toNum(observed);
    if (!Number.isFinite(x) || x <= 0) return null;
    let bestR = null;
    let bestErr = 1e9;
    for (const r of SPLIT_RATIOS) {
      const err = Math.abs(x / r - 1);
      if (err < bestErr) {
        bestErr = err;
        bestR = r;
      }
    }
    if (bestR == null || bestErr > relTol) return null;
    return bestR;
  }

  function splitCloseJumpRatio(points, effectiveDate) {
    const eff = parseDate(effectiveDate);
    if (!eff || !Array.isArray(points) || !points.length) return null;
    const before = points.filter((p) => parseDate(p.date) < eff);
    const onAfter = points.filter((p) => parseDate(p.date) >= eff);
    if (!before.length || !onAfter.length) return null;
    const cBefore = toNum(before[before.length - 1].close);
    const cAfter = toNum(onAfter[0].close);
    if (!(cBefore > 0 && cAfter > 0)) return null;
    return cAfter / cBefore;
  }

  function filterSplitsNeedingCloseBasisFix(points, events, relTol = 0.15) {
    if (!Array.isArray(events) || !events.length) return [];
    const out = [];
    for (const ev of events) {
      const jump = splitCloseJumpRatio(points, ev.date);
      if (jump == null) continue;
      const expected = nearestSplitRatio(jump, relTol);
      const mult = toNum(ev.mult);
      if (expected != null && Number.isFinite(mult)
        && Math.abs(expected - mult) <= Math.max(1e-6, relTol * Math.abs(mult))) {
        out.push({ date: parseDate(ev.date), mult });
      }
    }
    return out;
  }

  function detectSplitBoundary(points, splitMult, relTol = 0.15) {
    const target = toNum(splitMult);
    if (!Number.isFinite(target) || !Array.isArray(points) || points.length < 2) return null;
    for (let i = 1; i < points.length; i += 1) {
      const prev = toNum(points[i - 1].close);
      const cur = toNum(points[i].close);
      if (!(prev > 0 && cur > 0)) continue;
      const expected = nearestSplitRatio(cur / prev, relTol);
      if (expected != null && Math.abs(expected - target) <= Math.max(1e-6, relTol * Math.abs(target))) {
        return parseDate(points[i].date);
      }
    }
    return null;
  }

  function parseSplitEventsFromCorp(corpPayload, ticker) {
    const sym = String(ticker || "").trim().toUpperCase();
    if (!sym || !corpPayload) return [];
    const events = [];
    for (const ev of corpPayload.events || []) {
      const t = String(ev?.ticker || "").trim().toUpperCase();
      if (t !== sym) continue;
      const typ = String(ev?.type || "");
      if (typ !== "reverse_split" && typ !== "forward_split") continue;
      const ed = parseDate(ev?.execution_date);
      const rf = toNum(ev?.ratio_from);
      const rt = toNum(ev?.ratio_to);
      if (!ed || !Number.isFinite(rf) || !Number.isFinite(rt) || rt === 0) continue;
      const mult = rf / rt;
      if (mult > 0) events.push({ date: ed, mult });
    }
    return events.sort((a, b) => a.date.localeCompare(b.date));
  }

  function cumSplitFactor(fromDate, toDate, events) {
    const from = parseDate(fromDate);
    const to = parseDate(toDate);
    if (!from || !to || from === to || !events?.length) return 1;
    let mult = 1;
    if (from < to) {
      for (const ev of events) {
        if (from < ev.date && ev.date <= to) mult *= ev.mult;
      }
    } else {
      for (const ev of events) {
        if (to < ev.date && ev.date <= from) mult /= ev.mult;
      }
    }
    return Number.isFinite(mult) && mult > 0 ? mult : 1;
  }

  function medianPositive(vals) {
    const clean = (vals || []).map(toNum).filter((x) => x > 0).sort((a, b) => a - b);
    if (!clean.length) return NaN;
    const mid = Math.floor(clean.length / 2);
    return clean.length % 2 ? clean[mid] : 0.5 * (clean[mid - 1] + clean[mid]);
  }

  /**
   * Resolve split context: only activate discrete scaling when close actually jumps.
   */
  function resolveSplitContext(closePoints, splitEvents) {
    const events = Array.isArray(splitEvents) ? splitEvents : [];
    if (!events.length || !closePoints.length) {
      return {
        mode: "continuous",
        boundary: null,
        mult: null,
        filtered: [],
        postRefClose: medianPositive(closePoints.map((p) => p.close)),
      };
    }
    const filtered = filterSplitsNeedingCloseBasisFix(closePoints, events);
    let mult = null;
    let boundary = null;
    if (filtered.length) {
      mult = filtered[0].mult;
      boundary = detectSplitBoundary(closePoints, mult);
    }
    if (!boundary) {
      for (const ev of events) {
        const m = toNum(ev.mult);
        if (!(m > 0)) continue;
        const b = detectSplitBoundary(closePoints, m);
        if (b) {
          boundary = b;
          mult = m;
          break;
        }
      }
    }
    const postRefClose = boundary
      ? medianPositive(closePoints.filter((p) => parseDate(p.date) >= boundary).map((p) => p.close))
      : medianPositive(closePoints.map((p) => p.close));
    const mode = boundary && mult > 0 ? "discrete_split" : "continuous";
    return { mode, boundary, mult, filtered, postRefClose };
  }

  /**
   * Whether pre-boundary row needs mechanical scaling (guards double-scale).
   * Returns 'scale_close' | 'scale_nav_tr' | false
   */
  function preSplitScaleMode(point, ctx) {
    const { boundary, mult, postRefClose } = ctx;
    if (!boundary || !(mult > 0)) return false;
    const ds = parseDate(point.date);
    if (!ds || ds >= boundary) return false;
    const close = toNum(point.close);
    const adj = toNum(point.adj);
    const navTr = toNum(point.navTr);
    const ref = toNum(postRefClose);
    const isReverse = mult > 1.05;
    const isForward = mult < 0.95;
    if (ref > 0 && isReverse) {
      if (close >= ref * 0.55) return false;
      if (adj > 0 && adj >= ref * 0.55) return false;
    } else if (ref > 0 && isForward) {
      if (close <= ref * 1.25) return false;
      if (adj > 0 && adj <= ref * 1.25) return false;
    }
    if (close > 0 && navTr > 0) {
      const ratio = navTr / close;
      if (ratio >= mult * 0.85 && isReverse) {
        if (ref > 0 && close < ref * 0.55) return "scale_close";
        return false;
      }
      if (ratio > 2.5 && isReverse) return "scale_close";
    }
    if (adj > 0 && close > 0 && isReverse && adj / close > mult * 0.85) return false;
    if (ref > 0 && isForward && close > ref * 1.25) return "scale_close";
    if (ref > 0 && isReverse && close < ref * 0.55) return navTr > 0 ? "scale_nav_tr" : "scale_close";
    return navTr > 0 ? "scale_nav_tr" : "scale_close";
  }

  function etfTrPriceForPoint(point, ctx) {
    const close = toNum(point.close);
    if (!(close > 0)) return NaN;
    const adj = toNum(point.adj);
    const navTr = toNum(point.navTr);
    const scaleMode = preSplitScaleMode(point, ctx);
    if (scaleMode === "scale_close") return close * ctx.mult;
    if (scaleMode === "scale_nav_tr") return navTr * ctx.mult;
    if (adj > 0) return adj;
    if (navTr > 0) return navTr;
    return close;
  }

  function metricsRowToPoint(row) {
    return {
      date: parseDate(row && row.date),
      close: toNum(row && row.close_price) || toNum(row && row.nav),
      adj: toNum(row && row.etf_adj_close),
      navTr: toNum(row && row.nav_total_return),
      row,
    };
  }

  /**
   * Build split-aware TR prices for decay, backtest, and charts.
   * @returns {Array<{date, trEtfPx, trUndPx, tradeClose, trMode, row}>}
   */
  function buildTrSeriesFromMetrics(rows, splitEvents) {
    const sorted = (Array.isArray(rows) ? rows : [])
      .filter((row) => parseDate(row && row.date))
      .sort((a, b) => parseDate(a.date).localeCompare(parseDate(b.date)));
    const points = sorted.map(metricsRowToPoint).filter((p) => p.date && p.close > 0);
    if (!points.length) return [];
    const closePoints = points.map((p) => ({ date: p.date, close: p.close }));
    const ctx = resolveSplitContext(closePoints, splitEvents || []);
    return points.map((p) => {
      const ds = p.date;
      const scaleMode = preSplitScaleMode(p, ctx);
      let trMode = "continuous_adj";
      if (ctx.mode === "discrete_split") {
        if (ds >= ctx.boundary) trMode = "post_split";
        else if (scaleMode === "scale_nav_tr") trMode = "pre_split_nav_tr_scaled";
        else if (scaleMode === "scale_close") trMode = "pre_split_close_scaled";
        else trMode = "pre_split_unscaled";
      }
      const trEtfPx = etfTrPriceForPoint(p, ctx);
      const trUndPx = toNum(p.row && p.row.underlying_adj_close);
      return {
        date: ds,
        trEtfPx,
        trUndPx,
        tradeClose: p.close,
        trMode,
        row: p.row,
      };
    }).filter((x) => x.date && x.trEtfPx > 0 && x.trUndPx > 0);
  }

  const exported = {
    SPLIT_RATIOS,
    nearestSplitRatio,
    splitCloseJumpRatio,
    filterSplitsNeedingCloseBasisFix,
    detectSplitBoundary,
    parseSplitEventsFromCorp,
    cumSplitFactor,
    resolveSplitContext,
    preSplitScaleMode,
    etfTrPriceForPoint,
    buildTrSeriesFromMetrics,
    metricsRowToPoint,
  };

  if (typeof module !== "undefined" && module.exports) {
    module.exports = exported;
  }
  if (globalObj) {
    globalObj.PriceBasis = exported;
  }
})(typeof window !== "undefined" ? window : globalThis);
