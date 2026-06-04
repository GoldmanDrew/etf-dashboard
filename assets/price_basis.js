/**
 * Canonical split-aware total-return price series for ETF metrics rows.
 * Mirrors scripts/price_basis.py + build_data window logic.
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

  /**
   * Resolve split context: discrete scaling only when raw close jumps at the event.
   */
  function resolveSplitContext(closePoints, splitEvents) {
    const events = Array.isArray(splitEvents) ? splitEvents : [];
    if (!events.length || !closePoints.length) {
      return {
        mode: "continuous",
        boundary: null,
        mult: null,
        filtered: [],
      };
    }
    const filtered = filterSplitsNeedingCloseBasisFix(closePoints, events);
    let mult = null;
    let boundary = null;
    if (filtered.length) {
      mult = filtered[0].mult;
      boundary = detectSplitBoundary(closePoints, mult);
      if (!boundary) boundary = filtered[0].date;
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
    const mode = boundary && mult > 0 ? "discrete_split" : "continuous";
    return { mode, boundary, mult, filtered };
  }

  function isPreSplitDate(dateStr, ctx) {
    const ds = parseDate(dateStr);
    const boundary = parseDate(ctx?.boundary);
    const mult = toNum(ctx?.mult);
    return ctx?.mode === "discrete_split" && boundary && mult > 0 && ds && ds < boundary;
  }

  /**
   * ETF TR price on a common post-split (latest) basis when a discrete split applies.
   * When Yahoo adj is present it is authoritative; pre-split adj is mapped with mult.
   */
  function etfTrPriceForPoint(point, ctx) {
    const close = toNum(point.close);
    if (!(close > 0)) return NaN;
    const adj = toNum(point.adj);
    const navTr = toNum(point.navTr);
    const preSplit = isPreSplitDate(point.date, ctx);
    const mult = toNum(ctx?.mult);

    if (preSplit && mult > 0) {
      if (adj > 0) return adj * mult;
      if (navTr > 0 && close > 0 && mult > 1.05) {
        if (navTr / close >= mult * 0.85 || navTr / close > 2.5) return close * mult;
        return navTr * mult;
      }
      if (navTr > 0) return navTr * mult;
      return close * mult;
    }
    if (adj > 0) return adj;
    if (navTr > 0) return navTr;
    return close;
  }

  function trModeForPoint(point, ctx) {
    const close = toNum(point.close);
    const adj = toNum(point.adj);
    const navTr = toNum(point.navTr);
    const preSplit = isPreSplitDate(point.date, ctx);
    const mult = toNum(ctx?.mult);

    if (preSplit && mult > 0) {
      if (adj > 0) return "pre_split_adj_mapped";
      if (navTr > 0 && close > 0 && mult > 1.05
        && (navTr / close >= mult * 0.85 || navTr / close > 2.5)) {
        return "pre_split_close_scaled";
      }
      if (navTr > 0) return "pre_split_nav_tr_scaled";
      return "pre_split_close_scaled";
    }
    if (adj > 0) return ctx?.mode === "discrete_split" ? "post_split_adj" : "continuous_adj";
    if (navTr > 0) return "nav_tr_fallback";
    return "close_fallback";
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
      const trEtfPx = etfTrPriceForPoint(p, ctx);
      const trUndPx = toNum(p.row && p.row.underlying_adj_close);
      return {
        date: p.date,
        trEtfPx,
        trUndPx,
        tradeClose: p.close,
        trMode: trModeForPoint(p, ctx),
        row: p.row,
      };
    }).filter((x) => x.date && x.trEtfPx > 0 && x.trUndPx > 0);
  }

  function maxAbsLogReturn(series, valueKey) {
    let maxJump = 0;
    let at = null;
    for (let i = 1; i < series.length; i += 1) {
      const a = toNum(series[i - 1][valueKey]);
      const b = toNum(series[i][valueKey]);
      if (!(a > 0 && b > 0)) continue;
      const lr = Math.abs(Math.log(b / a));
      if (lr > maxJump) {
        maxJump = lr;
        at = series[i].date;
      }
    }
    return { maxAbsLogReturn: maxJump, maxJumpDate: at };
  }

  /**
   * Coverage summary for Decay / Backtest TR badges.
   */
  function summarizeTrCoverage(rows, splitEvents) {
    const sorted = (Array.isArray(rows) ? rows : [])
      .filter((row) => parseDate(row && row.date));
    const rawInputDays = sorted.length;
    const etfAdjDays = sorted.filter((r) => toNum(r.etf_adj_close) > 0).length;
    const undAdjDays = sorted.filter((r) => toNum(r.underlying_adj_close) > 0).length;
    const bothLegDays = sorted.filter((r) => {
      const pl = toNum(r.close_price) || toNum(r.nav);
      const ps = toNum(r.underlying_adj_close);
      return pl > 0 && ps > 0;
    }).length;

    const closePoints = sorted
      .map(metricsRowToPoint)
      .filter((p) => p.date && p.close > 0)
      .map((p) => ({ date: p.date, close: p.close }));
    const ctx = resolveSplitContext(closePoints, splitEvents || []);
    const trSeries = buildTrSeriesFromMetrics(rows, splitEvents);
    const trJointDays = trSeries.length;
    const droppedDays = Math.max(0, bothLegDays - trJointDays);

    const etfTrModes = {};
    for (const row of trSeries) {
      const m = row.trMode || "unknown";
      etfTrModes[m] = (etfTrModes[m] || 0) + 1;
    }

    const etfJump = maxAbsLogReturn(trSeries, "trEtfPx");
    const undJump = maxAbsLogReturn(trSeries, "trUndPx");

    const warnings = [];
    if (droppedDays > 0) {
      warnings.push(`${droppedDays} day(s) dropped (missing ETF or underlying TR price).`);
    }
    if (etfAdjDays < rawInputDays * 0.85) {
      warnings.push(`ETF Yahoo adj close missing on ${rawInputDays - etfAdjDays} of ${rawInputDays} rows.`);
    }
    if (undAdjDays < rawInputDays * 0.85) {
      warnings.push(`Underlying adj close missing on ${rawInputDays - undAdjDays} of ${rawInputDays} rows.`);
    }
    if (etfJump.maxAbsLogReturn > 0.35) {
      warnings.push(`Large ETF TR daily move (${(Math.expm1(etfJump.maxAbsLogReturn) * 100).toFixed(0)}%) on ${etfJump.maxJumpDate || "?"}.`);
    }

    let quality = "good";
    if (etfJump.maxAbsLogReturn > 0.35 || droppedDays > bothLegDays * 0.1) quality = "degraded";
    if (trJointDays < 20 || etfJump.maxAbsLogReturn > 0.75) quality = "poor";

    const primaryEtfBasis = (() => {
      if (ctx.mode === "discrete_split") {
        const mapped = (etfTrModes.pre_split_adj_mapped || 0) + (etfTrModes.post_split_adj || 0);
        if (mapped >= trJointDays * 0.5) return "yahoo_adj_mapped";
        if ((etfTrModes.pre_split_close_scaled || 0) > 0) return "close_scaled";
        return "split_adjusted";
      }
      if ((etfTrModes.continuous_adj || 0) >= trJointDays * 0.5) return "yahoo_adj";
      if ((etfTrModes.nav_tr_fallback || 0) > 0) return "nav_tr";
      return "mixed";
    })();

    return {
      rawInputDays,
      bothLegDays,
      trJointDays,
      droppedDays,
      etfAdjDays,
      undAdjDays,
      etfAdjCoveragePct: rawInputDays ? etfAdjDays / rawInputDays : 0,
      undAdjCoveragePct: rawInputDays ? undAdjDays / rawInputDays : 0,
      jointCoveragePct: rawInputDays ? trJointDays / rawInputDays : 0,
      splitMode: ctx.mode,
      splitBoundary: ctx.boundary,
      splitMult: ctx.mult,
      etfTrModes,
      primaryEtfBasis,
      undBasis: "yahoo_adj",
      maxEtfDailyLogReturn: etfJump.maxAbsLogReturn,
      maxUndDailyLogReturn: undJump.maxAbsLogReturn,
      quality,
      warnings,
    };
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
    isPreSplitDate,
    etfTrPriceForPoint,
    trModeForPoint,
    buildTrSeriesFromMetrics,
    summarizeTrCoverage,
    metricsRowToPoint,
  };

  if (typeof module !== "undefined" && module.exports) {
    module.exports = exported;
  }
  if (globalObj) {
    globalObj.PriceBasis = exported;
  }
})(typeof window !== "undefined" ? window : globalThis);
