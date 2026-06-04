/* global window, module */
(function initRealizedDecay(globalObj) {
  const TRADING_DAYS_PER_YEAR = 252;
  const DEFAULT_HORIZONS = [5, 20, 60, 120, 251];

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

  function logToSimplePeriod(logRet) {
    const x = toNum(logRet);
    if (!Number.isFinite(x)) return null;
    return Math.expm1(x);
  }

  /** Parse corporate_actions.json events for one ticker → sorted [{date, mult}]. */
  const INTEGER_SPLIT_FACTORS = [2, 3, 4, 5, 6, 10, 15, 20, 25, 50];
  const SPLIT_RATIOS = Array.from(
    new Set(INTEGER_SPLIT_FACTORS.concat(INTEGER_SPLIT_FACTORS.map((f) => 1 / f))),
  ).sort((a, b) => b - a);

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

  /** close_after / close_before at the first bar on/after effective (matches Python). */
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

  /**
   * Keep splits only when raw close jumps at the event (Yahoo already continuous → skip).
   * Mirrors scripts/split_adjustments.filter_splits_needing_close_basis_fix.
   */
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

  /**
   * First bar date where raw close jumps by a whitelisted split ratio (actual ex-bar).
   */
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

  /** Map price at fromDate onto toDate raw-close basis (same as Python cum_split_factor). */
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
   * ETF total-return price on the series latest-date basis.
   * When verified splits exist: NAV TR (distribution reinvestment) or raw close, scaled —
   * do not use stale pre-split etf_adj_close on a post-split end basis.
   * When Yahoo is continuous through the split: adj close (splits + divs), else NAV TR, else close.
   */
  function resolveSplitBoundary(closePoints, splitEvents) {
    const events = Array.isArray(splitEvents) ? splitEvents : [];
    if (!events.length || !closePoints.length) {
      return { boundary: null, mult: null, filtered: [] };
    }
    for (const ev of events) {
      const mult = toNum(ev.mult);
      if (!(mult > 0)) continue;
      const boundary = detectSplitBoundary(closePoints, mult);
      if (boundary) return { boundary, mult, filtered: [{ date: parseDate(ev.date), mult }] };
    }
    const filtered = filterSplitsNeedingCloseBasisFix(closePoints, events);
    if (filtered.length) {
      const boundary = detectSplitBoundary(closePoints, filtered[0].mult);
      if (boundary) return { boundary, mult: filtered[0].mult, filtered };
    }
    return { boundary: null, mult: null, filtered: [] };
  }

  function decayEtfTrPrice(point, activeSplitMult, splitBoundary) {
    const ds = parseDate(point && point.date);
    const close = toNum(point && point.close);
    const adj = toNum(point && point.adj);
    const navTr = toNum(point && point.navTr);
    if (!ds || !(close > 0)) return NaN;
    const boundary = parseDate(splitBoundary);
    const mult = toNum(activeSplitMult);
    if (boundary && mult > 0 && ds < boundary) {
      const pxBase = navTr > 0 ? navTr : close;
      return pxBase * mult;
    }
    if (adj > 0) return adj;
    if (navTr > 0) return navTr;
    return close;
  }

  /** @deprecated prefer prepareDecayTrRows — kept for unit tests */
  function etfTrPrice(row, splitEvents, latestDate) {
    const close = toNum(row && row.close_price) || toNum(row && row.nav);
    const point = {
      date: parseDate(row && row.date),
      close,
      adj: toNum(row && row.etf_adj_close),
      navTr: toNum(row && row.nav_total_return),
    };
    const points = close > 0 && point.date ? [{ date: point.date, close }] : [];
    const { boundary, mult } = resolveSplitBoundary(points, splitEvents || []);
    if (boundary && point.date < boundary) {
      return decayEtfTrPrice(point, mult, boundary);
    }
    if (points.length === 1 && Array.isArray(splitEvents) && splitEvents.length) {
      let scale = 1;
      for (const ev of splitEvents) {
        if (point.date < parseDate(ev.date)) scale *= toNum(ev.mult) || 1;
      }
      if (scale !== 1) return decayEtfTrPrice({ ...point, navTr: 0, adj: 0 }, scale, parseDate(splitEvents[0].date));
    }
    return decayEtfTrPrice(point, null, null);
  }

  function undTrPrice(row) {
    return toNum(row && row.underlying_adj_close);
  }

  /**
   * Normalize metrics rows to TR prices on a common latest-date basis for decay math.
   */
  function prepareDecayTrRows(rows, splitEvents) {
    const sorted = (Array.isArray(rows) ? rows : [])
      .filter((row) => parseDate(row && row.date))
      .sort((a, b) => parseDate(a.date).localeCompare(parseDate(b.date)));
    if (!sorted.length) return [];
    const points = sorted
      .map((row) => ({
        date: parseDate(row.date),
        close: toNum(row.close_price) || toNum(row.nav),
        adj: toNum(row.etf_adj_close),
        navTr: toNum(row.nav_total_return),
        row,
      }))
      .filter((p) => p.date && p.close > 0);
    if (!points.length) return [];
    const closePoints = points.map((p) => ({ date: p.date, close: p.close }));
    const { boundary: splitBoundary, mult: activeSplitMult } = resolveSplitBoundary(
      closePoints,
      splitEvents || [],
    );
    return points
      .map((p) => ({
        date: p.date,
        trEtfPx: decayEtfTrPrice(p, activeSplitMult, splitBoundary),
        trUndPx: undTrPrice(p.row),
      }))
      .filter((x) => x.date && x.trEtfPx > 0 && x.trUndPx > 0);
  }

  /**
   * Daily log hedge drag on total-return prices: d_t = β·r_U,t − r_L,t.
   */
  function buildDailyLogDragSeries(rows, beta) {
    const b = toNum(beta);
    if (!Number.isFinite(b)) return [];
    const clean = (Array.isArray(rows) ? rows : [])
      .map((row) => {
        const date = parseDate(row && row.date);
        const etfPx = toNum(row && row.trEtfPx) > 0
          ? toNum(row.trEtfPx)
          : etfTrPrice(row, [], null);
        const undPx = toNum(row && row.trUndPx) > 0
          ? toNum(row.trUndPx)
          : undTrPrice(row);
        return { date, etfPx, undPx };
      })
      .filter((x) => x.date && x.etfPx > 0 && x.undPx > 0)
      .sort((a, b2) => a.date.localeCompare(b2.date));
    if (clean.length < 2) return [];

    const out = [];
    for (let i = 1; i < clean.length; i += 1) {
      const rU = Math.log(clean[i].undPx / clean[i - 1].undPx);
      const rL = Math.log(clean[i].etfPx / clean[i - 1].etfPx);
      if (!Number.isFinite(rU) || !Number.isFinite(rL)) continue;
      out.push({
        date: clean[i].date,
        drag: b * rU - rL,
        rU,
        rL,
        etfPx: clean[i].etfPx,
        undPx: clean[i].undPx,
        etfPxPrev: clean[i - 1].etfPx,
        undPxPrev: clean[i - 1].undPx,
      });
    }
    return out;
  }

  function periodBorrowLog(borrowAnnual, obsDays) {
    const b = toNum(borrowAnnual);
    const n = Math.max(0, Math.floor(toNum(obsDays) || 0));
    if (!Number.isFinite(b) || n <= 0) return 0;
    return b * (n / TRADING_DAYS_PER_YEAR);
  }

  function slicePeriodMetrics(drags, dailySeries, startIdx, endIdx, borrowAnnual) {
    const slice = drags.slice(startIdx, endIdx + 1);
    const obs = slice.length;
    if (!obs) {
      return {
        grossLog: null,
        grossSimple: null,
        netLog: null,
        netSimple: null,
        borrowLog: null,
        obs: 0,
        etfStartPx: null,
        etfEndPx: null,
        undStartPx: null,
        undEndPx: null,
      };
    }
    const grossLog = slice.reduce((a, x) => a + x, 0);
    const borrowLog = periodBorrowLog(borrowAnnual, obs);
    const netLog = grossLog - borrowLog;
    const startRow = dailySeries[startIdx] || {};
    const endRow = dailySeries[endIdx] || {};
    return {
      grossLog,
      grossSimple: logToSimplePeriod(grossLog),
      netLog,
      netSimple: logToSimplePeriod(netLog),
      borrowLog,
      obs,
      startDate: startRow.date ? String(startRow.date) : null,
      endDate: endRow.date ? String(endRow.date) : null,
      etfStartPx: toNum(startRow.etfPxPrev),
      etfEndPx: toNum(endRow.etfPx),
      undStartPx: toNum(startRow.undPxPrev),
      undEndPx: toNum(endRow.undPx),
    };
  }

  function computeHorizonPeriodReturns(dailySeries, horizons, borrowAnnual) {
    const hs = Array.isArray(horizons) && horizons.length ? horizons : DEFAULT_HORIZONS;
    const series = Array.isArray(dailySeries) ? dailySeries : [];
    const drags = series.map((x) => toNum(x && x.drag)).filter(Number.isFinite);
    const n = drags.length;
    const endDate = n ? String(series[n - 1].date || "") : null;
    const rows = hs.map((hRaw) => {
      const h = Math.max(1, Math.floor(toNum(hRaw) || 0));
      const startIdx = Math.max(0, n - h);
      const endIdx = n - 1;
      const m = slicePeriodMetrics(drags, series, startIdx, endIdx, borrowAnnual);
      return {
        horizonDays: h,
        ...m,
        sufficient: m.obs >= h,
      };
    });
    return { horizons: rows, nDays: n, endDate, borrowAnnual: toNum(borrowAnnual) };
  }

  function buildRollingPeriodReturnSeries(dailySeries, windowDays, borrowAnnual) {
    const w = Math.max(1, Math.floor(toNum(windowDays) || 60));
    const series = Array.isArray(dailySeries) ? dailySeries : [];
    const drags = series.map((x) => toNum(x && x.drag));
    const out = [];
    for (let endIdx = w - 1; endIdx < series.length; endIdx += 1) {
      const startIdx = endIdx - w + 1;
      const dragSlice = drags.slice(startIdx, endIdx + 1).filter(Number.isFinite);
      if (dragSlice.length < w) continue;
      const grossLog = dragSlice.reduce((a, x) => a + x, 0);
      const borrowLog = periodBorrowLog(borrowAnnual, w);
      const netLog = grossLog - borrowLog;
      out.push({
        date: String(series[endIdx].date || ""),
        gross_period: logToSimplePeriod(grossLog),
        net_period: logToSimplePeriod(netLog),
        gross_log: grossLog,
        net_log: netLog,
        windowDays: w,
      });
    }
    return out;
  }

  const exported = {
    TRADING_DAYS_PER_YEAR,
    DEFAULT_HORIZONS,
    parseSplitEventsFromCorp,
    cumSplitFactor,
    nearestSplitRatio,
    splitCloseJumpRatio,
    filterSplitsNeedingCloseBasisFix,
    detectSplitBoundary,
    resolveSplitBoundary,
    prepareDecayTrRows,
    buildDailyLogDragSeries,
    computeHorizonPeriodReturns,
    buildRollingPeriodReturnSeries,
    logToSimplePeriod,
    periodBorrowLog,
    decayEtfTrPrice,
    etfTrPrice,
    undTrPrice,
  };

  if (typeof module !== "undefined" && module.exports) {
    module.exports = exported;
  }
  if (globalObj) {
    globalObj.RealizedDecay = exported;
  }
})(typeof window !== "undefined" ? window : globalThis);
