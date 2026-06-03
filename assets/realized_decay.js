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

  function logToSimplePeriod(logRet) {
    const x = toNum(logRet);
    if (!Number.isFinite(x)) return null;
    return Math.expm1(x);
  }

  /** ETF leg for log-drag: total-return NAV when present, else close/NAV. */
  function etfPriceForDrag(row) {
    const tr = toNum(row && row.nav_total_return);
    if (tr > 0) return tr;
    const adj = toNum(row && row.etf_adj_close);
    if (adj > 0) return adj;
    const nav = toNum(row && row.nav);
    if (nav > 0) return nav;
    const close = toNum(row && row.close_price);
    return close > 0 ? close : NaN;
  }

  /**
   * Daily log hedge drag: d_t = β·r_U,t − r_L,t.
   * @param {object[]} rows — sorted ascending by date; needs ETF px + underlying_adj_close
   * @param {number} beta — hedge ratio (record.delta)
   */
  function buildDailyLogDragSeries(rows, beta) {
    const b = toNum(beta);
    if (!Number.isFinite(b)) return [];
    const clean = (Array.isArray(rows) ? rows : [])
      .map((row) => ({
        date: String((row && row.date) || "").trim(),
        etfPx: etfPriceForDrag(row),
        undPx: toNum(row && row.underlying_adj_close),
      }))
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

  /**
   * Trailing period gross / net pair return for fixed horizons ending at last obs.
   * Gross = Σ d_t over window (≡ β·log(U_end/U_start) − log(L_end/L_start) with fixed β).
   * Net = grossLog − borrow_annual × (N/252).
   */
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

  /** Rolling trailing period gross / net at each end date for one lookback window. */
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
    buildDailyLogDragSeries,
    computeHorizonPeriodReturns,
    buildRollingPeriodReturnSeries,
    logToSimplePeriod,
    periodBorrowLog,
  };

  if (typeof module !== "undefined" && module.exports) {
    module.exports = exported;
  }
  if (globalObj) {
    globalObj.RealizedDecay = exported;
  }
})(typeof window !== "undefined" ? window : globalThis);
