/* global window, module */
(function initRealizedDecay(globalObj) {
  const TRADING_DAYS_PER_YEAR = 252;
  const DEFAULT_HORIZONS = [5, 20, 60, 120, 251];

  const PB = (typeof globalObj !== "undefined" && globalObj.PriceBasis)
    || (typeof window !== "undefined" && window.PriceBasis)
    || null;

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

  function prepareDecayTrRows(rows, splitEvents) {
    if (PB && typeof PB.buildTrSeriesFromMetrics === "function") {
      return PB.buildTrSeriesFromMetrics(rows, splitEvents).map((r) => ({
        date: r.date,
        trEtfPx: r.trEtfPx,
        trUndPx: r.trUndPx,
      }));
    }
    return [];
  }

  function parseSplitEventsFromCorp(corpPayload, ticker) {
    if (PB && PB.parseSplitEventsFromCorp) return PB.parseSplitEventsFromCorp(corpPayload, ticker);
    return [];
  }

  function etfTrPrice(row, splitEvents) {
    if (PB && PB.buildTrSeriesFromMetrics) {
      const tr = PB.buildTrSeriesFromMetrics([row], splitEvents || []);
      return tr.length ? tr[0].trEtfPx : NaN;
    }
    return NaN;
  }

  function undTrPrice(row) {
    return toNum(row && row.underlying_adj_close);
  }

  function buildDailyLogDragSeries(rows, beta) {
    const b = toNum(beta);
    if (!Number.isFinite(b)) return [];
    const clean = (Array.isArray(rows) ? rows : [])
      .map((row) => {
        const date = parseDate(row && row.date);
        const etfPx = toNum(row && row.trEtfPx);
        const undPx = toNum(row && row.trUndPx);
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

  const reexport = PB ? {
    cumSplitFactor: PB.cumSplitFactor,
    nearestSplitRatio: PB.nearestSplitRatio,
    splitCloseJumpRatio: PB.splitCloseJumpRatio,
    filterSplitsNeedingCloseBasisFix: PB.filterSplitsNeedingCloseBasisFix,
    detectSplitBoundary: PB.detectSplitBoundary,
    resolveSplitBoundary: PB.resolveSplitContext,
    decayEtfTrPrice: PB.etfTrPriceForPoint,
  } : {};

  const exported = {
    TRADING_DAYS_PER_YEAR,
    DEFAULT_HORIZONS,
    parseSplitEventsFromCorp,
    prepareDecayTrRows,
    buildDailyLogDragSeries,
    computeHorizonPeriodReturns,
    buildRollingPeriodReturnSeries,
    logToSimplePeriod,
    periodBorrowLog,
    etfTrPrice,
    undTrPrice,
    ...reexport,
  };

  if (typeof module !== "undefined" && module.exports) {
    module.exports = exported;
  }
  if (globalObj) {
    globalObj.RealizedDecay = exported;
  }
})(typeof window !== "undefined" ? window : globalThis);
