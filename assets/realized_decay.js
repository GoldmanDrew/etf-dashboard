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
   * ETF total-return price: Yahoo adj close (splits + divs), else NAV TR, else split-scaled close.
   */
  function etfTrPrice(row, splitEvents, latestDate) {
    const adj = toNum(row && row.etf_adj_close);
    if (adj > 0) return adj;
    const tr = toNum(row && row.nav_total_return);
    if (tr > 0) return tr;
    const raw = toNum(row && row.close_price) || toNum(row && row.nav);
    if (!(raw > 0)) return NaN;
    const ds = parseDate(row && row.date);
    const latest = parseDate(latestDate);
    if (!ds || !latest) return raw;
    return raw * cumSplitFactor(ds, latest, splitEvents);
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
    const latestDate = parseDate(sorted[sorted.length - 1].date);
    const events = Array.isArray(splitEvents) ? splitEvents : [];
    return sorted
      .map((row) => ({
        date: parseDate(row.date),
        trEtfPx: etfTrPrice(row, events, latestDate),
        trUndPx: undTrPrice(row),
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
    prepareDecayTrRows,
    buildDailyLogDragSeries,
    computeHorizonPeriodReturns,
    buildRollingPeriodReturnSeries,
    logToSimplePeriod,
    periodBorrowLog,
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
