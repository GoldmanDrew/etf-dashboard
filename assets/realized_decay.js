/* global window, module */
(function initRealizedDecay(globalObj) {
  const TRADING_DAYS_PER_YEAR = 252;
  // Borrow fees accrue Act/360 on collateral market value. Confirmed conventions:
  //   IBKR:        daily Borrow Fee = (Collateral Value x Fee Rate) / 360, accrued every
  //                calendar day (Collateral = 102% of prior settle, rounded up x shares).
  //   Clear Street: securities-lending stock loan, annualized rate x market value, daily
  //                accrual on the standard /360 day-count, billed monthly.
  // We count realized drag in trading days (252/yr ~ 365 calendar days held), so converting
  // the quoted annual fee to a held-period drag needs the Act/360 surcharge 365/360 ~ 1.0139.
  const BORROW_ACT360_FACTOR = 365 / 360;
  const DEFAULT_HORIZONS = [5, 20, 60, 120, 251];
  const MAX_CONTIGUOUS_METRICS_GAP_DAYS = 45;
  const HARD_LIFECYCLE_GAP_DAYS = 365;
  const ORPHAN_LEG_LOG_THRESHOLD = 0.35;
  const ORPHAN_LEG_COMPANION_MAX = 0.15;

  function isOrphanLegJump(rU, rL) {
    if (!Number.isFinite(rU) || !Number.isFinite(rL)) return false;
    if (Math.abs(rU) > ORPHAN_LEG_LOG_THRESHOLD && Math.abs(rL) < ORPHAN_LEG_COMPANION_MAX) return true;
    if (Math.abs(rL) > ORPHAN_LEG_LOG_THRESHOLD && Math.abs(rU) < ORPHAN_LEG_COMPANION_MAX) return true;
    return false;
  }

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

  function dateMs(ds) {
    const d = parseDate(ds);
    if (!d) return NaN;
    const t = Date.parse(`${d}T00:00:00Z`);
    return Number.isFinite(t) ? t : NaN;
  }

  function dateGapDays(a, b) {
    const ta = dateMs(a);
    const tb = dateMs(b);
    if (!Number.isFinite(ta) || !Number.isFinite(tb)) return NaN;
    return Math.round((tb - ta) / 86400000);
  }

  function logToSimplePeriod(logRet) {
    const x = toNum(logRet);
    if (!Number.isFinite(x)) return null;
    return Math.expm1(x);
  }

  function hasUsableMetricPrices(row) {
    const date = parseDate(row && row.date);
    const closeLike = toNum(row && row.close_price) || toNum(row && row.nav);
    const und = toNum(row && row.underlying_adj_close);
    const sourceUrl = String((row && row.source_url) || "");
    const sourceProvider = String((row && row.source_provider) || "").toLowerCase();
    const staleKind = String((row && row.stale_kind) || "").toLowerCase();
    if (!date || !(closeLike > 0) || !(und > 0)) return false;
    if (
      sourceUrl.startsWith("carry_forward://")
      || sourceProvider.startsWith("carry_forward")
      || staleKind === "carry_forward"
    ) return false;
    return true;
  }

  function latestContiguousRows(rows, maxGapDays = MAX_CONTIGUOUS_METRICS_GAP_DAYS) {
    const maxGap = Math.max(1, Math.floor(toNum(maxGapDays) || MAX_CONTIGUOUS_METRICS_GAP_DAYS));
    const dated = (Array.isArray(rows) ? rows : [])
      .map((row) => ({ row, date: parseDate(row && row.date) }))
      .filter((x) => x.date)
      .sort((a, b) => xDateCmp(a.date, b.date));
    if (dated.length < 2) return dated.map((x) => x.row);
    let startIdx = 0;
    for (let i = 1; i < dated.length; i += 1) {
      const gap = dateGapDays(dated[i - 1].date, dated[i].date);
      const sourceKey = (x) => [
        x && x.source_provider,
        x && x.source_url,
        x && x.status,
      ].map((v) => String(v || "").trim().toLowerCase()).join("|");
      const prevSrc = sourceKey(dated[i - 1].row);
      const curSrc = sourceKey(dated[i].row);
      const sourceChanged = Boolean(prevSrc || curSrc) && prevSrc !== curSrc;
      if (
        Number.isFinite(gap)
        && (gap > HARD_LIFECYCLE_GAP_DAYS || (gap > maxGap && sourceChanged))
      ) startIdx = i;
    }
    return dated.slice(startIdx).map((x) => x.row);
  }

  function xDateCmp(a, b) {
    return String(a || "").localeCompare(String(b || ""));
  }

  function prepareDecayTrRows(rows, splitEvents) {
    if (PB && typeof PB.buildTrSeriesFromMetrics === "function") {
      const usable = latestContiguousRows(
        (Array.isArray(rows) ? rows : []).filter(hasUsableMetricPrices),
      );
      return PB.buildTrSeriesFromMetrics(usable, splitEvents).map((r) => ({
        date: r.date,
        trEtfPx: r.trEtfPx,
        trUndPx: r.trUndPx,
        trMode: r.trMode,
      }));
    }
    return [];
  }

  function summarizeTrCoverage(rows, splitEvents) {
    if (PB && typeof PB.summarizeTrCoverage === "function") {
      const usable = latestContiguousRows(
        (Array.isArray(rows) ? rows : []).filter(hasUsableMetricPrices),
      );
      return PB.summarizeTrCoverage(usable, splitEvents);
    }
    return null;
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
      const gap = dateGapDays(clean[i - 1].date, clean[i].date);
      if (Number.isFinite(gap) && gap > HARD_LIFECYCLE_GAP_DAYS) continue;
      const rU = Math.log(clean[i].undPx / clean[i - 1].undPx);
      const rL = Math.log(clean[i].etfPx / clean[i - 1].etfPx);
      if (!Number.isFinite(rU) || !Number.isFinite(rL)) continue;
      if (isOrphanLegJump(rU, rL)) continue;
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
    // Act/360 borrow drag for a held window of n trading days (IBKR / Clear Street).
    return b * (n / TRADING_DAYS_PER_YEAR) * BORROW_ACT360_FACTOR;
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
    BORROW_ACT360_FACTOR,
    DEFAULT_HORIZONS,
    MAX_CONTIGUOUS_METRICS_GAP_DAYS,
    HARD_LIFECYCLE_GAP_DAYS,
    parseSplitEventsFromCorp,
    prepareDecayTrRows,
    latestContiguousRows,
    hasUsableMetricPrices,
    summarizeTrCoverage,
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
