/* global window, module */
/**
 * Realized pair decay contract (keep in sync with scripts/realized_gross_decay.py):
 *
 *   daily_drag_t = β · log(U_t/U_{t-1}) − log(L_t/L_{t-1})   on split-aware TR
 *   period_gross_log = Σ daily_drag over usable days in the window
 *     (= endpoint β·log(U_end/U_start) − log(L_end/L_start) when no interior skips)
 *   period_gross_simple = expm1(period_gross_log)   // short-favorable +
 *   period_net_log = period_gross_log − borrow × (N/252) × (365/360)
 *
 * Calendar gaps > MAX_PAIR_DRAG_GAP_DAYS do not form a drag day (carry-forward stitches).
 * Large |drag| with near-perfect simple leverage tracking is flagged convexity_day
 * (log measure ≠ 0 under perfect −2× on huge moves) — never silently zeroed.
 */
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
  // Skip pair-drag across holes larger than a long weekend. Carry-forward rows are
  // already dropped upstream; without this, May29→Jun16-style stitches count as one "day".
  const MAX_PAIR_DRAG_GAP_DAYS = 5;
  const ORPHAN_LEG_LOG_THRESHOLD = 0.35;
  const ORPHAN_LEG_COMPANION_MAX = 0.15;
  // |log-drag| above this with near-perfect simple tracking ⇒ convexity_day flag.
  const CONVEXITY_DRAG_LOG_THRESHOLD = 0.35;
  const CONVEXITY_SIMPLE_TRACK_EPS = 0.02;
  // Canonical period engine: log-drag (endpoint identity). Not simple log1p.
  const PAIR_DRAG_BASIS = "beta_log_minus_etf_log";

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
      // Lifecycle / ticker-reuse: cut on huge gaps always, or mid-size gaps when the
      // feed source flips. Shorter post-carry-forward stitches are skipped in
      // buildDailyLogDragSeries (MAX_PAIR_DRAG_GAP_DAYS) without discarding history.
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

  function prepareDecayTrRows(rows, etfSplitEvents, undSplitEvents) {
    if (PB && typeof PB.buildTrSeriesFromMetrics === "function") {
      const usable = latestContiguousRows(
        (Array.isArray(rows) ? rows : []).filter(hasUsableMetricPrices),
      );
      const undEv = Array.isArray(undSplitEvents) ? undSplitEvents : [];
      return PB.buildTrSeriesFromMetrics(usable, etfSplitEvents, undEv).map((r) => ({
        date: r.date,
        trEtfPx: r.trEtfPx,
        trUndPx: r.trUndPx,
        trMode: r.trMode,
      }));
    }
    return [];
  }

  function summarizeTrCoverage(rows, etfSplitEvents, undSplitEvents) {
    if (PB && typeof PB.summarizeTrCoverage === "function") {
      const usable = latestContiguousRows(
        (Array.isArray(rows) ? rows : []).filter(hasUsableMetricPrices),
      );
      const undEv = Array.isArray(undSplitEvents) ? undSplitEvents : [];
      return PB.summarizeTrCoverage(usable, etfSplitEvents, undEv);
    }
    return null;
  }

  function parseSplitEventsFromCorp(corpPayload, ticker) {
    if (PB && PB.parseSplitEventsFromCorp) return PB.parseSplitEventsFromCorp(corpPayload, ticker);
    return [];
  }

  function parseDecaySplitEvents(corpPayload, etfSym, undSym) {
    const etf = parseSplitEventsFromCorp(corpPayload, etfSym);
    const undTicker = String(undSym || "").trim().toUpperCase();
    const und = undTicker ? parseSplitEventsFromCorp(corpPayload, undTicker) : [];
    return { etf, und };
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

  function isConvexityDay(beta, rUSimple, rLSimple, dragLog) {
    const b = toNum(beta);
    if (!Number.isFinite(b) || !Number.isFinite(dragLog)) return false;
    if (Math.abs(dragLog) < CONVEXITY_DRAG_LOG_THRESHOLD) return false;
    const trackErr = Math.abs(rLSimple - b * rUSimple);
    return Number.isFinite(trackErr) && trackErr < CONVEXITY_SIMPLE_TRACK_EPS;
  }

  /**
   * Build daily log-drag series. Also returns meta.skippedGaps / meta.convexityDays
   * when called via buildDailyLogDragSeriesWithMeta; the plain export returns the array
   * and attaches ._meta for callers that want diagnostics.
   */
  function buildDailyLogDragSeriesWithMeta(rows, beta) {
    const b = toNum(beta);
    const meta = { skippedGaps: [], convexityDays: [], pairDragBasis: PAIR_DRAG_BASIS };
    if (!Number.isFinite(b)) return { series: [], meta };
    const clean = (Array.isArray(rows) ? rows : [])
      .map((row) => {
        const date = parseDate(row && row.date);
        const etfPx = toNum(row && row.trEtfPx);
        const undPx = toNum(row && row.trUndPx);
        return { date, etfPx, undPx };
      })
      .filter((x) => x.date && x.etfPx > 0 && x.undPx > 0)
      .sort((a, b2) => a.date.localeCompare(b2.date));
    if (clean.length < 2) return { series: [], meta };

    const out = [];
    for (let i = 1; i < clean.length; i += 1) {
      const gap = dateGapDays(clean[i - 1].date, clean[i].date);
      if (Number.isFinite(gap) && gap > MAX_PAIR_DRAG_GAP_DAYS) {
        meta.skippedGaps.push({
          from: clean[i - 1].date,
          to: clean[i].date,
          calendarGap: gap,
        });
        continue;
      }
      const rUSimple = clean[i].undPx / clean[i - 1].undPx - 1;
      const rLSimple = clean[i].etfPx / clean[i - 1].etfPx - 1;
      const rU = Math.log(clean[i].undPx / clean[i - 1].undPx);
      const rL = Math.log(clean[i].etfPx / clean[i - 1].etfPx);
      if (!Number.isFinite(rU) || !Number.isFinite(rL)) continue;
      if (isOrphanLegJump(rU, rL)) continue;
      const drag = b * rU - rL;
      if (!Number.isFinite(drag)) continue;
      const convexity = isConvexityDay(b, rUSimple, rLSimple, drag);
      if (convexity) {
        meta.convexityDays.push({
          date: clean[i].date,
          drag,
          rUSimple,
          rLSimple,
          simpleTrackErr: rLSimple - b * rUSimple,
        });
      }
      out.push({
        date: clean[i].date,
        drag,
        simplePnl: b * rUSimple - rLSimple,
        rU,
        rL,
        rUSimple,
        rLSimple,
        convexityDay: convexity,
        etfPx: clean[i].etfPx,
        undPx: clean[i].undPx,
        etfPxPrev: clean[i - 1].etfPx,
        undPxPrev: clean[i - 1].undPx,
      });
    }
    return { series: out, meta };
  }

  function buildDailyLogDragSeries(rows, beta) {
    const { series, meta } = buildDailyLogDragSeriesWithMeta(rows, beta);
    series._meta = meta;
    return series;
  }

  function periodBorrowLog(borrowAnnual, obsDays) {
    const b = toNum(borrowAnnual);
    const n = Math.max(0, Math.floor(toNum(obsDays) || 0));
    if (!Number.isFinite(b) || n <= 0) return 0;
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
        convexityDays: 0,
        convexityDragLog: 0,
      };
    }
    const grossLog = slice.reduce((a, x) => a + x, 0);
    const borrowLog = periodBorrowLog(borrowAnnual, obs);
    const netLog = grossLog - borrowLog;
    const startRow = dailySeries[startIdx] || {};
    const endRow = dailySeries[endIdx] || {};
    const windowRows = dailySeries.slice(startIdx, endIdx + 1);
    const convexityRows = windowRows.filter((r) => r && r.convexityDay);
    const convexityDragLog = convexityRows.reduce((a, r) => a + toNum(r.drag), 0);
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
      convexityDays: convexityRows.length,
      convexityDragLog: Number.isFinite(convexityDragLog) ? convexityDragLog : 0,
    };
  }

  function computeHorizonPeriodReturns(dailySeries, horizons, borrowAnnual) {
    const hs = Array.isArray(horizons) && horizons.length ? horizons : DEFAULT_HORIZONS;
    const series = Array.isArray(dailySeries) ? dailySeries : [];
    const drags = series.map((x) => toNum(x && x.drag)).filter(Number.isFinite);
    const n = drags.length;
    const endDate = n ? String(series[n - 1].date || "") : null;
    const meta = series._meta || {};
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
    return {
      horizons: rows,
      nDays: n,
      endDate,
      borrowAnnual: toNum(borrowAnnual),
      pairDragBasis: PAIR_DRAG_BASIS,
      skippedGaps: meta.skippedGaps || [],
      convexityDays: meta.convexityDays || [],
    };
  }

  /**
   * Collapse duplicate partial longer horizons into a single "available history" row
   * so thin listings (e.g. CBRZ ~25d) don't show identical 60/120/251 bars.
   */
  function collapsePartialHorizons(horizonResult) {
    const rows = Array.isArray(horizonResult && horizonResult.horizons)
      ? horizonResult.horizons.slice()
      : [];
    if (!rows.length) return horizonResult;
    const full = rows.filter((h) => h && h.sufficient);
    const partial = rows.filter((h) => h && !h.sufficient && Number(h.obs) > 0);
    if (!partial.length) return horizonResult;
    // Keep the shortest requested partial horizon as the representative available-history row.
    partial.sort((a, b) => Number(a.horizonDays) - Number(b.horizonDays));
    const rep = { ...partial[0], availableHistory: true };
    const out = full.concat([rep]);
    out.sort((a, b) => Number(a.horizonDays) - Number(b.horizonDays));
    return { ...horizonResult, horizons: out, collapsedPartials: partial.length };
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
    MAX_PAIR_DRAG_GAP_DAYS,
    CONVEXITY_DRAG_LOG_THRESHOLD,
    CONVEXITY_SIMPLE_TRACK_EPS,
    PAIR_DRAG_BASIS,
    parseSplitEventsFromCorp,
    parseDecaySplitEvents,
    prepareDecayTrRows,
    latestContiguousRows,
    hasUsableMetricPrices,
    summarizeTrCoverage,
    isConvexityDay,
    buildDailyLogDragSeriesWithMeta,
    buildDailyLogDragSeries,
    computeHorizonPeriodReturns,
    collapsePartialHorizons,
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
