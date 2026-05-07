/* global window, module */
(function initPairBacktest(globalObj) {
  function toNum(v) {
    if (typeof v === "number") return Number.isFinite(v) ? v : NaN;
    if (typeof v === "string") {
      const n = Number(v.trim().replace(/,/g, ""));
      return Number.isFinite(n) ? n : NaN;
    }
    return NaN;
  }

  function clamp(v, lo, hi) {
    return Math.max(lo, Math.min(hi, v));
  }

  function dailyBorrowRate(v) {
    const n = toNum(v);
    return Number.isFinite(n) ? Math.max(0, n) / 252 : 0;
  }

  function normalizeSeries(rows, opts) {
    const valueKey = (opts && opts.valueKey) || "total_return";
    const priceKey = (opts && opts.priceKey) || "close_price";
    if (!Array.isArray(rows)) return [];
    return rows
      .map((r) => {
        const value = toNum(r && (r[valueKey] ?? r.nav_total_return));
        const px = toNum(r && r[priceKey]);
        const fallback = Number.isFinite(value) && value > 0 ? value : px;
        const close = Number.isFinite(px) && px > 0 ? px : fallback;
        return {
          date: String((r && r.date) || ""),
          value: fallback,
          close,
          sharesTraded: toNum(r && (r.shares_traded ?? r.volume ?? r.daily_volume)),
          aum: toNum(r && r.aum),
          sharesOutstanding: toNum(r && r.shares_outstanding),
        };
      })
      .filter((r) => r.date && Number.isFinite(r.value) && r.value > 0 && Number.isFinite(r.close) && r.close > 0)
      .sort((a, b) => a.date.localeCompare(b.date));
  }

  function borrowMapFromRows(rows) {
    const out = {};
    if (!Array.isArray(rows)) return out;
    rows.forEach((r) => {
      const d = String((r && r.date) || "");
      const b = toNum(r && (r.borrow_current ?? r.borrow_fee_annual ?? r.borrow_net_annual));
      if (d && Number.isFinite(b)) out[d] = b;
    });
    return out;
  }

  function median(arr) {
    const vals = (arr || []).map(toNum).filter((v) => Number.isFinite(v) && v > 0).sort((a, b) => a - b);
    if (!vals.length) return NaN;
    const mid = Math.floor(vals.length / 2);
    return vals.length % 2 ? vals[mid] : 0.5 * (vals[mid - 1] + vals[mid]);
  }

  function alignPair(longRows, shortRows, opts) {
    const l = normalizeSeries(longRows, opts && opts.long);
    const s = normalizeSeries(shortRows, opts && opts.short);
    const sm = new Map(s.map((r) => [r.date, r]));
    const out = [];
    l.forEach((lr) => {
      const sr = sm.get(lr.date);
      if (sr) out.push({ date: lr.date, long: lr, short: sr });
    });
    return out.sort((a, b) => a.date.localeCompare(b.date));
  }

  function interpolateBorrow(date, bmap, fallback) {
    if (bmap && Object.prototype.hasOwnProperty.call(bmap, date)) return dailyBorrowRate(bmap[date]);
    return dailyBorrowRate(fallback);
  }

  function slippageCost(tradeNotional, refNotional, params) {
    const notional = Math.abs(toNum(tradeNotional));
    if (!Number.isFinite(notional) || notional <= 0) return 0;
    const floorBps = Math.max(0, toNum(params && params.floorBps) || 0);
    const impactBps = Math.max(0, toNum(params && params.impactBps) || 0);
    const capBps = Math.max(floorBps, toNum(params && params.capBps) || 100);
    const ref = Math.max(1, toNum(refNotional) || 0);
    const participation = notional / ref;
    const bps = Math.min(capBps, floorBps + impactBps * Math.sqrt(Math.max(0, participation)));
    return notional * (bps / 10000);
  }

  function exposureRatio(longMv, shortMvAbs) {
    const gross = Math.abs(longMv) + Math.abs(shortMvAbs);
    if (!Number.isFinite(gross) || gross <= 0) return 0;
    return Math.abs(longMv - shortMvAbs) / gross;
  }

  function rebalanceToDrift(longMv, shortMvAbs, hedgeToRatio) {
    const gross = Math.abs(longMv) + Math.abs(shortMvAbs);
    const targetRatio = clamp(toNum(hedgeToRatio) || 0, 0, 0.95);
    const direction = longMv >= shortMvAbs ? 1 : -1;
    const targetNet = direction * targetRatio * gross;
    return {
      targetLong: (gross + targetNet) / 2,
      targetShortAbs: (gross - targetNet) / 2,
    };
  }

  function runPairBacktest(params) {
    const aligned = alignPair(params && params.longRows, params && params.shortRows, params);
    if (aligned.length < 2) return { ok: false, error: "Need at least two overlapping close dates." };

    const initialGross = Math.max(1000, toNum(params && params.initialGross) || 100000);
    const rebalanceEvery = Math.max(1, Math.round(toNum(params && params.rebalanceEveryDays) || 5));
    const driftThreshold = clamp((toNum(params && params.driftThresholdPct) || 5) / 100, 0, 0.95);
    const hedgeToRatio = clamp((toNum(params && params.hedgeToPct) || 1) / 100, 0, 0.95);
    const costParams = {
      floorBps: toNum(params && params.floorBps),
      impactBps: toNum(params && params.impactBps),
      capBps: toNum(params && params.capBps),
    };
    const fallbackBorrowAnnual = toNum(params && params.fallbackBorrowAnnual);
    const borrowByDate = borrowMapFromRows(params && params.shortBorrowRows);

    const longMedianShares = median(aligned.map((p) => p.long.sharesTraded));
    const shortMedianShares = median(aligned.map((p) => p.short.sharesTraded));
    const longMedianDollarVol = Number.isFinite(longMedianShares) ? longMedianShares * aligned[0].long.close : NaN;
    const shortMedianDollarVol = Number.isFinite(shortMedianShares) ? shortMedianShares * aligned[0].short.close : NaN;

    let longQty = (initialGross / 2) / aligned[0].long.value;
    let shortQtyAbs = (initialGross / 2) / aligned[0].short.value;
    let prevLongValue = aligned[0].long.value;
    let prevShortValue = aligned[0].short.value;
    let cumLong = 0;
    let cumShort = 0;
    let cumBorrow = 0;
    let cumCosts = 0;
    let lastRebalanceIdx = 0;
    const rows = [{
      date: aligned[0].date,
      longPnl: 0,
      shortPnl: 0,
      borrow: 0,
      transactionCosts: 0,
      netPnl: 0,
      exposureRatio: 0,
      longMarketValue: longQty * aligned[0].long.value,
      shortMarketValueAbs: shortQtyAbs * aligned[0].short.value,
      rebalance: true,
      rebalanceReason: "initial",
    }];

    for (let i = 1; i < aligned.length; i += 1) {
      const p = aligned[i];
      const longPnl = longQty * (p.long.value - prevLongValue);
      const shortPnl = -shortQtyAbs * (p.short.value - prevShortValue);
      const borrow = shortQtyAbs * prevShortValue * interpolateBorrow(p.date, borrowByDate, fallbackBorrowAnnual);
      cumLong += longPnl;
      cumShort += shortPnl;
      cumBorrow += borrow;

      let longMv = longQty * p.long.value;
      let shortMvAbs = shortQtyAbs * p.short.value;
      const er = exposureRatio(longMv, shortMvAbs);
      const cadenceDue = (i - lastRebalanceIdx) >= rebalanceEvery;
      const driftDue = er > driftThreshold;
      let dayCost = 0;
      let reason = "";
      if (cadenceDue || driftDue) {
        const target = driftDue
          ? rebalanceToDrift(longMv, shortMvAbs, hedgeToRatio)
          : { targetLong: (longMv + shortMvAbs) / 2, targetShortAbs: (longMv + shortMvAbs) / 2 };
        const longTrade = target.targetLong - longMv;
        const shortTrade = target.targetShortAbs - shortMvAbs;
        const longRef = Number.isFinite(longMedianDollarVol) ? longMedianDollarVol : (Number.isFinite(p.long.aum) ? p.long.aum : initialGross);
        const shortRef = Number.isFinite(shortMedianDollarVol) ? shortMedianDollarVol : (Number.isFinite(p.short.aum) ? p.short.aum : initialGross);
        dayCost += slippageCost(longTrade, longRef, costParams);
        dayCost += slippageCost(shortTrade, shortRef, costParams);
        cumCosts += dayCost;
        longQty = target.targetLong / p.long.value;
        shortQtyAbs = target.targetShortAbs / p.short.value;
        longMv = target.targetLong;
        shortMvAbs = target.targetShortAbs;
        lastRebalanceIdx = i;
        reason = cadenceDue && driftDue ? "cadence+drift" : (driftDue ? "drift" : "cadence");
      }

      rows.push({
        date: p.date,
        longPnl: cumLong,
        shortPnl: cumShort,
        borrow: cumBorrow,
        transactionCosts: cumCosts,
        netPnl: cumLong + cumShort - cumBorrow - cumCosts,
        exposureRatio: exposureRatio(longMv, shortMvAbs),
        longMarketValue: longMv,
        shortMarketValueAbs: shortMvAbs,
        rebalance: Boolean(reason),
        rebalanceReason: reason,
      });
      prevLongValue = p.long.value;
      prevShortValue = p.short.value;
    }

    const last = rows[rows.length - 1];
    return {
      ok: true,
      rows,
      summary: {
        observations: rows.length,
        startDate: rows[0].date,
        endDate: last.date,
        longPnl: last.longPnl,
        shortPnl: last.shortPnl,
        borrow: last.borrow,
        transactionCosts: last.transactionCosts,
        netPnl: last.netPnl,
        rebalanceCount: rows.filter((r) => r.rebalance).length,
        longMedianSharesTraded: Number.isFinite(longMedianShares) ? longMedianShares : null,
        shortMedianSharesTraded: Number.isFinite(shortMedianShares) ? shortMedianShares : null,
      },
      settings: { initialGross, rebalanceEvery, driftThreshold, hedgeToRatio, costParams },
    };
  }

  /**
   * Long ETF vs short underlying with **notional** hedge ratio h:
   *   shortMV = h × longMV,  gross G = longMV + shortMV  ⇒  longMV = G/(1+h), shortMV = h·G/(1+h).
   * Positions: q_L = longMV/P_ETF, q_S = shortMV/P_und at each rebalance.
   * Borrow: constant annual on short MV, /252 per day.
   * Rows: per-day objects with close_price (or nav), underlying_adj_close, date.
   */
  function simulateInversePairBacktest(rows, opts) {
    const gross = Math.max(0, toNum(opts && opts.gross));
    const hedgeRatioH = Math.max(1e-8, toNum(opts && opts.hedgeRatio));
    const everyN = Math.max(1, Math.floor(toNum(opts && opts.everyNDays) || 5));
    const driftFrac = Math.max(0, toNum(opts && opts.driftPct) / 100);
    const maxNetGross = Math.max(0.0001, toNum(opts && opts.hedgeBackPct) / 100);
    const floorBps = Math.max(0, toNum(opts && opts.floorBps));
    const impactBps = Math.max(0, toNum(opts && opts.impactBps));
    const costCapBps = Math.max(floorBps + impactBps, toNum(opts && opts.costCapBps));
    const avgBorrowAnnual = Math.max(0, toNum(opts && opts.avgBorrowAnnual));

    if (!Array.isArray(rows) || rows.length < 2 || !Number.isFinite(gross) || gross <= 0) {
      return { ok: false, error: "Invalid rows or gross capital.", daily: [], rebalanceMarks: [], summary: {} };
    }

    const pts = [];
    for (const row of rows) {
      const pl = toNum(row && row.close_price) || toNum(row && row.nav);
      const ps = toNum(row && row.underlying_adj_close);
      const ds = String((row && row.date) || "").trim();
      if (!ds || !Number.isFinite(pl) || pl <= 0 || !Number.isFinite(ps) || ps <= 0) continue;
      pts.push({ date: ds, pl, ps });
    }
    if (pts.length < 3) {
      return {
        ok: false,
        error: "Need at least 3 days with ETF close (or NAV) and underlying adj. close.",
        daily: [],
        rebalanceMarks: [],
        summary: {},
      };
    }

    /** Target fraction of gross market value on the short leg when notionally hedged: short/(long+short) = h/(1+h). */
    function targetShortWeightNotional(h) {
      const hh = Math.max(1e-12, h);
      return hh / (1 + hh);
    }

    let qL = 0;
    let qS = 0;
    let lastRebal = -1;
    let daysSinceRebal = 0;
    let cumLong = 0;
    let cumShort = 0;
    let cumBorrow = 0;
    let cumTc = 0;
    const daily = [];
    const rebalanceMarks = [];

    function rebalanceAt(i, reason) {
      const { pl, ps, date } = pts[i];
      const h = hedgeRatioH;
      if (!(pl > 0) || !(ps > 0)) return false;
      const longMv = gross / (1 + h);
      const shortMv = (gross * h) / (1 + h);
      const qLNew = longMv / pl;
      const qSNew = shortMv / ps;
      if (lastRebal >= 0) {
        const tradeNotional = Math.abs(qLNew - qL) * pl + Math.abs(qSNew - qS) * ps;
        const feeBps = Math.min(costCapBps, floorBps + impactBps);
        cumTc += (tradeNotional * feeBps) / 10000;
      }
      qL = qLNew;
      qS = qSNew;
      lastRebal = i;
      daysSinceRebal = 0;
      rebalanceMarks.push({ date, reason });
      return true;
    }

    rebalanceAt(0, "inception");

    for (let i = 1; i < pts.length; i += 1) {
      const cur = pts[i];
      const prev = pts[i - 1];
      const dLong = qL * (cur.pl - prev.pl);
      const dShort = -qS * (cur.ps - prev.ps);
      const shortMvOpen = qS * prev.ps;
      const borrowDay = shortMvOpen * (avgBorrowAnnual / 252);
      cumLong += dLong;
      cumShort += dShort;
      cumBorrow += borrowDay;

      const grossMv = Math.abs(qL * cur.pl) + Math.abs(qS * cur.ps);
      const netMv = Math.abs(qL * cur.pl - qS * cur.ps);
      const netGross = grossMv > 1e-9 ? netMv / grossMv : 0;
      const wShort = grossMv > 1e-9 ? (qS * cur.ps) / grossMv : 0;
      const wTarget = targetShortWeightNotional(hedgeRatioH);
      daysSinceRebal += 1;

      let rebalReason = "";
      if (daysSinceRebal >= everyN) rebalReason = "calendar";
      else if (Number.isFinite(driftFrac) && driftFrac > 0 && Math.abs(wShort - wTarget) > driftFrac) {
        rebalReason = "drift";
      } else if (netGross > maxNetGross) rebalReason = "net/gross";

      if (rebalReason) rebalanceAt(i, rebalReason);

      const netPnl = cumLong + cumShort - cumBorrow - cumTc;
      daily.push({
        date: cur.date,
        netPnl,
        longPnl: cumLong,
        shortPnl: cumShort,
        borrow: -cumBorrow,
        tCosts: -cumTc,
        netGross,
        rebal: rebalReason || "",
      });
    }

    const summary = {
      netPnl: cumLong + cumShort - cumBorrow - cumTc,
      longPnl: cumLong,
      shortPnl: cumShort,
      borrowPaid: cumBorrow,
      tCosts: cumTc,
      nDays: pts.length,
      nRebalances: rebalanceMarks.length,
    };
    const chartRows = daily.map((d) => ({
      date: d.date,
      netPnl: d.netPnl,
      longPnl: d.longPnl,
      shortPnl: d.shortPnl,
      borrow: d.borrow,
      transactionCosts: d.tCosts,
      exposureRatio: d.netGross,
      rebalance: Boolean(d.rebal),
      rebalanceReason: d.rebal || "",
    }));
    return {
      ok: true,
      daily,
      rows: chartRows,
      rebalanceMarks,
      summary,
      inception: pts[0].date,
      end: pts[pts.length - 1].date,
    };
  }

  const exported = {
    alignPair,
    median,
    runPairBacktest,
    slippageCost,
    exposureRatio,
    simulateInversePairBacktest,
  };

  if (typeof module !== "undefined" && module.exports) module.exports = exported;
  if (globalObj) globalObj.PairBacktest = exported;
})(typeof window !== "undefined" ? window : globalThis);
