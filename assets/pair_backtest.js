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

  const exported = {
    alignPair,
    median,
    runPairBacktest,
    slippageCost,
    exposureRatio,
  };

  if (typeof module !== "undefined" && module.exports) module.exports = exported;
  if (globalObj) globalObj.PairBacktest = exported;
})(typeof window !== "undefined" ? window : globalThis);
