/* global globalThis, module */
(function initPairBacktest(globalObj) {
  function toNum(v) {
    if (typeof v === "number") return Number.isFinite(v) ? v : NaN;
    if (typeof v === "string") {
      const s = v.trim();
      if (!s) return NaN;
      const n = Number(s.replace(/,/g, ""));
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

  /** Minimum span before CAGR is meaningful (short windows annualize pathologically). */
  const MIN_TRADING_DAYS_FOR_CAGR = 63;
  /** ~one quarter; paired with MIN_TRADING_DAYS_FOR_CAGR so both must pass. */
  const MIN_CALENDAR_YEARS_FOR_CAGR = 0.25;

  /**
   * Expanding-window risk metrics on **mark-to-market equity** = gross + cumulative net PnL.
   * @param {Array<{date:string, netPnl:number}>} rows `result.rows` sorted ascending by date
   */
  function computePairBacktestRiskSeries(rows, gross) {
    const G = typeof gross === "number" && Number.isFinite(gross) && gross > 0 ? gross : 0;
    if (!Array.isArray(rows) || rows.length < 3 || !(G > 0)) return [];
    const sorted = [...rows].sort((a, b) => String(a.date).localeCompare(String(b.date)));
    const equity = [];
    for (const r of sorted) {
      const np = toNum(r.netPnl);
      if (!Number.isFinite(np)) return [];
      equity.push(G + np);
    }
    const parseD = (ds) => {
      const d = new Date(`${String(ds || "").trim()}T00:00:00Z`);
      return Number.isNaN(d.getTime()) ? null : d;
    };
    const MIN_I = 5;
    const out = [];
    const eq0abs = Math.abs(equity[0]);
    if (!(eq0abs > 1e-9)) return [];

    for (let i = MIN_I; i < sorted.length; i += 1) {
      const d0 = parseD(sorted[0].date);
      const di = parseD(sorted[i].date);
      const eq0 = equity[0];
      const eqI = equity[i];
      if (!d0 || !di) continue;

      const rets = [];
      for (let j = 1; j <= i; j += 1) {
        const prev = equity[j - 1];
        const cur = equity[j];
        const denom = Math.abs(prev) > 1e-9 ? Math.abs(prev) : 1e-9;
        rets.push((cur - prev) / denom);
      }
      const n = rets.length;
      if (n < 2) continue;
      const mean = rets.reduce((a, b) => a + b, 0) / n;
      let varSum = 0;
      for (let k = 0; k < rets.length; k += 1) varSum += (rets[k] - mean) ** 2;
      const std = Math.sqrt(varSum / (n - 1));
      const annVol = Number.isFinite(std) && std > 1e-12 ? std * Math.sqrt(252) : 0;
      const sharpe = std > 1e-12 ? (mean / std) * Math.sqrt(252) : 0;

      let peak = equity[0];
      let maxDd = 0;
      for (let k = 0; k <= i; k += 1) {
        const e = equity[k];
        if (e > peak) peak = e;
        if (peak > 1e-9) {
          const dd = (peak - e) / peak;
          if (dd > maxDd) maxDd = dd;
        }
      }

      const msPerYear = 365.25 * 86400000;
      const years = (di.getTime() - d0.getTime()) / msPerYear;
      const tradingSpan = i + 1;
      let cagr = NaN;
      if (
        years >= MIN_CALENDAR_YEARS_FOR_CAGR
        && tradingSpan >= MIN_TRADING_DAYS_FOR_CAGR
        && eq0 > 0
        && eqI > 0
      ) {
        cagr = (eqI / eq0) ** (1 / years) - 1;
      }

      out.push({
        date: sorted[i].date,
        cagr,
        annVol,
        maxDrawdown: maxDd,
        sharpe,
      });
    }
    return out;
  }

  /**
   * Two-leg hedge vs ETF close (or NAV) and underlying adj. close, with **notional** hedge ratio h = |MV_etf| / |MV_und|
   * on the two risk legs (split: MV_etf = h·G/(1+h), MV_und = G/(1+h) at rebalance).
   *
   * - **β ≥ 0**: short ETF, long underlying (borrow on ETF short only).
   * - **β < 0**: short ETF, short underlying (borrow on ETF short only; underlying short borrow 0%).
   *
   * **Borrow (etf-dashboard / short_favorable_positive):** `borrow_current` from `opts.borrowHistory`
   * is the canonical annual rate where **negative = fee (cost to short)**, **positive = rebate**.
   * Daily cash drag per $ ETF short at EOD is **−rate / 252** (trapezoid on ETF price). Forward-fill
   * the latest history point with `date <= row date`, then fall back to `opts.avgBorrowAnnual`
   * (same convention) when unknown.
   *
   * **Rebalancing:** Every **N** trading days, if **|β-adj net/gross − anchor|** exceeds
   * `netGrossTolerancePct` (percentage points), rebalance to target notionals and reset the anchor.
   * Anchor is snapshotted at inception and after each rebalance.
   *
   * Pass `opts.delta` (screener δ) or legacy `opts.beta`. `opts.hedgeRatio` is the magnitude h (UI default 1/|δ|).
   * `opts.slippageBps`: per-rebalance t-cost = `Math.max(0, slippageBps) / 10000` × traded notional (Diamond-Creek-Quant parity; no floor/impact fallback).
   */
  function simulateInversePairBacktest(rows, opts) {
    const gross = Math.max(0, toNum(opts && opts.gross));
    const hedgeRatioH = Math.max(1e-8, toNum(opts && opts.hedgeRatio));
    let betaRow = toNum(opts && opts.delta);
    if (!Number.isFinite(betaRow)) betaRow = toNum(opts && opts.beta);
    const shortEtfLongUnd = !(Number.isFinite(betaRow) && betaRow < 0);
    const everyN = Math.max(1, Math.floor(toNum(opts && opts.everyNDays) || 5));
    const tolPct = toNum(opts && opts.netGrossTolerancePct);
    const tolerance = Number.isFinite(tolPct) && tolPct >= 0 ? Math.min(tolPct / 100, 0.999) : 0.05;
    const slippageBps = Math.max(0, toNum(opts && opts.slippageBps));
    const borrowAnnualFallback = toNum(opts && opts.avgBorrowAnnual);
    const betaAbs = Number.isFinite(betaRow) && Math.abs(betaRow) > 1e-12
      ? Math.abs(betaRow)
      : hedgeRatioH;

    const borrowHist = Array.isArray(opts && opts.borrowHistory)
      ? [...opts.borrowHistory]
        .map((x) => ({ d: String((x && x.date) || "").trim(), b: toNum(x && x.borrow_current) }))
        .filter((x) => x.d && Number.isFinite(x.b))
        .sort((a, b) => a.d.localeCompare(b.d))
      : [];

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

    function targetEtfWeightInGross(h) {
      const hh = Math.max(1e-12, h);
      return hh / (1 + hh);
    }

    let borrowWalk = 0;
    let lastBorrowCanon = NaN;
    function advanceBorrowForDate(ds) {
      while (borrowWalk < borrowHist.length && borrowHist[borrowWalk].d <= ds) {
        lastBorrowCanon = borrowHist[borrowWalk].b;
        borrowWalk += 1;
      }
    }

    let qE = 0;
    let qU = 0;
    let lastRebal = -1;
    let daysSinceRebal = 0;
    let cumEtf = 0;
    let cumUnd = 0;
    let cumBorrow = 0;
    let cumTc = 0;
    const daily = [];
    const rebalanceMarks = [];

    function computeBetaNetGrossRatio(mvEtfAbs, mvUndAbs) {
      const adj = mvEtfAbs * betaAbs;
      const g = adj + mvUndAbs;
      return g > 1e-12 ? Math.abs(adj - mvUndAbs) / g : 0;
    }

    let anchorBetaNetGross = 0;

    function snapshotAnchor(i) {
      const p = pts[i];
      const mE = qE * p.pl;
      const mU = qU * p.ps;
      anchorBetaNetGross = computeBetaNetGrossRatio(mE, mU);
    }

    function rebalanceAt(i, reason) {
      const { pl, ps, date } = pts[i];
      const h = hedgeRatioH;
      if (!(pl > 0) || !(ps > 0)) return false;
      const mvEtf = (gross * h) / (1 + h);
      const mvUnd = gross / (1 + h);
      const qENew = mvEtf / pl;
      const qUNew = mvUnd / ps;
      if (lastRebal >= 0) {
        const tradeNotional = Math.abs(qENew - qE) * pl + Math.abs(qUNew - qU) * ps;
        cumTc += (tradeNotional * slippageBps) / 10000;
      }
      qE = qENew;
      qU = qUNew;
      lastRebal = i;
      daysSinceRebal = 0;
      rebalanceMarks.push({ date, reason });
      snapshotAnchor(i);
      return true;
    }

    rebalanceAt(0, "inception");
    advanceBorrowForDate(pts[0].date);

    for (let i = 1; i < pts.length; i += 1) {
      const cur = pts[i];
      const prev = pts[i - 1];
      const dpl = cur.pl - prev.pl;
      const dps = cur.ps - prev.ps;
      const dEtf = -qE * dpl;
      const dUnd = shortEtfLongUnd ? qU * dps : -qU * dps;
      cumEtf += dEtf;
      cumUnd += dUnd;

      advanceBorrowForDate(cur.date);
      const canon = Number.isFinite(lastBorrowCanon) ? lastBorrowCanon : borrowAnnualFallback;
      const annualDragPerShortDollar = Number.isFinite(canon) ? -canon : 0;
      const borrowBase = qE * 0.5 * (prev.pl + cur.pl);
      const borrowDay = borrowBase * (annualDragPerShortDollar / 252);
      cumBorrow += borrowDay;

      const mvEtfAbs = qE * cur.pl;
      const mvUndAbs = qU * cur.ps;
      const mvEtfBetaAdj = mvEtfAbs * betaAbs;
      const grossBeta = mvEtfBetaAdj + mvUndAbs;
      const netBeta = Math.abs(mvEtfBetaAdj - mvUndAbs);
      const netGrossBeta = grossBeta > 1e-12 ? netBeta / grossBeta : 0;
      const grossMvRaw = mvEtfAbs + mvUndAbs;
      const netMvRaw = Math.abs(mvEtfAbs - mvUndAbs);
      const netGrossRaw = grossMvRaw > 1e-12 ? netMvRaw / grossMvRaw : 0;
      const wEtfInGross = grossMvRaw > 1e-9 ? mvEtfAbs / grossMvRaw : 0;
      const wTarget = targetEtfWeightInGross(hedgeRatioH);
      daysSinceRebal += 1;

      let rebalReason = "";
      if (daysSinceRebal >= everyN) {
        if (Math.abs(netGrossBeta - anchorBetaNetGross) > tolerance) {
          rebalReason = "beta_net_gross";
          rebalanceAt(i, rebalReason);
        } else {
          daysSinceRebal = 0;
        }
      }

      const netPnl = cumEtf + cumUnd - cumBorrow - cumTc;
      daily.push({
        date: cur.date,
        netPnl,
        longPnl: cumEtf,
        shortPnl: cumUnd,
        borrow: -cumBorrow,
        tCosts: -cumTc,
        netGross: netGrossBeta,
        netGrossRaw,
        netGrossBeta,
        mvEtfAbs,
        mvUndAbs,
        mvEtfBetaAdj,
        wEtfInGross,
        wTarget,
        rebal: rebalReason || "",
      });
    }

    const summary = {
      netPnl: cumEtf + cumUnd - cumBorrow - cumTc,
      longPnl: cumEtf,
      shortPnl: cumUnd,
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
      exposureRatio: d.netGrossBeta,
      mvEtfAbs: d.mvEtfAbs,
      mvUndAbs: d.mvUndAbs,
      mvEtfBetaAdj: d.mvEtfBetaAdj,
      rebalance: Boolean(d.rebal),
      rebalanceReason: d.rebal || "",
    }));
    const legChartLabels = shortEtfLongUnd
      ? { etf: "ETF (short)", und: "Underlying (long)" }
      : { etf: "ETF (short)", und: "Underlying (short)" };
    return {
      ok: true,
      daily,
      rows: chartRows,
      rebalanceMarks,
      summary,
      inception: pts[0].date,
      end: pts[pts.length - 1].date,
      strategy: shortEtfLongUnd ? "short_etf_long_und" : "short_both",
      betaUsed: Number.isFinite(betaRow) ? betaRow : null,
      legChartLabels,
      breachTolerancePct: tolerance * 100,
    };
  }

  const exported = {
    alignPair,
    median,
    runPairBacktest,
    slippageCost,
    exposureRatio,
    simulateInversePairBacktest,
    computePairBacktestRiskSeries,
    MIN_TRADING_DAYS_FOR_CAGR,
    MIN_CALENDAR_YEARS_FOR_CAGR,
  };

  if (typeof module !== "undefined" && module.exports) module.exports = exported;
  if (globalObj) globalObj.PairBacktest = exported;
})(typeof globalThis !== "undefined" ? globalThis : typeof window !== "undefined" ? window : globalThis);
