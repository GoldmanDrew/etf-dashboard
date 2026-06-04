/* global window, module */
(function initTradeLab(globalObj) {
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

  function clamp(v, lo, hi) {
    return Math.max(lo, Math.min(hi, v));
  }

  function normCdf(x) {
    const z = toNum(x);
    if (!Number.isFinite(z)) return NaN;
    const a1 = 0.254829592;
    const a2 = -0.284496736;
    const a3 = 1.421413741;
    const a4 = -1.453152027;
    const a5 = 1.061405429;
    const p = 0.3275911;
    const sign = z < 0 ? -1 : 1;
    const t = 1 / (1 + p * Math.abs(z) / Math.sqrt(2));
    const erf = 1 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-(z * z) / 2);
    return 0.5 * (1 + sign * erf);
  }

  function bsPrice(params) {
    const spot = toNum(params && params.spot);
    const strike = toNum(params && params.strike);
    const vol = Math.max(1e-8, toNum(params && params.vol));
    const t = Math.max(0, toNum(params && params.ttmYears));
    const rate = Number.isFinite(toNum(params && params.rate)) ? toNum(params && params.rate) : 0;
    const type = String((params && params.type) || "call").toLowerCase();
    if (!Number.isFinite(spot) || !Number.isFinite(strike) || spot <= 0 || strike <= 0) return NaN;
    if (t <= 0) {
      if (type === "put") return Math.max(strike - spot, 0);
      return Math.max(spot - strike, 0);
    }
    const vsqrt = vol * Math.sqrt(t);
    const d1 = (Math.log(spot / strike) + (rate + 0.5 * vol * vol) * t) / vsqrt;
    const d2 = d1 - vsqrt;
    if (type === "put") {
      return strike * Math.exp(-rate * t) * normCdf(-d2) - spot * normCdf(-d1);
    }
    return spot * normCdf(d1) - strike * Math.exp(-rate * t) * normCdf(d2);
  }

  function optionPayoffAtExpiry(params) {
    const spot = toNum(params && params.spot);
    const strike = toNum(params && params.strike);
    const type = String((params && params.type) || "call").toLowerCase();
    if (!Number.isFinite(spot) || !Number.isFinite(strike)) return NaN;
    if (type === "put") return Math.max(strike - spot, 0);
    return Math.max(spot - strike, 0);
  }

  function signedQty(side, qty) {
    const q = toNum(qty);
    if (!Number.isFinite(q)) return 0;
    return String(side || "long").toLowerCase() === "short" ? -Math.abs(q) : Math.abs(q);
  }

  function equityLegPnl(leg, entry, finalSpot) {
    const q = signedQty(leg && leg.side, leg && leg.quantity);
    const e = toNum(entry);
    const f = toNum(finalSpot);
    if (!Number.isFinite(e) || !Number.isFinite(f) || !Number.isFinite(q)) return 0;
    return q * (f - e);
  }

  function remainingTtxDays(ttxDaysStart, elapsedYears) {
    const start = Math.max(0, toNum(ttxDaysStart));
    const elapsedDays = Math.max(0, toNum(elapsedYears)) * 365;
    return Math.max(0, start - elapsedDays);
  }

  function optionLegPnl(leg, spotNow, spotFinal, ttxDaysRemaining, volFallback, rate) {
    const contracts = Math.max(0, toNum(leg && leg.contracts));
    const multiplier = Math.max(1, toNum((leg && leg.multiplier) ?? 100));
    const strike = toNum(leg && leg.strike);
    const premium = Number.isFinite(toNum(leg && leg.premium)) ? toNum(leg.premium) : 0;
    const iv = Number.isFinite(toNum(leg && leg.iv)) ? toNum(leg.iv) : volFallback;
    if (!Number.isFinite(strike) || !Number.isFinite(contracts) || contracts <= 0) return { pnl: 0, mark: 0, payoff: 0 };
    const scenarioSpot = Number.isFinite(spotFinal) && spotFinal > 0
      ? spotFinal
      : (Number.isFinite(spotNow) && spotNow > 0 ? spotNow : NaN);
    if (!Number.isFinite(scenarioSpot) || scenarioSpot <= 0) return { pnl: 0, mark: 0, payoff: 0 };
    const tYears = Math.max(0, toNum(ttxDaysRemaining)) / 365;
    const type = String((leg && leg.type) || "call").toLowerCase();
    const mark = bsPrice({ spot: scenarioSpot, strike, vol: Math.max(0.01, iv), ttmYears: tYears, rate, type });
    const payoff = optionPayoffAtExpiry({ spot: scenarioSpot, strike, type });
    const side = String((leg && leg.side) || "buy").toLowerCase();
    const dir = side === "sell" ? -1 : 1;
    const markOrPayoff = tYears > 0 ? mark : payoff;
    const pnl = dir * contracts * multiplier * (markOrPayoff - premium);
    return { pnl, mark, payoff };
  }

  function evaluateStructurePnl(params) {
    const baseLegs = Array.isArray(params && params.baseLegs) ? params.baseLegs : [];
    const optionLegs = Array.isArray(params && params.optionLegs) ? params.optionLegs : [];
    const priceNow = (params && params.priceNow) || {};
    const priceFinal = (params && params.priceFinal) || {};
    const ttxDaysStart = Number.isFinite(toNum(params && params.ttxDays)) ? toNum(params.ttxDays) : 0;
    const volMap = (params && params.volMap) || {};
    const rate = Number.isFinite(toNum(params && params.rate)) ? toNum(params.rate) : 0;
    const elapsedYears = Number.isFinite(toNum(params && params.elapsedYears)) ? Math.max(0, toNum(params.elapsedYears)) : 0;
    const ttxDaysRemaining = params && params.ttxDaysIsRemaining === true
      ? Math.max(0, ttxDaysStart)
      : remainingTtxDays(ttxDaysStart, elapsedYears);
    const annualBorrowBySymbol = (params && params.annualBorrowBySymbol) || {};
    const defaultBorrowAnnual = Number.isFinite(toNum(params && params.defaultBorrowAnnual)) ? Math.max(0, toNum(params.defaultBorrowAnnual)) : 0;
    const shortProceedsRateAnnual = Number.isFinite(toNum(params && params.shortProceedsRateAnnual)) ? toNum(params.shortProceedsRateAnnual) : 0;

    let total = 0;
    const legRows = [];

    for (const leg of baseLegs) {
      const sym = String((leg && leg.symbol) || "").toUpperCase();
      if (!sym) continue;
      const entry = Number.isFinite(toNum(leg && leg.entry)) ? toNum(leg.entry) : toNum(priceNow[sym]);
      const finalSpot = toNum(priceFinal[sym]);
      const pnl = equityLegPnl(leg, entry, finalSpot);
      let financingPnl = 0;
      if (String((leg && leg.side) || "").toLowerCase() === "short" && Number.isFinite(entry)) {
        const qAbs = Math.abs(toNum(leg && leg.quantity));
        const shortNotional = qAbs * entry;
        const b = Number.isFinite(toNum(annualBorrowBySymbol[sym])) ? Math.max(0, toNum(annualBorrowBySymbol[sym])) : defaultBorrowAnnual;
        financingPnl = shortNotional * (shortProceedsRateAnnual - b) * elapsedYears;
      }
      total += pnl;
      total += financingPnl;
      legRows.push({ kind: "equity", symbol: sym, pnl, financingPnl });
    }

    for (const leg of optionLegs) {
      const sym = String((leg && leg.symbol) || "").toUpperCase();
      if (!sym) continue;
      const spotNow = toNum(priceNow[sym]);
      const spotFinal = toNum(priceFinal[sym]);
      const volFallback = Number.isFinite(toNum(volMap[sym])) ? toNum(volMap[sym]) : 0.6;
      const out = optionLegPnl(leg, spotNow, spotFinal, ttxDaysRemaining, volFallback, rate);
      total += out.pnl;
      legRows.push({ kind: "option", symbol: sym, pnl: out.pnl });
    }

    return { ok: true, totalPnl: total, legs: legRows };
  }

  function etfReturnFromUnderlying(params) {
    const leverage = toNum(params && params.leverage);
    const undRet = toNum(params && params.underlyingReturn);
    const sigma = Math.max(0, toNum(params && params.sigmaAnnual));
    const years = Math.max(1e-8, toNum(params && params.horizonYears));
    if (!Number.isFinite(leverage) || !Number.isFinite(undRet) || !Number.isFinite(sigma) || !Number.isFinite(years)) return NaN;
    const onePlusUnd = 1 + undRet;
    if (onePlusUnd <= 0) return NaN;
    const drag = 0.5 * leverage * (leverage - 1) * sigma * sigma * years;
    const etfLog = leverage * Math.log(onePlusUnd) - drag;
    return Math.exp(etfLog) - 1;
  }

  function resolveSpotBySymbol(params) {
    const underlyingSymbol = String((params && params.underlyingSymbol) || "").toUpperCase();
    const etfSymbol = String((params && params.etfSymbol) || "").toUpperCase();
    const spotUnderlying = toNum(params && params.spotUnderlying);
    const spotEtf = toNum(params && params.spotEtf);
    const spotBySymbol = { ...((params && params.spotBySymbol) || {}) };
    if (underlyingSymbol && !Number.isFinite(toNum(spotBySymbol[underlyingSymbol]))) {
      spotBySymbol[underlyingSymbol] = spotUnderlying;
    }
    if (etfSymbol && !Number.isFinite(toNum(spotBySymbol[etfSymbol]))) {
      spotBySymbol[etfSymbol] = spotEtf;
    }
    return spotBySymbol;
  }

  function resolveLeverageBySymbol(params) {
    const etfSymbol = String((params && params.etfSymbol) || "").toUpperCase();
    const leverage = toNum(params && params.leverage);
    const leverageBySymbol = { ...((params && params.leverageBySymbol) || {}) };
    if (etfSymbol && !Number.isFinite(toNum(leverageBySymbol[etfSymbol]))) {
      leverageBySymbol[etfSymbol] = leverage;
    }
    return leverageBySymbol;
  }

  function symbolsFromLegs(baseLegs, optionLegs, underlyingSymbol) {
    const used = new Set();
    for (const leg of [...baseLegs, ...optionLegs]) {
      const sym = String((leg && leg.symbol) || "").toUpperCase();
      if (sym) used.add(sym);
    }
    if (underlyingSymbol) used.add(String(underlyingSymbol).toUpperCase());
    return used;
  }

  function etfPathFromUnderlyingPath(undPath, leverage, volAnnual, horizonYears) {
    const steps = undPath.length - 1;
    return undPath.map((up, t) => {
      const undRet = up - 1;
      const elapsed = steps > 0 ? (t / steps) * horizonYears : horizonYears;
      const etfRet = etfReturnFromUnderlying({
        leverage,
        underlyingReturn: undRet,
        sigmaAnnual: volAnnual,
        horizonYears: Math.max(1e-8, elapsed),
      });
      return 1 + etfRet;
    });
  }

  function buildTradePriceMaps(params, undRet, sigmaAnnual) {
    const underlyingSymbol = String((params && params.underlyingSymbol) || "").toUpperCase();
    const etfSymbol = String((params && params.etfSymbol) || "").toUpperCase();
    const leverage = toNum(params && params.leverage);
    const horizonYears = toNum(params && params.horizonYears);
    const baseLegs = Array.isArray(params && params.baseLegs) ? params.baseLegs : [];
    const optionLegs = Array.isArray(params && params.optionLegs) ? params.optionLegs : [];
    const baseVol = Math.max(0.01, toNum(params && params.baseVolAnnual));
    const spotBySymbol = resolveSpotBySymbol(params);
    const leverageBySymbol = resolveLeverageBySymbol(params);
    const usedSymbols = symbolsFromLegs(baseLegs, optionLegs, underlyingSymbol);

    const priceNow = {};
    const priceFinal = {};
    const volMap = {};
    for (const sym of usedSymbols) {
      const spot = toNum(spotBySymbol[sym]);
      if (!Number.isFinite(spot)) continue;
      priceNow[sym] = spot;
      if (sym === underlyingSymbol) {
        priceFinal[sym] = spot * (1 + undRet);
        volMap[sym] = baseVol;
        continue;
      }
      const lev = Number.isFinite(toNum(leverageBySymbol[sym]))
        ? toNum(leverageBySymbol[sym])
        : (sym === etfSymbol ? leverage : NaN);
      if (!Number.isFinite(lev)) {
        priceFinal[sym] = spot;
        volMap[sym] = sigmaAnnual * 0.5;
        continue;
      }
      const etfRet = etfReturnFromUnderlying({ leverage: lev, underlyingReturn: undRet, sigmaAnnual, horizonYears });
      priceFinal[sym] = spot * (1 + etfRet);
      volMap[sym] = sigmaAnnual * Math.max(0.2, Math.abs(lev));
    }
    return { priceNow, priceFinal, volMap };
  }

  function buildTradeScenarioGrid(params) {
    const shockRows = Array.isArray(params && params.shockRows) ? params.shockRows : [];
    const volColumns = Array.isArray(params && params.volColumns) ? params.volColumns : [];
    const spotUnderlying = toNum(params && params.spotUnderlying);
    const spotEtf = toNum(params && params.spotEtf);
    const underlyingSymbol = String((params && params.underlyingSymbol) || "").toUpperCase();
    const etfSymbol = String((params && params.etfSymbol) || "").toUpperCase();
    const horizonYears = toNum(params && params.horizonYears);
    const baseLegs = Array.isArray(params && params.baseLegs) ? params.baseLegs : [];
    const optionLegs = Array.isArray(params && params.optionLegs) ? params.optionLegs : [];
    const ttxDays = toNum(params && params.ttxDays);
    const rate = Number.isFinite(toNum(params && params.rate)) ? toNum(params.rate) : 0;
    const spotBySymbol = resolveSpotBySymbol(params);
    const undSpot = toNum(spotBySymbol[underlyingSymbol]);

    if (!underlyingSymbol || !Number.isFinite(undSpot)) {
      return { ok: false, error: "Missing trade spot inputs." };
    }

    const rows = shockRows.map((row) => {
      const undRet = toNum(row && row.underlyingReturn);
      const cells = volColumns.map((col) => {
        const sigmaAnnual = Math.max(0.01, toNum(col && col.sigmaAnnual));
        const maps = buildTradePriceMaps({ ...params, baseVolAnnual: toNum(params && params.baseVolAnnual) }, undRet, sigmaAnnual);
        const etfRet = (etfSymbol && Number.isFinite(toNum(maps.priceNow[etfSymbol])) && Number.isFinite(toNum(maps.priceFinal[etfSymbol])))
          ? (maps.priceFinal[etfSymbol] / maps.priceNow[etfSymbol]) - 1
          : NaN;
        const out = evaluateStructurePnl({
          baseLegs,
          optionLegs,
          priceNow: maps.priceNow,
          priceFinal: maps.priceFinal,
          ttxDays,
          elapsedYears: horizonYears,
          annualBorrowBySymbol: (params && params.annualBorrowBySymbol) || {},
          defaultBorrowAnnual: (params && params.defaultBorrowAnnual) ?? 0,
          shortProceedsRateAnnual: (params && params.shortProceedsRateAnnual) ?? 0,
          volMap: maps.volMap,
          rate,
        });
        return { ok: true, pnl: out.totalPnl, etfRet, undRet };
      });
      return { sigmaMultiple: row && row.sigmaMultiple, underlyingReturn: undRet, cells };
    });
    return { ok: true, rows };
  }

  function percentile(sorted, p) {
    if (!Array.isArray(sorted) || !sorted.length) return NaN;
    const pp = clamp(toNum(p), 0, 1);
    const idx = (sorted.length - 1) * pp;
    const lo = Math.floor(idx);
    const hi = Math.ceil(idx);
    if (lo === hi) return sorted[lo];
    const w = idx - lo;
    return sorted[lo] * (1 - w) + sorted[hi] * w;
  }

  function maxDrawdown(path) {
    if (!Array.isArray(path) || path.length < 2) return NaN;
    let peak = path[0];
    let mdd = 0;
    for (let i = 1; i < path.length; i += 1) {
      peak = Math.max(peak, path[i]);
      const dd = (peak - path[i]) / Math.max(1e-12, peak);
      if (dd > mdd) mdd = dd;
    }
    return mdd;
  }

  function evaluateTradeMonteCarlo(params) {
    const etfPaths = Array.isArray(params && params.etfPaths) ? params.etfPaths : [];
    const underlyingPaths = Array.isArray(params && params.underlyingPaths) ? params.underlyingPaths : [];
    const pathsBySymbol = { ...((params && params.pathsBySymbol) || {}) };
    const baseLegs = Array.isArray(params && params.baseLegs) ? params.baseLegs : [];
    const optionLegs = Array.isArray(params && params.optionLegs) ? params.optionLegs : [];
    const underlyingSymbol = String((params && params.underlyingSymbol) || "").toUpperCase();
    const etfSymbol = String((params && params.etfSymbol) || "").toUpperCase();
    const spotBySymbol = resolveSpotBySymbol(params);
    const leverageBySymbol = resolveLeverageBySymbol(params);
    const spotUnderlying = toNum(spotBySymbol[underlyingSymbol]);
    const spotEtf = toNum(spotBySymbol[etfSymbol]);
    const ttxDaysStart = Math.max(0, toNum(params && params.ttxDaysStart));
    const horizonYears = Math.max(1e-8, toNum(params && params.horizonYears));
    const volAnnual = Math.max(0.01, toNum(params && params.volAnnual));
    const rate = Number.isFinite(toNum(params && params.rate)) ? toNum(params.rate) : 0;
    const annualBorrowBySymbol = (params && params.annualBorrowBySymbol) || {};
    const defaultBorrowAnnual = Number.isFinite(toNum(params && params.defaultBorrowAnnual)) ? Math.max(0, toNum(params.defaultBorrowAnnual)) : 0;
    const shortProceedsRateAnnual = Number.isFinite(toNum(params && params.shortProceedsRateAnnual)) ? toNum(params.shortProceedsRateAnnual) : 0;

    if (!underlyingPaths.length) return { ok: false, error: "Monte Carlo paths missing." };
    const usedSymbols = symbolsFromLegs(baseLegs, optionLegs, underlyingSymbol);
    if (underlyingSymbol) pathsBySymbol[underlyingSymbol] = underlyingPaths;
    if (etfSymbol && etfPaths.length && !pathsBySymbol[etfSymbol]) {
      pathsBySymbol[etfSymbol] = etfPaths;
    }
    for (const sym of usedSymbols) {
      if (sym === underlyingSymbol || pathsBySymbol[sym]) continue;
      const lev = toNum(leverageBySymbol[sym]);
      if (!Number.isFinite(lev)) continue;
      pathsBySymbol[sym] = underlyingPaths.map((up) => etfPathFromUnderlyingPath(up, lev, volAnnual, horizonYears));
    }

    const n = underlyingPaths.length;
    const steps = Math.max(1, underlyingPaths[0].length - 1);
    const horizonDays = Math.round(horizonYears * 252);

    const terminal = [];
    const mdds = [];
    for (let p = 0; p < n; p += 1) {
      const pnlPath = new Array(steps + 1);
      for (let t = 0; t <= steps; t += 1) {
        const ttxRemain = Math.max(0, ttxDaysStart - ((t / steps) * horizonDays));
        const priceNow = {};
        const priceFinal = {};
        const volMap = {};
        for (const sym of usedSymbols) {
          const spot0 = toNum(spotBySymbol[sym]);
          const symPaths = pathsBySymbol[sym];
          if (!Number.isFinite(spot0) || !Array.isArray(symPaths) || !symPaths[p]) continue;
          priceNow[sym] = spot0;
          priceFinal[sym] = spot0 * symPaths[p][t];
          volMap[sym] = sym === underlyingSymbol
            ? volAnnual
            : volAnnual * Math.max(0.2, Math.abs(toNum(leverageBySymbol[sym]) || 1));
        }
        const out = evaluateStructurePnl({
          baseLegs,
          optionLegs,
          priceNow,
          priceFinal,
          ttxDays: ttxRemain,
          elapsedYears: (t / steps) * horizonYears,
          ttxDaysIsRemaining: true,
          annualBorrowBySymbol,
          defaultBorrowAnnual,
          shortProceedsRateAnnual,
          volMap,
          rate,
        });
        pnlPath[t] = out.totalPnl;
      }
      terminal.push(pnlPath[steps]);
      mdds.push(maxDrawdown(pnlPath));
    }

    const sorted = terminal.slice().sort((a, b) => a - b);
    const mean = terminal.reduce((a, b) => a + b, 0) / terminal.length;
    const worstN = Math.max(1, Math.floor(0.05 * sorted.length));
    const cvar5 = sorted.slice(0, worstN).reduce((a, b) => a + b, 0) / worstN;
    const sortedDd = mdds.slice().sort((a, b) => a - b);
    return {
      ok: true,
      summary: {
        meanTerminal: mean,
        medianTerminal: percentile(sorted, 0.5),
        p05: percentile(sorted, 0.05),
        p95: percentile(sorted, 0.95),
        cvar5,
        probLoss: terminal.filter((x) => x < 0).length / terminal.length,
        ddMean: mdds.reduce((a, b) => a + b, 0) / mdds.length,
        ddP95: percentile(sortedDd, 0.95),
      },
      terminal,
    };
  }

  function buildTtxSensitivity(params) {
    const ttxList = Array.isArray(params && params.ttxList) ? params.ttxList : [1, 7, 14, 30, 60, 90];
    const out = [];
    for (const d of ttxList) {
      const grid = buildTradeScenarioGrid({ ...params, ttxDays: d });
      if (!grid.ok || !grid.rows.length || !grid.rows[0].cells.length) {
        out.push({ ttxDays: d, pnl: NaN });
        continue;
      }
      const midR = Math.floor(grid.rows.length / 2);
      const midC = Math.floor(grid.rows[midR].cells.length / 2);
      out.push({ ttxDays: d, pnl: grid.rows[midR].cells[midC].pnl });
    }
    return out;
  }

  const exported = {
    bsPrice,
    optionPayoffAtExpiry,
    equityLegPnl,
    optionLegPnl,
    evaluateStructurePnl,
    etfReturnFromUnderlying,
    etfPathFromUnderlyingPath,
    buildTradePriceMaps,
    buildTradeScenarioGrid,
    evaluateTradeMonteCarlo,
    buildTtxSensitivity,
  };

  if (typeof module !== "undefined" && module.exports) module.exports = exported;
  if (globalObj) globalObj.TradeLab = exported;
})(typeof window !== "undefined" ? window : globalThis);

