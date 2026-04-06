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

  function optionLegPnl(leg, spotNow, spotFinal, ttxDays, volFallback, rate) {
    const contracts = Math.max(0, toNum(leg && leg.contracts));
    const multiplier = Math.max(1, toNum((leg && leg.multiplier) ?? 100));
    const strike = toNum(leg && leg.strike);
    const premium = Number.isFinite(toNum(leg && leg.premium)) ? toNum(leg.premium) : 0;
    const iv = Number.isFinite(toNum(leg && leg.iv)) ? toNum(leg.iv) : volFallback;
    if (!Number.isFinite(strike) || !Number.isFinite(contracts) || contracts <= 0) return { pnl: 0, mark: 0, payoff: 0 };
    const tYears = Math.max(0, toNum(ttxDays)) / 365;
    const type = String((leg && leg.type) || "call").toLowerCase();
    const mark = bsPrice({ spot: spotNow, strike, vol: Math.max(0.01, iv), ttmYears: tYears, rate, type });
    const payoff = optionPayoffAtExpiry({ spot: spotFinal, strike, type });
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
    const ttxDays = Number.isFinite(toNum(params && params.ttxDays)) ? toNum(params.ttxDays) : 0;
    const volMap = (params && params.volMap) || {};
    const rate = Number.isFinite(toNum(params && params.rate)) ? toNum(params.rate) : 0;

    let total = 0;
    const legRows = [];

    for (const leg of baseLegs) {
      const sym = String((leg && leg.symbol) || "").toUpperCase();
      if (!sym) continue;
      const entry = Number.isFinite(toNum(leg && leg.entry)) ? toNum(leg.entry) : toNum(priceNow[sym]);
      const finalSpot = toNum(priceFinal[sym]);
      const pnl = equityLegPnl(leg, entry, finalSpot);
      total += pnl;
      legRows.push({ kind: "equity", symbol: sym, pnl });
    }

    for (const leg of optionLegs) {
      const sym = String((leg && leg.symbol) || "").toUpperCase();
      if (!sym) continue;
      const spotNow = toNum(priceNow[sym]);
      const spotFinal = toNum(priceFinal[sym]);
      const volFallback = Number.isFinite(toNum(volMap[sym])) ? toNum(volMap[sym]) : 0.6;
      const out = optionLegPnl(leg, spotNow, spotFinal, ttxDays, volFallback, rate);
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

  function buildTradeScenarioGrid(params) {
    const shockRows = Array.isArray(params && params.shockRows) ? params.shockRows : [];
    const volColumns = Array.isArray(params && params.volColumns) ? params.volColumns : [];
    const spotUnderlying = toNum(params && params.spotUnderlying);
    const spotEtf = toNum(params && params.spotEtf);
    const underlyingSymbol = String((params && params.underlyingSymbol) || "").toUpperCase();
    const etfSymbol = String((params && params.etfSymbol) || "").toUpperCase();
    const leverage = toNum(params && params.leverage);
    const horizonYears = toNum(params && params.horizonYears);
    const baseLegs = Array.isArray(params && params.baseLegs) ? params.baseLegs : [];
    const optionLegs = Array.isArray(params && params.optionLegs) ? params.optionLegs : [];
    const ttxDays = toNum(params && params.ttxDays);
    const rate = Number.isFinite(toNum(params && params.rate)) ? toNum(params.rate) : 0;
    const baseVol = Math.max(0.01, toNum(params && params.baseVolAnnual));

    if (!Number.isFinite(spotUnderlying) || !Number.isFinite(spotEtf) || !underlyingSymbol || !etfSymbol) {
      return { ok: false, error: "Missing trade spot inputs." };
    }

    const rows = shockRows.map((row) => {
      const undRet = toNum(row && row.underlyingReturn);
      const undFinal = spotUnderlying * (1 + undRet);
      const cells = volColumns.map((col) => {
        const sigmaAnnual = Math.max(0.01, toNum(col && col.sigmaAnnual));
        const etfRet = etfReturnFromUnderlying({ leverage, underlyingReturn: undRet, sigmaAnnual, horizonYears });
        const etfFinal = spotEtf * (1 + etfRet);
        const out = evaluateStructurePnl({
          baseLegs,
          optionLegs,
          priceNow: { [underlyingSymbol]: spotUnderlying, [etfSymbol]: spotEtf },
          priceFinal: { [underlyingSymbol]: undFinal, [etfSymbol]: etfFinal },
          ttxDays,
          volMap: { [underlyingSymbol]: baseVol, [etfSymbol]: sigmaAnnual * Math.max(0.2, Math.abs(leverage)) },
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
    const baseLegs = Array.isArray(params && params.baseLegs) ? params.baseLegs : [];
    const optionLegs = Array.isArray(params && params.optionLegs) ? params.optionLegs : [];
    const underlyingSymbol = String((params && params.underlyingSymbol) || "").toUpperCase();
    const etfSymbol = String((params && params.etfSymbol) || "").toUpperCase();
    const spotUnderlying = toNum(params && params.spotUnderlying);
    const spotEtf = toNum(params && params.spotEtf);
    const ttxDaysStart = Math.max(0, toNum(params && params.ttxDaysStart));
    const horizonYears = Math.max(1e-8, toNum(params && params.horizonYears));
    const volAnnual = Math.max(0.01, toNum(params && params.volAnnual));
    const rate = Number.isFinite(toNum(params && params.rate)) ? toNum(params.rate) : 0;

    if (!etfPaths.length || !underlyingPaths.length) return { ok: false, error: "Monte Carlo paths missing." };
    const n = Math.min(etfPaths.length, underlyingPaths.length);
    const steps = Math.max(1, etfPaths[0].length - 1);
    const horizonDays = Math.round(horizonYears * 252);

    const terminal = [];
    const mdds = [];
    for (let p = 0; p < n; p += 1) {
      const ep = etfPaths[p];
      const up = underlyingPaths[p];
      const pnlPath = new Array(steps + 1);
      for (let t = 0; t <= steps; t += 1) {
        const ttxRemain = Math.max(0, ttxDaysStart - ((t / steps) * horizonDays));
        const undNow = spotUnderlying * up[t];
        const etfNow = spotEtf * ep[t];
        const out = evaluateStructurePnl({
          baseLegs,
          optionLegs,
          priceNow: { [underlyingSymbol]: spotUnderlying, [etfSymbol]: spotEtf },
          priceFinal: { [underlyingSymbol]: undNow, [etfSymbol]: etfNow },
          ttxDays: ttxRemain,
          volMap: { [underlyingSymbol]: volAnnual, [etfSymbol]: volAnnual },
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
    buildTradeScenarioGrid,
    evaluateTradeMonteCarlo,
    buildTtxSensitivity,
  };

  if (typeof module !== "undefined" && module.exports) module.exports = exported;
  if (globalObj) globalObj.TradeLab = exported;
})(typeof window !== "undefined" ? window : globalThis);

