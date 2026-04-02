/* global window, module */
(function initScenarioReturns(globalObj) {
  const TRADING_DAYS = 252;
  const DEFAULT_VOL_MULTIPLIERS = [0.6, 0.8, 1.0, 1.2, 1.4];
  const DEFAULT_SHOCK_MULTIPLIERS = [-3, -1, -1 / 3, 0, 1 / 3, 1, 3];

  function toFiniteNumber(value) {
    if (typeof value === "number") return Number.isFinite(value) ? value : NaN;
    if (typeof value === "string") {
      const s = value.trim();
      if (!s) return NaN;
      const n = Number(s);
      return Number.isFinite(n) ? n : NaN;
    }
    return NaN;
  }

  function clamp(value, lo, hi) {
    return Math.max(lo, Math.min(hi, value));
  }

  function horizonToYears(horizonKey) {
    const k = String(horizonKey || "").toUpperCase();
    if (k === "1M") return 1 / 12;
    if (k === "3M") return 3 / 12;
    if (k === "6M") return 6 / 12;
    if (k === "1Y") return 1;
    return NaN;
  }

  function computeRealizedVolFromReturns(logReturns) {
    if (!Array.isArray(logReturns) || logReturns.length < 2) return NaN;
    const vals = logReturns.map(toFiniteNumber).filter(Number.isFinite);
    if (vals.length < 2) return NaN;
    const mean = vals.reduce((a, b) => a + b, 0) / vals.length;
    const variance = vals.reduce((a, b) => a + ((b - mean) ** 2), 0) / (vals.length - 1);
    if (!Number.isFinite(variance) || variance < 0) return NaN;
    return Math.sqrt(variance) * Math.sqrt(TRADING_DAYS);
  }

  function computeEwmaVolFromReturns(logReturns, lambda = 0.94) {
    if (!Array.isArray(logReturns) || logReturns.length < 2) return NaN;
    const vals = logReturns.map(toFiniteNumber).filter(Number.isFinite);
    if (vals.length < 2) return NaN;
    const lam = clamp(toFiniteNumber(lambda), 0, 0.999999);
    let ewmaVar = vals[0] * vals[0];
    for (let i = 1; i < vals.length; i += 1) {
      ewmaVar = lam * ewmaVar + (1 - lam) * vals[i] * vals[i];
    }
    if (!Number.isFinite(ewmaVar) || ewmaVar < 0) return NaN;
    return Math.sqrt(ewmaVar) * Math.sqrt(TRADING_DAYS);
  }

  function buildVolScenarios(baseVolAnnual, multipliers = DEFAULT_VOL_MULTIPLIERS) {
    const base = toFiniteNumber(baseVolAnnual);
    if (!Number.isFinite(base) || base <= 0) return [];
    return (multipliers || [])
      .map((m) => toFiniteNumber(m))
      .filter((m) => Number.isFinite(m) && m > 0)
      .map((m) => ({
        multiplier: m,
        sigmaAnnual: base * m,
      }));
  }

  function buildShockRows(bestVolAnnual, horizonYears, multipliers = DEFAULT_SHOCK_MULTIPLIERS) {
    const sigma = toFiniteNumber(bestVolAnnual);
    const years = toFiniteNumber(horizonYears);
    if (!Number.isFinite(sigma) || sigma <= 0 || !Number.isFinite(years) || years <= 0) return [];
    const horizonSigma = sigma * Math.sqrt(years);
    return (multipliers || [])
      .map((m) => toFiniteNumber(m))
      .filter(Number.isFinite)
      .map((m) => ({
        sigmaMultiple: m,
        // Map shocks in log-return space so simple return is always > -100%.
        underlyingReturn: Math.exp(m * horizonSigma) - 1,
      }));
  }

  function estimateEtfReturn(params) {
    const leverage = toFiniteNumber(params && params.leverage);
    const underlyingReturn = toFiniteNumber(params && params.underlyingReturn);
    const sigmaAnnual = toFiniteNumber(params && params.sigmaAnnual);
    const horizonYears = toFiniteNumber(params && params.horizonYears);
    const annualCarryDrag = toFiniteNumber((params && params.annualCarryDrag) ?? 0);
    const minReturn = toFiniteNumber((params && params.minReturn) ?? -0.9999);
    const maxReturn = toFiniteNumber(params && params.maxReturn);

    if (
      !Number.isFinite(leverage) ||
      !Number.isFinite(underlyingReturn) ||
      !Number.isFinite(sigmaAnnual) || sigmaAnnual < 0 ||
      !Number.isFinite(horizonYears) || horizonYears <= 0
    ) {
      return { ok: false, error: "Invalid model inputs." };
    }

    const carry = Number.isFinite(annualCarryDrag) ? annualCarryDrag : 0;
    const dragLog = 0.5 * leverage * (leverage - 1) * (sigmaAnnual ** 2) * horizonYears;
    const onePlusUnderlying = 1 + underlyingReturn;
    if (!Number.isFinite(onePlusUnderlying) || onePlusUnderlying <= 0) {
      return { ok: false, error: "Invalid underlying return for log compounding." };
    }
    const underlyingLog = Math.log(onePlusUnderlying);
    const etfLog = (leverage * underlyingLog) - dragLog - (carry * horizonYears);
    const raw = Math.exp(etfLog) - 1;
    if (!Number.isFinite(raw)) return { ok: false, error: "Non-finite model output." };

    const lo = Number.isFinite(minReturn) ? minReturn : -0.9999;
    const hi = Number.isFinite(maxReturn) ? maxReturn : NaN;
    let value = raw;
    if (Number.isFinite(lo)) value = Math.max(value, lo);
    if (Number.isFinite(hi)) value = Math.min(value, hi);
    return {
      ok: true,
      raw,
      value,
      clamped: value !== raw,
      drag: dragLog,
      etfLog,
    };
  }

  function buildScenarioGrid(params) {
    const leverage = toFiniteNumber(params && params.leverage);
    const bestVolAnnual = toFiniteNumber(params && params.bestVolAnnual);
    const horizonYears = toFiniteNumber(params && params.horizonYears);
    const annualCarryDrag = toFiniteNumber((params && params.annualCarryDrag) ?? 0);
    const minReturn = toFiniteNumber((params && params.minReturn) ?? -0.9999);
    const maxReturn = toFiniteNumber(params && params.maxReturn);

    if (!Number.isFinite(leverage) || !Number.isFinite(bestVolAnnual) || bestVolAnnual <= 0 || !Number.isFinite(horizonYears) || horizonYears <= 0) {
      return { ok: false, error: "Missing required scenario inputs." };
    }

    const volColumns = buildVolScenarios(bestVolAnnual, (params && params.volMultipliers) || DEFAULT_VOL_MULTIPLIERS);
    const shockRows = buildShockRows(bestVolAnnual, horizonYears, (params && params.shockMultipliers) || DEFAULT_SHOCK_MULTIPLIERS);
    if (!volColumns.length || !shockRows.length) return { ok: false, error: "No valid scenario rows/columns." };

    const rows = shockRows.map((shock) => ({
      sigmaMultiple: shock.sigmaMultiple,
      underlyingReturn: shock.underlyingReturn,
      cells: volColumns.map((vol) => estimateEtfReturn({
        leverage,
        underlyingReturn: shock.underlyingReturn,
        sigmaAnnual: vol.sigmaAnnual,
        horizonYears,
        annualCarryDrag,
        minReturn,
        maxReturn,
      })),
    }));

    return {
      ok: true,
      leverage,
      bestVolAnnual,
      horizonYears,
      annualCarryDrag: Number.isFinite(annualCarryDrag) ? annualCarryDrag : 0,
      volColumns,
      rows,
    };
  }

  function percentileFromSorted(sortedVals, p) {
    if (!Array.isArray(sortedVals) || sortedVals.length === 0) return NaN;
    const pp = clamp(toFiniteNumber(p), 0, 1);
    const idx = (sortedVals.length - 1) * pp;
    const lo = Math.floor(idx);
    const hi = Math.ceil(idx);
    if (lo === hi) return sortedVals[lo];
    const w = idx - lo;
    return sortedVals[lo] * (1 - w) + sortedVals[hi] * w;
  }

  function mulberry32(seed) {
    let a = (seed >>> 0) || 1;
    return function next() {
      a += 0x6D2B79F5;
      let t = a;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  function normalFactory(rng) {
    let spare = null;
    return function randn() {
      if (spare != null) {
        const v = spare;
        spare = null;
        return v;
      }
      let u = 0;
      let v = 0;
      while (u <= 1e-12) u = rng();
      while (v <= 1e-12) v = rng();
      const mag = Math.sqrt(-2.0 * Math.log(u));
      const z0 = mag * Math.cos(2 * Math.PI * v);
      const z1 = mag * Math.sin(2 * Math.PI * v);
      spare = z1;
      return z0;
    };
  }

  function computeMaxDrawdown(pathWealth) {
    if (!Array.isArray(pathWealth) || pathWealth.length < 2) return NaN;
    let peak = pathWealth[0];
    let maxDd = 0;
    for (let i = 1; i < pathWealth.length; i += 1) {
      const w = pathWealth[i];
      if (w > peak) peak = w;
      const dd = 1 - (w / peak);
      if (dd > maxDd) maxDd = dd;
    }
    return maxDd;
  }

  function parseThresholdList(raw, fallback = [0.25, 0.5, 1.0]) {
    if (Array.isArray(raw)) {
      const vals = raw.map(toFiniteNumber).filter(Number.isFinite);
      // Array inputs are already decimals (e.g. 0.25 for 25%).
      return vals.filter((x) => x >= 0).sort((a, b) => a - b);
    }
    if (typeof raw === "string") {
      const vals = raw
        .split(",")
        .map((s) => toFiniteNumber(s))
        .filter(Number.isFinite)
        .map((x) => (x > 2 ? x / 100 : x))
        .filter((x) => x >= 0)
        .sort((a, b) => a - b);
      return vals.length ? vals : fallback.slice();
    }
    return fallback.slice();
  }

  function simulateMonteCarloPaths(params) {
    const pathCount = Math.round(toFiniteNumber((params && params.pathCount) ?? 500));
    const sigmaAnnual = toFiniteNumber(params && params.sigmaAnnual);
    const leverage = toFiniteNumber(params && params.leverage);
    const horizonYears = toFiniteNumber(params && params.horizonYears);
    const annualCarryDrag = toFiniteNumber((params && params.annualCarryDrag) ?? 0);
    const seed = Math.round(toFiniteNumber((params && params.seed) ?? 42));
    // Short-mode ruin threshold in short-return terms, e.g. -0.8 = -80% short return.
    const ruinThreshold = toFiniteNumber((params && params.ruinThreshold) ?? -0.8);
    const upsideThresholds = parseThresholdList((params && params.upsideThresholds), [0.25, 0.5, 1.0]);

    if (!Number.isFinite(pathCount) || pathCount < 1) return { ok: false, error: "Invalid path count." };
    if (!Number.isFinite(sigmaAnnual) || sigmaAnnual <= 0) return { ok: false, error: "Invalid sigma." };
    if (!Number.isFinite(leverage)) return { ok: false, error: "Invalid leverage." };
    if (!Number.isFinite(horizonYears) || horizonYears <= 0) return { ok: false, error: "Invalid horizon." };

    const steps = Math.max(1, Math.round(horizonYears * TRADING_DAYS));
    const dt = horizonYears / steps;
    const sdStep = sigmaAnnual * Math.sqrt(dt);
    const dragStep = 0.5 * leverage * (leverage - 1) * (sigmaAnnual ** 2) * dt;
    const carryAnnual = Number.isFinite(annualCarryDrag) ? annualCarryDrag : 0;
    const rng = mulberry32(seed);
    const randn = normalFactory(rng);

    const paths = [];
    const terminalReturns = [];
    const maxDrawdowns = [];
    const pathVols = [];
    const breachShortRuin = [];
    const upsideTerminalCounts = new Array(upsideThresholds.length).fill(0);
    const upsideAnyCounts = new Array(upsideThresholds.length).fill(0);
    const stepValues = Array.from({ length: steps + 1 }, () => []);

    for (let p = 0; p < pathCount; p += 1) {
      const wealthPath = new Array(steps + 1);
      wealthPath[0] = 1;
      stepValues[0].push(1);
      let breachedShort = false;
      const pathLogRets = [];
      const crossed = new Array(upsideThresholds.length).fill(false);

      for (let t = 1; t <= steps; t += 1) {
        const z = randn();
        const undLogRet = sdStep * z;
        const etfLogRet = (leverage * undLogRet) - dragStep;
        const next = wealthPath[t - 1] * Math.exp(etfLogRet);
        wealthPath[t] = Number.isFinite(next) ? next : wealthPath[t - 1];
        pathLogRets.push(etfLogRet);
        stepValues[t].push(wealthPath[t]);

        const simpleRet = wealthPath[t] - 1;
        const carryAccrued = carryAnnual * (t * dt);
        const shortRet = -simpleRet - carryAccrued;
        if (!breachedShort && shortRet <= ruinThreshold) breachedShort = true;
        for (let k = 0; k < upsideThresholds.length; k += 1) {
          if (!crossed[k] && simpleRet >= upsideThresholds[k]) crossed[k] = true;
        }
      }

      paths.push(wealthPath);
      const terminal = wealthPath[steps] - 1;
      terminalReturns.push(terminal);
      maxDrawdowns.push(computeMaxDrawdown(wealthPath));
      const pv = computeRealizedVolFromReturns(pathLogRets);
      pathVols.push(Number.isFinite(pv) ? pv : NaN);
      breachShortRuin.push(breachedShort ? 1 : 0);
      for (let k = 0; k < upsideThresholds.length; k += 1) {
        if (terminal >= upsideThresholds[k]) upsideTerminalCounts[k] += 1;
        if (crossed[k]) upsideAnyCounts[k] += 1;
      }
    }

    const sortedTerm = terminalReturns.slice().sort((a, b) => a - b);
    const carryOverHorizon = carryAnnual * horizonYears;
    const shortTerminalReturns = terminalReturns.map((x) => -x - carryOverHorizon);
    const sortedShort = shortTerminalReturns.slice().sort((a, b) => a - b);
    const sortedDd = maxDrawdowns.slice().sort((a, b) => a - b);
    const worstN = Math.max(1, Math.floor(0.05 * sortedTerm.length));
    const cvar5 = sortedTerm.slice(0, worstN).reduce((a, b) => a + b, 0) / worstN;
    const cvar5Short = sortedShort.slice(0, worstN).reduce((a, b) => a + b, 0) / worstN;
    const finiteVols = pathVols.filter(Number.isFinite);
    const meanTerminal = sortedTerm.reduce((a, b) => a + b, 0) / sortedTerm.length;
    const meanShort = sortedShort.reduce((a, b) => a + b, 0) / sortedShort.length;

    const overlays = {
      mean: [],
      p05: [],
      p25: [],
      p50: [],
      p75: [],
      p95: [],
    };
    for (let t = 0; t <= steps; t += 1) {
      const vals = stepValues[t].slice().sort((a, b) => a - b);
      const mean = vals.reduce((a, b) => a + b, 0) / vals.length;
      overlays.mean.push(mean);
      overlays.p05.push(percentileFromSorted(vals, 0.05));
      overlays.p25.push(percentileFromSorted(vals, 0.25));
      overlays.p50.push(percentileFromSorted(vals, 0.5));
      overlays.p75.push(percentileFromSorted(vals, 0.75));
      overlays.p95.push(percentileFromSorted(vals, 0.95));
    }

    return {
      ok: true,
      settings: {
        pathCount,
        sigmaAnnual,
        leverage,
        horizonYears,
        annualCarryDrag,
        seed,
        steps,
        upsideThresholds,
        ruinThreshold,
      },
      paths,
      overlays,
      summary: {
        meanTerminal,
        medianTerminal: percentileFromSorted(sortedTerm, 0.5),
        stdTerminal: Math.sqrt(
          sortedTerm.reduce((acc, x) => {
            return acc + ((x - meanTerminal) ** 2);
          }, 0) / Math.max(1, sortedTerm.length - 1),
        ),
        minTerminal: sortedTerm[0],
        maxTerminal: sortedTerm[sortedTerm.length - 1],
        p05: percentileFromSorted(sortedTerm, 0.05),
        p25: percentileFromSorted(sortedTerm, 0.25),
        p50: percentileFromSorted(sortedTerm, 0.5),
        p75: percentileFromSorted(sortedTerm, 0.75),
        p95: percentileFromSorted(sortedTerm, 0.95),
        cvar5,
        probLoss: terminalReturns.filter((x) => x < 0).length / terminalReturns.length,
        probRuin: breachShortRuin.reduce((a, b) => a + b, 0) / breachShortRuin.length,
        ddMean: maxDrawdowns.reduce((a, b) => a + b, 0) / maxDrawdowns.length,
        ddP50: percentileFromSorted(sortedDd, 0.5),
        ddP95: percentileFromSorted(sortedDd, 0.95),
        avgPathAnnVol: finiteVols.length ? finiteVols.reduce((a, b) => a + b, 0) / finiteVols.length : NaN,
        upsideTerminalCounts: upsideThresholds.map((thr, i) => ({
          threshold: thr,
          count: upsideTerminalCounts[i],
          pct: upsideTerminalCounts[i] / pathCount,
        })),
        upsideAnyCounts: upsideThresholds.map((thr, i) => ({
          threshold: thr,
          count: upsideAnyCounts[i],
          pct: upsideAnyCounts[i] / pathCount,
        })),
        // Short-centric summary for short-ETF strategy interpretation.
        short: {
          meanTerminal: meanShort,
          medianTerminal: percentileFromSorted(sortedShort, 0.5),
          p05: percentileFromSorted(sortedShort, 0.05),
          p95: percentileFromSorted(sortedShort, 0.95),
          cvar5: cvar5Short,
          probLoss: shortTerminalReturns.filter((x) => x < 0).length / shortTerminalReturns.length,
          probRuin: breachShortRuin.reduce((a, b) => a + b, 0) / breachShortRuin.length,
        },
      },
    };
  }

  const exported = {
    horizonToYears,
    computeRealizedVolFromReturns,
    computeEwmaVolFromReturns,
    buildVolScenarios,
    buildShockRows,
    estimateEtfReturn,
    buildScenarioGrid,
    simulateMonteCarloPaths,
    parseThresholdList,
  };

  if (typeof module !== "undefined" && module.exports) {
    module.exports = exported;
  }
  if (globalObj) {
    globalObj.ScenarioReturns = exported;
  }
})(typeof window !== "undefined" ? window : globalThis);
