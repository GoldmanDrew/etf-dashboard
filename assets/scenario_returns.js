/* global window, module */
(function initScenarioReturns(globalObj) {
  const TRADING_DAYS = 252;
  const DEFAULT_VOL_MULTIPLIERS = [0.6, 0.8, 1.0, 1.2, 1.4];
  const DEFAULT_SHOCK_MULTIPLIERS = [-1, -2 / 3, -1 / 3, 0, 1 / 3, 2 / 3, 1];

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
    const minReturn = toFiniteNumber((params && params.minReturn) ?? -0.99);
    const maxReturn = toFiniteNumber((params && params.maxReturn) ?? 8.0);

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

    const lo = Number.isFinite(minReturn) ? minReturn : -0.99;
    const hi = Number.isFinite(maxReturn) ? maxReturn : 8.0;
    const value = clamp(raw, lo, hi);
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
    const minReturn = toFiniteNumber((params && params.minReturn) ?? -0.99);
    const maxReturn = toFiniteNumber((params && params.maxReturn) ?? 8.0);

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

  const exported = {
    horizonToYears,
    computeRealizedVolFromReturns,
    computeEwmaVolFromReturns,
    buildVolScenarios,
    buildShockRows,
    estimateEtfReturn,
    buildScenarioGrid,
  };

  if (typeof module !== "undefined" && module.exports) {
    module.exports = exported;
  }
  if (globalObj) {
    globalObj.ScenarioReturns = exported;
  }
})(typeof window !== "undefined" ? window : globalThis);
