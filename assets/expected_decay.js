/* global window, module */
(function initExpectedDecay(globalObj) {
  const DAYS_PER_YEAR = 252;
  const WEEKS_PER_YEAR = 52;
  const MONTHS_PER_YEAR = 12;

  function toFiniteNumber(value) {
    if (typeof value === "number") {
      return Number.isFinite(value) ? value : NaN;
    }
    if (typeof value === "string") {
      const cleaned = value.trim();
      if (cleaned === "") return NaN;
      const parsed = Number(cleaned);
      return Number.isFinite(parsed) ? parsed : NaN;
    }
    return NaN;
  }

  function parseSigmaAnnual(rawSigma) {
    const sigma = toFiniteNumber(rawSigma);
    if (!Number.isFinite(sigma) || sigma < 0) return NaN;
    // Accept both decimal (1.3) and percent-style (130).
    return sigma > 10 ? sigma / 100 : sigma;
  }

  function periodToYears(periodValue, periodUnit) {
    const value = toFiniteNumber(periodValue);
    if (!Number.isFinite(value) || value < 0) return NaN;

    const unit = String(periodUnit || "").toLowerCase();
    if (unit === "days") return value / DAYS_PER_YEAR;
    if (unit === "weeks") return value / WEEKS_PER_YEAR;
    if (unit === "months") return value / MONTHS_PER_YEAR;
    if (unit === "years") return value;
    return NaN;
  }

  function computeExpectedDecay(params) {
    const beta = toFiniteNumber(params && params.beta);
    const sigmaAnnual = parseSigmaAnnual(params && params.sigmaAnnual);
    const years = periodToYears(params && params.periodValue, params && params.periodUnit);

    if (!Number.isFinite(beta) || !Number.isFinite(sigmaAnnual) || !Number.isFinite(years)) {
      return {
        ok: false,
        error: "Invalid input(s).",
      };
    }

    const annualExpectedDecay = 0.5 * Math.abs(beta) * Math.abs(beta - 1) * (sigmaAnnual ** 2);
    const periodExpectedDecay = annualExpectedDecay * years;

    return {
      ok: true,
      beta,
      sigmaAnnual,
      periodYears: years,
      annualExpectedDecay,
      periodExpectedDecay,
    };
  }

  const exported = {
    computeExpectedDecay,
    parseSigmaAnnual,
    periodToYears,
  };

  if (typeof module !== "undefined" && module.exports) {
    module.exports = exported;
  }
  if (globalObj) {
    globalObj.computeExpectedDecay = computeExpectedDecay;
    globalObj.parseSigmaAnnual = parseSigmaAnnual;
    globalObj.periodToYears = periodToYears;
  }
})(typeof window !== "undefined" ? window : globalThis);
