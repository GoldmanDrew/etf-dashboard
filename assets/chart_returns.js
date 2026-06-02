/**
 * Chart-page return helpers — split-aware live spot vs window returns.
 * Used by index.html and tests/test_chart_returns.js.
 */
(function (root, factory) {
  const api = factory();
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = api;
  } else {
    root.ChartReturns = api;
  }
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  const INTEGER_SPLIT_FACTORS = [2, 3, 4, 5, 6, 10, 15, 20, 25, 50];
  const SPLIT_RATIOS = Array.from(
    new Set(
      INTEGER_SPLIT_FACTORS.concat(INTEGER_SPLIT_FACTORS.map((f) => 1 / f)),
    ),
  ).sort((a, b) => b - a);

  function nearestSplitRatio(observed, relTol = 0.075) {
    const x = Number(observed);
    if (!Number.isFinite(x) || x <= 0) return null;
    let bestR = null;
    let bestErr = 1e9;
    for (const r of SPLIT_RATIOS) {
      const err = Math.abs(x / r - 1);
      if (err < bestErr) {
        bestErr = err;
        bestR = r;
      }
    }
    if (bestR == null || bestErr > relTol) return null;
    return bestR;
  }

  function inferSplitFactorEndToLive(liveSpot, endClose, knownFactor, relTol = 0.075) {
    const k = Number(knownFactor);
    if (Number.isFinite(k) && k > 0) return k;
    const live = Number(liveSpot);
    const end = Number(endClose);
    if (!Number.isFinite(live) || !Number.isFinite(end) || live <= 0 || end <= 0) return 1;
    const ratio = live / end;
    if (Math.abs(ratio - 1) <= 0.02) return 1;
    const guessed = nearestSplitRatio(ratio, relTol);
    return guessed != null ? guessed : 1;
  }

  /**
   * Chain live spot onto window-end basis, then extend stored window price return.
   */
  function livePriceReturnFromWindow({
    liveSpot,
    endClose,
    priceReturn,
    splitFactorEndToAsof,
    splitFactorEndToLive,
  }) {
    const live = Number(liveSpot);
    const end = Number(endClose);
    const stored = Number(priceReturn);
    if (!Number.isFinite(live) || !Number.isFinite(end) || end <= 0) {
      return Number.isFinite(stored) ? stored : null;
    }
    const factor = inferSplitFactorEndToLive(live, end, splitFactorEndToLive ?? splitFactorEndToAsof);
    const liveOnEndBasis = live / factor;
    if (Number.isFinite(stored)) {
      return (liveOnEndBasis / end) * (1 + stored) - 1;
    }
    return (liveOnEndBasis / end) - 1;
  }

  /**
   * Total return with explicit dividend yield (fraction of split-adjusted start).
   */
  function liveAdjReturnFromWindow(args) {
    const priceRet = livePriceReturnFromWindow(args);
    const divYield = Number(args?.dividendYield);
    if (!Number.isFinite(priceRet)) return null;
    if (!Number.isFinite(divYield)) return priceRet;
    return priceRet + divYield;
  }

  function splitAdjustedDividendYield(totalDividends, startClose, splitFactorStartToEnd) {
    const div = Number(totalDividends);
    const start = Number(startClose);
    const fac = Number(splitFactorStartToEnd);
    const denom = start * (Number.isFinite(fac) && fac > 0 ? fac : 1);
    if (!Number.isFinite(div) || !Number.isFinite(denom) || denom <= 0) return null;
    return div / denom;
  }

  return {
    SPLIT_RATIOS,
    nearestSplitRatio,
    inferSplitFactorEndToLive,
    livePriceReturnFromWindow,
    liveAdjReturnFromWindow,
    splitAdjustedDividendYield,
  };
});
