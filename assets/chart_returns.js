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

  /**
   * Reconcile live spot to window-end close basis across a possible split.
   * When live/end already match (continuous Yahoo through a reverse split), ignore
   * stale corp-action factors that would wrongly scale live down ~6×.
   */
  function inferSplitFactorEndToLive(liveSpot, endClose, knownFactor, relTol = 0.075) {
    const live = Number(liveSpot);
    const end = Number(endClose);
    if (!Number.isFinite(live) || !Number.isFinite(end) || live <= 0 || end <= 0) return 1;
    const ratio = live / end;
    if (Math.abs(ratio - 1) <= 0.03) return 1;
    const k = Number(knownFactor);
    if (Number.isFinite(k) && k > 0 && k !== 1) {
      if (Math.abs(live / k / end - 1) <= Math.max(0.05, relTol)) return k;
    }
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
   * Extend Yahoo total-return (adj close) over the window using end adj as anchor.
   */
  function liveTrReturnFromWindow({
    liveSpot,
    endClose,
    endAdjClose,
    trReturn,
    priceReturn,
    splitFactorEndToAsof,
    splitFactorEndToLive,
    dividendYield,
  }) {
    const endRef = Number(endAdjClose) > 0 ? Number(endAdjClose) : Number(endClose);
    const storedTr = Number(trReturn);
    if (Number.isFinite(storedTr) && endRef > 0) {
      const live = Number(liveSpot);
      if (!Number.isFinite(live) || live <= 0) return storedTr;
      const factor = inferSplitFactorEndToLive(live, endRef, splitFactorEndToLive ?? splitFactorEndToAsof);
      const liveOnBasis = live / factor;
      return (liveOnBasis / endRef) * (1 + storedTr) - 1;
    }
    return liveAdjReturnFromWindow({
      liveSpot,
      endClose,
      priceReturn,
      splitFactorEndToAsof,
      splitFactorEndToLive,
      dividendYield,
    });
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
    liveTrReturnFromWindow,
    liveAdjReturnFromWindow,
    splitAdjustedDividendYield,
  };
});
