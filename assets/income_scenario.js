/* global window, module */
/**
 * income_scenario.js
 * ==================
 *
 * Client-side companion of ``scripts/income_schedule.py``.  Consumes the
 * NAV-normalized ``income_distribution_calibration`` block emitted into
 * ``dashboard_data.json`` and produces:
 *
 *   1. A weekly distribution rate ``d`` that scales with the scenario sigma
 *      via the structural capture ratio (Magis Capital April 2026
 *      "Bucket 2 Income ETF Structural Decay" research, cross-fund median
 *      ~0.65).
 *   2. The same mean-field closed-form scenario outputs the existing
 *      ``estimateIncomeStyleScenarioReturn`` produces, but with the
 *      calibrated ``d`` instead of the broken
 *      ``income_yield_trailing_annual = Sum(cash) / current_price``.
 *   3. A week-by-week schedule simulator that replays the historical
 *      template (transparency panel; sanity-check the mean-field).
 *
 * The math (validated within +1.9pp of MC in the research):
 *
 *     L         = expectedPutSpreadLossWeekly(u, sigma, T)
 *     q         = 1 - L - f / 52
 *     navDecay  = 1 - q^N
 *     dist      = d * (1 - q^N) / (1 - q)
 *     netShort  = navDecay - dist - borrow * T
 *
 *     Calibrated d:
 *       ratio_t  = blended_ratio_used    (from calibration block)
 *       bs_t     = expectedPutSpreadLossWeekly(0, sigma_scenario, 1)
 *       d_weekly = ratio_t * bs_t
 *
 * Backwards compatible: when no calibration block is present, callers can
 * fall back to a legacy scalar ``annualIncomeYield`` (kept for one release
 * so older builds keep working).
 */
(function initIncomeScenario(globalObj) {
  const PUT_SPREAD_SHORT_STRIKE = 0.95;
  const PUT_SPREAD_LONG_STRIKE = 0.88;
  const PUT_SPREAD_LEVERAGE = 2.0;
  const WEEKS_PER_YEAR = 52;
  const DEFAULT_EXPENSE_RATIO_ANNUAL = 0.0099;
  const DEFAULT_CROSS_FUND_RATIO = 0.65;
  const DEFAULT_TAIL_ADJUSTMENT_ANNUAL = 0.0;
  const RATIO_FULL_CONFIDENCE_N = 12;

  // Capture-ratio color bands surfaced in the UI.  Lower ratio = larger
  // structural edge for shorts.
  const RATIO_BANDS = {
    strong: 0.50,    // <0.50 -> strong edge
    typical: 0.75,   // 0.50-0.75 -> typical
    weak: 1.00,      // 0.75-1.00 -> weak edge
    // >1.00 -> adverse (fund overdistributes vs BS premium)
  };

  function _toNum(v) {
    if (typeof v === 'number') return Number.isFinite(v) ? v : NaN;
    if (typeof v === 'string') {
      const t = v.trim();
      if (!t) return NaN;
      const n = Number(t);
      return Number.isFinite(n) ? n : NaN;
    }
    return NaN;
  }

  function clamp(v, lo, hi) {
    if (!Number.isFinite(v)) return v;
    if (Number.isFinite(lo) && v < lo) return lo;
    if (Number.isFinite(hi) && v > hi) return hi;
    return v;
  }

  function normalCdf(x) {
    const z = Number(x);
    if (!Number.isFinite(z)) return z < 0 ? 0 : 1;
    const sign = z < 0 ? -1 : 1;
    const a = Math.abs(z) / Math.SQRT2;
    const p = 0.3275911;
    const t = 1 / (1 + p * a);
    const y = 1 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.exp(-a * a);
    return 0.5 * (1 + sign * y);
  }

  /**
   * Expected weekly put-spread loss on the 2x sleeve.
   *
   * Mirror of ``income_schedule.expected_put_spread_loss_weekly`` and the
   * legacy ``expectedPutSpreadLossWeekly`` in ``index.html``.  Kept here so
   * callers loading this module get a self-contained engine; the inline
   * function in index.html will be deprecated once all callers migrate.
   */
  function expectedPutSpreadLossWeekly(params) {
    const u = _toNum(params && params.underlyingReturn);
    const sigma = _toNum(params && params.sigmaAnnual);
    const t = _toNum(params && params.horizonYears);
    if (!Number.isFinite(u) || u <= -0.9999) return null;
    if (!Number.isFinite(sigma) || sigma <= 0) return null;
    if (!Number.isFinite(t) || t <= 0) return null;
    const tau = 1 / WEEKS_PER_YEAR;
    const muAnnual = Math.log1p(u) / t;
    const m = (PUT_SPREAD_LEVERAGE * muAnnual - PUT_SPREAD_LEVERAGE * sigma * sigma) * tau;
    const s = PUT_SPREAD_LEVERAGE * sigma * Math.sqrt(tau);
    if (!Number.isFinite(m) || !Number.isFinite(s) || s <= 0) return null;
    const spreadPut = (k) => {
      const alpha = (Math.log(k) - m) / s;
      const beta = alpha - s;
      const forward = Math.exp(m + 0.5 * s * s);
      return k * normalCdf(alpha) - forward * normalCdf(beta);
    };
    const loss = spreadPut(PUT_SPREAD_SHORT_STRIKE) - spreadPut(PUT_SPREAD_LONG_STRIKE);
    if (!Number.isFinite(loss)) return null;
    return Math.max(0, Math.min(PUT_SPREAD_SHORT_STRIKE - PUT_SPREAD_LONG_STRIKE, loss));
  }

  /**
   * Derive the calibrated weekly distribution rate for a given scenario sigma.
   *
   * @param {object} params
   * @param {number} params.sigmaAnnual          forecast underlying sigma at scenario
   * @param {object} [params.calibration]        ``income_distribution_calibration`` block
   * @param {number} [params.legacyAnnualYield]  fallback when block is absent
   * @param {number} [params.crossFundRatio]     fleet prior (defaults to research 0.65)
   * @returns {{
   *   weeklyDistribution: number|null,
   *   annualizedDistribution: number|null,
   *   ratioUsed: number|null,
   *   ratioSource: string,
   *   bsPremiumWeekly: number|null,
   *   confidence: string,
   *   note: string|null
   * }}
   */
  function calibratedWeeklyDistribution(params) {
    const sigma = _toNum(params && params.sigmaAnnual);
    const calibration = (params && params.calibration) || null;
    const crossFundRatio = _toNum((params && params.crossFundRatio));
    const fallbackRatio = Number.isFinite(crossFundRatio) ? crossFundRatio : DEFAULT_CROSS_FUND_RATIO;
    const legacyAnnual = _toNum(params && params.legacyAnnualYield);

    if (!Number.isFinite(sigma) || sigma <= 0) {
      return {
        weeklyDistribution: null,
        annualizedDistribution: null,
        ratioUsed: null,
        ratioSource: 'invalid_sigma',
        bsPremiumWeekly: null,
        confidence: 'none',
        note: 'Cannot calibrate without a positive scenario sigma.',
      };
    }
    const bs = expectedPutSpreadLossWeekly({
      underlyingReturn: 0,
      sigmaAnnual: sigma,
      horizonYears: 1,
    });

    let ratio = null;
    let ratioSource = 'none';
    let confidence = 'none';

    if (calibration) {
      const blended = _toNum(calibration.blended_ratio_used);
      const fundMedian = _toNum(calibration.fund_ratio_median);
      const conf = String(calibration.fund_ratio_confidence || '').toLowerCase();
      if (Number.isFinite(blended) && blended > 0) {
        ratio = blended;
        ratioSource = conf === 'high' ? 'fund_ratio_high' : `fund_ratio_${conf || 'blended'}`;
        confidence = conf || 'medium';
      } else if (Number.isFinite(fundMedian) && fundMedian > 0) {
        ratio = fundMedian;
        ratioSource = `fund_ratio_${conf || 'medium'}`;
        confidence = conf || 'medium';
      }
    }
    if (ratio == null) {
      ratio = fallbackRatio;
      ratioSource = 'cross_fund_prior';
      confidence = calibration ? 'low' : 'none';
    }
    if (!Number.isFinite(bs) || bs == null) {
      // BS unavailable -> last-ditch: legacy scalar treated as annualized run-rate
      if (Number.isFinite(legacyAnnual) && legacyAnnual > 0) {
        return {
          weeklyDistribution: legacyAnnual / WEEKS_PER_YEAR,
          annualizedDistribution: legacyAnnual,
          ratioUsed: ratio,
          ratioSource: 'legacy_scalar_fallback',
          bsPremiumWeekly: null,
          confidence: 'low',
          note: 'BS premium unavailable; using legacy annual scalar as distribution rate.',
        };
      }
      return {
        weeklyDistribution: null,
        annualizedDistribution: null,
        ratioUsed: ratio,
        ratioSource,
        bsPremiumWeekly: null,
        confidence,
        note: 'BS premium unavailable for this sigma.',
      };
    }
    const dWeekly = ratio * bs;
    return {
      weeklyDistribution: dWeekly,
      annualizedDistribution: dWeekly * WEEKS_PER_YEAR,
      ratioUsed: ratio,
      ratioSource,
      bsPremiumWeekly: bs,
      confidence,
      note: null,
    };
  }

  /**
   * Income scenario closed form using a calibrated weekly distribution rate.
   *
   * Matches the existing ``estimateIncomeStyleScenarioReturn`` in
   * ``index.html`` line-for-line, with two additions:
   *   - ``weeklyDistribution`` is taken directly (not derived from an
   *     annual scalar), so callers can swap calibration sources without
   *     refactoring the math.
   *   - Optional ``tailAdjustmentAnnual`` adds a constant additive on
   *     NAV decay to close the lognormal-vs-Student-t gap (~2pp per
   *     research).  Off by default.
   */
  function estimateIncomeStyleScenarioFromCalibration(params) {
    const underlyingReturn = _toNum(params && params.underlyingReturn);
    const sigmaAnnual = _toNum(params && params.sigmaAnnual);
    const t = _toNum(params && params.horizonYears);
    const weeklyDistribution = _toNum(params && params.weeklyDistribution);
    const annualBorrowCost = _toNum((params && params.annualBorrowCost) ?? 0);
    const expenseRatioAnnual = _toNum((params && params.expenseRatioAnnual) ?? DEFAULT_EXPENSE_RATIO_ANNUAL);
    const tailAdj = _toNum((params && params.tailAdjustmentAnnual) ?? DEFAULT_TAIL_ADJUSTMENT_ANNUAL);

    if (!Number.isFinite(weeklyDistribution) || weeklyDistribution < 0) return null;
    if (!Number.isFinite(t) || t <= 0) return null;
    const weeklySpreadLoss = expectedPutSpreadLossWeekly({
      underlyingReturn,
      sigmaAnnual,
      horizonYears: t,
    });
    if (weeklySpreadLoss == null) return null;
    const weeks = Math.max(1, Math.round(t * WEEKS_PER_YEAR));
    const weeklyExpense = Math.max(0, Number.isFinite(expenseRatioAnnual) ? expenseRatioAnnual : 0) / WEEKS_PER_YEAR;
    const q = clamp(1 - weeklySpreadLoss - weeklyExpense, 0.0001, 1.5);
    const navEndRatio = Math.pow(q, weeks);
    let navDecay = 1 - navEndRatio;
    if (Number.isFinite(tailAdj) && tailAdj !== 0) {
      navDecay += tailAdj * t;
    }
    const geomSum = Math.abs(1 - q) < 1e-9 ? weeks : (1 - Math.pow(q, weeks)) / (1 - q);
    const distributionsPaid = weeklyDistribution * geomSum;
    const borrowCost = Number.isFinite(annualBorrowCost) && annualBorrowCost > 0
      ? annualBorrowCost * t
      : 0;
    const netShortPnl = navDecay - distributionsPaid - borrowCost;
    return {
      weeklySpreadLoss,
      weeklyDistribution,
      navDecay,
      navReturn: -navDecay,
      distributionsPaid,
      borrowCost,
      netShortPnl,
      longTotalReturn: -navDecay + distributionsPaid,
      weeks,
      tailAdjustmentApplied: Number.isFinite(tailAdj) ? tailAdj : 0,
    };
  }

  /**
   * Week-by-week schedule simulator (Phase 4 transparency panel).
   *
   * Replays the calibration's ``template_yields`` for ``weeks`` steps,
   * applying ``yield_t * NAV_{t-1}`` cash and the same sleeve decay used
   * by the closed form.  Should produce identical aggregate numbers to
   * ``estimateIncomeStyleScenarioFromCalibration`` when ``template_yields``
   * is constant.
   */
  function simulateIncomeSchedule(params) {
    const underlyingReturn = _toNum(params && params.underlyingReturn);
    const sigmaAnnual = _toNum(params && params.sigmaAnnual);
    const t = _toNum(params && params.horizonYears);
    const annualBorrowCost = _toNum((params && params.annualBorrowCost) ?? 0);
    const expenseRatioAnnual = _toNum((params && params.expenseRatioAnnual) ?? DEFAULT_EXPENSE_RATIO_ANNUAL);
    const template = Array.isArray(params && params.templateYields) ? params.templateYields : [];
    if (!template.length) return null;
    if (!Number.isFinite(t) || t <= 0) return null;
    const weeklyLoss = expectedPutSpreadLossWeekly({
      underlyingReturn,
      sigmaAnnual,
      horizonYears: t,
    });
    if (weeklyLoss == null) return null;
    const weeks = Math.max(1, Math.round(t * WEEKS_PER_YEAR));
    const weeklyExpense = Math.max(0, Number.isFinite(expenseRatioAnnual) ? expenseRatioAnnual : 0) / WEEKS_PER_YEAR;
    const q = clamp(1 - weeklyLoss - weeklyExpense, 0.0001, 1.5);
    let nav = 1.0;
    let cash = 0;
    const path = new Array(weeks + 1);
    const cashPath = new Array(weeks + 1);
    path[0] = nav;
    cashPath[0] = 0;
    for (let i = 1; i <= weeks; i += 1) {
      const yWeek = _toNum(template[(i - 1) % template.length]);
      const payment = Number.isFinite(yWeek) && yWeek > 0 ? yWeek * nav : 0;
      cash += payment;
      nav = nav * q;
      path[i] = nav;
      cashPath[i] = cash;
    }
    const navDecay = 1 - nav;
    const borrowCost = Number.isFinite(annualBorrowCost) && annualBorrowCost > 0
      ? annualBorrowCost * t
      : 0;
    return {
      weeklySpreadLoss: weeklyLoss,
      navDecay,
      navReturn: -navDecay,
      distributionsPaid: cash,
      borrowCost,
      netShortPnl: navDecay - cash - borrowCost,
      longTotalReturn: -navDecay + cash,
      navPath: path,
      cashPath,
      weeks,
    };
  }

  /**
   * Forward gross pair-trade P&L (annualized) for a YieldBOOST row.
   *
   * Pair-trade P&L = NAV decay - cash distributions at flat underlying,
   * 1Y horizon, before borrow. This is what a ?-hedged short captures
   * structurally before borrow cost. It is the "Exp. edge (fwd)" headline
   * for YB rows and the anchor target for the Net edge inverse-variance
   * blend (decisions A3 + C2).
   *
   * Returns ``{ p50, p10, p90, navDecay, distributions, weeks,
   *             sigmaForwardAnnual, source }`` or ``null`` when calibration
   * data is missing.  ``p10``/``p90`` map the gross put-spread MC quantile
   * band onto the pair-P&L axis by holding the calibrated cash leg fixed:
   *     pair_p10 = gross_p10 - distributions
   *     pair_p90 = gross_p90 - distributions
   * which preserves the band width (sigma_forward = sigma_gross).
   *
   * For non-YB callers, use the existing ``expected_gross_decay_p*_annual``
   * fields directly; gross drag *is* the pair-trade P&L when there is no
   * cash leg.
   */
  function expectedPairPnlAnnual(params) {
    const sigma = _toNum(params && params.sigmaAnnual);
    const calibration = (params && params.calibration) || null;
    const horizonYears = _toNum((params && params.horizonYears) ?? 1);
    const expenseRatioAnnual = _toNum(
      (params && params.expenseRatioAnnual) ?? DEFAULT_EXPENSE_RATIO_ANNUAL
    );
    const grossBand = (params && params.grossBand) || null; // {p10, p50, p90}
    const tailAdj = _toNum(
      (params && params.tailAdjustmentAnnual) ?? DEFAULT_TAIL_ADJUSTMENT_ANNUAL
    );
    if (!Number.isFinite(sigma) || sigma <= 0) return null;
    if (!Number.isFinite(horizonYears) || horizonYears <= 0) return null;

    const calib = calibratedWeeklyDistribution({
      sigmaAnnual: sigma,
      calibration,
      crossFundRatio: DEFAULT_CROSS_FUND_RATIO,
    });
    if (!calib || calib.weeklyDistribution == null) return null;

    const scenario = estimateIncomeStyleScenarioFromCalibration({
      underlyingReturn: 0,
      sigmaAnnual: sigma,
      horizonYears,
      weeklyDistribution: calib.weeklyDistribution,
      annualBorrowCost: 0,
      expenseRatioAnnual,
      tailAdjustmentAnnual: tailAdj,
    });
    if (!scenario) return null;

    const navDecay = Number(scenario.navDecay);
    const distributions = Number(scenario.distributionsPaid);
    if (!Number.isFinite(navDecay) || !Number.isFinite(distributions)) return null;
    const pairP50 = navDecay - distributions;

    // Band mapping: hold cash leg fixed, shift gross MC quantiles to pair axis.
    let pairP10 = null;
    let pairP90 = null;
    let sigmaForward = null;
    if (grossBand) {
      const gP10 = _toNum(grossBand.p10);
      const gP90 = _toNum(grossBand.p90);
      if (Number.isFinite(gP10)) pairP10 = gP10 - distributions;
      if (Number.isFinite(gP90)) pairP90 = gP90 - distributions;
      if (Number.isFinite(pairP10) && Number.isFinite(pairP90)) {
        const width = Math.abs(pairP90 - pairP10);
        if (width > 0) sigmaForward = width / (2 * 1.2815515655446004);
      }
    }
    return {
      p50: pairP50,
      p10: pairP10,
      p90: pairP90,
      navDecay,
      distributions,
      weeks: scenario.weeks,
      sigmaForwardAnnual: sigmaForward,
      source: calib.ratioSource,
      ratioUsed: calib.ratioUsed,
      confidence: calib.confidence,
      bsPremiumWeekly: calib.bsPremiumWeekly,
    };
  }

  /**
   * Inverse-variance Bayesian blend (Normal-Normal conjugate).
   *
   * Posterior mean = w_F * mu_F + (1 - w_F) * mu_R where
   *     w_F = sigma_R^2 / (sigma_F^2 + sigma_R^2).
   *
   * Returns ``{ posteriorMean, weightForward, posteriorSigma, method }``
   * or ``null`` when the inputs are non-finite.  ``method`` is
   * ``'inverse_variance'`` when both sigmas are positive, or
   * ``'anchor_shift_fallback'`` when only ``sigma_forward`` is unknown /
   * zero (degenerate forward forecast  return forward mean, w_F=1).
   */
  function inverseVarianceBlend(params) {
    const muF = _toNum(params && params.muForward);
    const sigF = _toNum(params && params.sigmaForward);
    const muR = _toNum(params && params.muRealized);
    const sigR = _toNum(params && params.sigmaRealized);
    if (!Number.isFinite(muF) || !Number.isFinite(muR)) return null;
    const sigFok = Number.isFinite(sigF) && sigF > 0;
    const sigRok = Number.isFinite(sigR) && sigR > 0;
    if (!sigFok && !sigRok) {
      // Neither sigma known: fall back to forward as a confident point estimate.
      return {
        posteriorMean: muF,
        weightForward: 1.0,
        posteriorSigma: null,
        method: 'anchor_shift_fallback',
      };
    }
    if (!sigFok) {
      // Forward forecast has no usable band (point estimate). Treat as
      // confident point estimate -> anchor-shift to forward (E2 fallback).
      return {
        posteriorMean: muF,
        weightForward: 1.0,
        posteriorSigma: null,
        method: 'anchor_shift_fallback',
      };
    }
    if (!sigRok) {
      // Realized dispersion unknown, but forward band present: treat realized
      // as low-confidence (sigma_R -> infinity) -> forward dominates.
      return {
        posteriorMean: muF,
        weightForward: 1.0,
        posteriorSigma: sigF,
        method: 'anchor_shift_fallback',
      };
    }
    const vF = sigF * sigF;
    const vR = sigR * sigR;
    const denom = vF + vR;
    if (!Number.isFinite(denom) || denom <= 0) return null;
    const wF = vR / denom;
    const mu = wF * muF + (1 - wF) * muR;
    const posteriorSigma = Math.sqrt((vF * vR) / denom);
    return {
      posteriorMean: mu,
      weightForward: wF,
      posteriorSigma,
      method: 'inverse_variance',
    };
  }

  /**
   * Convenience: convert a Normal p10/p90 band into a sigma.
   * Returns ``null`` when the band is missing or degenerate.
   */
  function bandToSigma(p10, p90) {
    const lo = _toNum(p10);
    const hi = _toNum(p90);
    if (!Number.isFinite(lo) || !Number.isFinite(hi)) return null;
    const width = Math.abs(hi - lo);
    if (width <= 0) return null;
    return width / (2 * 1.2815515655446004);
  }

  /**
   * Band label for the capture ratio (UI color coding).
   */
  function captureRatioBand(ratio) {
    const r = _toNum(ratio);
    if (!Number.isFinite(r)) return 'unknown';
    if (r < RATIO_BANDS.strong) return 'strong';
    if (r < RATIO_BANDS.typical) return 'typical';
    if (r < RATIO_BANDS.weak) return 'weak';
    return 'adverse';
  }

  const exported = {
    PUT_SPREAD_SHORT_STRIKE,
    PUT_SPREAD_LONG_STRIKE,
    PUT_SPREAD_LEVERAGE,
    WEEKS_PER_YEAR,
    DEFAULT_EXPENSE_RATIO_ANNUAL,
    DEFAULT_CROSS_FUND_RATIO,
    DEFAULT_TAIL_ADJUSTMENT_ANNUAL,
    RATIO_FULL_CONFIDENCE_N,
    RATIO_BANDS,
    expectedPutSpreadLossWeekly,
    calibratedWeeklyDistribution,
    estimateIncomeStyleScenarioFromCalibration,
    simulateIncomeSchedule,
    captureRatioBand,
    expectedPairPnlAnnual,
    inverseVarianceBlend,
    bandToSigma,
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = exported;
  }
  if (globalObj) {
    globalObj.IncomeScenario = exported;
  }
})(typeof window !== 'undefined' ? window : globalThis);
