/**
 * Unit tests for ``assets/income_scenario.js``.
 *
 * Loads the shared Bucket 2 research fixture and validates that the
 * mean-field closed form reproduces published outputs (the engine is
 * frozen in the research within +1.9pp of MC).  Also covers the
 * calibration helper, capture-ratio bands, and the week-by-week
 * schedule simulator's consistency with the closed form.
 */

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const {
  expectedPutSpreadLossWeekly,
  calibratedWeeklyDistribution,
  estimateIncomeStyleScenarioFromCalibration,
  simulateIncomeSchedule,
  captureRatioBand,
  expectedPairPnlAnnual,
  inverseVarianceBlend,
  bandToSigma,
  DEFAULT_CROSS_FUND_RATIO,
  WEEKS_PER_YEAR,
} = require("../assets/income_scenario.js");

const FIXTURE = JSON.parse(
  fs.readFileSync(path.join(__dirname, "fixtures", "bucket2_research.json"), "utf8"),
);

function closedForm(dWeekly, sigma, borrow) {
  const out = estimateIncomeStyleScenarioFromCalibration({
    underlyingReturn: 0,
    sigmaAnnual: sigma,
    horizonYears: 1,
    weeklyDistribution: dWeekly,
    annualBorrowCost: borrow,
    expenseRatioAnnual: FIXTURE.constants.expense_ratio_annual,
  });
  if (!out) throw new Error("closed form returned null");
  return out;
}

// ---------------------------------------------------------------------------
// 1. Engine reproduces published research outputs for all 8 candidates
// ---------------------------------------------------------------------------
for (const candidate of FIXTURE.candidates) {
  test(`Bucket 2 research replicates: ${candidate.etf}`, () => {
    const out = closedForm(
      candidate.actual_distribution_weekly,
      candidate.sigma_annual,
      candidate.borrow_annual,
    );
    const tol = FIXTURE.tolerances;
    assert.ok(
      Math.abs(out.navDecay - candidate.nav_decay_annual) <= tol.nav_decay_pp,
      `${candidate.etf}: NAV decay ${out.navDecay.toFixed(3)} vs research ${candidate.nav_decay_annual}`,
    );
    assert.ok(
      Math.abs(out.distributionsPaid - candidate.distributions_paid_annual) <= tol.distributions_paid_pp,
      `${candidate.etf}: distributions ${out.distributionsPaid.toFixed(3)} vs research ${candidate.distributions_paid_annual}`,
    );
    assert.ok(
      Math.abs(out.netShortPnl - candidate.net_short_annual) <= tol.net_short_pp,
      `${candidate.etf}: net short ${out.netShortPnl.toFixed(3)} vs research ${candidate.net_short_annual}`,
    );
  });
}

// ---------------------------------------------------------------------------
// 2. ``expectedPutSpreadLossWeekly`` agrees with Python on MTYY-shape input
// ---------------------------------------------------------------------------
test("expected put-spread loss matches reference for sigma=0.674 (MTYY today)", () => {
  const L = expectedPutSpreadLossWeekly({
    underlyingReturn: 0,
    sigmaAnnual: 0.674,
    horizonYears: 1,
  });
  // Hand-computed reference (see plan): ~2.49% at sigma=0.674.
  assert.ok(L > 0.020 && L < 0.030, `unexpected L = ${L}`);
});

test("expected put-spread loss returns null on invalid input", () => {
  assert.equal(expectedPutSpreadLossWeekly({ underlyingReturn: 0, sigmaAnnual: 0, horizonYears: 1 }), null);
  assert.equal(expectedPutSpreadLossWeekly({ underlyingReturn: -2, sigmaAnnual: 0.5, horizonYears: 1 }), null);
});

// ---------------------------------------------------------------------------
// 3. Calibrated distribution rate behaves correctly
// ---------------------------------------------------------------------------
test("calibratedWeeklyDistribution scales with scenario sigma at fixed ratio", () => {
  const calibration = {
    blended_ratio_used: 0.65,
    fund_ratio_median: 0.65,
    fund_ratio_confidence: "high",
  };
  const lo = calibratedWeeklyDistribution({ sigmaAnnual: 0.5, calibration });
  const hi = calibratedWeeklyDistribution({ sigmaAnnual: 1.0, calibration });
  assert.ok(hi.weeklyDistribution > lo.weeklyDistribution);
  assert.equal(lo.ratioUsed, 0.65);
  assert.equal(lo.ratioSource, "fund_ratio_high");
});

test("calibratedWeeklyDistribution falls back to cross-fund prior when calibration absent", () => {
  const out = calibratedWeeklyDistribution({ sigmaAnnual: 0.7 });
  assert.equal(out.ratioSource, "cross_fund_prior");
  assert.equal(out.ratioUsed, DEFAULT_CROSS_FUND_RATIO);
  assert.equal(out.confidence, "none");
});

test("calibratedWeeklyDistribution uses fund ratio when present (high confidence)", () => {
  const calibration = {
    blended_ratio_used: 0.55,
    fund_ratio_median: 0.55,
    fund_ratio_confidence: "high",
  };
  const out = calibratedWeeklyDistribution({ sigmaAnnual: 0.8, calibration });
  assert.equal(out.ratioUsed, 0.55);
  assert.equal(out.ratioSource, "fund_ratio_high");
  // weekly ~= 0.55 * BS(0.8)
  const expectedBs = expectedPutSpreadLossWeekly({ underlyingReturn: 0, sigmaAnnual: 0.8, horizonYears: 1 });
  assert.ok(Math.abs(out.weeklyDistribution - 0.55 * expectedBs) < 1e-9);
});

test("calibratedWeeklyDistribution returns null weekly when sigma invalid", () => {
  const out = calibratedWeeklyDistribution({ sigmaAnnual: 0 });
  assert.equal(out.weeklyDistribution, null);
  assert.equal(out.ratioSource, "invalid_sigma");
});

// ---------------------------------------------------------------------------
// 4. Capture-ratio bands match the documented thresholds
// ---------------------------------------------------------------------------
test("captureRatioBand assigns colors correctly", () => {
  assert.equal(captureRatioBand(0.45), "strong");
  assert.equal(captureRatioBand(0.65), "typical");
  assert.equal(captureRatioBand(0.85), "weak");
  assert.equal(captureRatioBand(1.10), "adverse");
  assert.equal(captureRatioBand(NaN), "unknown");
});

// ---------------------------------------------------------------------------
// 5. MTYY/CWY divergence regression: with calibrated d both reach similar
//    long TR / net short, not the broken 163% vs 10% gap
// ---------------------------------------------------------------------------
test("MTYY and CWY converge under calibrated d (regression for 163% bug)", () => {
  // Both at the current dashboard's ? ? 0.7-0.9, capture ratio ~0.65,
  // and modest borrow (4-6%). With ?$/today_price the MTYY scenario long
  // TR was ~+2.5% while CWY was ~-10.4% at 0? 1M. With calibration, both
  // should be in the same band.
  const sigmaMtyy = 0.674;
  const sigmaCwy = 0.93;
  const calib = {
    blended_ratio_used: 0.65,
    fund_ratio_median: 0.65,
    fund_ratio_confidence: "high",
  };
  const dMtyy = calibratedWeeklyDistribution({ sigmaAnnual: sigmaMtyy, calibration: calib });
  const dCwy = calibratedWeeklyDistribution({ sigmaAnnual: sigmaCwy, calibration: calib });
  const mtyy = estimateIncomeStyleScenarioFromCalibration({
    underlyingReturn: 0,
    sigmaAnnual: sigmaMtyy,
    horizonYears: 1 / 12,
    weeklyDistribution: dMtyy.weeklyDistribution,
    annualBorrowCost: 0.056,
  });
  const cwy = estimateIncomeStyleScenarioFromCalibration({
    underlyingReturn: 0,
    sigmaAnnual: sigmaCwy,
    horizonYears: 1 / 12,
    weeklyDistribution: dCwy.weeklyDistribution,
    annualBorrowCost: 0.040,
  });
  // Both long-TRs should be negative (cash partly offsets sleeve decay).
  assert.ok(mtyy.longTotalReturn < 0, `MTYY long TR not negative: ${mtyy.longTotalReturn}`);
  assert.ok(cwy.longTotalReturn < 0, `CWY long TR not negative: ${cwy.longTotalReturn}`);
  // The absolute gap between the two should be small (< 0.10) - this is
  // the structural regression test that fixes the 163%-vs-10% divergence.
  const gap = Math.abs(mtyy.longTotalReturn - cwy.longTotalReturn);
  assert.ok(gap < 0.10, `MTYY/CWY long-TR gap too wide: ${gap}`);
});

// ---------------------------------------------------------------------------
// 6. Schedule simulator matches closed form when template is constant
// ---------------------------------------------------------------------------
test("schedule simulator matches closed form for constant template (research MTYY)", () => {
  const candidate = FIXTURE.candidates.find((c) => c.etf === "MTYY");
  const dWeekly = candidate.actual_distribution_weekly;
  const closed = closedForm(dWeekly, candidate.sigma_annual, candidate.borrow_annual);
  // Build a constant template that pays dWeekly as a fraction of NAV each week.
  // Use a 4-week cycle to verify cycling is harmless.
  const template = [dWeekly, dWeekly, dWeekly, dWeekly];
  const schedule = simulateIncomeSchedule({
    underlyingReturn: 0,
    sigmaAnnual: candidate.sigma_annual,
    horizonYears: 1,
    annualBorrowCost: candidate.borrow_annual,
    templateYields: template,
  });
  assert.ok(schedule != null);
  // Closed-form pays ``d * (1 - q^N)/(1 - q)`` against starting NAV (mean-field).
  // The schedule simulator pays ``y_t * NAV_{t-1}`` recursively, which equals
  // ``d * (q^0 + q^1 + ... + q^{N-1}) = d * (1 - q^N)/(1 - q)``.
  // So distributionsPaid must match closed form exactly.
  assert.ok(
    Math.abs(schedule.distributionsPaid - closed.distributionsPaid) < 1e-9,
    `schedule cash ${schedule.distributionsPaid} != closed-form cash ${closed.distributionsPaid}`,
  );
  assert.ok(Math.abs(schedule.navDecay - closed.navDecay) < 1e-9);
  assert.ok(Math.abs(schedule.netShortPnl - closed.netShortPnl) < 1e-9);
});

test("schedule simulator returns null when template is empty", () => {
  const out = simulateIncomeSchedule({
    underlyingReturn: 0,
    sigmaAnnual: 0.8,
    horizonYears: 1,
    annualBorrowCost: 0.05,
    templateYields: [],
  });
  assert.equal(out, null);
});

// ---------------------------------------------------------------------------
// 7. Tail adjustment widens NAV decay by the configured additive
// ---------------------------------------------------------------------------
test("tail adjustment additive applies to NAV decay only", () => {
  const base = estimateIncomeStyleScenarioFromCalibration({
    underlyingReturn: 0,
    sigmaAnnual: 0.88,
    horizonYears: 1,
    weeklyDistribution: 0.019,
    annualBorrowCost: 0.08,
  });
  const adj = estimateIncomeStyleScenarioFromCalibration({
    underlyingReturn: 0,
    sigmaAnnual: 0.88,
    horizonYears: 1,
    weeklyDistribution: 0.019,
    annualBorrowCost: 0.08,
    tailAdjustmentAnnual: 0.04,
  });
  assert.ok(Math.abs(adj.navDecay - base.navDecay - 0.04) < 1e-9);
  // Distributions unaffected
  assert.ok(Math.abs(adj.distributionsPaid - base.distributionsPaid) < 1e-9);
});

// ---------------------------------------------------------------------------
// 8. Pure null handling
// ---------------------------------------------------------------------------
test("estimateIncomeStyleScenarioFromCalibration returns null when weekly d is missing", () => {
  const out = estimateIncomeStyleScenarioFromCalibration({
    underlyingReturn: 0,
    sigmaAnnual: 0.7,
    horizonYears: 1,
    weeklyDistribution: NaN,
    annualBorrowCost: 0.05,
  });
  assert.equal(out, null);
});

// ---------------------------------------------------------------------------
// 9. expectedPairPnlAnnual + inverseVarianceBlend (decisions A3 + B2 + C2)
// ---------------------------------------------------------------------------
test("expectedPairPnlAnnual matches research net_short + borrow for each candidate", () => {
  const tol = FIXTURE.tolerances.net_short_pp;
  for (const candidate of FIXTURE.candidates) {
    const sigma = candidate.sigma_annual;
    const dashboardBs = expectedPutSpreadLossWeekly({
      underlyingReturn: 0, sigmaAnnual: sigma, horizonYears: 1,
    });
    const ratio = candidate.actual_distribution_weekly / dashboardBs;
    const result = expectedPairPnlAnnual({
      calibration: {
        blended_ratio_used: ratio,
        fund_ratio_median: ratio,
        fund_ratio_confidence: "high",
      },
      sigmaAnnual: sigma,
      horizonYears: 1,
    });
    assert.ok(result, `${candidate.etf}: pair P&L should be computable`);
    const expectedPair = candidate.net_short_annual + candidate.borrow_annual;
    assert.ok(
      Math.abs(result.p50 - expectedPair) <= tol,
      `${candidate.etf}: pair ${result.p50.toFixed(3)} vs research ${expectedPair.toFixed(3)}`,
    );
    // Closed-form identity always holds: pair = navDecay - distributions
    assert.ok(Math.abs(result.p50 - (result.navDecay - result.distributions)) < 1e-9);
  }
});

test("expectedPairPnlAnnual band maps gross MC quantiles with cash leg fixed", () => {
  const calibration = {
    blended_ratio_used: 0.65,
    fund_ratio_confidence: "high",
  };
  const grossBand = { p10: 0.55, p50: 0.70, p90: 0.85 };
  const result = expectedPairPnlAnnual({
    calibration, sigmaAnnual: 0.80, horizonYears: 1, grossBand,
  });
  assert.ok(result);
  assert.ok(Math.abs(result.p10 - (0.55 - result.distributions)) < 1e-9);
  assert.ok(Math.abs(result.p90 - (0.85 - result.distributions)) < 1e-9);
  assert.ok(Math.abs((result.p90 - result.p10) - (0.85 - 0.55)) < 1e-9);
  assert.ok(result.sigmaForwardAnnual > 0);
});

test("expectedPairPnlAnnual returns null when sigma is missing", () => {
  // sigma <= 0 -> can't price; null
  assert.equal(expectedPairPnlAnnual({ calibration: { blended_ratio_used: 0.65 }, sigmaAnnual: 0 }), null);
  assert.equal(expectedPairPnlAnnual({ calibration: { blended_ratio_used: 0.65 }, sigmaAnnual: NaN }), null);
  // calibration absent is OK -- falls back to cross-fund prior (intentional for
  // new YB funds with no own-fund history yet).
  const fallback = expectedPairPnlAnnual({ calibration: null, sigmaAnnual: 0.8 });
  assert.ok(fallback);
  assert.ok(Number.isFinite(fallback.p50));
});

test("bandToSigma reproduces the Normal p10/p90 identity", () => {
  const sig = bandToSigma(0.20, 0.40);
  const expected = 0.20 / (2 * 1.2815515655446004);
  assert.ok(Math.abs(sig - expected) < 1e-12);
  assert.equal(bandToSigma(NaN, 0.40), null);
  assert.equal(bandToSigma(0.40, 0.40), null);
});

test("inverseVarianceBlend matches the conjugate identity", () => {
  const res = inverseVarianceBlend({
    muForward: 0.30, sigmaForward: 0.10,
    muRealized: 0.10, sigmaRealized: 0.10,
  });
  assert.ok(res);
  // Equal sigmas => w_F = 0.5 exactly
  assert.ok(Math.abs(res.weightForward - 0.5) < 1e-12);
  assert.ok(Math.abs(res.posteriorMean - 0.20) < 1e-12);
  assert.equal(res.method, "inverse_variance");

  // sigma_F -> 0 forces full forward weight
  const resF = inverseVarianceBlend({
    muForward: 0.30, sigmaForward: 1e-9,
    muRealized: 0.10, sigmaRealized: 0.10,
  });
  assert.ok(resF.weightForward > 0.999);
  assert.ok(Math.abs(resF.posteriorMean - 0.30) < 1e-6);

  // sigma_R -> 0 forces full realized weight
  const resR = inverseVarianceBlend({
    muForward: 0.30, sigmaForward: 0.10,
    muRealized: 0.10, sigmaRealized: 1e-9,
  });
  assert.ok(resR.weightForward < 1e-3);
  assert.ok(Math.abs(resR.posteriorMean - 0.10) < 1e-6);

  // No realized sigma -> anchor-shift fallback (forward dominates)
  const resA = inverseVarianceBlend({
    muForward: 0.30, sigmaForward: 0.10,
    muRealized: 0.10, sigmaRealized: null,
  });
  assert.equal(resA.method, "anchor_shift_fallback");
  assert.equal(resA.weightForward, 1.0);
  assert.ok(Math.abs(resA.posteriorMean - 0.30) < 1e-9);

  // No forward band (sigma_F=null) -> anchor-shift to forward (E2 fallback
  // for yieldboost_put_spread_point rows).
  const resO = inverseVarianceBlend({
    muForward: 0.30, sigmaForward: null,
    muRealized: 0.10, sigmaRealized: 0.10,
  });
  assert.equal(resO.method, "anchor_shift_fallback");
  assert.equal(resO.weightForward, 1.0);
  assert.ok(Math.abs(resO.posteriorMean - 0.30) < 1e-9);

  // Both sigmas missing: still anchor-shift to forward.
  const resN = inverseVarianceBlend({
    muForward: 0.30, sigmaForward: null,
    muRealized: 0.10, sigmaRealized: null,
  });
  assert.equal(resN.method, "anchor_shift_fallback");
  assert.equal(resN.weightForward, 1.0);
});
