const test = require("node:test");
const assert = require("node:assert/strict");

const {
  horizonToYears,
  computeRealizedVolFromReturns,
  computeEwmaVolFromReturns,
  estimateEtfReturn,
  buildScenarioGrid,
} = require("../assets/scenario_returns.js");

test("horizon conversion", () => {
  assert.equal(horizonToYears("1M"), 1 / 12);
  assert.equal(horizonToYears("3M"), 3 / 12);
  assert.equal(horizonToYears("6M"), 6 / 12);
  assert.equal(horizonToYears("1Y"), 1);
});

test("realized vol matches sample std annualization", () => {
  const rets = [0.01, -0.02, 0.015, -0.005];
  const out = computeRealizedVolFromReturns(rets);
  const mean = rets.reduce((a, b) => a + b, 0) / rets.length;
  const variance = rets.reduce((a, b) => a + ((b - mean) ** 2), 0) / (rets.length - 1);
  const expected = Math.sqrt(variance) * Math.sqrt(252);
  assert.ok(Math.abs(out - expected) < 1e-12);
});

test("ewma vol matches recursive definition", () => {
  const rets = [0.01, -0.02, 0.015];
  const lambda = 0.94;
  let ewmaVar = rets[0] * rets[0];
  ewmaVar = lambda * ewmaVar + (1 - lambda) * rets[1] * rets[1];
  ewmaVar = lambda * ewmaVar + (1 - lambda) * rets[2] * rets[2];
  const expected = Math.sqrt(ewmaVar) * Math.sqrt(252);
  const out = computeEwmaVolFromReturns(rets, lambda);
  assert.ok(Math.abs(out - expected) < 1e-12);
});

test("etf return model includes drag and carry", () => {
  const out = estimateEtfReturn({
    leverage: -2,
    underlyingReturn: -0.1,
    sigmaAnnual: 0.8,
    horizonYears: 1,
    annualCarryDrag: 0.05,
  });
  assert.equal(out.ok, true);
  const expected = (-2 * -0.1) - (0.5 * -2 * -3 * (0.8 ** 2) * 1) - 0.05;
  assert.ok(Math.abs(out.raw - expected) < 1e-12);
});

test("scenario grid deterministic shape and center shock", () => {
  const grid = buildScenarioGrid({
    leverage: -2,
    bestVolAnnual: 1.0,
    horizonYears: 1,
    annualCarryDrag: 0,
    volMultipliers: [1.0],
    shockMultipliers: [0],
  });
  assert.equal(grid.ok, true);
  assert.equal(grid.volColumns.length, 1);
  assert.equal(grid.rows.length, 1);
  const cell = grid.rows[0].cells[0];
  assert.equal(cell.ok, true);
  assert.ok(Math.abs(cell.raw - (-3.0)) < 1e-12); // only volatility drag term remains
});
