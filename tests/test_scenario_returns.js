const test = require("node:test");
const assert = require("node:assert/strict");

const {
  horizonToYears,
  computeRealizedVolFromReturns,
  computeEwmaVolFromReturns,
  estimateEtfReturn,
  buildScenarioGrid,
  buildShockRows,
  simulateMonteCarloPaths,
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
  const expected = Math.exp(((-2) * Math.log(1 - 0.1)) - (0.5 * -2 * -3 * (0.8 ** 2) * 1) - 0.05) - 1;
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
  assert.ok(Math.abs(cell.raw - (Math.exp(-3.0) - 1)) < 1e-12); // only drag remains in log space
});

test("shock mapping uses 1/3, 1, 3 sigma ladder and stays above -100%", () => {
  const shocks = buildShockRows(1.2, 1, [-3, -1, -1 / 3, 0, 1 / 3, 1, 3]);
  assert.equal(shocks.length, 7);
  for (const row of shocks) {
    assert.ok(row.underlyingReturn > -1, `invalid underlying return ${row.underlyingReturn}`);
  }
  const mids = shocks.map((r) => r.sigmaMultiple);
  assert.deepEqual(mids, [-3, -1, -1 / 3, 0, 1 / 3, 1, 3]);
});

test("monte carlo deterministic for same seed", () => {
  const a = simulateMonteCarloPaths({
    pathCount: 100,
    sigmaAnnual: 0.8,
    leverage: -2,
    horizonYears: 0.5,
    annualCarryDrag: 0.04,
    seed: 123,
    ruinThreshold: -0.8,
    upsideThresholds: [0.25, 0.5, 1.0],
  });
  const b = simulateMonteCarloPaths({
    pathCount: 100,
    sigmaAnnual: 0.8,
    leverage: -2,
    horizonYears: 0.5,
    annualCarryDrag: 0.04,
    seed: 123,
    ruinThreshold: -0.8,
    upsideThresholds: [0.25, 0.5, 1.0],
  });
  assert.equal(a.ok, true);
  assert.equal(b.ok, true);
  assert.equal(a.summary.meanTerminal, b.summary.meanTerminal);
  assert.equal(a.summary.p95, b.summary.p95);
});

test("monte carlo path/step sizing and risk stats", () => {
  const out = simulateMonteCarloPaths({
    pathCount: 250,
    sigmaAnnual: 1.0,
    leverage: -2,
    horizonYears: 1,
    annualCarryDrag: 0.05,
    seed: 42,
    ruinThreshold: -0.8,
    upsideThresholds: [0.25, 0.5, 1.0],
  });
  assert.equal(out.ok, true);
  assert.equal(out.paths.length, 250);
  assert.equal(out.paths[0].length, out.settings.steps + 1);
  assert.ok(Number.isFinite(out.summary.ddMean));
  assert.ok(Number.isFinite(out.summary.cvar5));
  assert.ok(out.summary.probLoss >= 0 && out.summary.probLoss <= 1);
  assert.ok(out.summary.probRuin >= 0 && out.summary.probRuin <= 1);
  assert.equal(out.summary.upsideTerminalCounts.length, 3);
  assert.equal(out.summary.upsideAnyCounts.length, 3);
  assert.ok(out.summary.short.probLoss >= 0 && out.summary.short.probLoss <= 1);
  assert.ok(out.summary.short.probRuin >= 0 && out.summary.short.probRuin <= 1);
});

test("short-loss probability aligns with ETF-up probability", () => {
  const out = simulateMonteCarloPaths({
    pathCount: 400,
    sigmaAnnual: 0.9,
    leverage: -2,
    horizonYears: 0.5,
    annualCarryDrag: 0.03,
    seed: 7,
    ruinThreshold: -0.8,
    upsideThresholds: [0],
  });
  assert.equal(out.ok, true);
  const pEtfUpTerminal = out.summary.upsideTerminalCounts[0].pct;
  assert.ok(Math.abs(out.summary.short.probLoss - pEtfUpTerminal) < 1e-12);
});

test("upside thresholds from decimal array are not re-scaled", () => {
  const out = simulateMonteCarloPaths({
    pathCount: 50,
    sigmaAnnual: 0.7,
    leverage: -2,
    horizonYears: 0.25,
    annualCarryDrag: 0.02,
    seed: 11,
    ruinThreshold: -0.8,
    upsideThresholds: [0.25, 1.0, 5.0], // 25%, 100%, 500%
  });
  assert.equal(out.ok, true);
  assert.deepEqual(out.settings.upsideThresholds, [0.25, 1.0, 5.0]);
  assert.equal(out.summary.upsideTerminalCounts.length, 3);
});
