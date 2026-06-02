/**
 * Unit tests for assets/chart_returns.js — split-aware live window returns.
 */

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  nearestSplitRatio,
  inferSplitFactorEndToLive,
  livePriceReturnFromWindow,
  liveAdjReturnFromWindow,
  splitAdjustedDividendYield,
} = require("../assets/chart_returns.js");

test("nearestSplitRatio detects 1-for-6 reverse split ratio", () => {
  const r = nearestSplitRatio(23 / 3.934);
  assert.ok(r != null);
  assert.ok(Math.abs(r - 6) < 0.5 || Math.abs(r - (1 / 6)) < 0.05);
});

test("MTYY live return uses split factor not raw start", () => {
  const ret = livePriceReturnFromWindow({
    liveSpot: 23,
    endClose: 3.934,
    priceReturn: (3.934 / 4.709) - 1,
    splitFactorEndToAsof: 6,
  });
  assert.ok(Number.isFinite(ret));
  assert.ok(ret < 0, `expected negative return, got ${ret}`);
  assert.ok(ret > -0.25, `return too negative: ${ret}`);
});

test("heuristic infers split when metadata missing", () => {
  const factor = inferSplitFactorEndToLive(23, 3.934, null);
  assert.ok(factor >= 5 && factor <= 7);
  const ret = livePriceReturnFromWindow({
    liveSpot: 23,
    endClose: 3.934,
    priceReturn: -0.16,
    splitFactorEndToAsof: null,
  });
  assert.ok(ret > -0.25 && ret < 0.05);
});

test("adj return adds dividend yield on split-adjusted start", () => {
  const adj = liveAdjReturnFromWindow({
    liveSpot: 23,
    endClose: 3.934,
    priceReturn: -0.16,
    splitFactorEndToAsof: 6,
    dividendYield: 0.06,
  });
  const px = livePriceReturnFromWindow({
    liveSpot: 23,
    endClose: 3.934,
    priceReturn: -0.16,
    splitFactorEndToAsof: 6,
  });
  assert.equal(adj, px + 0.06);
});

test("splitAdjustedDividendYield scales start by split factor", () => {
  const y = splitAdjustedDividendYield(0.371, 4.709, 1);
  assert.ok(Math.abs(y - 0.371 / 4.709) < 1e-6);
});

test("no split: live chains from end close", () => {
  const ret = livePriceReturnFromWindow({
    liveSpot: 150,
    endClose: 149.78,
    priceReturn: -0.14,
    splitFactorEndToAsof: 1,
  });
  assert.ok(Math.abs(ret - ((150 / 149.78) * (1 - 0.14) - 1)) < 1e-6);
});
