/**
 * Unit tests for assets/chart_returns.js — split-aware live window returns.
 */

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  nearestSplitRatio,
  inferSplitFactorEndToLive,
  livePriceReturnFromWindow,
  liveTrReturnFromWindow,
  liveAdjReturnFromWindow,
  splitAdjustedDividendYield,
  splitAwareWindowPriceReturn,
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

test("ignore stale split factor when live and end are same basis", () => {
  const ret = livePriceReturnFromWindow({
    liveSpot: 23,
    endClose: 22.94,
    priceReturn: (22.94 / 64.08) - 1,
    splitFactorEndToAsof: 6,
  });
  assert.ok(Math.abs(ret - ((23 / 22.94) * (1 + (22.94 / 64.08 - 1)) - 1)) < 1e-6);
  assert.ok(ret > -0.7 && ret < -0.5, `expected ~-64%, got ${ret}`);
});

test("liveTrReturnFromWindow chains adj return from end adj close", () => {
  const tr = liveTrReturnFromWindow({
    liveSpot: 23,
    endClose: 22.94,
    endAdjClose: 22.94,
    trReturn: -0.37,
    splitFactorEndToAsof: 6,
  });
  assert.ok(Number.isFinite(tr));
  assert.ok(tr > -0.45 && tr < -0.25);
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

test("KORU-style forward split cliff is repaired without stored factor", () => {
  const ret = splitAwareWindowPriceReturn({
    startClose: 445.88,
    endClose: 21.87,
    splitFactorStartToEnd: 1,
    fallback: -0.950951,
  });
  assert.ok(Number.isFinite(ret));
  assert.ok(ret > -0.10, `expected repaired return, got ${ret}`);
  assert.ok(Math.abs(ret - ((21.87 / (445.88 * 0.05)) - 1)) < 1e-6);
});

test("genuine LETF drawdown is not mistaken for a split", () => {
  const ret = splitAwareWindowPriceReturn({
    startClose: 999.25,
    endClose: 419.19,
    splitFactorStartToEnd: 1,
    fallback: -0.58,
  });
  assert.ok(Math.abs(ret - ((419.19 / 999.25) - 1)) < 1e-6);
});

test("SNDQ-style reverse-split window does not invent a forward 1/N repair", () => {
  // Yahoo 3M path ~274 → 28.6 looks like 0.1 (1-for-10 forward) but is a
  // reverse-split LETF grind; stored return already matches the chart (~-90%).
  const stored = 28.6 / 274.399994 - 1;
  const ret = splitAwareWindowPriceReturn({
    startClose: 274.399994,
    endClose: 28.6,
    splitFactorStartToEnd: 1,
    fallback: stored,
    splitEvents: [{ date: "2026-07-21", mult: 10 }],
    startDate: "2026-04-23",
    endDate: "2026-07-21",
  });
  assert.ok(Math.abs(ret - stored) < 1e-9, `got ${ret}, want ${stored}`);
  assert.ok(ret < -0.85, "must stay near the chart drawdown, not +4%");
});
