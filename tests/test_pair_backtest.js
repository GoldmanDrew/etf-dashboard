const test = require("node:test");
const assert = require("node:assert/strict");

const {
  runPairBacktest,
  exposureRatio,
  slippageCost,
} = require("../assets/pair_backtest.js");

function row(date, value, extra = {}) {
  return {
    date,
    nav_total_return: value,
    close_price: value,
    shares_traded: 1_000_000,
    ...extra,
  };
}

test("flat pair accrues borrow on short leg", () => {
  const rows = [
    row("2026-01-02", 100),
    row("2026-01-05", 100),
  ];
  const out = runPairBacktest({
    longRows: rows,
    shortRows: rows,
    fallbackBorrowAnnual: 0.252,
    initialGross: 100000,
    rebalanceEveryDays: 99,
    floorBps: 0,
    impactBps: 0,
  });
  assert.equal(out.ok, true);
  assert.equal(Math.round(out.summary.borrow), 50);
  assert.equal(Math.round(out.summary.netPnl), -50);
});

test("total-return value drives PnL, not raw close", () => {
  const longRows = [
    row("2026-01-02", 100, { close_price: 100 }),
    row("2026-01-05", 110, { close_price: 90 }),
  ];
  const shortRows = [
    row("2026-01-02", 100),
    row("2026-01-05", 100),
  ];
  const out = runPairBacktest({
    longRows,
    shortRows,
    initialGross: 100000,
    rebalanceEveryDays: 99,
    floorBps: 0,
    impactBps: 0,
  });
  assert.equal(out.ok, true);
  assert.equal(Math.round(out.summary.longPnl), 5000);
  assert.equal(Math.round(out.summary.netPnl), 5000);
});

test("drift rebalance hedges back to requested net/gross ratio", () => {
  const longRows = [
    row("2026-01-02", 100),
    row("2026-01-05", 120),
  ];
  const shortRows = [
    row("2026-01-02", 100),
    row("2026-01-05", 100),
  ];
  const out = runPairBacktest({
    longRows,
    shortRows,
    initialGross: 100000,
    rebalanceEveryDays: 99,
    driftThresholdPct: 5,
    hedgeToPct: 1,
    floorBps: 0,
    impactBps: 0,
  });
  assert.equal(out.ok, true);
  const last = out.rows[out.rows.length - 1];
  assert.equal(last.rebalanceReason, "drift");
  assert.ok(Math.abs(last.exposureRatio - 0.01) < 1e-9);
});

test("slippage increases with participation and exposure ratio is net over gross", () => {
  assert.equal(exposureRatio(50, 50), 0);
  assert.equal(exposureRatio(60, 40), 0.2);
  const low = slippageCost(10_000, 1_000_000, { floorBps: 1, impactBps: 20, capBps: 100 });
  const high = slippageCost(100_000, 1_000_000, { floorBps: 1, impactBps: 20, capBps: 100 });
  assert.ok(high > low);
});
