const test = require("node:test");
const assert = require("node:assert/strict");

const {
  runPairBacktest,
  exposureRatio,
  slippageCost,
  simulateInversePairBacktest,
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

test("inverse pair: flat prices only borrow drag on short leg", () => {
  const rows = [];
  for (let d = 1; d <= 10; d += 1) {
    const day = `2024-01-${String(d).padStart(2, "0")}`;
    rows.push({ date: day, close_price: 10, underlying_adj_close: 20 });
  }
  const out = simulateInversePairBacktest(rows, {
    gross: 10000,
    hedgeRatio: 2,
    everyNDays: 100,
    driftPct: 50,
    hedgeBackPct: 99,
    floorBps: 0,
    impactBps: 0,
    avgBorrowAnnual: 0.1,
  });
  assert.equal(out.ok, true);
  assert.ok(out.summary.borrowPaid > 0);
  assert.equal(out.summary.longPnl, 0);
  assert.equal(out.summary.shortPnl, 0);
  assert.equal(out.summary.tCosts, 0);
});

test("inverse pair (β≥0): rising ETF flat und — short ETF leg loses", () => {
  const rows = [];
  for (let i = 0; i < 5; i += 1) {
    rows.push({
      date: `2024-02-${String(i + 1).padStart(2, "0")}`,
      close_price: 10 + i,
      underlying_adj_close: 50,
    });
  }
  const out = simulateInversePairBacktest(rows, {
    gross: 50000,
    hedgeRatio: 1,
    beta: 2,
    everyNDays: 100,
    driftPct: 50,
    hedgeBackPct: 99,
    floorBps: 0,
    impactBps: 0,
    avgBorrowAnnual: 0,
  });
  assert.equal(out.ok, true);
  assert.equal(out.strategy, "short_etf_long_und");
  assert.ok(out.summary.longPnl < 0);
  assert.equal(out.summary.shortPnl, 0);
});

test("inverse pair (β<0): rising und flat ETF — short underlying leg loses", () => {
  const rows = [];
  for (let i = 0; i < 5; i += 1) {
    rows.push({
      date: `2024-03-${String(i + 1).padStart(2, "0")}`,
      close_price: 10,
      underlying_adj_close: 50 + i,
    });
  }
  const out = simulateInversePairBacktest(rows, {
    gross: 50000,
    hedgeRatio: 1,
    beta: -1,
    everyNDays: 100,
    driftPct: 50,
    hedgeBackPct: 99,
    floorBps: 0,
    impactBps: 0,
    avgBorrowAnnual: 0,
  });
  assert.equal(out.ok, true);
  assert.equal(out.strategy, "short_both");
  assert.equal(out.summary.longPnl, 0);
  assert.ok(out.summary.shortPnl < 0);
});

test("short both flat prices: net/gross is ~0 (not 100% from signed MV bug)", () => {
  const rows = [];
  for (let d = 1; d <= 10; d += 1) {
    rows.push({
      date: `2024-05-${String(d).padStart(2, "0")}`,
      close_price: 20,
      underlying_adj_close: 100,
    });
  }
  const out = simulateInversePairBacktest(rows, {
    gross: 200000,
    hedgeRatio: 1,
    beta: -2,
    everyNDays: 100,
    driftPct: 99,
    hedgeBackPct: 99,
    floorBps: 0,
    impactBps: 0,
    avgBorrowAnnual: 0.05,
  });
  assert.equal(out.ok, true);
  const mid = out.daily[Math.floor(out.daily.length / 2)];
  assert.ok(Number.isFinite(mid.netGross));
  assert.ok(mid.netGross < 0.02, `expected small net/gross, got ${mid.netGross}`);
});
