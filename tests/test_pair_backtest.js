const test = require("node:test");
const assert = require("node:assert/strict");

const {
  runPairBacktest,
  exposureRatio,
  slippageCost,
  simulateInversePairBacktest,
  computePairBacktestRiskSeries,
  MIN_TRADING_DAYS_FOR_CAGR,
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

test("flat prices: only borrow drag on short leg (canonical negative fee)", () => {
  const rows = [];
  for (let d = 1; d <= 10; d += 1) {
    const day = `2024-01-${String(d).padStart(2, "0")}`;
    rows.push({ date: day, close_price: 10, underlying_adj_close: 20 });
  }
  const out = simulateInversePairBacktest(rows, {
    gross: 10000,
    hedgeRatio: 2,
    everyNDays: 100,
    netGrossTolerancePct: 50,
    slippageBps: 0,
    avgBorrowAnnual: -0.1,
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
    netGrossTolerancePct: 50,
    slippageBps: 0,
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
    netGrossTolerancePct: 50,
    slippageBps: 0,
    avgBorrowAnnual: 0,
  });
  assert.equal(out.ok, true);
  assert.equal(out.strategy, "short_both");
  assert.equal(out.summary.longPnl, 0);
  assert.ok(out.summary.shortPnl < 0);
});

test("moving prices: looser tolerance ⇒ fewer rebalances than tight tolerance", () => {
  const rows = [];
  for (let i = 0; i < 40; i += 1) {
    rows.push({
      date: `2024-04-${String(i + 1).padStart(2, "0")}`,
      close_price: 40 + i * 0.4,
      underlying_adj_close: 90 + i * 0.1,
    });
  }
  const baseOpts = {
    gross: 200000,
    hedgeRatio: 2,
    beta: 2,
    everyNDays: 1,
    slippageBps: 5,
    avgBorrowAnnual: 0,
  };
  const loose = simulateInversePairBacktest(rows, { ...baseOpts, netGrossTolerancePct: 15 });
  const tight = simulateInversePairBacktest(rows, { ...baseOpts, netGrossTolerancePct: 1 });
  assert.equal(loose.ok, true);
  assert.equal(tight.ok, true);
  assert.ok(loose.summary.nRebalances <= tight.summary.nRebalances);
});

test("slippage bps scales transaction costs when there is turnover", () => {
  const rows = [];
  for (let i = 0; i < 20; i += 1) {
    rows.push({
      date: `2024-05-${String(i + 1).padStart(2, "0")}`,
      close_price: 25 + i * 0.2,
      underlying_adj_close: 80 + i * 0.05,
    });
  }
  const baseOpts = {
    gross: 150000,
    hedgeRatio: 1.5,
    beta: 1.5,
    everyNDays: 2,
    netGrossTolerancePct: 3,
    avgBorrowAnnual: 0,
  };
  const hi = simulateInversePairBacktest(rows, { ...baseOpts, slippageBps: 20 });
  const lo = simulateInversePairBacktest(rows, { ...baseOpts, slippageBps: 5 });
  assert.equal(hi.ok, true);
  assert.equal(lo.ok, true);
  assert.ok(hi.summary.tCosts >= lo.summary.tCosts - 1e-9);
});

test("borrowHistory time series increases drag vs flat fallback when fees are higher", () => {
  const rows = [];
  for (let d = 1; d <= 6; d += 1) {
    rows.push({
      date: `2024-06-${String(d).padStart(2, "0")}`,
      close_price: 10,
      underlying_adj_close: 20,
    });
  }
  const hist = [
    { date: "2024-06-01", borrow_current: -0.3 },
    { date: "2024-06-04", borrow_current: -0.2 },
  ];
  const baseOpts = {
    gross: 10000,
    hedgeRatio: 1,
    beta: 1,
    everyNDays: 100,
    netGrossTolerancePct: 50,
    slippageBps: 0,
    avgBorrowAnnual: -0.01,
  };
  const withHist = simulateInversePairBacktest(rows, { ...baseOpts, borrowHistory: hist });
  const flat = simulateInversePairBacktest(rows, { ...baseOpts, borrowHistory: [] });
  assert.equal(withHist.ok, true);
  assert.ok(withHist.summary.borrowPaid > flat.summary.borrowPaid * 2);
});

test("computePairBacktestRiskSeries: flat equity implies zero vol and Sharpe", () => {
  const rows = [];
  for (let d = 1; d <= 12; d += 1) {
    rows.push({
      date: `2024-01-${String(d).padStart(2, "0")}`,
      close_price: 10,
      underlying_adj_close: 20,
    });
  }
  const out = simulateInversePairBacktest(rows, {
    gross: 100000,
    hedgeRatio: 1,
    beta: 1,
    everyNDays: 100,
    netGrossTolerancePct: 50,
    slippageBps: 0,
    avgBorrowAnnual: 0,
  });
  assert.equal(out.ok, true);
  const risk = computePairBacktestRiskSeries(out.rows, 100000);
  assert.ok(risk.length >= 3);
  const tail = risk[risk.length - 1];
  assert.ok(Number.isFinite(tail.annVol));
  assert.ok(tail.annVol < 1e-6);
  assert.ok(Math.abs(tail.sharpe) < 1e-6);
  assert.equal(tail.maxDrawdown, 0);
  assert.ok(!Number.isFinite(tail.cagr), "CAGR suppressed on short windows");
});

test("computePairBacktestRiskSeries: CAGR finite only after min span", () => {
  const rows = [];
  const start = new Date(Date.UTC(2023, 0, 3));
  const nPrice = MIN_TRADING_DAYS_FOR_CAGR + 45;
  for (let k = 0; k < nPrice; k += 1) {
    const d = new Date(start.getTime() + k * 86400000);
    const ds = d.toISOString().slice(0, 10);
    rows.push({ date: ds, close_price: 10, underlying_adj_close: 20 });
  }
  const out = simulateInversePairBacktest(rows, {
    gross: 100000,
    hedgeRatio: 1,
    beta: 1,
    everyNDays: 100,
    netGrossTolerancePct: 50,
    slippageBps: 0,
    avgBorrowAnnual: 0,
  });
  assert.equal(out.ok, true);
  const risk = computePairBacktestRiskSeries(out.rows, 100000);
  assert.ok(risk.length >= 2);
  const tail = risk[risk.length - 1];
  assert.ok(Number.isFinite(tail.cagr), "CAGR defined once min trading days and calendar span pass");
});

test("daily rows include leg MVs for exposure chart", () => {
  const rows = [];
  for (let d = 1; d <= 8; d += 1) {
    rows.push({
      date: `2024-07-${String(d).padStart(2, "0")}`,
      close_price: 12,
      underlying_adj_close: 40,
    });
  }
  const out = simulateInversePairBacktest(rows, {
    gross: 80000,
    hedgeRatio: 2,
    beta: 2,
    everyNDays: 100,
    netGrossTolerancePct: 50,
    slippageBps: 0,
    avgBorrowAnnual: 0,
  });
  assert.equal(out.ok, true);
  const row0 = out.rows[0];
  assert.ok(Number.isFinite(row0.mvEtfAbs) && row0.mvEtfAbs > 0);
  assert.ok(Number.isFinite(row0.mvUndAbs) && row0.mvUndAbs > 0);
  assert.ok(Number.isFinite(row0.mvEtfBetaAdj));
});

test("short both flat prices: raw net/gross is ~0 (balanced notionals)", () => {
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
    netGrossTolerancePct: 50,
    slippageBps: 0,
    avgBorrowAnnual: -0.05,
  });
  assert.equal(out.ok, true);
  const mid = out.daily[Math.floor(out.daily.length / 2)];
  assert.ok(Number.isFinite(mid.netGrossRaw));
  assert.ok(mid.netGrossRaw < 0.02, `expected small raw net/gross, got ${mid.netGrossRaw}`);
});
