const test = require("node:test");
const assert = require("node:assert/strict");

const {
  buildDailyLogDragSeries,
  computeHorizonPeriodReturns,
  buildRollingPeriodReturnSeries,
  logToSimplePeriod,
  periodBorrowLog,
  prepareDecayTrRows,
  etfTrPrice,
  cumSplitFactor,
} = require("../assets/realized_decay.js");

function makeFlatSeries(n, etfDrift = 0, undDrift = 0) {
  const rows = [];
  let ep = 100;
  let up = 50;
  for (let i = 0; i < n; i += 1) {
    const ds = `2024-01-${String(i + 1).padStart(2, "0")}`;
    rows.push({
      date: ds,
      close_price: ep,
      underlying_adj_close: up,
    });
    ep *= 1 + etfDrift;
    up *= 1 + undDrift;
  }
  return rows;
}

test("flat prices → zero period return", () => {
  const daily = buildDailyLogDragSeries(makeFlatSeries(10), 2);
  const h = computeHorizonPeriodReturns(daily, [5], 0.05);
  assert.ok(Math.abs(h.horizons[0].grossLog) < 1e-10);
  assert.ok(Math.abs(h.horizons[0].grossSimple) < 1e-10);
});

test("net subtracts borrow over the period", () => {
  const daily = buildDailyLogDragSeries(makeFlatSeries(20, -0.005, 0), 2);
  const borrow = 0.252; // 100% annual → 20/252 ≈ 7.9% over 20d log borrow drag
  const h = computeHorizonPeriodReturns(daily, [20], borrow);
  const row = h.horizons[0];
  assert.ok(row.grossLog > 0);
  assert.ok(Math.abs(row.netLog - (row.grossLog - periodBorrowLog(borrow, row.obs))) < 1e-12);
  assert.ok(row.netSimple < row.grossSimple);
});

test("period gross equals endpoint log drag", () => {
  const rows = makeFlatSeries(25, -0.01, 0.002);
  const beta = 2;
  const daily = buildDailyLogDragSeries(rows, beta);
  const h = computeHorizonPeriodReturns(daily, [20], 0);
  const row = h.horizons[0];
  const start = rows[rows.length - 21];
  const end = rows[rows.length - 1];
  const endpoint = beta * Math.log(end.underlying_adj_close / start.underlying_adj_close)
    - Math.log(end.close_price / start.close_price);
  assert.ok(Math.abs(row.grossLog - endpoint) < 1e-9);
  assert.ok(Math.abs(row.etfStartPx - start.close_price) < 1e-9);
  assert.ok(Math.abs(row.etfEndPx - end.close_price) < 1e-9);
  assert.ok(Math.abs(row.undStartPx - start.underlying_adj_close) < 1e-9);
  assert.ok(Math.abs(row.undEndPx - end.underlying_adj_close) < 1e-9);
});

test("rolling period series length", () => {
  const daily = buildDailyLogDragSeries(makeFlatSeries(40, -0.002, 0.001), 2);
  const roll = buildRollingPeriodReturnSeries(daily, 20, 0.1);
  assert.equal(roll.length, daily.length - 19);
  roll.forEach((r) => {
    assert.ok(Number.isFinite(r.gross_period));
    assert.ok(Number.isFinite(r.net_period));
  });
});

test("logToSimplePeriod", () => {
  assert.ok(Math.abs(logToSimplePeriod(0.1) - 0.105170918) < 1e-6);
});

test("prefers etf_adj_close over raw close for TR drag", () => {
  const rows = [
    { date: "2024-01-01", close_price: 100, etf_adj_close: 100, underlying_adj_close: 50 },
    { date: "2024-01-02", close_price: 50, etf_adj_close: 99, underlying_adj_close: 50 },
  ];
  const tr = prepareDecayTrRows(rows, []);
  const daily = buildDailyLogDragSeries(tr, 2);
  assert.equal(daily.length, 1);
  assert.ok(Math.abs(daily[0].drag - (2 * Math.log(50 / 50) - Math.log(99 / 100))) < 1e-9);
});

test("cumSplitFactor scales pre-split close to latest basis", () => {
  const events = [{ date: "2024-01-02", mult: 6 }];
  assert.ok(Math.abs(cumSplitFactor("2024-01-01", "2024-01-02", events) - 6) < 1e-9);
  const px = etfTrPrice({ date: "2024-01-01", close_price: 4, underlying_adj_close: 1 }, events, "2024-01-02");
  assert.ok(Math.abs(px - 24) < 1e-9);
});
