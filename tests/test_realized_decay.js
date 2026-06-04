const test = require("node:test");
const assert = require("node:assert/strict");

require("../assets/price_basis.js");

const {
  buildDailyLogDragSeries,
  computeHorizonPeriodReturns,
  buildRollingPeriodReturnSeries,
  logToSimplePeriod,
  periodBorrowLog,
  prepareDecayTrRows,
  etfTrPrice,
  cumSplitFactor,
  filterSplitsNeedingCloseBasisFix,
} = require("../assets/realized_decay.js");

function makeFlatSeries(n, etfDrift = 0, undDrift = 0) {
  const rows = [];
  let ep = 100;
  let up = 50;
  const t0 = Date.parse("2024-01-02T12:00:00Z");
  for (let i = 0; i < n; i += 1) {
    const ds = new Date(t0 + i * 86400000).toISOString().slice(0, 10);
    rows.push({
      date: ds,
      close_price: ep,
      etf_adj_close: ep,
      underlying_adj_close: up,
    });
    ep *= 1 + etfDrift;
    up *= 1 + undDrift;
  }
  return rows;
}

test("flat prices → zero period return", () => {
  const daily = buildDailyLogDragSeries(prepareDecayTrRows(makeFlatSeries(10), []), 2);
  const h = computeHorizonPeriodReturns(daily, [5], 0.05);
  assert.ok(Math.abs(h.horizons[0].grossLog) < 1e-10);
  assert.ok(Math.abs(h.horizons[0].grossSimple) < 1e-10);
});

test("net subtracts borrow over the period", () => {
  const daily = buildDailyLogDragSeries(prepareDecayTrRows(makeFlatSeries(20, -0.005, 0), []), 2);
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
  const tr = prepareDecayTrRows(rows, []);
  const daily = buildDailyLogDragSeries(tr, beta);
  const h = computeHorizonPeriodReturns(daily, [20], 0);
  const row = h.horizons[0];
  const start = tr[tr.length - 21];
  const end = tr[tr.length - 1];
  const endpoint = beta * Math.log(end.trUndPx / start.trUndPx)
    - Math.log(end.trEtfPx / start.trEtfPx);
  assert.ok(Math.abs(row.grossLog - endpoint) < 1e-9);
  assert.ok(Math.abs(row.etfStartPx - start.trEtfPx) < 1e-9);
  assert.ok(Math.abs(row.etfEndPx - end.trEtfPx) < 1e-9);
});

test("rolling period series length", () => {
  const daily = buildDailyLogDragSeries(prepareDecayTrRows(makeFlatSeries(40, -0.002, 0.001), []), 2);
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
  const tr = prepareDecayTrRows([
    { date: "2024-01-01", close_price: 4, underlying_adj_close: 1 },
    { date: "2024-01-02", close_price: 24, underlying_adj_close: 1 },
  ], events);
  assert.ok(Math.abs(tr[0].trEtfPx - 24) < 1e-9);
});

test("filter skips continuous Yahoo MTYY reverse split", () => {
  const points = [
    { date: "2026-05-28", close: 24.0 },
    { date: "2026-06-01", close: 23.604 },
    { date: "2026-06-02", close: 22.99 },
  ];
  const events = [{ date: "2026-06-02", mult: 6 }];
  assert.deepEqual(filterSplitsNeedingCloseBasisFix(points, events), []);
});

test("MTYY issuer pre-split to post-split: decay gross not ~-80%", () => {
  const rows = [];
  for (let i = 0; i < 55; i += 1) {
    const day = String(10 + (i % 20)).padStart(2, "0");
    rows.push({
      date: `2026-04-${day}`,
      close_price: 4.3 - i * 0.002,
      etf_adj_close: 4.2 - i * 0.002,
      nav_total_return: 4.35 - i * 0.002,
      underlying_adj_close: 170 - i * 0.1,
    });
  }
  rows.push(
    { date: "2026-05-28", close_price: 4.0, etf_adj_close: null, nav_total_return: 4.12, underlying_adj_close: 151.64 },
    { date: "2026-06-01", close_price: 23.604, etf_adj_close: 23.604, nav_total_return: 23.65, underlying_adj_close: 136.08 },
    { date: "2026-06-02", close_price: 22.99, etf_adj_close: 22.99, nav_total_return: 23.02, underlying_adj_close: 136.08 },
  );
  const events = [{ date: "2026-06-02", mult: 6 }];
  const tr = prepareDecayTrRows(rows, events);
  const daily = buildDailyLogDragSeries(tr, 0.5);
  const h = computeHorizonPeriodReturns(daily, [20, 60], 0.09);
  const row20 = h.horizons.find((x) => x.horizonDays === 20);
  assert.ok(row20.grossSimple > -0.35, `20d gross too negative: ${row20.grossSimple}`);
  assert.ok(row20.etfEndPx / row20.etfStartPx > 0.75 && row20.etfEndPx / row20.etfStartPx < 1.25,
    `etf px ratio ${row20.etfStartPx} -> ${row20.etfEndPx}`);
});

test("distribution reinvestment scales with verified split", () => {
  const rows = [
    { date: "2026-05-15", close_price: 4.4, etf_adj_close: 4.33, nav_total_return: 4.55, underlying_adj_close: 177.0 },
    { date: "2026-05-28", close_price: 4.0, etf_adj_close: null, nav_total_return: 4.12, underlying_adj_close: 151.64 },
    { date: "2026-06-02", close_price: 22.99, etf_adj_close: 22.99, nav_total_return: 23.02, underlying_adj_close: 136.08 },
  ];
  const tr = prepareDecayTrRows(rows, [{ date: "2026-06-02", mult: 6 }]);
  const may15 = tr.find((x) => x.date === "2026-05-15");
  const may28 = tr.find((x) => x.date === "2026-05-28");
  assert.ok(may15.trEtfPx > 24 && may15.trEtfPx < 28, `May15 TR px ${may15.trEtfPx}`);
  assert.ok(may28.trEtfPx > 23 && may28.trEtfPx < 27, `May28 TR px ${may28.trEtfPx}`);
  assert.ok(Math.abs(may15.trEtfPx - 4.55 * 6) < 0.5);
});

test("no double-scale when navTr already inflated vs close", () => {
  const rows = [
    { date: "2026-05-20", close_price: 4.2, nav_total_return: 250, underlying_adj_close: 160 },
    { date: "2026-06-02", close_price: 23, etf_adj_close: 23, nav_total_return: 23, underlying_adj_close: 136 },
  ];
  const tr = prepareDecayTrRows(rows, [{ date: "2026-06-02", mult: 6 }]);
  const pre = tr.find((x) => x.date === "2026-05-20");
  assert.ok(pre.trEtfPx < 30, `double-scale leak ${pre.trEtfPx}`);
});
