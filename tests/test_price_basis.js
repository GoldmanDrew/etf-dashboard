const test = require("node:test");
const assert = require("node:assert/strict");

require("../assets/price_basis.js");
const PB = require("../assets/price_basis.js");
require("../assets/realized_decay.js");
const RD = require("../assets/realized_decay.js");

test("filter skips continuous Yahoo MTYY reverse split", () => {
  const points = [
    { date: "2026-05-28", close: 24.0 },
    { date: "2026-06-01", close: 23.604 },
    { date: "2026-06-02", close: 22.99 },
  ];
  const events = [{ date: "2026-06-02", mult: 6 }];
  assert.deepEqual(PB.filterSplitsNeedingCloseBasisFix(points, events), []);
  const ctx = PB.resolveSplitContext(points, events);
  assert.equal(ctx.mode, "continuous");
});

test("discrete split scales pre-split close not inflated navTr", () => {
  const rows = [
    { date: "2026-05-28", close_price: 4.0, nav_total_return: 264, underlying_adj_close: 150 },
    { date: "2026-06-02", close_price: 22.99, etf_adj_close: 22.99, nav_total_return: 23.02, underlying_adj_close: 136 },
  ];
  const tr = PB.buildTrSeriesFromMetrics(rows, [{ date: "2026-06-02", mult: 6 }]);
  const pre = tr.find((x) => x.date === "2026-05-28");
  assert.ok(pre.trEtfPx < 30, `expected ~24 not ${pre.trEtfPx}`);
  assert.ok(pre.trEtfPx > 20 && pre.trEtfPx < 30, `TR px ${pre.trEtfPx}`);
  assert.ok(
    pre.trMode === "pre_split_close_scaled" || pre.trMode === "pre_split_nav_tr_scaled",
    pre.trMode,
  );
});

test("MTYY issuer path: decay gross in sane band", () => {
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
  const tr = RD.prepareDecayTrRows(rows, events);
  const daily = RD.buildDailyLogDragSeries(tr, 0.5);
  const h = RD.computeHorizonPeriodReturns(daily, [5, 20], 0.09);
  const row5 = h.horizons.find((x) => x.horizonDays === 5);
  const row20 = h.horizons.find((x) => x.horizonDays === 20);
  assert.ok(row5.grossSimple > -0.5 && row5.grossSimple < 0.5, `5d gross ${row5.grossSimple}`);
  assert.ok(row20.grossSimple > -0.35 && row20.grossSimple < 0.35, `20d gross ${row20.grossSimple}`);
  assert.ok(row20.etfEndPx / row20.etfStartPx > 0.75 && row20.etfEndPx / row20.etfStartPx < 1.25);
});

test("forward split window: start scaled to end basis", () => {
  const rows = [
    { date: "2026-04-01", close_price: 100, etf_adj_close: 100, underlying_adj_close: 50 },
    { date: "2026-04-02", close_price: 100 / 3, etf_adj_close: 100 / 3, underlying_adj_close: 50 },
  ];
  const tr = PB.buildTrSeriesFromMetrics(rows, [{ date: "2026-04-02", mult: 1 / 3 }]);
  assert.ok(Math.abs(tr[0].trEtfPx - tr[1].trEtfPx) < 0.02);
});

test("realized_decay re-exports filter from price_basis", () => {
  const points = [
    { date: "2026-01-23", close: 421.25 },
    { date: "2026-01-26", close: 36.25 },
  ];
  const events = [{ date: "2026-01-26", mult: 0.1 }];
  assert.equal(RD.filterSplitsNeedingCloseBasisFix(points, events).length, 1);
});
