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

test("NBIZ dual reverse splits use close×cum factors (no June geom-mean cliff)", () => {
  const rows = [
    { date: "2026-05-28", close_price: 1.305, etf_adj_close: 13.05, shares_outstanding: 11555000, nav: 1.3001, underlying_adj_close: 226.34 },
    { date: "2026-05-29", close_price: 1.255, etf_adj_close: 12.55, shares_outstanding: 12715000, nav: 1.2456, underlying_adj_close: 231.09 },
    { date: "2026-06-01", close_price: 8.600, etf_adj_close: 25.80, shares_outstanding: 36415000, nav: 0.8858, underlying_adj_close: 264.51 },
    { date: "2026-06-02", close_price: 9.100, etf_adj_close: 27.30, shares_outstanding: 3641500, nav: 0.9122, underlying_adj_close: 260.58 },
    { date: "2026-06-03", close_price: 9.760, etf_adj_close: 29.28, shares_outstanding: 2711500, nav: 9.7451, underlying_adj_close: 251.68 },
    { date: "2026-07-20", close_price: 12.17, etf_adj_close: 36.51, shares_outstanding: 515494, nav: 12.1979, underlying_adj_close: 182.62 },
    { date: "2026-07-21", close_price: 22.80, etf_adj_close: 22.80, shares_outstanding: 790494, nav: 22.8638, underlying_adj_close: 216.92 },
  ];
  const events = [
    { date: "2026-06-03", mult: 10 },
    { date: "2026-07-21", mult: 3 },
  ];
  const ctx = PB.resolveSplitContext(
    rows.map((r) => ({ date: r.date, close: r.close_price, adj: r.etf_adj_close })),
    events,
    rows,
  );
  assert.equal(ctx.mode, "multi_discrete_split");
  assert.equal(ctx.boundaries.length, 2);
  const tr = PB.buildTrSeriesFromMetrics(rows, events);
  const byDate = Object.fromEntries(tr.map((r) => [r.date, r]));
  assert.ok(Math.abs(byDate["2026-05-29"].trEtfPx - 1.255 * 30) < 1e-6, byDate["2026-05-29"].trEtfPx);
  assert.ok(Math.abs(byDate["2026-06-01"].trEtfPx - 8.6 * 3) < 1e-6, byDate["2026-06-01"].trEtfPx);
  assert.ok(Math.abs(byDate["2026-06-02"].trEtfPx - 9.1 * 3) < 1e-6, byDate["2026-06-02"].trEtfPx);
  assert.ok(Math.abs(byDate["2026-07-20"].trEtfPx - 12.17 * 3) < 1e-6, byDate["2026-07-20"].trEtfPx);
  assert.ok(Math.abs(byDate["2026-07-21"].trEtfPx - 22.80) < 1e-6, byDate["2026-07-21"].trEtfPx);
  const cov = PB.summarizeTrCoverage(rows, events);
  assert.equal(cov.splitMode, "multi_discrete_split");
  assert.ok(cov.maxUnexplainedEtfDailyLogReturn < 0.35, cov.maxUnexplainedEtfDailyLogReturn);
  assert.equal(cov.warnings.some((w) => w.includes("Large unexplained ETF TR daily move")), false);
});

test("filter accepts NBIZ market-obscured 1-for-3 reverse split", () => {
  const points = [
    { date: "2026-07-17", close: 11.50, adj: 11.50 },
    { date: "2026-07-20", close: 12.17, adj: 12.17 },
    { date: "2026-07-21", close: 22.80, adj: 22.80 },
    { date: "2026-07-22", close: 21.40, adj: 21.40 },
  ];
  const events = [{ date: "2026-07-21", mult: 3 }];
  const verified = PB.filterSplitsNeedingCloseBasisFix(points, events);
  assert.deepEqual(verified, [{ date: "2026-07-21", mult: 3 }]);
  const rows = [
    { date: "2026-07-20", close_price: 12.17, etf_adj_close: 12.17, underlying_adj_close: 100 },
    { date: "2026-07-21", close_price: 22.80, etf_adj_close: 22.80, underlying_adj_close: 119 },
  ];
  const tr = PB.buildTrSeriesFromMetrics(rows, events);
  const pre = tr.find((x) => x.date === "2026-07-20");
  assert.ok(Math.abs(pre.trEtfPx - 36.51) < 1e-6, `pre TR ${pre.trEtfPx}`);
  const post = tr.find((x) => x.date === "2026-07-21");
  assert.ok(Math.abs(post.trEtfPx - 22.80) < 1e-6);
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

test("APLZ 1-for-5 reverse split: declared 5x accepted when jump is 5.64x", () => {
  const rows = [
    { date: "2026-05-27", close_price: 2.565, etf_adj_close: 2.565, nav: 2.5625, shares_outstanding: 2405000, underlying_adj_close: 10 },
    { date: "2026-06-01", close_price: 2.66, etf_adj_close: 2.66, nav: 2.6527, shares_outstanding: 2420000, underlying_adj_close: 10.1 },
    { date: "2026-06-02", close_price: 2.66, etf_adj_close: 2.66, nav: 2.6616, shares_outstanding: 484000, underlying_adj_close: 10.2 },
    { date: "2026-06-03", close_price: 15.0, etf_adj_close: 15.0, nav: 15.0602, shares_outstanding: 484000, underlying_adj_close: 11 },
  ];
  const events = [{ date: "2026-06-03", mult: 5 }];
  const ctx = PB.resolveSplitContext(
    rows.map((r) => ({ date: r.date, close: r.close_price, adj: r.etf_adj_close })),
    events,
    rows,
  );
  assert.equal(ctx.mode, "discrete_split");
  assert.equal(ctx.mult, 5);
  const tr = PB.buildTrSeriesFromMetrics(rows, events);
  const cov = PB.summarizeTrCoverage(rows, events);
  assert.ok(cov.maxEtfDailyLogReturn < 0.35, `max jump ${cov.maxEtfDailyLogReturn}`);
  assert.equal(cov.splitMode, "discrete_split");
  const pre = tr.find((x) => x.date === "2026-05-27");
  assert.ok(pre.trEtfPx > 12 && pre.trEtfPx < 14, `pre-split TR ${pre.trEtfPx}`);
});

test("matchSplitToPriceJump trusts declared mult within 18%", () => {
  assert.equal(PB.matchSplitToPriceJump(5.64, 5), 5);
  assert.equal(PB.nearestSplitRatio(5.64), 6);
});

test("APLZ backfilled adj is not double-scaled onto post-split basis", () => {
  const rows = [
    { date: "2026-05-27", close_price: 2.565, etf_adj_close: 12.825, underlying_adj_close: 10 },
    { date: "2026-06-03", close_price: 15.0, etf_adj_close: 15.0, underlying_adj_close: 11 },
  ];
  const tr = PB.buildTrSeriesFromMetrics(rows, [{ date: "2026-06-03", mult: 5 }]);
  const pre = tr.find((x) => x.date === "2026-05-27");
  assert.ok(Math.abs(pre.trEtfPx - 12.825) < 0.01, `pre-split TR ${pre.trEtfPx}`);
  const cov = PB.summarizeTrCoverage(rows, [{ date: "2026-06-03", mult: 5 }]);
  assert.ok(cov.maxEtfDailyLogReturn < 0.35, `max jump ${cov.maxEtfDailyLogReturn}`);
});

test("APLX 3-for-1 adj basis switch: no split cliff in TR", () => {
  const rows = [
    { date: "2026-03-05", close_price: 16.94, etf_adj_close: 5.647, underlying_adj_close: 10 },
    { date: "2026-03-09", close_price: 15.377, etf_adj_close: 5.126, underlying_adj_close: 10.1 },
    { date: "2026-03-10", close_price: 15.71, etf_adj_close: 15.71, underlying_adj_close: 10.2 },
    { date: "2026-03-11", close_price: 17.08, etf_adj_close: 17.08, underlying_adj_close: 10.3 },
  ];
  const events = [{ date: "2026-03-10", mult: 1 / 3 }];
  const ctx = PB.resolveSplitContext(
    rows.map((r) => ({ date: r.date, close: r.close_price, adj: r.etf_adj_close })),
    events,
    rows,
  );
  assert.equal(ctx.mode, "adj_basis_switch");
  const tr = PB.buildTrSeriesFromMetrics(rows, events);
  let maxJump = 0;
  for (let i = 1; i < tr.length; i += 1) {
    const lr = Math.abs(Math.log(tr[i].trEtfPx / tr[i - 1].trEtfPx));
    if (lr > maxJump) maxJump = lr;
  }
  assert.ok(maxJump < 0.35, `split cliff ${maxJump}`);
  const pre = tr.find((x) => x.date === "2026-03-09");
  const post = tr.find((x) => x.date === "2026-03-10");
  assert.ok(Math.abs(pre.trEtfPx - 5.126) < 0.02);
  assert.ok(Math.abs(post.trEtfPx - 15.71 / 3) < 0.15);
});

test("realized_decay re-exports filter from price_basis", () => {
  const points = [
    { date: "2026-01-23", close: 421.25 },
    { date: "2026-01-26", close: 36.25 },
  ];
  const events = [{ date: "2026-01-26", mult: 0.1 }];
  assert.equal(RD.filterSplitsNeedingCloseBasisFix(points, events).length, 1);
});

function maxEtfJump(tr) {
  let maxJump = 0;
  let at = null;
  for (let i = 1; i < tr.length; i += 1) {
    const lr = Math.abs(Math.log(tr[i].trEtfPx / tr[i - 1].trEtfPx));
    if (lr > maxJump) {
      maxJump = lr;
      at = tr[i].date;
    }
  }
  return { maxJump, at };
}

test("provider basis jump before declared reverse split is segment-scaled", () => {
  const rows = [
    { date: "2026-04-14", close_price: 47.70, etf_adj_close: 954.0, underlying_adj_close: 27.20 },
    { date: "2026-04-15", close_price: 47.80, etf_adj_close: 956.0, underlying_adj_close: 27.29 },
    {
      date: "2026-04-16",
      close_price: 2.59,
      etf_adj_close: 51.8,
      underlying_adj_close: 28.40,
      source_url: "https://axsetf.filepoint.live/assets/data/NSDEAXS2.04162026.csv",
    },
    { date: "2026-04-17", close_price: 2.61, etf_adj_close: 52.2, underlying_adj_close: 28.56 },
    { date: "2026-04-30", close_price: 46.60, etf_adj_close: 932.0, underlying_adj_close: 27.40 },
    { date: "2026-05-01", close_price: 45.63, etf_adj_close: 2.2815, underlying_adj_close: 27.09 },
  ];
  const tr = PB.buildTrSeriesFromMetrics(rows, [{ date: "2026-05-01", mult: 20 }]);
  const byDate = Object.fromEntries(tr.map((r) => [r.date, r]));
  assert.ok(Math.abs(byDate["2026-04-15"].trEtfPx - 47.8) < 1.5, byDate["2026-04-15"].trEtfPx);
  assert.ok(Math.abs(byDate["2026-04-16"].trEtfPx - 51.8) < 1.5, byDate["2026-04-16"].trEtfPx);
  const { maxJump, at } = maxEtfJump(tr);
  assert.ok(maxJump < 0.35, `basis cliff on ${at}: ${maxJump}`);
});

test("oscillating provider basis segments before split are normalized", () => {
  const rows = [
    { date: "2026-04-15", close_price: 36.40, etf_adj_close: 728.0, underlying_adj_close: 6.47 },
    { date: "2026-04-16", close_price: 1.83, etf_adj_close: 36.6, underlying_adj_close: 6.48 },
    { date: "2026-04-17", close_price: 1.955, etf_adj_close: 39.1, underlying_adj_close: 6.72 },
    { date: "2026-04-22", close_price: 2.27, etf_adj_close: 45.4, underlying_adj_close: 7.27 },
    { date: "2026-04-23", close_price: 41.16, etf_adj_close: 823.2, underlying_adj_close: 6.91 },
    { date: "2026-04-24", close_price: 40.966, etf_adj_close: 2.0483, underlying_adj_close: 6.90 },
  ];
  const tr = PB.buildTrSeriesFromMetrics(rows, [{ date: "2026-04-24", mult: 20 }]);
  const byDate = Object.fromEntries(tr.map((r) => [r.date, r.trEtfPx]));
  assert.ok(Math.abs(byDate["2026-04-15"] - 36.4) < 1.5, byDate["2026-04-15"]);
  assert.ok(Math.abs(byDate["2026-04-17"] - 39.1) < 1.5, byDate["2026-04-17"]);
  assert.ok(Math.abs(byDate["2026-04-23"] - 41.16) < 1.5, byDate["2026-04-23"]);
  const { maxJump, at } = maxEtfJump(tr);
  assert.ok(maxJump < 0.35, `oscillating basis cliff on ${at}: ${maxJump}`);
});

test("fabricated adj cliff (QBTZ pre x3 / post /3) falls back to close basis", () => {
  const rows = [
    { date: "2026-03-18", close_price: 41.94, etf_adj_close: 125.82, underlying_adj_close: 18.0 },
    { date: "2026-03-19", close_price: 43.98, etf_adj_close: 131.94, underlying_adj_close: 18.2 },
    { date: "2026-03-20", close_price: 45.63, etf_adj_close: 136.89, underlying_adj_close: 19.3 },
    { date: "2026-03-23", close_price: 41.73, etf_adj_close: 13.91, underlying_adj_close: 25.74 },
    { date: "2026-03-24", close_price: 44.30, etf_adj_close: 14.77, underlying_adj_close: 25.5 },
    { date: "2026-03-25", close_price: 42.95, etf_adj_close: 14.32, underlying_adj_close: 27.5 },
  ];
  const events = [{ date: "2026-03-23", mult: 3 }];
  const cliffs = PB.findFabricatedAdjCliffs(
    rows.map((r) => ({ date: r.date, close: r.close_price, adj: r.etf_adj_close })),
    events,
  );
  assert.equal(cliffs.length, 1);
  assert.equal(cliffs[0].date, "2026-03-23");
  assert.ok(Math.abs(cliffs[0].factor - 9) < 0.5, cliffs[0].factor);
  const tr = PB.buildTrSeriesFromMetrics(rows, events);
  assert.equal(tr.length, rows.length);
  for (let i = 0; i < rows.length; i += 1) {
    assert.ok(Math.abs(tr[i].trEtfPx / rows[i].close_price - 1) < 1e-9, `${tr[i].date}: ${tr[i].trEtfPx}`);
  }
  const { maxJump, at } = maxEtfJump(tr);
  assert.ok(maxJump < 0.25, `fabricated cliff leaked at ${at}: ${maxJump}`);
});

test("legit back-adjusted reverse split is not treated as fabricated", () => {
  const points = [
    { date: "2026-05-01", close: 3.72, adj: 37.2 },
    { date: "2026-05-04", close: 3.79, adj: 37.9 },
    { date: "2026-05-05", close: 37.51, adj: 37.51 },
  ];
  assert.deepEqual(PB.findFabricatedAdjCliffs(points, [{ date: "2026-05-05", mult: 10 }]), []);
});

test("delayed reverse-split adj reset is attributed to declared split", () => {
  const rows = [
    { date: "2026-06-01", close_price: 3.934, etf_adj_close: 0.655667, nav_total_return: 293.12, underlying_adj_close: 149.78 },
    { date: "2026-06-02", close_price: 22.99, etf_adj_close: 3.831667, nav_total_return: 1716.90, underlying_adj_close: 136.08 },
    { date: "2026-06-03", close_price: 22.905, etf_adj_close: 3.8175, nav_total_return: 1702.10, underlying_adj_close: 126.55 },
    { date: "2026-06-11", close_price: 21.83, etf_adj_close: 21.83, nav_total_return: 1654.27, underlying_adj_close: 120.15 },
    { date: "2026-06-12", close_price: 21.70, etf_adj_close: 21.70, nav_total_return: 1669.27, underlying_adj_close: 123.97 },
  ];
  const events = [{ date: "2026-06-02", mult: 6 }];
  assert.deepEqual(
    PB.findFabricatedAdjCliffs(rows.map((r) => ({ date: r.date, close: r.close_price, adj: r.etf_adj_close })), events),
    [],
  );
  const tr = PB.buildTrSeriesFromMetrics(rows, events);
  const byDate = Object.fromEntries(tr.map((r) => [r.date, r]));
  assert.ok(Math.abs(byDate["2026-06-02"].trEtfPx - 22.99) < 0.01, byDate["2026-06-02"].trEtfPx);
  assert.ok(Math.abs(byDate["2026-06-11"].trEtfPx - 21.83) < 0.01, byDate["2026-06-11"].trEtfPx);
  const { maxJump, at } = maxEtfJump(tr);
  assert.ok(maxJump < 0.35, `delayed reset leaked nav TR on ${at}: ${maxJump}`);
});

test("undeclared back-adjustment (close jump, smooth adj) is not fabricated", () => {
  const points = [
    { date: "2024-01-01", close: 100, adj: 100 },
    { date: "2024-01-02", close: 50, adj: 99 },
  ];
  assert.deepEqual(PB.findFabricatedAdjCliffs(points, []), []);
});

test("future split does not rewrite old split-sized market move", () => {
  const rows = [
    { date: "2024-08-21", close_price: 448.0, etf_adj_close: 430.208099, underlying_adj_close: 62.377998 },
    { date: "2024-08-22", close_price: 753.200012, etf_adj_close: 723.287415, underlying_adj_close: 60.481998 },
    { date: "2024-08-23", close_price: 773.200012, etf_adj_close: 742.493103, underlying_adj_close: 61.324001 },
  ];
  const events = [{ date: "2026-03-19", mult: 2 }];
  const ctx = PB.resolveSplitContext(
    rows.map((r) => ({ date: r.date, close: r.close_price, adj: r.etf_adj_close })),
    events,
    rows,
  );
  assert.equal(ctx.mode, "continuous");
  const tr = PB.buildTrSeriesFromMetrics(rows, events);
  assert.ok(Math.abs(tr[0].trEtfPx - 430.208099) < 1e-6, tr[0].trEtfPx);
});

test("coverage does not warn on underlying-explained leveraged move", () => {
  const rows = [];
  let etf = 10;
  let und = 1000;
  for (let i = 0; i < 23; i += 1) {
    const d = new Date(Date.parse("2026-05-20T00:00:00Z") + i * 86400000).toISOString().slice(0, 10);
    rows.push({ date: d, close_price: etf, etf_adj_close: etf, underlying_adj_close: und });
    etf *= 0.995;
    und *= 1.002;
  }
  rows.push(
    { date: "2026-06-24", close_price: 3.52, etf_adj_close: 3.52, underlying_adj_close: 1914.46 },
    { date: "2026-06-25", close_price: 1.98, etf_adj_close: 1.98, underlying_adj_close: 2335.00 },
    { date: "2026-06-26", close_price: 2.39, etf_adj_close: 2.39, underlying_adj_close: 2090.71 },
  );
  const cov = PB.summarizeTrCoverage(rows, []);
  assert.equal(cov.quality, "good");
  assert.equal(cov.warnings.some((w) => w.includes("Large unexplained ETF TR daily move")), false);
  assert.ok(cov.maxEtfDailyLogReturn > 0.35);
  assert.equal(cov.maxUnexplainedEtfDailyLogReturn, 0);
});

test("scaleMetricsSeriesToLatestShareBasis removes SNDQ reverse-split close cliff", () => {
  const rows = [
    { date: "2026-07-17", close_price: 4.19, etf_adj_close: 41.9, nav: 4.20, shares_outstanding: 1000000 },
    { date: "2026-07-20", close_price: 4.0, etf_adj_close: 40.0, nav: 4.01, shares_outstanding: 1000000 },
    { date: "2026-07-21", close_price: 28.6, etf_adj_close: 28.6, nav: 28.57, shares_outstanding: 100000 },
  ];
  const events = [{ date: "2026-07-21", mult: 10 }];
  const scaled = PB.scaleMetricsSeriesToLatestShareBasis(rows, events);
  const pre = scaled.find((r) => r.date === "2026-07-20");
  const post = scaled.find((r) => r.date === "2026-07-21");
  assert.ok(pre.share_basis_factor > 5, `pre factor ${pre.share_basis_factor}`);
  assert.equal(post.share_basis_factor, 1);
  assert.ok(Math.abs(pre.close_plot - 40) < 1e-6, `pre close_plot ${pre.close_plot}`);
  assert.ok(Math.abs(post.close_plot - 28.6) < 1e-6, `post close_plot ${post.close_plot}`);
  // No vertical spike: pre and post on same basis stay in the same order of magnitude.
  assert.ok(pre.close_plot / post.close_plot < 2.5);
});
