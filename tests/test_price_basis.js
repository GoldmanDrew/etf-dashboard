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
