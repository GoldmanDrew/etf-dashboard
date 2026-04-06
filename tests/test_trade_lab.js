const test = require("node:test");
const assert = require("node:assert/strict");

const {
  bsPrice,
  optionPayoffAtExpiry,
  equityLegPnl,
  evaluateStructurePnl,
  buildTradeScenarioGrid,
  evaluateTradeMonteCarlo,
  buildTtxSensitivity,
} = require("../assets/trade_lab.js");

test("equity leg pnl long/short sign", () => {
  assert.equal(equityLegPnl({ side: "long", quantity: 10 }, 100, 105), 50);
  assert.equal(equityLegPnl({ side: "short", quantity: 10 }, 100, 105), -50);
});

test("option payoff at expiry sanity", () => {
  assert.equal(optionPayoffAtExpiry({ spot: 120, strike: 100, type: "call" }), 20);
  assert.equal(optionPayoffAtExpiry({ spot: 80, strike: 100, type: "put" }), 20);
});

test("black-scholes positive and monotonic with spot for calls", () => {
  const low = bsPrice({ spot: 90, strike: 100, vol: 0.4, ttmYears: 0.25, rate: 0, type: "call" });
  const high = bsPrice({ spot: 110, strike: 100, vol: 0.4, ttmYears: 0.25, rate: 0, type: "call" });
  assert.ok(Number.isFinite(low) && Number.isFinite(high));
  assert.ok(high > low);
});

test("structure pnl aggregates base and option legs", () => {
  const out = evaluateStructurePnl({
    baseLegs: [{ symbol: "AAA", side: "long", quantity: 10, entry: 100 }],
    optionLegs: [{ symbol: "AAA", type: "call", side: "buy", strike: 100, contracts: 1, premium: 2, iv: 0.4 }],
    priceNow: { AAA: 100 },
    priceFinal: { AAA: 110 },
    ttxDays: 0,
  });
  assert.equal(out.ok, true);
  assert.ok(out.totalPnl > 0);
});

test("short financing applies borrow less proceeds", () => {
  const out = evaluateStructurePnl({
    baseLegs: [{ symbol: "ETF", side: "short", quantity: 100, entry: 100 }],
    optionLegs: [],
    priceNow: { ETF: 100 },
    priceFinal: { ETF: 100 },
    elapsedYears: 1,
    annualBorrowBySymbol: { ETF: 0.3 },
    shortProceedsRateAnnual: 0.05,
  });
  // No spot move; financing should be negative by (5%-30%) * 10,000 = -2,500.
  assert.equal(Math.round(out.totalPnl), -2500);
});

test("trade scenario grid deterministic shape", () => {
  const grid = buildTradeScenarioGrid({
    baseLegs: [{ symbol: "UND", side: "short", quantity: 100, entry: 100 }, { symbol: "ETF", side: "long", quantity: 100, entry: 100 }],
    optionLegs: [],
    underlyingSymbol: "UND",
    etfSymbol: "ETF",
    spotUnderlying: 100,
    spotEtf: 100,
    leverage: -2,
    horizonYears: 0.5,
    baseVolAnnual: 0.8,
    shockRows: [{ sigmaMultiple: -1, underlyingReturn: -0.2 }, { sigmaMultiple: 0, underlyingReturn: 0 }],
    volColumns: [{ sigmaAnnual: 0.6 }, { sigmaAnnual: 1.0 }],
    ttxDays: 30,
  });
  assert.equal(grid.ok, true);
  assert.equal(grid.rows.length, 2);
  assert.equal(grid.rows[0].cells.length, 2);
  assert.ok(Number.isFinite(grid.rows[0].cells[0].pnl));
});

test("trade monte carlo deterministic for fixed paths", () => {
  const path = new Array(6).fill(0).map((_, i) => 1 + i * 0.01);
  const out = evaluateTradeMonteCarlo({
    baseLegs: [{ symbol: "UND", side: "long", quantity: 100, entry: 100 }],
    optionLegs: [],
    underlyingSymbol: "UND",
    etfSymbol: "ETF",
    spotUnderlying: 100,
    spotEtf: 100,
    ttxDaysStart: 30,
    horizonYears: 0.25,
    volAnnual: 0.6,
    etfPaths: [path, path],
    underlyingPaths: [path, path],
  });
  assert.equal(out.ok, true);
  assert.equal(out.terminal.length, 2);
  assert.equal(out.terminal[0], out.terminal[1]);
});

test("trade monte carlo financing reduces short pnl when borrow exceeds proceeds", () => {
  const path = new Array(6).fill(1);
  const outNoFin = evaluateTradeMonteCarlo({
    baseLegs: [{ symbol: "ETF", side: "short", quantity: 100, entry: 100 }],
    optionLegs: [],
    underlyingSymbol: "UND",
    etfSymbol: "ETF",
    spotUnderlying: 100,
    spotEtf: 100,
    ttxDaysStart: 30,
    horizonYears: 1,
    volAnnual: 0.6,
    etfPaths: [path],
    underlyingPaths: [path],
    annualBorrowBySymbol: { ETF: 0.0 },
    shortProceedsRateAnnual: 0.0,
  });
  const outFin = evaluateTradeMonteCarlo({
    baseLegs: [{ symbol: "ETF", side: "short", quantity: 100, entry: 100 }],
    optionLegs: [],
    underlyingSymbol: "UND",
    etfSymbol: "ETF",
    spotUnderlying: 100,
    spotEtf: 100,
    ttxDaysStart: 30,
    horizonYears: 1,
    volAnnual: 0.6,
    etfPaths: [path],
    underlyingPaths: [path],
    annualBorrowBySymbol: { ETF: 0.2 },
    shortProceedsRateAnnual: 0.05,
  });
  assert.ok(outNoFin.ok && outFin.ok);
  assert.ok(outFin.summary.meanTerminal < outNoFin.summary.meanTerminal);
});

test("ttx sensitivity returns requested tenors", () => {
  const arr = buildTtxSensitivity({
    baseLegs: [{ symbol: "UND", side: "long", quantity: 10, entry: 100 }],
    optionLegs: [{ symbol: "UND", type: "put", side: "buy", strike: 95, contracts: 1, premium: 1.5, iv: 0.5 }],
    underlyingSymbol: "UND",
    etfSymbol: "ETF",
    spotUnderlying: 100,
    spotEtf: 100,
    leverage: -2,
    horizonYears: 0.25,
    baseVolAnnual: 0.7,
    shockRows: [{ sigmaMultiple: 0, underlyingReturn: 0 }],
    volColumns: [{ sigmaAnnual: 0.7 }],
    ttxList: [1, 7, 30],
  });
  assert.deepEqual(arr.map((x) => x.ttxDays), [1, 7, 30]);
});

