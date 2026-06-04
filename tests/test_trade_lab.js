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
  valuationHorizonToYears,
  valuationHorizonToDays,
  remainingOptionDte,
  calendarDteFromExpiry,
  tradeLabStorageKey,
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

test("trade scenario grid supports multiple ETF symbols on one underlying", () => {
  const grid = buildTradeScenarioGrid({
    baseLegs: [
      { symbol: "APLD", side: "short", quantity: 100, entry: 32 },
      { symbol: "APLX", side: "long", quantity: 100, entry: 14 },
      { symbol: "APLZ", side: "long", quantity: 100, entry: 8 },
    ],
    optionLegs: [],
    underlyingSymbol: "APLD",
    etfSymbol: "APLX",
    spotBySymbol: { APLD: 32, APLX: 14, APLZ: 8 },
    leverageBySymbol: { APLX: 1.995, APLZ: -1.9876 },
    spotUnderlying: 32,
    spotEtf: 14,
    leverage: 1.995,
    horizonYears: 0.5,
    baseVolAnnual: 0.8,
    shockRows: [{ sigmaMultiple: 0, underlyingReturn: 0 }],
    volColumns: [{ sigmaAnnual: 0.8 }],
    ttxDays: 30,
  });
  assert.equal(grid.ok, true);
  assert.equal(grid.rows.length, 1);
  assert.ok(Number.isFinite(grid.rows[0].cells[0].pnl));
});

test("trade monte carlo derives sibling ETF paths from underlying paths", () => {
  const path = [1, 1.02, 1.04];
  const out = evaluateTradeMonteCarlo({
    baseLegs: [
      { symbol: "APLD", side: "short", quantity: 100, entry: 32 },
      { symbol: "APLX", side: "long", quantity: 100, entry: 14 },
      { symbol: "APLZ", side: "long", quantity: 100, entry: 8 },
    ],
    optionLegs: [],
    underlyingSymbol: "APLD",
    etfSymbol: "APLX",
    spotBySymbol: { APLD: 32, APLX: 14, APLZ: 8 },
    leverageBySymbol: { APLX: 2, APLZ: -2 },
    spotUnderlying: 32,
    spotEtf: 14,
    ttxDaysStart: 30,
    horizonYears: 0.25,
    volAnnual: 0.6,
    underlyingPaths: [path],
  });
  assert.equal(out.ok, true);
  assert.equal(out.terminal.length, 1);
  assert.ok(Number.isFinite(out.summary.meanTerminal));
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

test("scenario grid scales with option contracts when premium equals auto mark", () => {
  const spot = 47.88;
  const strike = 32;
  const iv = 0.8;
  const ttxDays = 600;
  const rate = 0.04;
  const autoPremium = bsPrice({ spot, strike, vol: iv, ttmYears: ttxDays / 365, rate, type: "put" });
  const base = {
    baseLegs: [
      { symbol: "APLD", side: "long", quantity: 10000, entry: spot },
      { symbol: "APLZ", side: "short", quantity: 1000000, entry: 5.66 },
      { symbol: "APLX", side: "short", quantity: 100000, entry: 14.7 },
    ],
    optionLegs: [{ symbol: "APLD", type: "put", side: "buy", strike, premium: autoPremium, iv }],
    underlyingSymbol: "APLD",
    etfSymbol: "APLX",
    spotBySymbol: { APLD: spot, APLX: 14.7, APLZ: 5.66 },
    leverageBySymbol: { APLX: 1.995, APLZ: -1.9876 },
    spotUnderlying: spot,
    spotEtf: 14.7,
    leverage: 1.995,
    horizonYears: 0.5,
    baseVolAnnual: 0.8,
    shockRows: [{ sigmaMultiple: 0, underlyingReturn: 0 }],
    volColumns: [{ sigmaAnnual: 0.8 }],
    ttxDays,
    rate,
    defaultBorrowAnnual: 0.05,
    shortProceedsRateAnnual: 0.01,
  };
  const pnl1 = buildTradeScenarioGrid({ ...base, optionLegs: [{ ...base.optionLegs[0], contracts: 1 }] }).rows[0].cells[0].pnl;
  const pnl1000 = buildTradeScenarioGrid({ ...base, optionLegs: [{ ...base.optionLegs[0], contracts: 1000 }] }).rows[0].cells[0].pnl;
  assert.notEqual(pnl1, pnl1000);
  assert.ok(Math.abs(pnl1000 - pnl1) > 1000);
});

test("scenario grid option pnl responds to underlying shock", () => {
  const spot = 100;
  const strike = 95;
  const iv = 0.5;
  const premium = bsPrice({ spot, strike, vol: iv, ttmYears: 30 / 365, rate: 0, type: "put" });
  const base = {
    baseLegs: [],
    optionLegs: [{ symbol: "UND", type: "put", side: "buy", strike, contracts: 100, premium, iv }],
    underlyingSymbol: "UND",
    etfSymbol: "ETF",
    spotUnderlying: spot,
    spotEtf: 100,
    leverage: 1,
    horizonYears: 0.5,
    baseVolAnnual: 0.7,
    volColumns: [{ sigmaAnnual: 0.7 }],
    ttxDays: 30,
    rate: 0,
  };
  const up = buildTradeScenarioGrid({
    ...base,
    shockRows: [{ sigmaMultiple: 3, underlyingReturn: 0.5 }],
  }).rows[0].cells[0].pnl;
  const down = buildTradeScenarioGrid({
    ...base,
    shockRows: [{ sigmaMultiple: -3, underlyingReturn: -0.5 }],
  }).rows[0].cells[0].pnl;
  assert.ok(up < down, "long put should gain more when underlying falls");
});

test("valuation horizon helpers", () => {
  assert.ok(Math.abs(valuationHorizonToYears("1M") - 1 / 12) < 1e-9);
  assert.equal(valuationHorizonToDays("30D"), 30);
  assert.equal(remainingOptionDte(90, 30), 60);
  assert.equal(remainingOptionDte(30, 90), 0);
});

test("equity-only pnl changes with valuation horizon at flat underlying", () => {
  const base = {
    baseLegs: [
      { symbol: "APLD", side: "short", quantity: 100, entry: 47 },
      { symbol: "APLZ", side: "long", quantity: 15000, entry: 5.66 },
    ],
    optionLegs: [],
    underlyingSymbol: "APLD",
    etfSymbol: "APLX",
    spotBySymbol: { APLD: 47, APLX: 14.7, APLZ: 5.66 },
    leverageBySymbol: { APLX: 2, APLZ: -1.9876 },
    spotUnderlying: 47,
    spotEtf: 14.7,
    leverage: 2,
    baseVolAnnual: 0.8,
    shockRows: [{ sigmaMultiple: 0, underlyingReturn: 0 }],
    volColumns: [{ sigmaAnnual: 0.8 }],
    ttxDays: 30,
  };
  const pnl1m = buildTradeScenarioGrid({ ...base, horizonYears: 1 / 12 }).rows[0].cells[0].pnl;
  const pnl3m = buildTradeScenarioGrid({ ...base, horizonYears: 3 / 12 }).rows[0].cells[0].pnl;
  assert.notEqual(pnl1m, pnl3m);
});

test("equity-only pnl unchanged when only option DTE changes", () => {
  const base = {
    baseLegs: [{ symbol: "APLD", side: "long", quantity: 100, entry: 47 }],
    optionLegs: [],
    underlyingSymbol: "APLD",
    etfSymbol: "APLX",
    spotBySymbol: { APLD: 47, APLX: 14.7 },
    leverageBySymbol: { APLX: 2 },
    spotUnderlying: 47,
    spotEtf: 14.7,
    leverage: 2,
    horizonYears: 1 / 12,
    baseVolAnnual: 0.8,
    shockRows: [{ sigmaMultiple: 0, underlyingReturn: 0 }],
    volColumns: [{ sigmaAnnual: 0.8 }],
  };
  const pnl30 = buildTradeScenarioGrid({ ...base, ttxDays: 30 }).rows[0].cells[0].pnl;
  const pnl90 = buildTradeScenarioGrid({ ...base, ttxDays: 90 }).rows[0].cells[0].pnl;
  assert.equal(pnl30, pnl90);
});

test("calendar DTE from expiry is non-negative", () => {
  const dte = calendarDteFromExpiry("2099-12-31", new Date("2026-01-01T12:00:00Z"));
  assert.ok(dte > 0);
  assert.equal(tradeLabStorageKey("apld", "aplx"), "tradeLab:APLD:APLX");
});
