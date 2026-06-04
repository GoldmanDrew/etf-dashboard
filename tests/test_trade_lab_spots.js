const test = require("node:test");
const assert = require("node:assert/strict");

const {
  isFreshOptionsRow,
  resolveSymbolSpot,
  resolveTradeSpots,
} = require("../assets/trade_lab_spots.js");

test("fresh options cache row accepted", () => {
  assert.equal(isFreshOptionsRow({ spot: 47.08, stale: false, cache_age_seconds: 60 }), true);
});

test("stale options cache row rejected", () => {
  assert.equal(isFreshOptionsRow({ spot: 47.08, stale: true, cache_age_seconds: 60 }), false);
});

test("underlying uses fresh intraday when options cache stale", () => {
  const out = resolveSymbolSpot({
    symbol: "APLD",
    underlyingSymbol: "APLD",
    optionsCache: { symbols: { APLD: { spot: 47.08, stale: true, cache_age_seconds: 86400 } } },
    intradaySpot: { by_underlying: { APLD: { last: 44.58, stale: false, source: "tradier_spot" } } },
  });
  assert.equal(out.ok, true);
  assert.equal(out.spot, 44.58);
  assert.equal(out.source, "tradier_spot");
});

test("etf symbol with stale cache only is not ok", () => {
  const out = resolveSymbolSpot({
    symbol: "APLX",
    underlyingSymbol: "APLD",
    optionsCache: { symbols: { APLX: { spot: 14.7, stale: true, cache_age_seconds: 86400 } } },
    intradaySpot: { by_underlying: { APLD: { last: 44.58, stale: false } } },
  });
  assert.equal(out.ok, false);
  assert.equal(out.stale, true);
  assert.equal(out.spot, 14.7);
});

test("missing spot returns no numeric bySymbol entry", () => {
  const out = resolveTradeSpots({
    symbols: ["XYZ"],
    underlyingSymbol: "XYZ",
    optionsCache: { symbols: {} },
    intradaySpot: { by_underlying: {} },
  });
  assert.equal(out.hasStale, true);
  assert.equal(out.bySymbol.XYZ, undefined);
});
