const test = require("node:test");
const assert = require("node:assert/strict");

const { estimateEtfReturn, horizonToYears } = require("../assets/scenario_returns.js");

const DIV_YIELD_WINDOW_YEARS = { "1M": 1 / 12, "3M": 3 / 12, "6M": 6 / 12, "12M": 1 };
const MAX_LETF_DIV_ADD_ANNUAL = 0.05;

function dividendCashAdditiveForHorizon(r, windowKey, horizonYears) {
  if (r?.is_yieldboost === true) return 0;
  const win = r?.dividend_adjustment?.[windowKey];
  if (win?.dividend_lumpy === true) return 0;
  const yw = Number(win?.etf_dividend_yield_recurring ?? win?.etf_dividend_yield);
  const winY = DIV_YIELD_WINDOW_YEARS[windowKey] ?? 0.5;
  if (!Number.isFinite(yw) || yw <= 0 || !Number.isFinite(horizonYears) || horizonYears <= 0) return 0;
  let annualized = yw / winY;
  if (r?.scenario_style === "letf_vol_drag") {
    annualized = Math.min(annualized, MAX_LETF_DIV_ADD_ANNUAL);
  }
  return annualized * horizonYears;
}

function computeExpectedEtfReturnFlatUnd(r, horizonLabel = "3M") {
  const horizonYears = horizonToYears(horizonLabel);
  const lev = Number(r?.delta);
  const sigma = Number(r?.forecast_vol_underlying_annual);
  if (!Number.isFinite(horizonYears) || horizonYears <= 0 || !Number.isFinite(lev) || !Number.isFinite(sigma)) {
    return null;
  }
  const res = estimateEtfReturn({
    leverage: lev,
    underlyingReturn: 0,
    sigmaAnnual: sigma,
    horizonYears,
    annualCarryDrag: 0,
    minReturn: -0.9999,
  });
  if (!res.ok) return null;
  const divAdd = dividendCashAdditiveForHorizon(r, horizonLabel, horizonYears);
  return res.value + (divAdd || 0);
}

test("SMUP-like record uses horizon-matched window not polluted 6M yield", () => {
  const r = {
    symbol: "SMUP",
    scenario_style: "letf_vol_drag",
    delta: 1.99696,
    forecast_vol_underlying_annual: 1.076162,
    dividend_adjustment: {
      "3M": { etf_dividend_yield: 0, etf_dividend_yield_recurring: 0, dividend_lumpy: false },
      "6M": {
        etf_dividend_yield: 0.796226,
        etf_dividend_yield_recurring: 0,
        dividend_lumpy: true,
      },
    },
  };
  const out = computeExpectedEtfReturnFlatUnd(r, "3M");
  assert.ok(out != null && out < 0, `expected negative 3M return, got ${out}`);
  const navOnly = estimateEtfReturn({
    leverage: r.delta,
    underlyingReturn: 0,
    sigmaAnnual: r.forecast_vol_underlying_annual,
    horizonYears: horizonToYears("3M"),
    annualCarryDrag: 0,
  }).value;
  assert.ok(Math.abs(out - navOnly) < 1e-6);
});

test("recurring small 3M yield adds to nav path", () => {
  const r = {
    scenario_style: "letf_vol_drag",
    delta: 2,
    forecast_vol_underlying_annual: 0.4,
    dividend_adjustment: {
      "3M": {
        etf_dividend_yield: 0.01,
        etf_dividend_yield_recurring: 0.01,
        dividend_lumpy: false,
      },
    },
  };
  const out = computeExpectedEtfReturnFlatUnd(r, "3M");
  const navOnly = estimateEtfReturn({
    leverage: 2,
    underlyingReturn: 0,
    sigmaAnnual: 0.4,
    horizonYears: horizonToYears("3M"),
    annualCarryDrag: 0,
  }).value;
  assert.ok(out > navOnly);
  assert.ok(Math.abs(out - navOnly - 0.01) < 1e-9);
});
