const test = require("node:test");
const assert = require("node:assert/strict");

const {
  isMarketBackedMetricsRow,
  isIssuerStaleMetricsRow,
  isPremiumDiscountEligibleRow,
  premiumDiscountPct,
  issuerNavForPlot,
} = require("../assets/metrics_prem_disc.js");

const SNDQ_MARKET_BACKED = {
  date: "2026-07-16",
  ticker: "SNDQ",
  nav: 2.6829,
  close_price: 3.91,
  stale_kind: "market_backed_no_issuer_nav",
  source_provider: "market_backed",
  source_url: "market_backed://SNDQ?from=2026-07-14#session=2026-07-16",
  issuer_asof_date: "2026-07-14",
  market_asof_date: "2026-07-16",
  premium_discount_eligible: false,
  premium_discount_status: "issuer_stale",
};

test("SNDQ market-backed row is issuer-stale and not prem/disc eligible", () => {
  assert.equal(isMarketBackedMetricsRow(SNDQ_MARKET_BACKED), true);
  assert.equal(isIssuerStaleMetricsRow(SNDQ_MARKET_BACKED), true);
  assert.equal(isPremiumDiscountEligibleRow(SNDQ_MARKET_BACKED), false);
  // Ungated math would report ~+45.74% — must be suppressed.
  const ungated = ((3.91 - 2.6829) / 2.6829) * 100;
  assert.ok(Math.abs(ungated - 45.74) < 0.05);
  assert.equal(premiumDiscountPct(2.6829, 3.91, SNDQ_MARKET_BACKED), null);
  assert.equal(issuerNavForPlot(SNDQ_MARKET_BACKED), null);
});

test("same-session eligible row returns close vs NAV percent", () => {
  const row = {
    nav: 2.68,
    close_price: 2.69,
    premium_discount_eligible: true,
    issuer_asof_date: "2026-07-14",
    market_asof_date: "2026-07-14",
  };
  const pct = premiumDiscountPct(2.68, 2.69, row);
  assert.ok(Number.isFinite(pct));
  assert.ok(Math.abs(pct - ((2.69 - 2.68) / 2.68) * 100) < 1e-9);
  assert.equal(issuerNavForPlot(row), 2.68);
});

test("legacy rows without eligibility flag still block asof mismatch", () => {
  const row = {
    nav: 10,
    close_price: 12,
    issuer_asof_date: "2026-07-14",
    market_asof_date: "2026-07-16",
  };
  assert.equal(isPremiumDiscountEligibleRow(row), false);
  assert.equal(premiumDiscountPct(10, 12, row), null);
});

test("carry-forward rows are issuer-stale", () => {
  const row = {
    nav: 5,
    close_price: 5.1,
    stale_kind: "carry_forward",
    source_provider: "carry_forward",
    source_url: "carry_forward://TEST?from=2026-07-09",
  };
  assert.equal(isIssuerStaleMetricsRow(row), true);
  assert.equal(isPremiumDiscountEligibleRow(row), false);
  assert.equal(issuerNavForPlot(row), null);
});

test("issuer_lag and |prem|>10% are blocked even when eligibility flag is true", () => {
  const lag = {
    nav: 29.65,
    close_price: 14.84,
    stale_kind: "issuer_lag",
    premium_discount_eligible: true,
    issuer_asof_date: "2026-05-19",
    market_asof_date: "2026-05-19",
  };
  assert.equal(isIssuerStaleMetricsRow(lag), true);
  assert.equal(isPremiumDiscountEligibleRow(lag), false);
  assert.equal(premiumDiscountPct(29.65, 14.84, lag), null);
  assert.equal(issuerNavForPlot(lag), null);

  const absurd = {
    nav: 14.4528,
    close_price: 21.67,
    premium_discount_eligible: true,
    issuer_asof_date: "2026-06-26",
    market_asof_date: "2026-06-26",
  };
  assert.equal(isPremiumDiscountEligibleRow(absurd), false);
  assert.equal(premiumDiscountPct(14.4528, 21.67, absurd), null);
});
