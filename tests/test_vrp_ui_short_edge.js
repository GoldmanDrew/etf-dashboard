/**
 * Unit tests for assets/vrp_ui.js — short-YieldBOOST structural-edge helpers.
 * These guard the de-biased ranking: net edge drives the headline, the
 * put-spread vol edge is a sign-corrected second-order overlay.
 */

const test = require("node:test");
const assert = require("node:assert/strict");

const VrpUi = require("../assets/vrp_ui.js");

test("shortSignalFor tiers off net_edge_p50_annual (short_favorable_positive)", () => {
  assert.equal(VrpUi.shortSignalFor(0.20, 0.05).tier, "strong");
  assert.equal(VrpUi.shortSignalFor(0.07, 0.0).tier, "short");
  assert.equal(VrpUi.shortSignalFor(0.01).tier, "lean");
  assert.equal(VrpUi.shortSignalFor(-0.05).tier, "avoid");
  assert.equal(VrpUi.shortSignalFor(null).tier, "none");
});

test("STRONG SHORT requires a positive p05 lower band", () => {
  // Same median, but downside tail dips negative -> demote to plain short.
  assert.equal(VrpUi.shortSignalFor(0.20, -0.01).tier, "short");
});

test("not-shortable caps the label but keeps it visible", () => {
  const s = VrpUi.shortSignalFor(0.30, 0.10, { shortable: false });
  assert.equal(s.tier, "blocked");
  assert.equal(s.label, "short blocked");
});

test("shortThesisAlignment negates the put-spread edge", () => {
  const rich = VrpUi.shortThesisAlignment(20);
  assert.equal(rich.alignmentPp, -20);
  assert.equal(rich.direction, "headwind");
  const cheap = VrpUi.shortThesisAlignment(-15);
  assert.equal(cheap.alignmentPp, 15);
  assert.equal(cheap.direction, "tailwind");
  assert.equal(VrpUi.shortThesisAlignment(null).direction, "unknown");
});

test("syncBadge surfaces server quote_sync verdict", () => {
  const bad = VrpUi.syncBadge({ quote_sync: { sync_ok: false, sync_reason: "gap" } });
  assert.equal(bad.ok, false);
  assert.equal(bad.label, "NOT SYNCED");
  const good = VrpUi.syncBadge({ quote_sync: { sync_ok: true, quote_sync_gap_hours: 0.2 } });
  assert.equal(good.ok, true);
  const none = VrpUi.syncBadge({});
  assert.equal(none.ok, null);
});

test("fmtPctAnnual formats fractions as signed percentages", () => {
  assert.equal(VrpUi.fmtPctAnnual(0.75), "+75.0%");
  assert.equal(VrpUi.fmtPctAnnual(-0.05, 0), "-5%");
  assert.equal(VrpUi.fmtPctAnnual(null), "\u2014");
});
