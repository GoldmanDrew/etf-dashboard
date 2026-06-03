/**
 * Unit tests for assets/vrp_ui.js — short-YieldBOOST structural-edge helpers.
 * These guard the de-biased ranking: net edge drives the headline, the
 * put-spread vol edge is a sign-corrected second-order overlay.
 */

const test = require("node:test");
const assert = require("node:assert/strict");

const VrpUi = require("../assets/vrp_ui.js");

test("shortSignalFor tiers off net_edge_p50_annual (short_favorable_positive)", () => {
  assert.equal(VrpUi.shortSignalFor(0.20, 0.05).tier, "top");
  assert.equal(VrpUi.shortSignalFor(0.07, 0.0).tier, "good");
  assert.equal(VrpUi.shortSignalFor(0.01).tier, "thin");
  assert.equal(VrpUi.shortSignalFor(-0.05).tier, "skip");
  assert.equal(VrpUi.shortSignalFor(null).tier, "none");
});

test("Top requires a positive p05 lower band", () => {
  assert.equal(VrpUi.shortSignalFor(0.20, -0.01).tier, "good");
});

test("no locate caps the label but keeps it visible", () => {
  const s = VrpUi.shortSignalFor(0.30, 0.10, { shortable: false });
  assert.equal(s.tier, "nolocate");
  assert.equal(s.label, "No locate");
});

test("shortThesisAlignment negates the put-spread edge", () => {
  const rich = VrpUi.shortThesisAlignment(20);
  assert.equal(rich.label, "hurts");
  const cheap = VrpUi.shortThesisAlignment(-15);
  assert.equal(cheap.label, "helps");
  assert.equal(VrpUi.shortThesisAlignment(null).direction, "unknown");
});

test("hedgeRichnessFor uses plain words", () => {
  assert.equal(VrpUi.hedgeRichnessFor(15).label, "rich");
  assert.equal(VrpUi.hedgeRichnessFor(-15).label, "cheap");
});

test("freshLabel uses yes and stale", () => {
  assert.equal(VrpUi.freshLabel({ quote_sync: { sync_ok: true } }).label, "yes");
  assert.equal(VrpUi.freshLabel({ quote_sync: { sync_ok: false } }).label, "stale");
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
