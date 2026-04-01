const test = require("node:test");
const assert = require("node:assert/strict");

const {
  computeExpectedDecay,
  parseSigmaAnnual,
} = require("../assets/expected_decay.js");

test("beta=2 sigma=1.3 T=1/12", () => {
  const out = computeExpectedDecay({
    beta: 2,
    sigmaAnnual: 1.3,
    periodValue: 1,
    periodUnit: "months",
  });
  assert.equal(out.ok, true);
  assert.ok(Math.abs(out.periodExpectedDecay - 0.1408333333) < 1e-9);
});

test("beta=-2 sigma=1.3 T=1/12", () => {
  const out = computeExpectedDecay({
    beta: -2,
    sigmaAnnual: 1.3,
    periodValue: 1,
    periodUnit: "months",
  });
  assert.equal(out.ok, true);
  assert.ok(Math.abs(out.periodExpectedDecay - 0.4225) < 1e-9);
});

test("beta=1 gives zero decay", () => {
  const out = computeExpectedDecay({
    beta: 1,
    sigmaAnnual: 1.3,
    periodValue: 6,
    periodUnit: "months",
  });
  assert.equal(out.ok, true);
  assert.equal(out.periodExpectedDecay, 0);
  assert.equal(out.annualExpectedDecay, 0);
});

test("sigma parsing 130 and 1.3 are equivalent", () => {
  assert.equal(parseSigmaAnnual(130), 1.3);
  assert.equal(parseSigmaAnnual(1.3), 1.3);
});
