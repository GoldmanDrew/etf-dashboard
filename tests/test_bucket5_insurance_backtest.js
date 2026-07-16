const fs = require('fs');
const path = require('path');
const test = require('node:test');
const assert = require('node:assert/strict');

test('wrapper points at product JSON and mounts Bucket5Product', () => {
  const src = fs.readFileSync(
    path.join(__dirname, '../assets/bucket5_insurance_backtest.js'),
    'utf8',
  );
  assert.ok(src.includes('data/bucket5_product.json'));
  assert.ok(src.includes('bucket5_product_dashboard.v1'));
  assert.ok(src.includes('Bucket5Product'));
  assert.ok(src.includes('Bucket5InsurancePage'));
});

test('shared product UI module is present', () => {
  const ui = fs.readFileSync(
    path.join(__dirname, '../assets/bucket5_product.js'),
    'utf8',
  );
  assert.ok(ui.includes('bucket5_product_dashboard.v1'));
  assert.match(ui, /Overview|Regime|Daily/);
  assert.ok(ui.includes('Bucket5Product'));
  assert.ok(ui.includes('b5p-tabs'));
  assert.ok(ui.includes('b5p-cards'));
  assert.ok(ui.includes('data-b5p-zoom'));
  assert.ok(ui.includes('data-b5p-range'));
  assert.ok(ui.includes('zoomLogicalRange'));
});

test('shared product CSS is present and scoped', () => {
  const css = fs.readFileSync(
    path.join(__dirname, '../assets/bucket5_product.css'),
    'utf8',
  );
  assert.ok(css.includes('.b5p-root'));
  assert.ok(css.includes('.b5p-cards'));
  assert.ok(css.includes('.b5p-tabs'));
  assert.ok(css.includes('--b5p-measure'));
  assert.ok(css.includes('.b5p-lw-toolbar'));
  assert.ok(css.includes('.b5p-lw-btn'));
});

test('App hash router keeps Bucket 5 mounted for regime/daily deep links', () => {
  const html = fs.readFileSync(path.join(__dirname, '../index.html'), 'utf8');
  assert.match(html, /hash\.startsWith\('#\/bucket5\/'\)/);
  assert.ok(html.includes("hash === '#/bucket5'"));
});
