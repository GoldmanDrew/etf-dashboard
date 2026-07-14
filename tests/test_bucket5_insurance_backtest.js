const fs = require('fs');
const path = require('path');

describe('Bucket5 product page', () => {
  test('wrapper points at product JSON and mounts Bucket5Product', () => {
    const src = fs.readFileSync(
      path.join(__dirname, '../assets/bucket5_insurance_backtest.js'),
      'utf8',
    );
    expect(src).toContain("data/bucket5_product.json");
    expect(src).toContain('bucket5_product_dashboard.v1');
    expect(src).toContain('Bucket5Product');
    expect(src).toContain('Bucket5InsurancePage');
  });

  test('shared product UI module is present', () => {
    const ui = fs.readFileSync(
      path.join(__dirname, '../assets/bucket5_product.js'),
      'utf8',
    );
    expect(ui).toContain('bucket5_product_dashboard.v1');
    expect(ui).toMatch(/Overview|Regime|Daily/);
    expect(ui).toContain('Bucket5Product');
  });
});
