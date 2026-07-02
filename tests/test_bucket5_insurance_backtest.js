/** @jest-environment jsdom */
const { Bucket5InsurancePage, DATA_URL } = require('../assets/bucket5_insurance_backtest.js');

test('exports Bucket5InsurancePage and data url', () => {
  expect(typeof Bucket5InsurancePage).toBe('function');
  expect(DATA_URL).toBe('data/bucket5_insurance_backtest.json');
});
