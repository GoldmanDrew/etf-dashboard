/**
 * Bucket 5 Product page (etf-dashboard) — SPX-0DTE-style Overview / Regime / Daily.
 * Loads data/bucket5_product.json (falls back to legacy filename).
 * Renders via shared Bucket5Product.mount from assets/bucket5_product.js.
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory(require('react'));
  } else {
    root.Bucket5InsuranceBacktest = factory(root.React);
  }
})(typeof self !== 'undefined' ? self : this, function (React) {
  const { useState, useEffect, useRef } = React;

  const DATA_URLS = ['data/bucket5_product.json', 'data/bucket5_insurance_backtest.json'];

  function parseSubFromHash() {
    try {
      const raw = (location.hash || '').replace(/^#/, '');
      if (!raw.startsWith('/bucket5')) return null;
      const parts = raw.split('?')[0].split('/').filter(Boolean);
      if (parts[1] === 'regime' || parts[1] === 'daily' || parts[1] === 'overview') return parts[1];
    } catch (_e) { /* ignore */ }
    return null;
  }

  function Bucket5InsurancePage({ onBack, onNavigateToChart }) {
    const hostRef = useRef(null);
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
      let cancelled = false;
      const bust = Math.floor(Date.now() / 60000);

      async function load() {
        let lastErr = null;
        for (const url of DATA_URLS) {
          try {
            const r = await fetch(`${url}?t=${bust}`, { cache: 'no-store' });
            if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
            const d = await r.json();
            if (cancelled) return;
            if (d.schema !== 'bucket5_product_dashboard.v1') {
              throw new Error(
                `Unexpected schema ${d.schema || '(none)'} — rebuild with scripts/build_bucket5_product.py`,
              );
            }
            setLoading(false);
            setError(null);
            const Product = (typeof self !== 'undefined' ? self : window).Bucket5Product;
            if (hostRef.current && Product) {
              Product.mount(hostRef.current, d, { sub: parseSubFromHash() || undefined });
            } else if (hostRef.current) {
              hostRef.current.innerHTML =
                '<p style="color:var(--accent-red)">Bucket5Product UI missing — load assets/bucket5_product.js</p>';
            }
            return;
          } catch (e) {
            lastErr = e;
          }
        }
        if (!cancelled) {
          setLoading(false);
          setError(lastErr ? lastErr.message || String(lastErr) : 'load failed');
        }
      }
      load();
      return () => {
        cancelled = true;
      };
    }, []);

    return React.createElement(
      'div',
      { className: 'backtest-page b5-product-page' },
      React.createElement(
        'div',
        { className: 'b5p-toolbar' },
        React.createElement('button', { type: 'button', className: 'topbar-btn', onClick: onBack }, '← Back'),
        React.createElement(
          'button',
          {
            type: 'button',
            className: 'topbar-btn',
            onClick: () => onNavigateToChart && onNavigateToChart('UVIX'),
          },
          'Chart UVIX',
        ),
        React.createElement(
          'button',
          {
            type: 'button',
            className: 'topbar-btn',
            onClick: () => onNavigateToChart && onNavigateToChart('SVIX'),
          },
          'Chart SVIX',
        ),
      ),
      loading &&
        React.createElement(
          'p',
          { style: { padding: 12, color: 'var(--text-muted)' } },
          'Loading Bucket 5 product dashboard…',
        ),
      error &&
        React.createElement(
          'div',
          null,
          React.createElement(
            'p',
            { style: { color: 'var(--accent-red)', marginTop: 8 } },
            `Bucket 5 product unavailable: ${error}`,
          ),
          React.createElement(
            'p',
            { style: { color: 'var(--text-muted)', fontSize: 13, marginTop: 8 } },
            'In ls-algo: python scripts/build_bucket5_product_dashboard.py --copy-etf-dashboard',
          ),
        ),
      React.createElement('div', { ref: hostRef, id: 'b5-product-host', className: 'b5p-host' }),
    );
  }

  return { Bucket5InsurancePage, DATA_URL: DATA_URLS[0] };
});
