/**
 * FoF chart — native SVG compare (FoF vs weighted basket index).
 */
(function (root, factory) {
  const api = factory();
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = api;
  } else {
    root.FoFChart = api;
  }
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  const COLORS = {
    fof: '#3b82f6',
    basket: '#f97316',
    spread: '#22c55e',
    grid: 'rgba(120, 130, 160, 0.25)',
    text: 'rgba(200, 210, 230, 0.85)',
  };

  function finiteNums(arr) {
    if (!Array.isArray(arr)) return [];
    return arr.map((x) => Number(x)).filter((x) => Number.isFinite(x));
  }

  function yScale(values, height, pad) {
    const vals = finiteNums(values);
    if (!vals.length) return () => height / 2;
    let min = Math.min(...vals);
    let max = Math.max(...vals);
    if (min === max) {
      min -= 1;
      max += 1;
    }
    const span = max - min || 1;
    return (v) => {
      const n = Number(v);
      if (!Number.isFinite(n)) return null;
      return pad + (height - 2 * pad) * (1 - (n - min) / span);
    };
  }

  function buildPath(xs, ys, toY) {
    const parts = [];
    for (let i = 0; i < xs.length; i += 1) {
      const y = toY(ys[i]);
      if (y == null) continue;
      parts.push(`${parts.length ? 'L' : 'M'}${xs[i].toFixed(2)},${y.toFixed(2)}`);
    }
    return parts.join(' ');
  }

  function renderFoFCompareChart(container, payload, opts) {
    opts = opts || {};
    const width = Number(opts.width) || 900;
    const height = Number(opts.height) || 320;
    const padL = 48;
    const padR = 16;
    const padT = 24;
    const padB = 36;
    const innerW = width - padL - padR;
    const innerH = height - padT - padB;

    if (!payload || !payload.ok || !Array.isArray(payload.dates) || !payload.dates.length) {
      if (container) {
        container.innerHTML = '<div class="scenario-warning">FoF basket chart unavailable — rebuild dashboard after metrics backfill.</div>';
      }
      return null;
    }

    const n = payload.dates.length;
    const xs = payload.dates.map((_, i) => padL + (n <= 1 ? innerW / 2 : (i / (n - 1)) * innerW));
    const allY = []
      .concat(payload.fof_pct || [])
      .concat(payload.basket_pct || [])
      .concat(payload.spread_pct || []);
    const toY = yScale(allY, innerH, 8);
    const y0 = padT + toY(0);

    const fofPath = buildPath(xs, payload.fof_pct || [], (v) => padT + toY(v));
    const basketPath = buildPath(xs, payload.basket_pct || [], (v) => padT + toY(v));
    const spreadPath = buildPath(xs, payload.spread_pct || [], (v) => padT + toY(v));

    const fofEnd = payload.fof_end_pct;
    const basketEnd = payload.basket_end_pct;
    const spreadEnd = payload.spread_end_pct;
    const fmtEnd = (v) => (Number.isFinite(Number(v)) ? `${Number(v) >= 0 ? '+' : ''}${Number(v).toFixed(2)}%` : '—');

    const svg = `<svg viewBox="0 0 ${width} ${height}" width="100%" height="${height}" role="img" aria-label="FoF vs basket percent change">
      <rect x="0" y="0" width="${width}" height="${height}" fill="rgba(10,14,23,0.6)" rx="8"/>
      <line x1="${padL}" y1="${y0}" x2="${width - padR}" y2="${y0}" stroke="${COLORS.grid}" stroke-dasharray="4 4"/>
      <path d="${basketPath}" fill="none" stroke="${COLORS.basket}" stroke-width="2.2"/>
      <path d="${fofPath}" fill="none" stroke="${COLORS.fof}" stroke-width="2.2"/>
      <path d="${spreadPath}" fill="none" stroke="${COLORS.spread}" stroke-width="1.6" stroke-dasharray="5 4" opacity="0.9"/>
      <text x="${padL}" y="16" fill="${COLORS.basket}" font-size="11">Basket ${fmtEnd(basketEnd)}</text>
      <text x="${padL + 130}" y="16" fill="${COLORS.fof}" font-size="11">${opts.fofLabel || 'FoF'} ${fmtEnd(fofEnd)}</text>
      <text x="${padL + 260}" y="16" fill="${COLORS.spread}" font-size="11">Spread ${fmtEnd(spreadEnd)}</text>
      <text x="${padL}" y="${height - 10}" fill="${COLORS.text}" font-size="10">${payload.start_date || ''} → ${payload.end_date || ''} · ${payload.n_days || n} days</text>
    </svg>`;

    if (container) container.innerHTML = svg;
    return svg;
  }

  return {
    renderFoFCompareChart,
    COLORS,
  };
});
