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

  const RANGE_DAYS = {
    '1M': 21,
    '3M': 63,
    '6M': 126,
    '12M': 252,
    ALL: 0,
    YTD: 0,
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

  function pctFromIndex(indexSeries) {
    if (!Array.isArray(indexSeries) || indexSeries.length < 1) return [];
    const base = Number(indexSeries[0]);
    if (!Number.isFinite(base) || base <= 0) return indexSeries.map(() => null);
    return indexSeries.map((v) => {
      const n = Number(v);
      if (!Number.isFinite(n)) return null;
      return ((n / base) - 1) * 100;
    });
  }

  function downsample(dates, fofPct, basketPct, spreadPct, maxPoints) {
    const n = dates.length;
    if (n <= maxPoints) {
      return { dates, fofPct, basketPct, spreadPct };
    }
    const step = Math.max(1, Math.floor(n / maxPoints));
    const idxs = [];
    for (let i = 0; i < n; i += step) idxs.push(i);
    if (idxs[idxs.length - 1] !== n - 1) idxs.push(n - 1);
    const pick = (arr) => idxs.map((i) => arr[i]);
    return {
      dates: pick(dates),
      fofPct: pick(fofPct),
      basketPct: pick(basketPct),
      spreadPct: pick(spreadPct),
    };
  }

  /**
   * Slice a full chart payload to a TradingView-style range key (1M, 3M, 6M, …).
   * Prefers fof_index/basket_index/all_dates when present (post-fix v3 payloads).
   */
  function sliceFoFChartPayload(payload, rangeKey) {
    if (!payload || !payload.ok) return payload;
    const key = String(rangeKey || '6M').toUpperCase();
    const allDates = payload.all_dates || payload.dates || [];
    const fofIdx = payload.fof_index;
    const basketIdx = payload.basket_index;

    if (!allDates.length || !Array.isArray(fofIdx) || !Array.isArray(basketIdx)) {
      return payload;
    }
    if (fofIdx.length !== allDates.length || basketIdx.length !== allDates.length) {
      return payload;
    }

    let sliceDates = allDates;
    if (key === 'YTD') {
      const endYear = String(allDates[allDates.length - 1] || '').slice(0, 4);
      sliceDates = allDates.filter((d) => String(d).slice(0, 4) === endYear);
    } else {
      const days = RANGE_DAYS[key];
      if (days > 0 && allDates.length > days) {
        sliceDates = allDates.slice(-days);
      }
    }
    if (sliceDates.length < 2) return payload;

    const startIdx = allDates.indexOf(sliceDates[0]);
    const endIdx = allDates.indexOf(sliceDates[sliceDates.length - 1]);
    if (startIdx < 0 || endIdx < 0) return payload;

    const fofWin = fofIdx.slice(startIdx, endIdx + 1);
    const basketWin = basketIdx.slice(startIdx, endIdx + 1);
    const fofPct = pctFromIndex(fofWin);
    const basketPct = pctFromIndex(basketWin);
    const spreadPct = fofPct.map((v, i) => {
      const b = basketPct[i];
      if (v == null || b == null) return null;
      return b - v;
    });

    const ds = downsample(sliceDates, fofPct, basketPct, spreadPct, 130);
    const fofEnd = fofPct[fofPct.length - 1];
    const basketEnd = basketPct[basketPct.length - 1];
    const spreadEnd = (basketEnd != null && fofEnd != null) ? basketEnd - fofEnd : null;

    return {
      ...payload,
      dates: ds.dates,
      fof_pct: ds.fofPct,
      basket_pct: ds.basketPct,
      spread_pct: ds.spreadPct,
      fof_end_pct: fofEnd,
      basket_end_pct: basketEnd,
      spread_end_pct: spreadEnd,
      n_days: sliceDates.length,
      start_date: sliceDates[0],
      end_date: sliceDates[sliceDates.length - 1],
      window_days: RANGE_DAYS[key] || sliceDates.length,
    };
  }

  function renderFoFCompareChart(container, payload, opts) {
    opts = opts || {};
    const rangeKey = opts.range || '6M';
    const sliced = sliceFoFChartPayload(payload, rangeKey);
    const width = Number(opts.width) || 900;
    const height = Number(opts.height) || 320;
    const padL = 48;
    const padR = 16;
    const padT = 24;
    const padB = 36;
    const innerW = width - padL - padR;
    const innerH = height - padT - padB;

    if (!sliced || !sliced.ok || !Array.isArray(sliced.dates) || !sliced.dates.length) {
      if (container) {
        container.innerHTML = '<div class="scenario-warning">FoF basket chart unavailable — rebuild dashboard after metrics backfill.</div>';
      }
      return null;
    }

    const n = sliced.dates.length;
    const xs = sliced.dates.map((_, i) => padL + (n <= 1 ? innerW / 2 : (i / (n - 1)) * innerW));
    const allY = []
      .concat(sliced.fof_pct || [])
      .concat(sliced.basket_pct || [])
      .concat(sliced.spread_pct || []);
    const toY = yScale(allY, innerH, 8);
    const y0 = padT + toY(0);

    const fofPath = buildPath(xs, sliced.fof_pct || [], (v) => padT + toY(v));
    const basketPath = buildPath(xs, sliced.basket_pct || [], (v) => padT + toY(v));
    const spreadPath = buildPath(xs, sliced.spread_pct || [], (v) => padT + toY(v));

    const fofEnd = sliced.fof_end_pct;
    const basketEnd = sliced.basket_end_pct;
    const spreadEnd = sliced.spread_end_pct;
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
      <text x="${padL}" y="${height - 10}" fill="${COLORS.text}" font-size="10">${sliced.start_date || ''} → ${sliced.end_date || ''} · ${sliced.n_days || n} days</text>
    </svg>`;

    if (container) container.innerHTML = svg;
    return svg;
  }

  return {
    renderFoFCompareChart,
    sliceFoFChartPayload,
    COLORS,
  };
});
