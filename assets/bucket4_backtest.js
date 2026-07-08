/**
 * Bucket 4 (inverse_decay_bucket4) dashboard helpers — precomputed artifact consumers.
 * Production method: short inverse ETF + short underlying, Kelly/QCQP weights, VCR cadence h.
 */
(function (globalObj) {
  'use strict';

  const BUCKET4_BACKTEST_URL = 'data/bucket4_backtest.json';
  const BUCKET4_STATE_URL = 'data/bucket4_backtest_state.json';

  let _artifactCache = null;
  let _artifactPromise = null;

  function normSym(x) {
    return String(x || '').trim().toUpperCase().replace(/\./g, '-');
  }

  function pairKey(etf, und) {
    return `${normSym(etf)}|${normSym(und)}`;
  }

  function isBucket4Record(record) {
    if (!record) return false;
    const sb = String(record.screener_bucket || '').toLowerCase();
    if (sb === 'bucket_4') return true;
    return Boolean(record.protected)
      && String(record.product_class || '').toLowerCase() === 'inverse';
  }

  function parseBucket4BacktestRoute(hash) {
    const h = String(hash || '').trim();
    const primary = /^#\/bucket-4(?:\/backtest)?(?:\/([^/?#]+))?$/i.exec(h);
    if (primary) {
      return {
        matches: true,
        preloadSymbol: primary[1] ? decodeURIComponent(primary[1]).toUpperCase() : '',
        legacy: false,
      };
    }
    const legacy = /^#\/backtest-flow(?:\/([^/?#]+))?$/i.exec(h);
    if (legacy) {
      return {
        matches: true,
        preloadSymbol: legacy[1] ? decodeURIComponent(legacy[1]).toUpperCase() : '',
        legacy: true,
      };
    }
    const chartCompat = /^#\/chart\/([^/?#]+)\/backtest-flow$/i.exec(h);
    if (chartCompat) {
      return {
        matches: true,
        preloadSymbol: decodeURIComponent(chartCompat[1]).toUpperCase(),
        legacy: true,
        compatibility: true,
      };
    }
    return { matches: false, preloadSymbol: '', legacy: false, compatibility: false };
  }

  async function loadArtifact({ force = false } = {}) {
    if (!force && _artifactCache) return _artifactCache;
    if (!force && _artifactPromise) return _artifactPromise;
    _artifactPromise = (async () => {
      const cacheBust = `?t=${Math.floor(Date.now() / 60000)}`;
      const res = await fetch(`${BUCKET4_BACKTEST_URL}${cacheBust}`, { cache: 'no-store' });
      if (!res.ok) throw new Error(`bucket4 backtest HTTP ${res.status}`);
      const data = JSON.parse(await res.text());
      _artifactCache = data;
      return data;
    })();
    try {
      return await _artifactPromise;
    } catch (e) {
      _artifactPromise = null;
      throw e;
    }
  }

  function sliceEquityWindow(artifact, startDate, endDate) {
    const dates = artifact?.sim_dates;
    const equity = artifact?.port_equity;
    if (!Array.isArray(dates) || !Array.isArray(equity) || dates.length !== equity.length) {
      return { dates: [], equity: [], error: 'Missing portfolio equity series.' };
    }
    let i0 = 0;
    let i1 = dates.length - 1;
    const start = String(startDate || '').trim();
    const end = String(endDate || '').trim();
    if (start) {
      const ix = dates.findIndex((d) => d >= start);
      if (ix >= 0) i0 = ix;
    }
    if (end) {
      let ix = dates.length - 1;
      for (let i = dates.length - 1; i >= 0; i -= 1) {
        if (dates[i] <= end) { ix = i; break; }
      }
      i1 = ix;
    }
    if (i0 > i1) return { dates: [], equity: [], error: 'Invalid date window.' };
    const dSlice = dates.slice(i0, i1 + 1);
    let eSlice = equity.slice(i0, i1 + 1);
    if (i0 > 0 && eSlice.length) {
      const base = equity[i0 - 1];
      if (Number.isFinite(base) && base > 0) {
        eSlice = eSlice.map((v) => v / base);
      }
    }
    return { dates: dSlice, equity: eSlice, error: null };
  }

  function portfolioEquityChartResult(artifact, { startDate = '', endDate = '', gross = 100000 } = {}) {
    const win = sliceEquityWindow(artifact, startDate, endDate);
    if (win.error || win.dates.length < 2) {
      return { ok: false, error: win.error || 'Need at least two days in window.' };
    }
    const g = Number(gross);
    const notional = Number.isFinite(g) && g > 0 ? g : 100000;
    const rows = win.dates.map((date, i) => {
      const eq = Number(win.equity[i]);
      const net = Number.isFinite(eq) ? notional * (eq - 1) : 0;
      return {
        date,
        netPnl: net,
        longPnl: net,
        shortPnl: 0,
        borrow: 0,
        distributions: 0,
        transactionCosts: 0,
        rebalance: false,
      };
    });
    const last = rows[rows.length - 1];
    return {
      ok: true,
      rows,
      inception: win.dates[0],
      end: win.dates[win.dates.length - 1],
      legChartLabels: { etf: 'Portfolio (net)', und: '—' },
      summary: {
        netPnl: last?.netPnl || 0,
        longPnl: last?.longPnl || 0,
        shortPnl: 0,
        borrowPaid: 0,
        distributionsPaid: 0,
        tCosts: 0,
        nRebalances: 0,
      },
    };
  }

  function pairHFromArtifact(artifact, etf, und) {
    if (!artifact?.h_state) return null;
    const st = artifact.h_state[pairKey(etf, und)];
    const h = Number(st?.h_last);
    return Number.isFinite(h) && h > 0 ? h : null;
  }

  function pairCadenceHint(artifact, etf, und) {
    const st = artifact?.h_state?.[pairKey(etf, und)];
    if (!st) return null;
    return {
      hLast: st.h_last,
      lastIntervalDays: st.last_interval_days,
      nRebalances: st.n_rebalances,
    };
  }

  function fmtPctAnnual(x, digits = 1) {
    const v = Number(x);
    if (!Number.isFinite(v)) return '—';
    return `${(v * 100).toFixed(digits)}%`;
  }

  const exported = {
    BUCKET4_BACKTEST_URL,
    BUCKET4_STATE_URL,
    isBucket4Record,
    parseBucket4BacktestRoute,
    loadArtifact,
    sliceEquityWindow,
    portfolioEquityChartResult,
    pairHFromArtifact,
    pairCadenceHint,
    fmtPctAnnual,
    pairKey,
    normSym,
  };

  if (typeof module !== 'undefined' && module.exports) module.exports = exported;
  if (globalObj) globalObj.Bucket4Backtest = exported;
})(typeof globalThis !== 'undefined' ? globalThis : typeof window !== 'undefined' ? window : globalThis);
