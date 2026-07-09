/**
 * Bucket 4 (inverse_decay_bucket4) dashboard helpers — precomputed artifact consumers.
 * Production method: short inverse ETF + short underlying, Kelly/QCQP weights, VCR cadence h.
 */
(function (globalObj) {
  'use strict';

  const BUCKET4_BACKTEST_URL = 'data/bucket4_backtest.json';
  const BUCKET4_STATE_URL = 'data/bucket4_backtest_state.json';
  const BUCKET4_PAIR_BASE_URL = 'data/bucket4_pairs';

  let _artifactCache = null;
  let _artifactPromise = null;
  const _pairShardCache = new Map();
  const _pairShardPromises = new Map();

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
    return { matches: false, preloadSymbol: '', legacy: false, compatibility: false };
  }

  function parseBucket4PairRoute(hash) {
    const h = String(hash || '').trim();
    const m = /^#\/bucket-4\/pair\/([^/?#]+)$/i.exec(h);
    return m
      ? { matches: true, symbol: decodeURIComponent(m[1]).toUpperCase() }
      : { matches: false, symbol: '' };
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

  function pairShardUrl(symbol, artifact) {
    const sym = normSym(symbol);
    const rows = Array.isArray(artifact?.pair_manifest) ? artifact.pair_manifest : [];
    const hit = rows.find((p) => normSym(p?.etf) === sym);
    return hit?.shard_url || `${BUCKET4_PAIR_BASE_URL}/${encodeURIComponent(sym)}.json`;
  }

  async function loadPairShard(symbol, { force = false, artifact = null } = {}) {
    const sym = normSym(symbol);
    if (!sym) throw new Error('missing pair symbol');
    if (!force && _pairShardCache.has(sym)) return _pairShardCache.get(sym);
    if (!force && _pairShardPromises.has(sym)) return _pairShardPromises.get(sym);
    const url = pairShardUrl(sym, artifact);
    const promise = (async () => {
      const sep = url.includes('?') ? '&' : '?';
      const res = await fetch(`${url}${sep}t=${Math.floor(Date.now() / 60000)}`, { cache: 'no-store' });
      if (!res.ok) throw new Error(`bucket4 pair ${sym} HTTP ${res.status}`);
      const data = JSON.parse(await res.text());
      _pairShardCache.set(sym, data);
      return data;
    })();
    _pairShardPromises.set(sym, promise);
    try {
      return await promise;
    } catch (e) {
      _pairShardPromises.delete(sym);
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

  function pairSeriesFromArtifact(artifact, etf) {
    const sym = normSym(etf);
    const direct = artifact?.pair_series?.[sym];
    if (direct?.daily) return direct;
    const rows = Array.isArray(artifact?.pairs) ? artifact.pairs : [];
    const meta = rows.find((p) => normSym(p?.etf) === sym) || null;
    return meta ? { etf: sym, underlying: normSym(meta.underlying), summary: meta, daily: null } : null;
  }

  function sliceDailyPath(daily, startDate = '', endDate = '') {
    const dates = Array.isArray(daily?.dates) ? daily.dates : [];
    if (!dates.length) return { dates: [], i0: 0, i1: -1, error: 'Missing pair daily series.' };
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
    if (i0 > i1) return { dates: [], i0, i1, error: 'Invalid date window.' };
    return { dates: dates.slice(i0, i1 + 1), i0, i1, error: null };
  }

  function summaryFromReturns(dates, rets) {
    const vals = (Array.isArray(rets) ? rets : []).map(Number).filter(Number.isFinite);
    if (vals.length < 2) return { cagr: null, annVol: null, sharpe: null, maxDrawdown: null };
    let eq = 1;
    let peak = 1;
    let maxDd = 0;
    vals.forEach((r) => {
      eq *= (1 + Math.max(-0.95, Math.min(0.95, r)));
      peak = Math.max(peak, eq);
      maxDd = Math.min(maxDd, peak > 0 ? (eq / peak - 1) : 0);
    });
    const first = Date.parse(`${dates?.[0] || ''}T12:00:00Z`);
    const last = Date.parse(`${dates?.[dates.length - 1] || ''}T12:00:00Z`);
    const years = Number.isFinite(first) && Number.isFinite(last) && last > first
      ? (last - first) / (365.25 * 86400000)
      : vals.length / 252;
    const mean = vals.reduce((a, b) => a + b, 0) / vals.length;
    const variance = vals.reduce((a, b) => a + (b - mean) ** 2, 0) / Math.max(1, vals.length - 1);
    const vol = Math.sqrt(variance) * Math.sqrt(252);
    return {
      cagr: years > 0 && eq > 0 ? (eq ** (1 / years) - 1) : null,
      annVol: vol,
      sharpe: vol > 0 ? (mean * 252) / vol : null,
      maxDrawdown: maxDd,
    };
  }

  function pairChartResultFromArtifact(artifact, etf, { startDate = '', endDate = '', gross = 100000 } = {}) {
    const pair = pairSeriesFromArtifact(artifact, etf);
    const daily = pair?.daily;
    if (!pair || !daily) {
      return { ok: false, error: 'Production-book path unavailable here. Open the Bucket 4 Pair Report for the full screener row.' };
    }
    const win = sliceDailyPath(daily, startDate, endDate);
    if (win.error || win.dates.length < 2) {
      return { ok: false, error: win.error || 'Need at least two days in the selected window.' };
    }
    const notionalRaw = Number(gross);
    const notional = Number.isFinite(notionalRaw) && notionalRaw > 0 ? notionalRaw : 100000;
    const eq = (Array.isArray(daily.equity) ? daily.equity : []).slice(win.i0, win.i1 + 1).map(Number);
    const rets = (Array.isArray(daily.ret) ? daily.ret : []).slice(win.i0, win.i1 + 1).map(Number);
    const borrow = (Array.isArray(daily.borrow_cost) ? daily.borrow_cost : []).slice(win.i0, win.i1 + 1).map(Number);
    const fees = (Array.isArray(daily.rebalance_fee) ? daily.rebalance_fee : []).slice(win.i0, win.i1 + 1).map(Number);
    const rebalance = (Array.isArray(daily.rebalance) ? daily.rebalance : []).slice(win.i0, win.i1 + 1);
    const hUsed = (Array.isArray(daily.h_used) ? daily.h_used : []).slice(win.i0, win.i1 + 1).map(Number);
    const baseEq = Number(eq[0]);
    const scale = Number.isFinite(baseEq) && baseEq > 0 ? notional / baseEq : notional;
    let cumBorrow = 0;
    let cumFees = 0;
    const rows = win.dates.map((date, i) => {
      const equity = Number(eq[i]);
      const ret = Number(rets[i]);
      cumBorrow += Number.isFinite(borrow[i]) ? borrow[i] * scale : 0;
      cumFees += Number.isFinite(fees[i]) ? fees[i] * scale : 0;
      const netPnl = Number.isFinite(equity) && Number.isFinite(baseEq) ? (equity - baseEq) * scale : 0;
      return {
        date,
        netPnl,
        longPnl: netPnl + cumBorrow + cumFees,
        shortPnl: 0,
        borrow: cumBorrow,
        distributions: 0,
        transactionCosts: cumFees,
        rebalance: Boolean(rebalance[i]),
        rebalanceReason: Boolean(rebalance[i]) ? 'B4 policy' : '',
        exposureRatio: Number.isFinite(hUsed[i]) ? hUsed[i] : null,
        dailyRet: Number.isFinite(ret) ? ret : null,
      };
    });
    const stats = summaryFromReturns(win.dates, rets);
    const last = rows[rows.length - 1] || {};
    return {
      ok: true,
      rows,
      inception: win.dates[0],
      end: win.dates[win.dates.length - 1],
      legChartLabels: { etf: 'B4 net before costs', und: 'Underlying leg' },
      summary: {
        netPnl: last.netPnl || 0,
        longPnl: last.longPnl || 0,
        shortPnl: 0,
        borrowPaid: cumBorrow,
        distributionsPaid: 0,
        tCosts: cumFees,
        nRebalances: rows.filter((r) => r.rebalance).length,
        cagr: stats.cagr,
        annVol: stats.annVol,
        sharpe: stats.sharpe,
        maxDrawdown: stats.maxDrawdown,
      },
      pairSummary: pair.summary || {},
      rebalanceLog: Array.isArray(pair.rebalance_log) ? pair.rebalance_log : [],
    };
  }

  function defaultBookConfig(artifact) {
    const weights = artifact?.default_weights || {};
    const pairs = Array.isArray(artifact?.pairs) ? artifact.pairs : [];
    const out = {};
    pairs.forEach((p) => {
      const sym = normSym(p?.etf);
      if (!sym) return;
      const w = Number(weights[sym] ?? p?.portfolio_weight ?? p?.weight ?? 0);
      out[sym] = { enabled: w > 0, weight: Number.isFinite(w) && w > 0 ? w : 0 };
    });
    return out;
  }

  function normalizeBookWeights(config) {
    const entries = Object.entries(config || {})
      .map(([sym, cfg]) => [normSym(sym), { enabled: cfg?.enabled !== false, weight: Number(cfg?.weight) || 0 }])
      .filter(([sym]) => sym);
    const positive = entries.filter(([, cfg]) => cfg.enabled && cfg.weight > 0);
    const total = positive.reduce((s, [, cfg]) => s + cfg.weight, 0);
    const out = {};
    entries.forEach(([sym, cfg]) => {
      out[sym] = {
        enabled: cfg.enabled,
        weight: cfg.enabled && cfg.weight > 0 && total > 0 ? cfg.weight / total : 0,
      };
    });
    return out;
  }

  function recomputeBookFromPairs(artifact, config, { startDate = '', endDate = '', gross = 100000 } = {}) {
    const normCfg = normalizeBookWeights(config || defaultBookConfig(artifact));
    const active = Object.entries(normCfg).filter(([, cfg]) => cfg.enabled && cfg.weight > 0);
    if (!active.length) return { ok: false, error: 'Select at least one Bucket 4 pair.' };
    const dateSet = new Set();
    active.forEach(([sym]) => {
      const daily = pairSeriesFromArtifact(artifact, sym)?.daily;
      (daily?.dates || []).forEach((d) => dateSet.add(d));
    });
    const dates = Array.from(dateSet).sort();
    const start = String(startDate || '').trim();
    const end = String(endDate || '').trim();
    const windowDates = dates.filter((d) => (!start || d >= start) && (!end || d <= end));
    if (windowDates.length < 2) return { ok: false, error: 'Need at least two common book dates.' };
    const dateIndex = new Map(windowDates.map((d, i) => [d, i]));
    const returns = new Array(windowDates.length).fill(0);
    const contribution = {};
    active.forEach(([sym, cfg]) => {
      const daily = pairSeriesFromArtifact(artifact, sym)?.daily;
      const retByDate = new Map((daily?.dates || []).map((d, i) => [d, Number(daily?.ret?.[i])]));
      contribution[sym] = new Array(windowDates.length).fill(0);
      windowDates.forEach((d) => {
        const i = dateIndex.get(d);
        const r = retByDate.get(d);
        if (!Number.isFinite(r)) return;
        const wr = cfg.weight * Math.max(-0.95, Math.min(0.95, r));
        returns[i] += wr;
        contribution[sym][i] = wr;
      });
    });
    let eq = 1;
    const equity = returns.map((r, i) => {
      if (i > 0) eq *= (1 + Math.max(-0.95, Math.min(0.95, r)));
      return eq;
    });
    const notional = Number.isFinite(Number(gross)) && Number(gross) > 0 ? Number(gross) : 100000;
    const rows = windowDates.map((date, i) => {
      const net = notional * (equity[i] - 1);
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
    const stats = summaryFromReturns(windowDates, returns);
    return {
      ok: true,
      dates: windowDates,
      returns,
      equity,
      contribution,
      normalizedWeights: normCfg,
      rows,
      inception: windowDates[0],
      end: windowDates[windowDates.length - 1],
      legChartLabels: { etf: 'Custom book', und: 'Production overlay' },
      summary: {
        netPnl: rows[rows.length - 1]?.netPnl || 0,
        longPnl: rows[rows.length - 1]?.longPnl || 0,
        shortPnl: 0,
        borrowPaid: 0,
        distributionsPaid: 0,
        tCosts: 0,
        nRebalances: 0,
        ...stats,
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

  function pairManifestRows(artifact) {
    if (Array.isArray(artifact?.pair_manifest)) return artifact.pair_manifest;
    return (Array.isArray(artifact?.pairs) ? artifact.pairs : []).map((p) => ({
      ...p,
      etf: normSym(p?.etf),
      underlying: normSym(p?.underlying),
      in_production_book: true,
      production_status: 'production',
      gate_reason: 'production_book',
      model_status: 'ok',
      shard_url: `${BUCKET4_PAIR_BASE_URL}/${encodeURIComponent(normSym(p?.etf))}.json`,
      production_weight: p?.portfolio_weight,
    }));
  }

  function exportPairManifestCsv(rows) {
    const cols = [
      'etf', 'underlying', 'production_status', 'gate_reason', 'model_status', 'entry_date',
      'latest_date', 'production_weight', 'cagr', 'ann_vol', 'sharpe', 'max_drawdown',
      'bucket4_net_edge_annual', 'borrow', 'beta', 'vol_underlying_annual', 'n_rebalances',
    ];
    const esc = (v) => {
      if (v == null) return '';
      const s = String(v);
      return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    };
    return [cols.join(','), ...(Array.isArray(rows) ? rows : []).map((r) => cols.map((c) => esc(r?.[c])).join(','))].join('\n');
  }

  const exported = {
    BUCKET4_BACKTEST_URL,
    BUCKET4_STATE_URL,
    BUCKET4_PAIR_BASE_URL,
    isBucket4Record,
    parseBucket4BacktestRoute,
    parseBucket4PairRoute,
    loadArtifact,
    pairShardUrl,
    loadPairShard,
    sliceEquityWindow,
    portfolioEquityChartResult,
    pairSeriesFromArtifact,
    pairChartResultFromArtifact,
    defaultBookConfig,
    normalizeBookWeights,
    recomputeBookFromPairs,
    pairHFromArtifact,
    pairCadenceHint,
    fmtPctAnnual,
    pairManifestRows,
    exportPairManifestCsv,
    pairKey,
    normSym,
  };

  if (typeof module !== 'undefined' && module.exports) module.exports = exported;
  if (globalObj) globalObj.Bucket4Backtest = exported;
})(typeof globalThis !== 'undefined' ? globalThis : typeof window !== 'undefined' ? window : globalThis);
