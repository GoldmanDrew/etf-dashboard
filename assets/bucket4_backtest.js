/**
 * Bucket 4 (inverse_decay_bucket4) dashboard helpers — precomputed artifact consumers.
 * Production method: short inverse ETF + short underlying, v6 opt2 + crash-budget
 * weights (trim-only, cash residual), VCR cadence h.
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

  /** Chart-page hash panel → activePanel. Keeps drip (`backtest-flow`) distinct from issuer `flow`. */
  function mapChartDefaultPanel(panel) {
    const p = String(panel || '').trim().toLowerCase();
    if (p === 'backtest') return 'backtest';
    if (p === 'backtest-flow') return 'backtest-flow';
    if (p === 'flow') return 'flow';
    if (p === 'trade') return 'trade';
    if (p === 'decay') return 'decay';
    if (p === 'basket') return 'basket';
    if (p === 'vrp') return 'vrp';
    if (p === 'borrow') return 'borrow';
    if (p === 'stats') return 'stats';
    if (p === 'scenarios') return 'scenarios';
    return 'chart';
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
    if (artifact?.schema === 'bucket4_backtest.v4') {
      const manifest = Array.isArray(artifact?.pair_manifest) ? artifact.pair_manifest : [];
      if (!manifest.some((p) => normSym(p?.etf) === sym)) {
        throw new Error(`bucket4 pair ${sym} is not in the authoritative production ledger`);
      }
    }
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
    return { dates: dSlice, equity: eSlice, i0, i1, error: null };
  }

  function portfolioEquityChartResult(artifact, { startDate = '', endDate = '', gross = 100000 } = {}) {
    const win = sliceEquityWindow(artifact, startDate, endDate);
    if (win.error || win.dates.length < 2) {
      return { ok: false, error: win.error || 'Need at least two days in window.' };
    }
    const g = Number(gross);
    const notional = Number.isFinite(g) && g > 0 ? g : 100000;
    const sourceBudget = Number(artifact?.sleeve_budget_usd);
    const dollarScale = Number.isFinite(sourceBudget) && sourceBudget > 0 ? notional / sourceBudget : 1;
    const costs = artifact?.costs || {};
    const borrowDaily = (costs.borrow_cost_usd || []).slice(win.i0, win.i1 + 1).map(Number);
    const marginDaily = (costs.margin_cost_usd || []).slice(win.i0, win.i1 + 1).map(Number);
    const txnDaily = (costs.txn_cost_usd || []).slice(win.i0, win.i1 + 1).map(Number);
    let borrowCum = 0;
    let txnCum = 0;
    const rows = win.dates.map((date, i) => {
      const eq = Number(win.equity[i]);
      const net = Number.isFinite(eq) ? notional * (eq - 1) : 0;
      borrowCum += ((Number.isFinite(borrowDaily[i]) ? borrowDaily[i] : 0)
        + (Number.isFinite(marginDaily[i]) ? marginDaily[i] : 0)) * dollarScale;
      txnCum += (Number.isFinite(txnDaily[i]) ? txnDaily[i] : 0) * dollarScale;
      return {
        date,
        netPnl: net,
        longPnl: net + borrowCum + txnCum,
        shortPnl: 0,
        borrow: borrowCum,
        distributions: 0,
        transactionCosts: txnCum,
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
        borrowPaid: borrowCum,
        distributionsPaid: 0,
        tCosts: txnCum,
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

  /**
   * Scale a unit-capital B4 daily path (`initial_capital: 1.0`) into dollar chart rows.
   * Accepts either an inline `pair_series` block or a full `bucket4_pairs/{ETF}.json` shard.
   */
  function pairChartResultFromDaily(pairMeta, daily, {
    startDate = '',
    endDate = '',
    gross = 100000,
    rebalanceLog = null,
  } = {}) {
    if (!daily || !Array.isArray(daily.dates) || !daily.dates.length) {
      return { ok: false, error: 'Missing pair daily series.' };
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
    const bookActivity = (Array.isArray(daily.book_activity) ? daily.book_activity : []).slice(win.i0, win.i1 + 1);
    const hUsed = (Array.isArray(daily.h_used) ? daily.h_used : []).slice(win.i0, win.i1 + 1).map(Number);
    const exportedDrawdown = (Array.isArray(daily.drawdown) ? daily.drawdown : []).slice(win.i0, win.i1 + 1).map(Number);
    const grossExp = (Array.isArray(daily.gross_exposure) ? daily.gross_exposure : []).slice(win.i0, win.i1 + 1).map(Number);
    const totalGrossU = (Array.isArray(daily.total_gross) ? daily.total_gross : []).slice(win.i0, win.i1 + 1).map(Number);
    const netPnlDollars = (Array.isArray(daily.net_pnl_dollars) ? daily.net_pnl_dollars : []).slice(win.i0, win.i1 + 1).map(Number);
    const pricePnlDollars = (Array.isArray(daily.price_pnl_cum_dollars) ? daily.price_pnl_cum_dollars : []).slice(win.i0, win.i1 + 1).map(Number);
    const grossDollars = (Array.isArray(daily.gross_exposure_dollars) ? daily.gross_exposure_dollars : []).slice(win.i0, win.i1 + 1).map(Number);
    const borrowCumDollars = (Array.isArray(daily.borrow_cost_cum_dollars) ? daily.borrow_cost_cum_dollars : []).slice(win.i0, win.i1 + 1).map(Number);
    const txnCumDollars = (Array.isArray(daily.txn_cost_cum_dollars) ? daily.txn_cost_cum_dollars : []).slice(win.i0, win.i1 + 1).map(Number);
    const reasonPath = (Array.isArray(daily.rebalance_reason) ? daily.rebalance_reason : []).slice(win.i0, win.i1 + 1);
    const etfLegU = (Array.isArray(daily.etf_leg_pnl_cum) ? daily.etf_leg_pnl_cum : []).slice(win.i0, win.i1 + 1).map(Number);
    const undLegU = (Array.isArray(daily.underlying_leg_pnl_cum) ? daily.underlying_leg_pnl_cum : []).slice(win.i0, win.i1 + 1).map(Number);
    const borrowCumU = (Array.isArray(daily.borrow_cost_cum) ? daily.borrow_cost_cum : []).slice(win.i0, win.i1 + 1).map(Number);
    const tcostCumU = (Array.isArray(daily.tcost_cum) ? daily.tcost_cum : []).slice(win.i0, win.i1 + 1).map(Number);
    const baseEq = Number(eq[0]);
    const scale = Number.isFinite(baseEq) && baseEq > 0 ? notional / baseEq : notional;
    const sourceBasis = Number(pairMeta?.notional_basis_usd || pairMeta?.summary?.notional_basis_usd);
    const actualDollarScale = Number.isFinite(sourceBasis) && sourceBasis > 0 ? notional / sourceBasis : 1;
    const actualLedger = pairMeta?.ledger_mode === 'actual_dollar' || netPnlDollars.some(Number.isFinite);
    let runningPeak = -Infinity;
    let cumBorrow = 0;
    let cumFees = 0;
    const rows = win.dates.map((date, i) => {
      const equity = Number(eq[i]);
      const ret = Number(rets[i]);
      const dayBorrow = Number.isFinite(borrow[i]) ? borrow[i] * scale : 0;
      const dayFee = Number.isFinite(fees[i]) ? fees[i] * scale : 0;
      cumBorrow += dayBorrow;
      cumFees += dayFee;
      const netPnl = actualLedger && Number.isFinite(netPnlDollars[i])
        ? netPnlDollars[i] * actualDollarScale
        : (Number.isFinite(equity) && Number.isFinite(baseEq) ? (equity - baseEq) * scale : 0);
      const etfLeg = actualLedger && Number.isFinite(pricePnlDollars[i])
        ? pricePnlDollars[i] * actualDollarScale
        : (Number.isFinite(etfLegU[i]) ? etfLegU[i] * scale : null);
      const undLeg = Number.isFinite(undLegU[i]) ? undLegU[i] * scale : null;
      const totalGross = actualLedger && Number.isFinite(grossDollars[i])
        ? grossDollars[i] * actualDollarScale
        : Number.isFinite(totalGrossU[i])
        ? totalGrossU[i] * scale
        : (Number.isFinite(etfLeg) && Number.isFinite(undLeg) ? etfLeg + undLeg : null);
      const borrowScaled = actualLedger && Number.isFinite(borrowCumDollars[i])
        ? borrowCumDollars[i] * actualDollarScale
        : (Number.isFinite(borrowCumU[i]) ? borrowCumU[i] * scale : cumBorrow);
      const tcostScaled = actualLedger && Number.isFinite(txnCumDollars[i])
        ? txnCumDollars[i] * actualDollarScale
        : (Number.isFinite(tcostCumU[i]) ? tcostCumU[i] * scale : cumFees);
      const derivedEquity = equity;
      if (Number.isFinite(derivedEquity)) runningPeak = Math.max(runningPeak, derivedEquity);
      const derivedDrawdown = Number.isFinite(derivedEquity) && runningPeak > 0
        ? derivedEquity / runningPeak - 1
        : null;
      return {
        date,
        netPnl,
        longPnl: Number.isFinite(etfLeg) ? etfLeg : (netPnl + borrowScaled + tcostScaled),
        shortPnl: Number.isFinite(undLeg) ? undLeg : 0,
        borrow: borrowScaled,
        distributions: 0,
        transactionCosts: tcostScaled,
        totalGross,
        // Contract exports drawdown. Recompute only as a backwards-compatible
        // guard; missing data must never be coerced into a false 0% path.
        drawdown: Number.isFinite(exportedDrawdown[i]) ? exportedDrawdown[i] : derivedDrawdown,
        h: Number.isFinite(hUsed[i]) ? hUsed[i] : null,
        gross: Number.isFinite(grossExp[i]) ? grossExp[i] : null,
        rebalance: Boolean(rebalance[i]),
        rebalanceReason: reasonPath[i] || (Boolean(rebalance[i]) ? 'B4 policy' : ''),
        bookActivity: Boolean(bookActivity[i]),
        exposureRatio: Number.isFinite(hUsed[i]) ? hUsed[i] : null,
        dailyRet: Number.isFinite(ret) ? ret : null,
        equityUnit: Number.isFinite(equity) ? equity : null,
        equityDollars: Number.isFinite(equity) ? equity * scale : null,
      };
    });
    const stats = summaryFromReturns(win.dates, rets);
    const last = rows[rows.length - 1] || {};
    const log = Array.isArray(rebalanceLog)
      ? rebalanceLog
      : (Array.isArray(pairMeta?.rebalance_log) ? pairMeta.rebalance_log : []);
    return {
      ok: true,
      notional,
      scale,
      rows,
      inception: win.dates[0],
      end: win.dates[win.dates.length - 1],
      legChartLabels: actualLedger
        ? { etf: 'Price PnL', und: 'Other / financing' }
        : { etf: 'ETF leg PnL', und: 'Underlying leg PnL' },
      summary: {
        netPnl: last.netPnl || 0,
        longPnl: last.longPnl || 0,
        shortPnl: last.shortPnl || 0,
        totalGross: last.totalGross || 0,
        borrowPaid: last.borrow || cumBorrow,
        distributionsPaid: 0,
        tCosts: last.transactionCosts || cumFees,
        nRebalances: rows.filter((r) => r.rebalance).length,
        nDays: rows.length,
        cagr: stats.cagr,
        annVol: stats.annVol,
        sharpe: stats.sharpe,
        maxDrawdown: stats.maxDrawdown,
      },
      pairSummary: pairMeta?.summary || pairMeta || {},
      rebalanceLog: log,
    };
  }

  function pairChartResultFromArtifact(artifact, etf, { startDate = '', endDate = '', gross = 100000 } = {}) {
    const pair = pairSeriesFromArtifact(artifact, etf);
    const daily = pair?.daily;
    if (!pair || !daily) {
      return { ok: false, error: 'Production-book path unavailable here. Open the Bucket 4 Pair Report for the full screener row.' };
    }
    return pairChartResultFromDaily(pair, daily, {
      startDate,
      endDate,
      gross,
      rebalanceLog: pair.rebalance_log,
    });
  }

  /** Scale a loaded `bucket4_pairs/{ETF}.json` shard (gated or production) into dollar chart rows. */
  function pairChartResultFromShard(shard, { startDate = '', endDate = '', gross = 100000 } = {}) {
    if (!shard || !shard.daily) {
      return { ok: false, error: 'Pair shard unavailable.' };
    }
    return pairChartResultFromDaily(shard, shard.daily, {
      startDate,
      endDate,
      gross,
      rebalanceLog: shard.rebalance_log,
    });
  }

  /** Overlay production portfolio equity vs custom book on one PairBacktestChart result. */
  function overlayBookChartResult(productionResult, customResult) {
    if (!customResult?.ok || !Array.isArray(customResult.rows) || customResult.rows.length < 2) {
      return productionResult?.ok ? productionResult : { ok: false, error: customResult?.error || 'Custom book unavailable.' };
    }
    if (!productionResult?.ok || !Array.isArray(productionResult.rows)) {
      return {
        ...customResult,
        legChartLabels: { etf: 'Custom book', und: '—' },
      };
    }
    const prodByDate = new Map(productionResult.rows.map((r) => [r.date, r]));
    const rows = customResult.rows.map((r) => {
      const prod = prodByDate.get(r.date);
      const prodNet = prod && Number.isFinite(Number(prod.netPnl)) ? Number(prod.netPnl) : null;
      return {
        ...r,
        // Net = custom; longPnl series = production overlay; suppress cost lines.
        longPnl: prodNet != null ? prodNet : r.netPnl,
        shortPnl: NaN,
        borrow: 0,
        distributions: 0,
        transactionCosts: 0,
      };
    });
    return {
      ...customResult,
      rows,
      legChartLabels: { etf: 'Production book', und: '—' },
      overlayMode: 'prod_vs_custom',
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

  function targetDeployedFraction(artifact) {
    const d = Number(artifact?.deployed_fraction);
    if (Number.isFinite(d) && d > 0) return Math.min(1, Math.max(0, d));
    const parity = artifact?.parity || {};
    if (parity.custom_book_match_deployed_fraction === false) return 1;
    return 1;
  }

  function maxNameWeight(artifact) {
    const m = Number(artifact?.parity?.max_weight);
    if (Number.isFinite(m) && m > 0) return m;
    return 0.35;
  }

  function normalizeBookWeights(config, opts) {
    const preserveCash = opts?.preserveCash !== false;
    const targetDeployed = opts?.targetDeployed;
    const maxW = opts?.maxWeight;
    const entries = Object.entries(config || {})
      .map(([sym, cfg]) => [normSym(sym), { enabled: cfg?.enabled !== false, weight: Number(cfg?.weight) || 0 }])
      .filter(([sym]) => sym);
    const positive = entries.filter(([, cfg]) => cfg.enabled && cfg.weight > 0);
    let total = positive.reduce((s, [, cfg]) => s + cfg.weight, 0);
    // Cap per-name (crash / opt2 max_weight spirit) before scaling.
    if (Number.isFinite(maxW) && maxW > 0) {
      positive.forEach(([, cfg]) => {
        if (cfg.weight > maxW) cfg.weight = maxW;
      });
      total = positive.reduce((s, [, cfg]) => s + cfg.weight, 0);
    }
    let scale = 1;
    if (total > 1 + 1e-9) {
      scale = 1 / total;
    } else if (
      Number.isFinite(targetDeployed) &&
      targetDeployed > 0 &&
      total > 1e-12 &&
      Math.abs(total - targetDeployed) > 1e-6
    ) {
      // Equal / edge presets: scale to production deployed fraction (keep cash).
      scale = targetDeployed / total;
    } else if (!preserveCash && total > 0) {
      scale = 1 / total;
    }
    const out = {};
    entries.forEach(([sym, cfg]) => {
      let w = cfg.enabled && cfg.weight > 0 ? cfg.weight * scale : 0;
      if (Number.isFinite(maxW) && maxW > 0 && w > maxW) w = maxW;
      out[sym] = { enabled: cfg.enabled, weight: w };
    });
    return out;
  }

  function recomputeBookFromPairs(artifact, config, { startDate = '', endDate = '', gross = 100000 } = {}) {
    if (artifact?.research_reblend_enabled === false || artifact?.parity?.research_reblend_enabled === false) {
      return { ok: false, error: 'Research reblending is disabled for the authoritative production ledger.' };
    }
    const matchDeployed = artifact?.parity?.custom_book_match_deployed_fraction !== false;
    const normCfg = normalizeBookWeights(config || defaultBookConfig(artifact), {
      preserveCash: true,
      targetDeployed: matchDeployed ? targetDeployedFraction(artifact) : undefined,
      maxWeight: maxNameWeight(artifact),
    });
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
    const deployed = active.reduce((s, [, cfg]) => s + cfg.weight, 0);
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
      deployedFraction: deployed,
      cashResidual: Math.max(0, 1 - deployed),
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
    mapChartDefaultPanel,
    loadArtifact,
    pairShardUrl,
    loadPairShard,
    sliceEquityWindow,
    portfolioEquityChartResult,
    pairSeriesFromArtifact,
    pairChartResultFromDaily,
    pairChartResultFromArtifact,
    pairChartResultFromShard,
    overlayBookChartResult,
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
