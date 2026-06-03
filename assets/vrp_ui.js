/* global window, module */
(function initVrpUi(globalObj) {
  const SIGNAL_THRESHOLDS = { STRONG_SELL: 30, SELL: 10, FADE: -10 };

  function isFiniteNum(v) {
    return v != null && Number.isFinite(Number(v));
  }

  function fmtEdgePp(v) {
    if (!isFiniteNum(v)) return '\u2014';
    const n = Number(v);
    return (n >= 0 ? '+' : '') + n.toFixed(1) + 'pp';
  }

  function fmtIvPp(v) {
    if (!isFiniteNum(v)) return '\u2014';
    const n = Number(v) * 100;
    return (n >= 0 ? '+' : '') + n.toFixed(1) + 'pp';
  }

  function fmtUsd(v) {
    if (!isFiniteNum(v)) return '\u2014';
    return '$' + Number(v).toFixed(2);
  }

  function edgeColor(edgePp) {
    if (!isFiniteNum(edgePp)) return 'var(--text-muted)';
    const e = Number(edgePp);
    if (e >= SIGNAL_THRESHOLDS.SELL) return '#10b981';
    if (e >= 0) return 'var(--text-muted)';
    if (e >= SIGNAL_THRESHOLDS.FADE) return '#f59e0b';
    return '#ef4444';
  }

  function gradePillStyle(grade) {
    const g = String(grade || '?').toUpperCase();
    if (g === 'A') return { color: '#fff', background: '#10b981' };
    if (g === 'B') return { color: '#10b981', background: 'rgba(16,185,129,0.15)' };
    if (g === 'C') return { color: '#f59e0b', background: 'rgba(245,158,11,0.15)' };
    if (g === 'D') return { color: '#fff', background: '#6b7280' };
    return { color: 'var(--text-muted)', background: 'transparent' };
  }

  function signalFor(edgePp, opts) {
    opts = opts || {};
    if (!isFiniteNum(edgePp)) {
      return { label: '\u2014', color: 'var(--text-muted)', bg: 'transparent', weight: 400 };
    }
    const e = Number(edgePp);
    if (opts.blockSignal && e > 0) {
      return { label: 'AVOID DATA', color: '#fff', bg: '#6b7280', weight: 700 };
    }
    if (e >= SIGNAL_THRESHOLDS.STRONG_SELL) {
      return { label: 'STRONG SELL', color: '#fff', bg: '#10b981', weight: 700 };
    }
    if (e >= SIGNAL_THRESHOLDS.SELL) {
      return { label: 'sell', color: '#10b981', bg: 'rgba(16,185,129,0.15)', weight: 600 };
    }
    if (e >= 0) return { label: 'lean sell', color: '#10b981', bg: 'transparent', weight: 500 };
    if (e >= SIGNAL_THRESHOLDS.FADE) {
      return { label: 'lean fade', color: '#f59e0b', bg: 'transparent', weight: 500 };
    }
    return { label: 'fade', color: '#fff', bg: '#ef4444', weight: 700 };
  }

  function hintFor(key, dict) {
    if (!key || !dict || !dict.fields) return '';
    const entry = dict.fields[key];
    return (entry && entry.tooltip) ? String(entry.tooltip) : '';
  }

  // ── Short-YieldBOOST structural edge (the unbiased primary ranker) ────────
  // net_edge_p50_annual uses edge_sign_convention = short_favorable_positive:
  // higher = more attractive to SHORT the YB ETF. This is annualized and
  // comparable across names — unlike edge_pp_of_max_loss (a 1-week put-spread
  // vol overlay in % of that spread's max-loss).
  const SHORT_SIGNAL = { STRONG: 0.15, SHORT: 0.05 };

  function fmtPctAnnual(v, digits) {
    if (!isFiniteNum(v)) return '\u2014';
    const n = Number(v) * 100;
    const d = digits == null ? 1 : digits;
    return (n >= 0 ? '+' : '') + n.toFixed(d) + '%';
  }

  function shortEdgeColor(p50) {
    if (!isFiniteNum(p50)) return 'var(--text-muted)';
    const e = Number(p50);
    if (e >= SHORT_SIGNAL.STRONG) return '#10b981';
    if (e >= SHORT_SIGNAL.SHORT) return '#34d399';
    if (e > 0) return 'var(--text-muted)';
    return '#ef4444';
  }

  // Headline SHORT signal from the screener net annual edge. p05>0 required for
  // the STRONG tier so a wide downside tail can't earn the top label. When the
  // name is not cleanly shortable (purgatory / no borrowable shares) the label
  // is capped but the row still sorts by p50.
  function shortSignalFor(p50, p05, opts) {
    opts = opts || {};
    if (!isFiniteNum(p50)) {
      return { label: '\u2014', color: 'var(--text-muted)', bg: 'transparent', weight: 400, tier: 'none' };
    }
    const e = Number(p50);
    if (opts.shortable === false) {
      return {
        label: 'No locate',
        color: '#fff',
        bg: '#6b7280',
        weight: 600,
        tier: 'nolocate',
        title: 'High modeled edge but IBKR has no borrowable shares (purgatory / no locate).',
      };
    }
    if (e >= SHORT_SIGNAL.STRONG && (!isFiniteNum(p05) || Number(p05) > 0)) {
      return { label: 'Top', color: '#fff', bg: '#10b981', weight: 700, tier: 'top' };
    }
    if (e >= SHORT_SIGNAL.SHORT) {
      return { label: 'Good', color: '#10b981', bg: 'rgba(16,185,129,0.15)', weight: 600, tier: 'good' };
    }
    if (e > 0) return { label: 'Thin', color: '#10b981', bg: 'transparent', weight: 500, tier: 'thin' };
    return { label: 'Skip', color: '#fff', bg: '#ef4444', weight: 700, tier: 'skip' };
  }

  // Sign-corrected: rich fund hedge = headwind to shorting the YB ETF.
  function shortThesisAlignment(edgePp) {
    if (!isFiniteNum(edgePp)) return { alignmentPp: null, direction: 'unknown', label: '\u2014', color: 'var(--text-muted)' };
    const a = -Number(edgePp);
    if (a > 1) return { alignmentPp: a, direction: 'tailwind', label: 'helps', color: '#10b981' };
    if (a < -1) return { alignmentPp: a, direction: 'headwind', label: 'hurts', color: '#f59e0b' };
    return { alignmentPp: a, direction: 'neutral', label: 'neutral', color: 'var(--text-muted)' };
  }

  // Fund hedge richness vs model (plain words — not spread trade verbs).
  function hedgeRichnessFor(edgePp, opts) {
    opts = opts || {};
    if (!isFiniteNum(edgePp)) return { label: '\u2014', color: 'var(--text-muted)' };
    if (opts.blockSignal) return { label: 'stale', color: '#6b7280' };
    const e = Number(edgePp);
    if (e >= SIGNAL_THRESHOLDS.SELL) return { label: 'rich', color: '#f59e0b' };
    if (e >= 0) return { label: 'fair', color: 'var(--text-muted)' };
    if (e >= SIGNAL_THRESHOLDS.FADE) return { label: 'fair', color: 'var(--text-muted)' };
    return { label: 'cheap', color: '#10b981' };
  }

  function freshLabel(row) {
    row = row || {};
    const qs = row.quote_sync || {};
    if (qs.sync_ok === false) {
      return { label: 'stale', color: '#ef4444', title: qs.sync_reason || 'inputs not synchronized' };
    }
    if (qs.sync_ok === true) {
      return { label: 'yes', color: '#10b981', title: qs.sync_reason || 'within window' };
    }
    return { label: '\u2014', color: 'var(--text-muted)', title: '' };
  }

  // Borrow: live fee when locate exists; else ~hist avg (see borrow_carry on row).
  function formatBorrowCarry(row) {
    row = row || {};
    const meta = row.borrow_carry || {};
    const display = meta.display_annual;
    if (display == null || !isFiniteNum(display)) {
      return {
        text: '\u2014',
        color: 'var(--text-muted)',
        isHistorical: false,
        title: meta.tooltip || 'No live or historical borrow on file',
      };
    }
    const isHistorical = meta.source === 'hist_avg' || meta.source === 'hist_med60';
    const tag = isHistorical ? ' ' + (meta.source_label || '~hist') : '';
    return {
      text: fmtPctAnnual(display) + tag,
      color: '#ef4444',
      isHistorical,
      title: meta.tooltip || '',
    };
  }

  // Cross-dataset quote-sync badge. Prefers the server-computed row.quote_sync
  // (timestamp divergence among sleeve quote / underlying quote / holdings /
  // screener); falls back to client freshness if absent.
  function syncBadge(row) {
    row = row || {};
    const qs = row.quote_sync || {};
    if (qs.sync_ok === false) {
      return { ok: false, label: 'NOT SYNCED', color: '#fff', bg: '#ef4444', detail: qs.sync_reason || 'inputs not synchronized' };
    }
    if (qs.sync_ok === true) {
      const gap = isFiniteNum(qs.quote_sync_gap_hours) ? Math.round(Number(qs.quote_sync_gap_hours)) + 'h' : '';
      return { ok: true, label: 'synced', color: '#10b981', bg: 'rgba(16,185,129,0.15)', detail: (qs.sync_reason || 'within window') + (gap ? ' · gap ' + gap : '') };
    }
    return { ok: null, label: '\u2014', color: 'var(--text-muted)', bg: 'transparent', detail: 'sync not evaluated' };
  }

  function ageMinutesSince(isoLike) {
    if (!isoLike) return null;
    const t = new Date(isoLike).getTime();
    if (!Number.isFinite(t)) return null;
    return Math.max(0, Math.round((Date.now() - t) / 60000));
  }

  function holdingsAgeDays(isoDate) {
    if (!isoDate) return null;
    const t = new Date(String(isoDate).split('T')[0] + 'T16:00:00-04:00').getTime();
    if (!Number.isFinite(t)) return null;
    return Math.max(0, Math.round((Date.now() - t) / (24 * 3600 * 1000)));
  }

  function rowFreshness(row) {
    row = row || {};
    const optMs = row.options_as_of ? new Date(row.options_as_of).getTime() : null;
    const undMs = row.underlying_options_as_of ? new Date(row.underlying_options_as_of).getTime() : null;
    const hMs = row.holdings_as_of
      ? new Date(String(row.holdings_as_of).split('T')[0] + 'T16:00:00-04:00').getTime()
      : null;
    const optAge = optMs ? Math.max(0, Math.round((Date.now() - optMs) / 60000)) : null;
    const undAge = undMs ? Math.max(0, Math.round((Date.now() - undMs) / 60000)) : null;
    const holdingsDays = hMs ? Math.max(0, Math.floor((Date.now() - hMs) / 86400000)) : null;
    const worst = Math.max(optAge ?? 0, undAge ?? 0);
    const grade = String(row.data_grade || '').toUpperCase() || '?';
    let label;
    if (worst >= 24 * 60) label = Math.round(worst / 60) + 'h old';
    else if (worst >= 60) label = Math.round(worst / 60) + 'h old';
    else label = worst + 'm old';
    const tone = grade === 'A' || grade === 'B' ? 'good' : grade === 'C' ? 'warn' : 'bad';
    const className = tone === 'good' ? 'fresh-good' : tone === 'warn' ? 'fresh-warn' : 'fresh-bad';
    const blockSignal = grade === 'C' || grade === 'D';
    return {
      optAge,
      undAge,
      holdingsDays,
      worst,
      label,
      className,
      tone,
      grade,
      blockSignal,
      reason: row.data_grade_reason || '',
      skew: Number(row.iv_expiry_skew_days) || 0,
    };
  }

  function fleetFreshness(data, vrpHealth) {
    const built = data && data.build_time ? new Date(data.build_time) : null;
    if (!built || Number.isNaN(built.getTime())) {
      return { label: 'Freshness: Unknown', className: 'fresh-warn', detail: 'no build_time' };
    }
    const buildMin = ageMinutesSince(data.build_time);
    const optMin = ageMinutesSince(vrpHealth && vrpHealth.last_options_refresh);
    const worstSleeveOptMin = Number.isFinite(Number(vrpHealth && vrpHealth.worst_sleeve_options_age_minutes))
      ? Number(vrpHealth.worst_sleeve_options_age_minutes) : null;
    const worstUndOptMin = Number.isFinite(Number(vrpHealth && vrpHealth.worst_underlying_options_age_minutes))
      ? Number(vrpHealth.worst_underlying_options_age_minutes) : null;
    const worstUndSym = vrpHealth && vrpHealth.worst_underlying_symbol
      ? String(vrpHealth.worst_underlying_symbol).toUpperCase() : null;
    const holdingsDays = vrpHealth && vrpHealth.holdings_age_trading_days != null
      ? Number(vrpHealth.holdings_age_trading_days)
      : holdingsAgeDays(vrpHealth && vrpHealth.holdings_as_of);
    const target = Number(data && data.borrow_refresh_interval_minutes) > 0
      ? Number(data.borrow_refresh_interval_minutes)
      : 30;
    const ftpOk = Boolean(data && data.ibkr_ftp_success);
    let bottleneck = { kind: 'build', minutes: buildMin };
    const candidates = [
      { kind: 'options_cache', minutes: optMin },
      { kind: 'worst_sleeve_quote', minutes: worstSleeveOptMin },
      { kind: 'worst_underlying_quote', minutes: worstUndOptMin },
    ];
    for (let i = 0; i < candidates.length; i += 1) {
      const c = candidates[i];
      if (Number.isFinite(c.minutes) && c.minutes > (bottleneck.minutes ?? 0)) bottleneck = c;
    }
    if (Number.isFinite(holdingsDays) && holdingsDays > 0) {
      const holdMin = holdingsDays * 24 * 60;
      if (holdMin > (bottleneck.minutes ?? 0)) {
        bottleneck = { kind: 'holdings', minutes: holdMin, days: holdingsDays };
      }
    }
    const minutes = bottleneck.minutes ?? buildMin;
    const isMissingChain = Array.isArray(vrpHealth && vrpHealth.missing_chain_ybs)
      && vrpHealth.missing_chain_ybs.length > 0;
    const warnAt = Math.max(target + 10, 45);
    const staleAt = Math.max(target * 2 + 10, 90);
    const labelKind = bottleneck.kind === 'holdings'
      ? 'holdings ' + bottleneck.days + 'd'
      : bottleneck.kind === 'worst_sleeve_quote'
        ? 'worst sleeve quote'
        : bottleneck.kind === 'worst_underlying_quote'
          ? (worstUndSym ? worstUndSym + ' und. quote' : 'worst und. quote')
          : bottleneck.kind === 'options_cache'
            ? 'options cache'
            : 'build';
    if (bottleneck.kind === 'holdings' && bottleneck.days >= 3) {
      return { label: 'Stale holdings (' + bottleneck.days + 'd)', className: 'fresh-bad', detail: 'bottleneck=' + labelKind };
    }
    if (minutes >= 24 * 60) {
      return { label: 'Stale (' + Math.round(minutes / 60) + 'h old, ' + labelKind + ')', className: 'fresh-bad', detail: 'bottleneck=' + labelKind };
    }
    if (minutes >= staleAt) {
      return { label: 'Stale (' + minutes + 'm old, ' + labelKind + ')', className: 'fresh-bad', detail: 'bottleneck=' + labelKind };
    }
    if (!ftpOk || minutes >= warnAt || isMissingChain) {
      const extra = isMissingChain
        ? vrpHealth.missing_chain_ybs.length + ' chains missing'
        : labelKind;
      return { label: 'Degraded (' + minutes + 'm old, ' + extra + ')', className: 'fresh-warn', detail: 'bottleneck=' + labelKind };
    }
    return { label: 'Fresh (' + minutes + 'm old, ' + labelKind + ')', className: 'fresh-good', detail: 'bottleneck=' + labelKind };
  }

  function flowStaleReasonLabel(reason) {
    const r = String(reason || '').toLowerCase();
    if (r === 'issuer_publish_lag') return 'T+1 lag';
    if (r === 'session_lag') return 'session lag';
    if (r === 'missing_metrics') return 'missing metrics';
    if (r === 'quality_excluded') return 'excluded';
    if (r === 'current_session') return 'current';
    return reason || 'stale';
  }

  function _freshPill(label, detail, tone) {
    const className = tone === 'good' ? 'fresh-good' : tone === 'warn' ? 'fresh-warn' : 'fresh-bad';
    return { label: label, className: className, detail: detail || '', tone: tone };
  }

  function domainFreshnessPills(freshnessSummary, data, vrpHealth) {
    const opts = (freshnessSummary && freshnessSummary.options) || {};
    const met = (freshnessSummary && freshnessSummary.metrics) || {};
    const flow = (freshnessSummary && freshnessSummary.flow) || {};
    const maxH = Number(freshnessSummary?.thresholds?.max_underlying_hours) || 48;
    const maxMin = maxH * 60;
    const warnAt = Math.max(24 * 60, maxMin * 0.5);
    const staleAt = maxMin;

    const optAge = Number.isFinite(Number(opts.oldest_enforced_symbol_age_minutes))
      ? Number(opts.oldest_enforced_symbol_age_minutes)
      : (Number.isFinite(Number(opts.oldest_symbol_age_minutes)) ? Number(opts.oldest_symbol_age_minutes) : null);
    let optionsTone = 'good';
    if (optAge != null && optAge >= staleAt) optionsTone = 'bad';
    else if (optAge != null && optAge >= warnAt) optionsTone = 'warn';
    const optLabel = optAge == null
      ? 'Options: —'
      : (optAge >= 24 * 60
        ? 'Options: ' + Math.round(optAge / 60) + 'h'
        : 'Options: ' + optAge + 'm');
    const options = _freshPill(optLabel, opts.oldest_enforced_symbol || opts.oldest_symbol || '', optionsTone);

    const blockers = Number(met.flow_blockers_prior_stale) || 0;
    const staleOk = Number(met.latest_stale_ok) || 0;
    const partial = Number(met.latest_missing) || 0;
    let metricsTone = 'good';
    if (blockers > 0) metricsTone = 'bad';
    else if (staleOk > 0 || partial > 0) metricsTone = 'warn';
    const metrics = _freshPill(
      blockers > 0 ? ('Metrics: ' + blockers + ' blockers') : ('Metrics: ' + (Number(met.latest_ok) || 0) + ' ok'),
      met.latest_date || '',
      metricsTone,
    );

    const actionable = Number(flow.underlyings_actionable_stale) || 0;
    const issuerLag = Number(flow.underlyings_issuer_publish_lag) || 0;
    let flowTone = 'good';
    if (actionable > 0) flowTone = 'bad';
    else if (issuerLag > 0) flowTone = 'warn';
    const flowLabel = actionable > 0
      ? ('Flow: ' + actionable + ' stale')
      : (issuerLag > 0 ? ('Flow: ' + issuerLag + ' T+1') : 'Flow: ok');
    const flowPill = _freshPill(flowLabel, flow.latest_date || '', flowTone);

    return { options: options, metrics: metrics, flow: flowPill };
  }

  const exported = {
    SIGNAL_THRESHOLDS,
    SHORT_SIGNAL,
    fmtEdgePp,
    fmtIvPp,
    fmtUsd,
    fmtPctAnnual,
    edgeColor,
    shortEdgeColor,
    gradePillStyle,
    signalFor,
    shortSignalFor,
    shortThesisAlignment,
    hedgeRichnessFor,
    freshLabel,
    formatBorrowCarry,
    syncBadge,
    hintFor,
    rowFreshness,
    fleetFreshness,
    flowStaleReasonLabel,
    domainFreshnessPills,
  };

  if (typeof module !== 'undefined' && module.exports) module.exports = exported;
  if (globalObj) globalObj.VrpUi = exported;
})(typeof window !== 'undefined' ? window : globalThis);
