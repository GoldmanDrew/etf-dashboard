/**
 * Autocallables tab — single-stock autocallable ETFs (Bucket 6).
 *
 * Consumes data/autocallables.json, written by ls-algo
 * scripts/export_autocallables_public.py. That file is a field-allowlist
 * projection of the private watch panel: the issuer's barrier ladder and fund
 * profile, plus metrics computed from public price and borrow series. The
 * watch artifact it derives from carries per-underlying book exposure and is
 * never published here.
 *
 * These funds are WATCH-ONLY. ls-algo hard-excludes them from every sleeve, so
 * nothing in this tab is a position or a recommendation.
 *
 * Two gates decide the status badge:
 *   GATE 1  TRADEABLE  enough borrow to short at size AND buy it back
 *   GATE 2  LIVE       the stock has fallen far enough that barrier delta is real
 *
 * Plain React.createElement: asset files are not run through the page's
 * in-browser Babel, so there is no JSX here. Same pattern as
 * assets/bucket5_insurance_backtest.js.
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory(require('react'));
  } else {
    root.Autocallables = factory(root.React);
  }
})(typeof self !== 'undefined' ? self : this, function (React) {
  const h = React.createElement;
  const { useState, useEffect, useMemo } = React;

  const DATA_URL = 'data/autocallables.json';
  const SCHEMA = 'autocallables-1';

  // ── formatting ────────────────────────────────────────────────────────────
  const isNum = (v) => v !== null && v !== undefined && Number.isFinite(Number(v));
  const pct = (v, nd = 1) => (isNum(v) ? (100 * Number(v)).toFixed(nd) + '%' : '—');
  const spct = (v, nd = 1) => (isNum(v) ? (Number(v) > 0 ? '+' : '') + (100 * Number(v)).toFixed(nd) + '%' : '—');
  const num = (v, nd = 2) => (isNum(v) ? Number(v).toFixed(nd) : '—');
  const int = (v) => (isNum(v) ? Math.round(Number(v)).toLocaleString() : '—');
  const usd = (v) => (isNum(v) ? '$' + Math.round(Number(v)).toLocaleString() : '—');
  const txt = (v) => (v === null || v === undefined || v === '' ? '—' : String(v));

  const STATUS_CLASS = {
    'BOTH GATES': 'ac-both', 'GATE 1': 'ac-g1', 'GATE 2': 'ac-g2', 'ASLEEP': 'ac-asleep',
  };

  // ── data ──────────────────────────────────────────────────────────────────
  async function load() {
    const bust = Math.floor(Date.now() / 60000);
    const r = await fetch(`${DATA_URL}?t=${bust}`, { cache: 'no-store' });
    if (!r.ok) throw new Error(`HTTP ${r.status} for ${DATA_URL}`);
    const d = await r.json();
    if (d.schema !== SCHEMA) {
      throw new Error(`unexpected schema ${d.schema || '(none)'} — rebuild with `
        + 'ls-algo scripts/export_autocallables_public.py');
    }
    return d;
  }

  function useAutocallables() {
    const [state, setState] = useState({ data: null, error: null, loading: true });
    useEffect(() => {
      let cancelled = false;
      load()
        .then((d) => { if (!cancelled) setState({ data: d, error: null, loading: false }); })
        .catch((e) => { if (!cancelled) setState({ data: null, error: String(e.message || e), loading: false }); });
      return () => { cancelled = true; };
    }, []);
    return state;
  }

  // ── list ──────────────────────────────────────────────────────────────────
  const COLUMNS = [
    { key: 'etf', label: 'ETF' },
    { key: 'underlying', label: 'Stock' },
    { key: 'status', label: 'Status' },
    { key: 'nearest_ko_distance', label: '→KO call', r: true, hint: 'Move UP in the stock to the nearest autocall. A live position gets reset there.' },
    { key: 'nearest_ki_distance', label: '→KI put', r: true, hint: 'Move DOWN to the nearest knock-in — where the barrier delta goes live.' },
    { key: 'tranches', label: 'Tranches', r: true, hint: 'Knocked in / autocalled, out of the ladder.' },
    { key: 'weighted_coupon_rate', label: 'Coupon', r: true },
    { key: 'delta_40d', label: 'Delta', r: true },
    { key: 'borrow_fee_annual', label: 'Borrow', r: true },
    { key: 'shares_available', label: 'Shares', r: true },
  ];

  function cellFor(f, key) {
    switch (key) {
      case 'etf': return h('span', { className: 'ac-tick' }, f.etf);
      case 'underlying': return h('span', { className: 'ac-und' }, f.underlying);
      case 'status':
        return h('span', { className: `ac-badge ${STATUS_CLASS[f.status] || 'ac-asleep'}` }, txt(f.status));
      case 'nearest_ko_distance':
        return h('span', { className: f.autocall_imminent ? 'ac-warn' : '' },
          spct(f.nearest_ko_distance) + (f.autocall_imminent ? ' !' : ''));
      case 'nearest_ki_distance':
        return h('span', { className: f.gate2_live ? 'ac-bad' : '' }, spct(f.nearest_ki_distance));
      case 'tranches': {
        const n = f.n_tranches;
        if (!isNum(n)) return h('span', { className: 'ac-mute' }, '—');
        const ki = f.tranches_ki_breached || 0;
        const ko = f.tranches_ko_triggered || 0;
        return h('span', { className: ki > 0 ? 'ac-bad' : '' }, `${ki} ki / ${ko} ko of ${n}`);
      }
      case 'weighted_coupon_rate': return pct(f.weighted_coupon_rate, 1);
      case 'delta_40d': return h('b', null, num(f.delta_40d));
      case 'borrow_fee_annual': return pct(f.borrow_fee_annual, 2);
      case 'shares_available': return int(f.shares_available);
      default: return txt(f[key]);
    }
  }

  function AutocallablesTable({ data, onSelect }) {
    const funds = (data && data.funds) || [];
    const c = (data && data.counts) || {};
    const notes = (data && data.data_notes) || [];

    return h('div', { className: 'ac-wrap' },
      h('div', { className: 'ac-head' },
        h('div', null,
          h('span', { className: 'ac-title' }, 'Single-Stock Autocallables'),
          h('span', { className: 'ac-sub' }, 'watch only · never sized')),
        h('div', { className: 'ac-counts' },
          h('span', null, `${c.gate1_open ?? 0}/${c.funds ?? 0} tradeable`),
          h('span', null, `${c.gate2_open ?? 0}/${c.funds ?? 0} live`),
          h('span', { className: (c.autocall_imminent ?? 0) > 0 ? 'ac-warn' : '' },
            `${c.autocall_imminent ?? 0} autocall imminent`),
          h('span', { className: 'ac-mute' }, `${c.barrier_sourced ?? 0}/${c.funds ?? 0} barriers sourced`))),

      notes.map((n, i) => h('div', { className: 'ac-note', key: i }, n)),

      h('div', { className: 'ac-scroll' },
        h('table', { className: 'ac-table' },
          h('thead', null, h('tr', null, COLUMNS.map((col) =>
            h('th', { key: col.key, className: col.r ? 'ac-r' : null, title: col.hint || null }, col.label)))),
          h('tbody', null, funds.map((f) =>
            h('tr', {
              key: f.etf,
              tabIndex: 0,
              onClick: () => onSelect && onSelect(f.etf),
              onKeyDown: (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onSelect && onSelect(f.etf); } },
              title: `Open ${f.etf} detail`,
            }, COLUMNS.map((col) =>
              h('td', { key: col.key, className: col.r ? 'ac-r' : null }, cellFor(f, col.key)))))))),

      h('div', { className: 'ac-foot' },
        'Watch-only: these funds are excluded from every sleeve. ',
        '→KO is the move up to the nearest autocall (', h('b', null, '!'),
        ' = imminent, a live position would be reset); →KI the move down to the nearest knock-in. ',
        `Generated ${txt(data && data.generated_utc)}.`));
  }

  // ── barrier ladder ────────────────────────────────────────────────────────
  function BarrierLadder({ fund, tranches }) {
    const ref = fund && fund.initial_reference_price;
    const rows = (tranches || []).filter((t) => isNum(t.ko_price) || isNum(t.ki_price));
    if (!isNum(ref) || !rows.length) {
      return h('p', { className: 'ac-empty' },
        'No published barrier ladder for this fund — the issuer holdings file could not be parsed.');
    }
    const spot = isNum(fund.spot_vs_initial) ? Number(fund.spot_vs_initial) * Number(ref) : null;

    const prices = [];
    rows.forEach((t) => { [t.ko_price, t.ki_price].forEach((p) => { if (isNum(p)) prices.push(Number(p)); }); });
    prices.push(Number(ref));
    if (spot !== null) prices.push(spot);
    const lo = Math.min.apply(null, prices) * 0.92;
    const hi = Math.max.apply(null, prices) * 1.06;

    const W = 680, H = 300, padL = 62, padR = 16, padT = 16, padB = 30;
    const y = (p) => padT + (hi - p) / (hi - lo) * (H - padT - padB);
    const colW = (W - padL - padR) / rows.length;
    const cx = (i) => padL + colW * (i + 0.5);

    const line = (p, stroke, dash, key) => h('line', {
      key, x1: padL, x2: W - padR, y1: y(p), y2: y(p),
      stroke, strokeWidth: 1.3, strokeDasharray: dash || null,
    });
    const label = (p, s, fill, key) => h('text', {
      key, x: padL - 7, y: y(p) + 3.5, textAnchor: 'end',
      fontSize: 10, fill, fontFamily: 'JetBrains Mono, monospace',
    }, s);

    const marks = [];
    rows.forEach((t, i) => {
      marks.push(h('line', {
        key: `v${i}`, x1: cx(i), x2: cx(i),
        y1: padT + 2, y2: H - padB, stroke: 'var(--border)', strokeWidth: 1,
      }));
      if (isNum(t.ko_price)) {
        const triggered = spot !== null && spot >= Number(t.ko_price);
        marks.push(h('circle', {
          key: `ko${i}`, cx: cx(i), cy: y(Number(t.ko_price)), r: 4.5,
          fill: triggered ? 'var(--warning)' : 'none',
          stroke: 'var(--warning)', strokeWidth: 1.6,
        }));
      }
      if (isNum(t.ki_price)) {
        const breached = spot !== null && spot <= Number(t.ki_price);
        marks.push(h('circle', {
          key: `ki${i}`, cx: cx(i), cy: y(Number(t.ki_price)), r: 4.5,
          fill: breached ? 'var(--negative)' : 'none',
          stroke: 'var(--negative)', strokeWidth: 1.6,
        }));
      }
      marks.push(h('text', {
        key: `t${i}`, x: cx(i), y: H - padB + 15, textAnchor: 'middle',
        fontSize: 9.5, fill: 'var(--text-muted)', fontFamily: 'JetBrains Mono, monospace',
      }, `T${i + 1}`));
    });

    return h('div', null,
      h('svg', {
        className: 'ac-ladder', viewBox: `0 0 ${W} ${H}`, role: 'img',
        'aria-label': `Barrier ladder for ${fund.etf}: ${rows.length} tranches, `
          + `knock-out barriers above and knock-in barriers below the initial reference price`,
      },
        marks,
        line(Number(ref), 'var(--text-secondary)', null, 'ref'),
        label(Number(ref), `ref ${num(ref, 2)}`, 'var(--text-secondary)', 'lref'),
        spot !== null ? line(spot, 'var(--accent-cyan)', '5 3', 'spot') : null,
        spot !== null ? label(spot, `spot ${num(spot, 2)}`, 'var(--accent-cyan)', 'lspot') : null),
      h('div', { className: 'ac-legend' },
        h('span', { className: 'lg-ko' }, 'KO — autocall (filled = triggered)'),
        h('span', { className: 'lg-ki' }, 'KI — knock-in (filled = breached)'),
        h('span', { className: 'lg-spot' }, 'spot')));
  }

  // ── detail ────────────────────────────────────────────────────────────────
  function KV({ rows }) {
    return h('dl', { className: 'ac-kv' }, rows.filter(Boolean).map(([k, v], i) =>
      [h('dt', { key: `k${i}` }, k), h('dd', { key: `v${i}` }, v)]));
  }

  function TermSheet({ tranches }) {
    if (!tranches || !tranches.length) {
      return h('p', { className: 'ac-empty' }, 'No tranche detail available.');
    }
    const cols = ['Tranche', 'Maturity', 'KO', 'KO px', 'KI', 'KI px', 'Coupon bar', 'Coupon', 'KI mon.', 'Put'];
    return h('div', { className: 'ac-scroll' },
      h('table', { className: 'ac-table' },
        h('thead', null, h('tr', null, cols.map((c, i) =>
          h('th', { key: c, className: i >= 2 && i !== 8 && i !== 9 ? 'ac-r' : null }, c)))),
        h('tbody', null, tranches.map((t, i) =>
          h('tr', { key: t.id || i, style: { cursor: 'default' } },
            h('td', { className: 'ac-tick' }, txt(t.id)),
            h('td', null, txt(t.maturity)),
            h('td', { className: 'ac-r' }, pct(t.ko_barrier, 0)),
            h('td', { className: 'ac-r ac-warn' }, num(t.ko_price, 2)),
            h('td', { className: 'ac-r' }, pct(t.ki_barrier, 0)),
            h('td', { className: 'ac-r ac-bad' }, num(t.ki_price, 2)),
            h('td', { className: 'ac-r' }, pct(t.coupon_barrier, 0)),
            h('td', { className: 'ac-r' }, pct(t.coupon_rate, 2)),
            h('td', null, txt(t.ki_monitoring)),
            h('td', null, txt(t.put_type)))))));
  }

  function AutocallableDetail({ data, etf, onBack }) {
    const fund = useMemo(
      () => ((data && data.funds) || []).find((f) => f.etf === etf) || null,
      [data, etf],
    );
    const det = (data && data.detail && data.detail[etf]) || {};
    const terms = det.terms || {};
    const profile = det.profile || {};

    if (!fund) {
      return h('div', { className: 'ac-detail' },
        h('button', { className: 'ac-back', onClick: onBack }, '← Autocallables'),
        h('p', { className: 'ac-empty' }, `${etf} is not in the current artifact.`));
    }

    return h('div', { className: 'ac-detail' },
      h('button', { className: 'ac-back', onClick: onBack }, '← Autocallables'),
      h('div', { className: 'ac-dhead' },
        h('span', { className: 'ac-dtick' }, fund.etf),
        h('span', { className: 'ac-dund' },
          `${txt(profile.underlying_name || fund.underlying)} (${txt(fund.underlying)})`),
        h('span', { className: `ac-badge ${STATUS_CLASS[fund.status] || 'ac-asleep'}` }, txt(fund.status))),
      profile.objective ? h('p', { className: 'ac-dobjective' }, profile.objective) : null,

      terms.error ? h('div', { className: 'ac-note' }, terms.error) : null,

      h('div', { className: 'ac-panels' },
        h('div', { className: 'ac-panel' },
          h('h4', null, `Barrier ladder — ${txt(terms.n_tranches)} tranches as of ${txt(terms.position_date)}`),
          h(BarrierLadder, { fund, tranches: terms.tranches }))),

      h('div', { className: 'ac-panels', style: { marginTop: 16 } },
        h('div', { className: 'ac-panel' },
          h('h4', null, 'Term sheet — one row per tranche'),
          h(TermSheet, { tranches: terms.tranches }))),

      h('div', { className: 'ac-panels ac-two', style: { marginTop: 16 } },
        h('div', { className: 'ac-panel' },
          h('h4', null, 'Barrier proximity'),
          h(KV, { rows: [
            ['Nearest KO (call side)', spct(fund.nearest_ko_distance)],
            ['KO price', num(fund.nearest_ko_price, 2)],
            ['Nearest KI (put side)', spct(fund.nearest_ki_distance)],
            ['KI price', num(fund.nearest_ki_price, 2)],
            ['Nearer side', txt(fund.nearest_barrier_side)],
            ['Autocall imminent', fund.autocall_imminent ? 'yes' : 'no'],
            ['Tranches knocked in', `${fund.tranches_ki_breached ?? '—'} of ${txt(fund.n_tranches)}`],
            ['Tranches autocalled', `${fund.tranches_ko_triggered ?? '—'} of ${txt(fund.n_tranches)}`],
            ['Coupon-eligible', pct(fund.coupon_eligible_frac, 0)],
            ['Principal at risk', pct(fund.principal_at_risk_frac, 0)],
            ['Gate 2 basis', txt(fund.gate2_basis)],
          ] })),

        h('div', { className: 'ac-panel' },
          h('h4', null, 'Fund profile'),
          h(KV, { rows: [
            ['Issuer', txt(fund.issuer)],
            ['CUSIP', txt(profile.cusip)],
            ['Inception', txt(profile.inception_date || fund.inception_date)],
            ['Days since launch', int(fund.days_since_launch)],
            ['Management fee', txt(profile.management_fee)],
            ['Expense ratio', txt(profile.expense_ratio)],
            ['Distribution frequency', txt(profile.distribution_frequency)],
            ['Initial reference', num(terms.initial_reference_price || fund.initial_reference_price, 2)],
            ['NAV / share', num(terms.nav_per_share, 2)],
            ['Shares outstanding', int(terms.shares_outstanding)],
          ] }),
          (profile.documents && profile.documents.length)
            ? h('div', { style: { marginTop: 14 } },
              h('h4', null, 'Documents'),
              h('ul', { className: 'ac-docs' }, profile.documents.map((d, i) =>
                h('li', { key: i }, h('a', {
                  href: d.url, target: '_blank', rel: 'noopener noreferrer',
                }, d.label)))))
            : null,
          profile.product_page
            ? h('p', { style: { marginTop: 12 } },
              h('a', {
                href: profile.product_page, target: '_blank', rel: 'noopener noreferrer',
                style: { color: 'var(--accent-blue)', fontSize: 12 },
              }, 'Issuer product page ↗'))
            : null),

        h('div', { className: 'ac-panel' },
          h('h4', null, 'Price & tracking'),
          h(KV, { rows: [
            ['Spot vs initial', isNum(fund.spot_vs_initial) ? num(fund.spot_vs_initial, 4) : '—'],
            ['Underlying vs inception', spct(fund.und_vs_inception)],
            ['Underlying max drawdown', spct(fund.und_max_drawdown_since_inception)],
            ['Delta (40d)', num(fund.delta_40d)],
            ['Delta peak to date', num(fund.delta_peak_to_date)],
            ['Convexity gap', num(fund.convexity_gap)],
            ['Upside capture', num(fund.upside_capture)],
            ['Downside capture', num(fund.downside_capture)],
            ['NAV vs par', spct(fund.nav_vs_par)],
            ['Premium / discount', spct(fund.premium_discount_to_nav)],
            ['Observations', int(fund.n_obs)],
          ] })),

        h('div', { className: 'ac-panel' },
          h('h4', null, 'Liquidity, borrow & income'),
          h(KV, { rows: [
            ['In borrow feed', fund.in_borrow_feed ? 'yes' : 'no'],
            ['Shares available', int(fund.shares_available)],
            ['Borrow fee', pct(fund.borrow_fee_annual, 2)],
            ['ADV (20d shares)', int(fund.adv_shares_20d)],
            ['ADV (20d USD)', usd(fund.adv_usd_20d)],
            ['Weighted coupon', pct(fund.weighted_coupon_rate, 2)],
            ['Distribution (ann.)', pct(fund.distribution_annualized, 1)],
            ['Last distribution', pct(fund.distribution_last_pct, 2)],
            ['Months paid consecutively', int(fund.months_paid_consecutive)],
            ['Distribution cut flag', fund.distribution_cut_flag ? 'yes' : 'no'],
            ['Hedged carry (ann.)', pct(fund.hedged_carry_ann, 1)
              + (fund.hedged_carry_confidence === 'low' ? ' (low conf.)' : '')],
          ] }))),

      h('div', { className: 'ac-foot' },
        'Barrier terms are from the issuer’s published daily holdings file',
        terms.source_file ? ` (${terms.source_file})` : '',
        '. Watch-only: this fund is excluded from every sleeve.'));
  }

  // ── page ──────────────────────────────────────────────────────────────────
  function AutocallablesPage({ selected, onSelect, onBack, onCount }) {
    const { data, error, loading } = useAutocallables();

    // The tab badge cannot count these from `records` -- they are watch-only and
    // live in their own artifact -- so report the count up once it is known.
    useEffect(() => {
      if (onCount && data && Array.isArray(data.funds)) onCount(data.funds.length);
    }, [data, onCount]);

    if (loading) return h('p', { className: 'ac-empty' }, 'Loading autocallables…');
    if (error) {
      return h('div', { className: 'ac-wrap' },
        h('p', { className: 'ac-empty' }, `Autocallables unavailable — ${error}`),
        h('p', { className: 'ac-empty' },
          'In ls-algo: python scripts/export_autocallables_public.py'));
    }
    if (selected) return h(AutocallableDetail, { data, etf: selected, onBack });
    return h(AutocallablesTable, { data, onSelect });
  }

  return {
    AutocallablesPage, AutocallablesTable, AutocallableDetail, BarrierLadder,
    load, DATA_URL, SCHEMA,
  };
});
