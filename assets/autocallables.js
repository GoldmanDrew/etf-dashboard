/**
 * Autocallables tab — single-stock autocallable ETFs (Bucket 6).
 *
 * Consumes data/autocallables.json, written by ls-algo
 * scripts/export_autocallables_public.py. That file is a field-allowlist
 * projection of the private watch panel: the issuer's barrier ladder and fund
 * profile, plus metrics computed from public price and borrow series. The watch
 * artifact it derives from carries per-underlying book exposure and is never
 * published here.
 *
 * These funds are WATCH-ONLY. ls-algo hard-excludes them from every sleeve, so
 * nothing in this tab is a position or a recommendation.
 *
 * COLOUR: knock-out and knock-in are not two categories, they are the two poles
 * of a price axis around the initial reference — KO above, KI below. Encoding
 * them categorically produced the original amber/red pair: two warm hues doing a
 * job that wants opposition. It failed the lightness band against the card
 * surface, and no darker amber/red re-step cleared the normal-vision floor.
 * Blue/red is a diverging pair and passes every check with wide margin (CVD dE
 * 26.7, normal 35.2). Spot and the reference line are annotations, not series,
 * so they wear a neutral text token and never a hue.
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
  const { useState, useEffect, useMemo, useCallback } = React;

  const DATA_URL = 'data/autocallables.json';
  const SCHEMA = 'autocallables-1';

  // Gate 2 opens within 20% of a published knock-in. The list meter is scaled to
  // 50%, which comfortably contains the widest observed distance (~48%).
  const GATE2_THRESHOLD = 0.20;
  const METER_SCALE = 0.50;

  // ── formatting ────────────────────────────────────────────────────────────
  const isNum = (v) => v !== null && v !== undefined && Number.isFinite(Number(v));
  const pct = (v, nd = 1) => (isNum(v) ? (100 * Number(v)).toFixed(nd) + '%' : '—');
  const spct = (v, nd = 1) => (isNum(v) ? (Number(v) > 0 ? '+' : '') + (100 * Number(v)).toFixed(nd) + '%' : '—');
  const num = (v, nd = 2) => (isNum(v) ? Number(v).toFixed(nd) : '—');
  const int = (v) => (isNum(v) ? Math.round(Number(v)).toLocaleString() : '—');
  const usd = (v) => (isNum(v) ? '$' + Math.round(Number(v)).toLocaleString() : '—');
  const txt = (v) => (v === null || v === undefined || v === '' ? '—' : String(v));

  // A ratio-to-initial reads as a stray decimal beside a column of percentages.
  const vsInitial = (v) => (isNum(v) ? spct(Number(v) - 1) : '—');

  const GATE2_BASIS = { published_barrier: 'Issuer barrier', inception_proxy: 'Inception proxy' };
  const basisLabel = (v) => GATE2_BASIS[v] || txt(v);

  // The artifact ships a *_reason string for every field it could not source.
  // Rendering the em-dash alone turns "not collected" into "looks broken".
  const withReason = (value, reason) => (
    value === '—' && reason
      ? h('span', { className: 'ac-mute', title: reason }, '— ', h('i', { className: 'ac-why' }, 'why'))
      : value
  );

  // "Nomura1 ATC" is the issuer's row label; the ladder position is what matters.
  const trancheLabel = (t, i) => {
    const m = String(t.id || '').match(/^([A-Za-z]+)/);
    return { n: 'T' + (i + 1), issuer: m ? m[1] : '' };
  };

  const STATUS_CLASS = {
    'BOTH GATES': 'ac-both', 'GATE 1': 'ac-g1', 'GATE 2': 'ac-g2', 'ASLEEP': 'ac-asleep',
  };

  function hitLabel(isKo, hit) {
    if (isKo) return hit ? 'Already autocalled' : 'Not yet reached';
    return hit ? 'Already knocked in' : 'Not yet breached';
  }

  // ── data ──────────────────────────────────────────────────────────────────
  async function load() {
    const bust = Math.floor(Date.now() / 60000);
    const r = await fetch(DATA_URL + '?t=' + bust, { cache: 'no-store' });
    if (!r.ok) throw new Error('HTTP ' + r.status + ' for ' + DATA_URL);
    const d = await r.json();
    if (d.schema !== SCHEMA) {
      throw new Error('unexpected schema ' + (d.schema || '(none)')
        + ' — rebuild with ls-algo scripts/export_autocallables_public.py');
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

  // ── small pieces ──────────────────────────────────────────────────────────
  function StatusBadge({ status }) {
    return h('span', { className: 'ac-badge ' + (STATUS_CLASS[status] || 'ac-asleep') }, txt(status));
  }

  /** One mark per tranche: blue = autocalled, red = knocked in, hollow = pending. */
  function TranchePips({ fund }) {
    const n = fund.n_tranches;
    if (!isNum(n) || n <= 0) return h('span', { className: 'ac-mute' }, '—');
    const ki = fund.tranches_ki_breached || 0;
    const ko = fund.tranches_ko_triggered || 0;
    const pips = [];
    for (let i = 0; i < n; i += 1) {
      const cls = i < ki ? 'ac-pip ac-pip-ki' : (i < ki + ko ? 'ac-pip ac-pip-ko' : 'ac-pip');
      pips.push(h('i', { key: i, className: cls }));
    }
    return h('span', {
      className: 'ac-pips',
      title: ki + ' knocked in, ' + ko + ' autocalled, of ' + n + ' tranches',
    }, pips);
  }

  /**
   * Distance to the nearest knock-in as a bar. Length is proportional to the
   * distance, so a SHORT bar means close; the tick marks the 20% Gate 2 point.
   */
  function ProximityMeter({ distance }) {
    if (!isNum(distance)) return null;
    const d = Math.min(Math.abs(Number(distance)), METER_SCALE);
    const w = (d / METER_SCALE) * 100;
    const live = Math.abs(Number(distance)) <= GATE2_THRESHOLD;
    return h('span', {
      className: 'ac-meter',
      title: pct(Math.abs(distance)) + ' from the nearest knock-in; Gate 2 opens inside '
        + pct(GATE2_THRESHOLD, 0),
    },
      h('i', { className: 'ac-meter-fill' + (live ? ' ac-meter-live' : ''), style: { width: w + '%' } }),
      h('i', { className: 'ac-meter-tick', style: { left: ((GATE2_THRESHOLD / METER_SCALE) * 100) + '%' } }));
  }

  // ── barrier ladder ────────────────────────────────────────────────────────
  function BarrierLadder({ fund, tranches, compact }) {
    const [hover, setHover] = useState(null);

    const ref = fund && fund.initial_reference_price;
    const rows = (tranches || []).filter((t) => isNum(t.ko_price) || isNum(t.ki_price));
    if (!isNum(ref) || !rows.length) {
      return h('p', { className: 'ac-empty' }, compact
        ? 'no published ladder'
        : 'No published barrier ladder — the issuer holdings file could not be parsed for this fund.');
    }
    const spot = isNum(fund.spot_vs_initial) ? Number(fund.spot_vs_initial) * Number(ref) : null;

    const prices = [Number(ref)];
    rows.forEach((t) => { [t.ko_price, t.ki_price].forEach((p) => { if (isNum(p)) prices.push(Number(p)); }); });
    if (spot !== null) prices.push(spot);
    const lo = Math.min.apply(null, prices) * 0.94;
    const hi = Math.max.apply(null, prices) * 1.05;

    const W = compact ? 300 : 960;
    const H = compact ? 180 : 400;
    const padL = compact ? 12 : 104;
    const padR = compact ? 12 : 24;
    const padT = compact ? 12 : 20;
    const padB = compact ? 20 : 34;

    const y = (p) => padT + (hi - p) / (hi - lo) * (H - padT - padB);
    const colW = (W - padL - padR) / rows.length;
    const cx = (i) => padL + colW * (i + 0.5);
    const refY = y(Number(ref));
    const kids = [];

    // Above the reference a tranche autocalls; below it knocks in. Very low alpha
    // so the two regions read pre-attentively without competing with the marks.
    kids.push(h('rect', {
      key: 'zko', x: padL, y: padT, width: W - padL - padR, height: Math.max(0, refY - padT),
      fill: 'var(--accent-blue)', opacity: 0.055,
    }));
    kids.push(h('rect', {
      key: 'zki', x: padL, y: refY, width: W - padL - padR, height: Math.max(0, H - padB - refY),
      fill: 'var(--accent-red)', opacity: 0.055,
    }));

    rows.forEach((t, i) => {
      kids.push(h('line', {
        key: 'v' + i, x1: cx(i), x2: cx(i), y1: padT + 2, y2: H - padB,
        stroke: 'var(--border)', strokeWidth: 1,
      }));
    });

    kids.push(h('line', {
      key: 'ref', x1: padL, x2: W - padR, y1: refY, y2: refY,
      stroke: 'var(--text-secondary)', strokeWidth: 1.4,
    }));
    if (spot !== null) {
      kids.push(h('line', {
        key: 'spot', x1: padL, x2: W - padR, y1: y(spot), y2: y(spot),
        stroke: 'var(--text-secondary)', strokeWidth: 1.4, strokeDasharray: '5 4', opacity: 0.85,
      }));
    }
    if (!compact) {
      kids.push(h('text', {
        key: 'lref', x: padL - 9, y: refY + 3.5, textAnchor: 'end',
        fontSize: 10.5, fill: 'var(--text-secondary)', fontFamily: 'JetBrains Mono, monospace',
      }, 'ref ' + num(ref, 2)));
      if (spot !== null) {
        kids.push(h('text', {
          key: 'lspot', x: padL - 9, y: y(spot) + 3.5, textAnchor: 'end',
          fontSize: 10.5, fill: 'var(--text-secondary)', fontFamily: 'JetBrains Mono, monospace',
        }, 'spot ' + num(spot, 2)));
      }
    }

    const addMarker = (t, i, side) => {
      const price = side === 'ko' ? t.ko_price : t.ki_price;
      if (!isNum(price)) return;
      const hit = side === 'ko'
        ? (spot !== null && spot >= Number(price))
        : (spot !== null && spot <= Number(price));
      const stroke = side === 'ko' ? 'var(--accent-blue)' : 'var(--accent-red)';
      const key = side + i;
      const enter = () => setHover({
        side: side, t: t, i: i, hit: hit,
        xPct: (cx(i) / W) * 100, yPct: (y(Number(price)) / H) * 100,
      });
      kids.push(h('circle', {
        key: key, cx: cx(i), cy: y(Number(price)), r: compact ? 3.4 : 5,
        fill: hit ? stroke : 'var(--bg-card)', stroke: stroke, strokeWidth: 1.8,
      }));
      if (!compact) {
        // Hit target larger than the mark itself.
        kids.push(h('circle', {
          key: key + 'h', cx: cx(i), cy: y(Number(price)), r: 13, fill: 'transparent',
          style: { cursor: 'pointer' },
          onMouseEnter: enter, onMouseLeave: () => setHover(null),
        }));
      }
    };
    rows.forEach((t, i) => { addMarker(t, i, 'ko'); addMarker(t, i, 'ki'); });

    rows.forEach((t, i) => {
      kids.push(h('text', {
        key: 't' + i, x: cx(i), y: H - padB + (compact ? 13 : 17), textAnchor: 'middle',
        fontSize: compact ? 8.5 : 10, fill: 'var(--text-muted)', fontFamily: 'JetBrains Mono, monospace',
      }, 'T' + (i + 1)));
    });

    let tip = null;
    if (hover) {
      const t = hover.t;
      const isKo = hover.side === 'ko';
      tip = h('div', {
        className: 'ac-tip',
        style: {
          left: hover.xPct + '%', top: hover.yPct + '%',
          transform: hover.xPct > 66 ? 'translate(-104%, -112%)' : 'translate(10px, -112%)',
        },
      },
        h('div', { className: 'ac-tip-h' }, 'T' + (hover.i + 1) + ' · ',
          h('span', { className: isKo ? 'ac-ko' : 'ac-ki' }, isKo ? 'knock-out' : 'knock-in')),
        h('div', { className: 'ac-tip-r' }, h('span', null, 'Barrier'), h('b', null, pct(isKo ? t.ko_barrier : t.ki_barrier, 0))),
        h('div', { className: 'ac-tip-r' }, h('span', null, 'Price'), h('b', null, num(isKo ? t.ko_price : t.ki_price, 2))),
        h('div', { className: 'ac-tip-r' }, h('span', null, 'Coupon'), h('b', null, pct(t.coupon_rate, 2))),
        h('div', { className: 'ac-tip-r' }, h('span', null, 'Maturity'), h('b', null, txt(t.maturity))),
        h('div', { className: 'ac-tip-s' }, hitLabel(isKo, hover.hit)));
    }

    return h('div', { className: 'ac-ladder-wrap' },
      h('svg', {
        className: compact ? 'ac-ladder ac-ladder-sm' : 'ac-ladder',
        viewBox: '0 0 ' + W + ' ' + H,
        preserveAspectRatio: 'none',
        role: 'img',
        'aria-label': 'Barrier ladder for ' + fund.etf + ': ' + rows.length
          + ' tranches. Knock-out barriers above the initial reference price, knock-in barriers below.',
      }, kids),
      tip,
      compact ? null : h('div', { className: 'ac-legend' },
        h('span', { className: 'lg-ko' }, 'KO — autocall (filled = triggered)'),
        h('span', { className: 'lg-ki' }, 'KI — knock-in (filled = breached)'),
        h('span', { className: 'lg-neutral' }, 'reference & spot')));
  }

  // ── KPI row ───────────────────────────────────────────────────────────────
  function Kpi({ label, value, sub, tone }) {
    return h('div', { className: 'ac-kpi' },
      h('div', { className: 'ac-kpi-l' }, label),
      h('div', { className: 'ac-kpi-v' + (tone ? ' ' + tone : '') }, value),
      sub ? h('div', { className: 'ac-kpi-s' }, sub) : null);
  }

  function KpiRow({ fund }) {
    return h('div', { className: 'ac-kpis' },
      h(Kpi, {
        label: '→ Knock-in', value: spct(fund.nearest_ki_distance),
        tone: fund.gate2_live ? 'ac-bad' : '',
        sub: fund.gate2_live ? 'Gate 2 open' : 'the move down we want',
      }),
      h(Kpi, {
        label: '→ Autocall', value: spct(fund.nearest_ko_distance),
        tone: fund.autocall_imminent ? 'ac-warn' : '',
        sub: fund.autocall_imminent ? 'imminent — would reset' : 'the move up that resets',
      }),
      h(Kpi, { label: 'Weighted coupon', value: pct(fund.weighted_coupon_rate, 1), sub: 'across live tranches' }),
      h(Kpi, { label: 'Hedge delta (40d)', value: num(fund.delta_40d), sub: 'peak ' + num(fund.delta_peak_to_date) }),
      h(Kpi, {
        label: 'Tranches', value: h(TranchePips, { fund: fund }),
        sub: (fund.tranches_ki_breached != null ? fund.tranches_ki_breached : '—') + ' in · '
          + (fund.tranches_ko_triggered != null ? fund.tranches_ko_triggered : '—') + ' called · '
          + txt(fund.n_tranches) + ' total',
      }));
  }

  function KV({ rows }) {
    return h('dl', { className: 'ac-kv' }, rows.filter(Boolean).map((r, i) =>
      [h('dt', { key: 'k' + i }, r[0]), h('dd', { key: 'v' + i }, r[1])]));
  }

  // ── term sheet ────────────────────────────────────────────────────────────
  function TermSheet({ tranches }) {
    if (!tranches || !tranches.length) {
      return h('p', { className: 'ac-empty' }, 'No tranche detail available.');
    }
    const cols = [
      ['Tranche', false], ['Maturity', false], ['KO', true], ['KO px', true],
      ['KI', true], ['KI px', true], ['Coupon bar', true], ['Coupon', true],
      ['KI mon.', false], ['Put', false],
    ];
    return h('div', { className: 'ac-scroll' },
      h('table', { className: 'ac-table ac-table-flush' },
        h('thead', null, h('tr', null, cols.map((c) =>
          h('th', { key: c[0], className: c[1] ? 'ac-r' : null }, c[0])))),
        h('tbody', null, tranches.map((t, i) => {
          const lab = trancheLabel(t, i);
          return h('tr', { key: t.id || i, className: 'ac-static' },
            h('td', null,
              h('span', { className: 'ac-tick' }, lab.n),
              lab.issuer ? h('span', { className: 'ac-issuer' }, ' ' + lab.issuer) : null),
            h('td', null, txt(t.maturity)),
            h('td', { className: 'ac-r' }, pct(t.ko_barrier, 0)),
            h('td', { className: 'ac-r ac-ko' }, num(t.ko_price, 2)),
            h('td', { className: 'ac-r' }, pct(t.ki_barrier, 0)),
            h('td', { className: 'ac-r ac-ki' }, num(t.ki_price, 2)),
            h('td', { className: 'ac-r' }, pct(t.coupon_barrier, 0)),
            h('td', { className: 'ac-r' }, pct(t.coupon_rate, 2)),
            h('td', null, txt(t.ki_monitoring)),
            h('td', null, txt(t.put_type)));
        }))));
  }

  // ── list ──────────────────────────────────────────────────────────────────
  const COLUMNS = [
    { key: 'etf', label: 'ETF' },
    { key: 'underlying', label: 'Stock' },
    { key: 'status', label: 'Status' },
    { key: 'nearest_ko_distance', label: '→KO call', r: true, hint: 'Move UP in the stock to the nearest autocall. A live position gets reset there.' },
    { key: 'nearest_ki_distance', label: '→KI put', r: true, hint: 'Move DOWN to the nearest knock-in, where the barrier delta goes live. Bar length is that distance; the tick is the 20% Gate 2 threshold.' },
    { key: 'tranches', label: 'Tranches', hint: 'One mark per tranche: blue = autocalled, red = knocked in, hollow = pending.' },
    { key: 'weighted_coupon_rate', label: 'Coupon', r: true },
    { key: 'delta_40d', label: 'Delta', r: true },
    { key: 'borrow', label: 'Borrow', r: true, hint: 'IBKR short-stock file. Most of these funds are absent from it entirely — no lendable supply, which is why Gate 1 is shut.' },
  ];

  const SORT_VALUE = {
    etf: (f) => f.etf,
    underlying: (f) => f.underlying,
    status: (f) => ['BOTH GATES', 'GATE 2', 'GATE 1', 'ASLEEP'].indexOf(f.status),
    tranches: (f) => (f.tranches_ko_triggered || 0) + (f.tranches_ki_breached || 0),
    borrow: (f) => (f.in_borrow_feed === false ? -1 : (f.shares_available || 0)),
  };
  const sortValue = (f, key) => (SORT_VALUE[key] ? SORT_VALUE[key](f) : f[key]);

  function cellFor(f, key) {
    switch (key) {
      case 'etf': return h('span', { className: 'ac-tick' }, f.etf);
      case 'underlying': return h('span', { className: 'ac-und' }, f.underlying);
      case 'status': return h(StatusBadge, { status: f.status });
      case 'nearest_ko_distance':
        return h('span', { className: f.autocall_imminent ? 'ac-warn' : '' },
          spct(f.nearest_ko_distance) + (f.autocall_imminent ? ' !' : ''));
      case 'nearest_ki_distance':
        return h('span', { className: 'ac-ki-cell' },
          h(ProximityMeter, { distance: f.nearest_ki_distance }),
          h('span', { className: f.gate2_live ? 'ac-bad' : '' }, spct(f.nearest_ki_distance)));
      case 'tranches': return h(TranchePips, { fund: f });
      case 'weighted_coupon_rate': return pct(f.weighted_coupon_rate, 1);
      case 'delta_40d': return h('b', null, num(f.delta_40d));
      case 'borrow':
        return f.in_borrow_feed === false
          ? h('span', { className: 'ac-mute', title: 'Absent from the IBKR short-stock file — no lendable supply at any price.' }, 'not in feed')
          : h('span', null, int(f.shares_available) + ' @ ' + pct(f.borrow_fee_annual, 1));
      default: return txt(f[key]);
    }
  }

  function AutocallablesTable({ data, onSelect }) {
    const [sort, setSort] = useState({ key: null, desc: true });
    const funds = (data && data.funds) || [];

    const rows = useMemo(() => {
      if (!sort.key) return funds;
      const out = funds.slice();
      out.sort((a, b) => {
        const av = sortValue(a, sort.key);
        const bv = sortValue(b, sort.key);
        if (av === bv) return 0;
        if (av === null || av === undefined) return 1;
        if (bv === null || bv === undefined) return -1;
        const c = typeof av === 'string' ? av.localeCompare(bv) : av - bv;
        return sort.desc ? -c : c;
      });
      return out;
    }, [funds, sort]);

    const onSort = useCallback((key) => {
      setSort((s) => (s.key === key ? { key: key, desc: !s.desc } : { key: key, desc: true }));
    }, []);

    const c = (data && data.counts) || {};
    const notes = (data && data.data_notes) || [];

    return h('div', { className: 'ac-wrap' },
      h('div', { className: 'ac-head' },
        h('div', null,
          h('span', { className: 'ac-title' }, 'Single-Stock Autocallables'),
          h('span', { className: 'ac-sub' }, 'watch only · never sized')),
        h('div', { className: 'ac-counts' },
          h('span', null, (c.gate1_open || 0) + '/' + (c.funds || 0) + ' tradeable'),
          h('span', null, (c.gate2_open || 0) + '/' + (c.funds || 0) + ' live'),
          h('span', { className: (c.autocall_imminent || 0) > 0 ? 'ac-warn' : '' },
            (c.autocall_imminent || 0) + ' autocall imminent'),
          h('span', { className: 'ac-mute' },
            (c.barrier_sourced || 0) + '/' + (c.funds || 0) + ' barriers sourced'))),

      notes.map((n, i) => h('div', { className: 'ac-note', key: i }, n)),

      h('div', { className: 'ac-scroll' },
        h('table', { className: 'ac-table' },
          h('thead', null, h('tr', null, COLUMNS.map((col) =>
            h('th', {
              key: col.key,
              className: [col.r ? 'ac-r' : null, 'ac-sortable', sort.key === col.key ? 'ac-sorted' : null]
                .filter(Boolean).join(' '),
              title: col.hint || 'Sort',
              onClick: () => onSort(col.key),
            },
              col.label,
              sort.key === col.key ? h('span', { className: 'ac-arrow' }, sort.desc ? ' ▼' : ' ▲') : null)))),
          h('tbody', null, rows.map((f) =>
            h('tr', {
              key: f.etf, tabIndex: 0,
              onClick: () => onSelect && onSelect(f.etf),
              onKeyDown: (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onSelect && onSelect(f.etf); } },
              title: 'Open ' + f.etf + ' detail',
            }, COLUMNS.map((col) =>
              h('td', { key: col.key, className: col.r ? 'ac-r' : null }, cellFor(f, col.key)))))))),

      h('div', { className: 'ac-foot' },
        'Watch-only: these funds are excluded from every sleeve. ',
        '→KO is the move up to the nearest autocall (', h('b', null, '!'),
        ' = imminent, a live position would be reset); →KI the move down to the nearest knock-in, ',
        'with the bar showing that distance against the 20% Gate 2 threshold. ',
        'Generated ' + txt(data && data.generated_utc) + '.'));
  }

  // ── ladders: small multiples ──────────────────────────────────────────────
  function LaddersGrid({ data, onSelect }) {
    const funds = (data && data.funds) || [];
    return h('div', { className: 'ac-wrap' },
      h('div', { className: 'ac-head' },
        h('div', null,
          h('span', { className: 'ac-title' }, 'Barrier ladders'),
          h('span', { className: 'ac-sub' }, 'all eight funds, same convention')),
        h('div', { className: 'ac-legend ac-legend-inline' },
          h('span', { className: 'lg-ko' }, 'KO above'),
          h('span', { className: 'lg-ki' }, 'KI below'),
          h('span', { className: 'lg-neutral' }, 'ref & spot'))),
      h('div', { className: 'ac-grid' }, funds.map((f) => {
        const terms = ((data.detail || {})[f.etf] || {}).terms || {};
        return h('button', {
          key: f.etf, className: 'ac-cell', type: 'button',
          onClick: () => onSelect && onSelect(f.etf),
          title: 'Open ' + f.etf + ' detail',
        },
          h('div', { className: 'ac-cell-h' },
            h('span', { className: 'ac-tick' }, f.etf),
            h('span', { className: 'ac-und' }, f.underlying),
            h(StatusBadge, { status: f.status })),
          h(BarrierLadder, { fund: f, tranches: terms.tranches, compact: true }),
          h('div', { className: 'ac-cell-f' },
            h('span', { className: f.gate2_live ? 'ac-bad' : 'ac-mute' }, '→KI ' + spct(f.nearest_ki_distance)),
            h('span', { className: f.autocall_imminent ? 'ac-warn' : 'ac-mute' }, '→KO ' + spct(f.nearest_ko_distance))));
      })),
      h('div', { className: 'ac-foot' },
        'Each panel is scaled to its own fund, so heights are not comparable between panels — ',
        'the shape shows where spot sits within that fund’s own ladder. Click a panel to open it.'));
  }

  // ── detail ────────────────────────────────────────────────────────────────
  const SUBTABS = [
    { key: 'overview', label: 'Overview' },
    { key: 'terms', label: 'Terms' },
    { key: 'fund', label: 'Fund' },
    { key: 'tracking', label: 'Tracking' },
  ];

  function AutocallableDetail({ data, etf, onBack }) {
    const [sub, setSub] = useState('overview');
    useEffect(() => { setSub('overview'); }, [etf]);

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
        h('p', { className: 'ac-empty' }, etf + ' is not in the current artifact.'));
    }

    const overview = () => h('div', null,
      h(KpiRow, { fund: fund }),
      terms.error ? h('div', { className: 'ac-note' }, terms.error) : null,
      h('div', { className: 'ac-panel' },
        h('h4', null, 'Barrier ladder — ' + txt(terms.n_tranches) + ' tranches as of ' + txt(terms.position_date)),
        h(BarrierLadder, { fund: fund, tranches: terms.tranches })),
      profile.objective
        ? h('div', { className: 'ac-panel' },
          h('h4', null, 'Objective'),
          h('p', { className: 'ac-prose' }, profile.objective))
        : null);

    const termsPanel = () => h('div', null,
      h('div', { className: 'ac-panel' },
        h('h4', null, 'Term sheet — one row per tranche'),
        h(TermSheet, { tranches: terms.tranches })),
      h('div', { className: 'ac-panel' },
        h('h4', null, 'Barrier proximity'),
        h(KV, { rows: [
          ['Nearest KO (call side)', spct(fund.nearest_ko_distance)],
          ['KO price', num(fund.nearest_ko_price, 2)],
          ['Nearest KI (put side)', spct(fund.nearest_ki_distance)],
          ['KI price', num(fund.nearest_ki_price, 2)],
          ['Nearer side', txt(fund.nearest_barrier_side)],
          ['Autocall imminent', fund.autocall_imminent ? 'yes' : 'no'],
          ['Spot vs initial', vsInitial(fund.spot_vs_initial)],
          ['Tranches knocked in', (fund.tranches_ki_breached != null ? fund.tranches_ki_breached : '—') + ' of ' + txt(fund.n_tranches)],
          ['Tranches autocalled', (fund.tranches_ko_triggered != null ? fund.tranches_ko_triggered : '—') + ' of ' + txt(fund.n_tranches)],
          ['Coupon-eligible', pct(fund.coupon_eligible_frac, 0)],
          ['Principal at risk', pct(fund.principal_at_risk_frac, 0)],
          ['Gate 2 basis', basisLabel(fund.gate2_basis)],
          ['Barrier source file', txt(terms.source_file)],
        ] })));

    const fundPanel = () => h('div', { className: 'ac-panels ac-two' },
      h('div', { className: 'ac-panel' },
        h('h4', null, 'Profile'),
        h(KV, { rows: [
          ['Issuer', txt(fund.issuer)],
          ['CUSIP', txt(profile.cusip)],
          ['Inception', txt(profile.inception_date || fund.inception_date)],
          ['Days since launch', int(fund.days_since_launch)],
          ['Management fee', txt(profile.management_fee)],
          ['Expense ratio', txt(profile.expense_ratio)],
          ['Initial reference', num(terms.initial_reference_price || fund.initial_reference_price, 2)],
          ['NAV / share', num(terms.nav_per_share, 2)],
          ['Shares outstanding', int(terms.shares_outstanding)],
          ['AUM', withReason(usd(fund.aum_usd), fund.aum_usd_reason)],
        ] }),
        profile.product_page
          ? h('p', { style: { marginTop: 14 } },
            h('a', {
              href: profile.product_page, target: '_blank', rel: 'noopener noreferrer', className: 'ac-link',
            }, 'Issuer product page ↗'))
          : null),
      h('div', { className: 'ac-panel' },
        h('h4', null, 'Income & distributions'),
        h(KV, { rows: [
          ['Distribution frequency', txt(profile.distribution_frequency)],
          ['Weighted coupon', pct(fund.weighted_coupon_rate, 2)],
          ['Distribution (ann.)', pct(fund.distribution_annualized, 1)],
          ['Last distribution', pct(fund.distribution_last_pct, 2)],
          ['Months paid consecutively', int(fund.months_paid_consecutive)],
          ['Distribution cut flag', fund.distribution_cut_flag ? 'yes' : 'no'],
          ['Distribution drag (ann.)', pct(fund.distribution_drag_ann, 1)],
        ] }),
        (profile.documents && profile.documents.length)
          ? h('div', { style: { marginTop: 16 } },
            h('h4', null, 'Documents'),
            h('ul', { className: 'ac-docs' }, profile.documents.map((d, i) =>
              h('li', { key: i }, h('a', {
                href: d.url, target: '_blank', rel: 'noopener noreferrer', className: 'ac-link',
              }, d.label)))))
          : null));

    const tracking = () => h('div', { className: 'ac-panels ac-two' },
      h('div', { className: 'ac-panel' },
        h('h4', null, 'Price & tracking'),
        h(KV, { rows: [
          ['Spot vs initial', vsInitial(fund.spot_vs_initial)],
          ['Underlying vs inception', spct(fund.und_vs_inception)],
          ['Underlying max drawdown', spct(fund.und_max_drawdown_since_inception)],
          ['Delta (40d)', num(fund.delta_40d)],
          ['Delta peak to date', num(fund.delta_peak_to_date)],
          ['Delta trend (20d)', num(fund.delta_trend_20d, 4)],
          ['Convexity gap', num(fund.convexity_gap)],
          ['Upside capture', num(fund.upside_capture)],
          ['Downside capture', num(fund.downside_capture)],
          ['NAV vs par', spct(fund.nav_vs_par)],
          ['Premium / discount', withReason(spct(fund.premium_discount_to_nav), fund.premium_discount_reason)],
          ['Observations', int(fund.n_obs)],
          ['Price staleness', isNum(fund.price_staleness_days) ? int(fund.price_staleness_days) + ' d' : '—'],
        ] })),
      h('div', { className: 'ac-panel' },
        h('h4', null, 'Liquidity, borrow & carry'),
        h(KV, { rows: [
          ['In borrow feed', fund.in_borrow_feed ? 'yes' : 'no'],
          ['Shares available', fund.in_borrow_feed === false
            ? h('span', { className: 'ac-mute', title: 'Absent from the IBKR short-stock file — no lendable supply.' }, 'not in feed')
            : int(fund.shares_available)],
          ['Borrow fee', fund.in_borrow_feed === false
            ? h('span', { className: 'ac-mute' }, '—')
            : pct(fund.borrow_fee_annual, 2)],
          ['ADV (20d shares)', int(fund.adv_shares_20d)],
          ['ADV (20d USD)', usd(fund.adv_usd_20d)],
          ['Bid/ask spread', withReason(isNum(fund.bid_ask_spread_bps) ? int(fund.bid_ask_spread_bps) + ' bp' : '—', fund.bid_ask_reason)],
          ['Hedged carry (ann.)', h('span', null, pct(fund.hedged_carry_ann, 1),
            fund.hedged_carry_confidence === 'low'
              ? h('span', { className: 'ac-flag', title: txt(fund.hedged_carry_caveat) }, ' low conf.')
              : null)],
          ['Carry breakeven move', spct(fund.carry_breakeven_move)],
          ['Max holding days', int(fund.max_holding_days)],
        ] })));

    const panels = { overview: overview, terms: termsPanel, fund: fundPanel, tracking: tracking };

    return h('div', { className: 'ac-detail' },
      h('div', { className: 'ac-dbar' },
        h('button', { className: 'ac-back', onClick: onBack }, '← Autocallables'),
        h('span', { className: 'ac-dtick' }, fund.etf),
        h('span', { className: 'ac-dund' },
          txt(profile.underlying_name || fund.underlying) + ' (' + txt(fund.underlying) + ')'),
        h(StatusBadge, { status: fund.status })),
      h('div', { className: 'ac-subtabs' }, SUBTABS.map((s) =>
        h('button', {
          key: s.key, type: 'button',
          className: 'ac-subtab' + (sub === s.key ? ' on' : ''),
          onClick: () => setSub(s.key),
        }, s.label))),
      h('div', { className: 'ac-subbody' }, (panels[sub] || overview)()),
      h('div', { className: 'ac-foot' },
        'Barrier terms are from the issuer’s published daily holdings file',
        terms.source_file ? ' (' + terms.source_file + ')' : '',
        '. Watch-only: this fund is excluded from every sleeve.'));
  }

  // ── page ──────────────────────────────────────────────────────────────────
  const VIEWS = [
    { key: 'watchlist', label: 'Watchlist' },
    { key: 'ladders', label: 'Ladders' },
  ];

  function AutocallablesPage({ selected, onSelect, onBack, onCount }) {
    const state = useAutocallables();
    const [view, setView] = useState('watchlist');
    const data = state.data;

    // The tab badge cannot count these from `records` -- they are watch-only and
    // live in their own artifact -- so report the count up once it is known.
    useEffect(() => {
      if (onCount && data && Array.isArray(data.funds)) onCount(data.funds.length);
    }, [data, onCount]);

    if (state.loading) return h('p', { className: 'ac-empty' }, 'Loading autocallables…');
    if (state.error) {
      return h('div', { className: 'ac-wrap' },
        h('p', { className: 'ac-empty' }, 'Autocallables unavailable — ' + state.error),
        h('p', { className: 'ac-empty' }, 'In ls-algo: python scripts/export_autocallables_public.py'));
    }
    if (selected) return h(AutocallableDetail, { data: data, etf: selected, onBack: onBack });

    return h('div', null,
      h('div', { className: 'ac-views' }, VIEWS.map((v) =>
        h('button', {
          key: v.key, type: 'button',
          className: 'ac-view' + (view === v.key ? ' on' : ''),
          onClick: () => setView(v.key),
        }, v.label))),
      view === 'ladders'
        ? h(LaddersGrid, { data: data, onSelect: onSelect })
        : h(AutocallablesTable, { data: data, onSelect: onSelect }));
  }

  return {
    AutocallablesPage: AutocallablesPage,
    AutocallablesTable: AutocallablesTable,
    AutocallableDetail: AutocallableDetail,
    LaddersGrid: LaddersGrid,
    BarrierLadder: BarrierLadder,
    KpiRow: KpiRow,
    TermSheet: TermSheet,
    TranchePips: TranchePips,
    ProximityMeter: ProximityMeter,
    load: load,
    DATA_URL: DATA_URL,
    SCHEMA: SCHEMA,
  };
});
