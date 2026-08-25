/**
 * Bucket 6 watch panel — single-stock autocallable ETFs.
 *
 * Consumes data/bucket6_watch.json, written by ls-algo scripts/bucket6_watch.py
 * and pulled here by github.extra_files. Field contract and the reasoning behind
 * every column live in that repo's docs/Bucket6_Dashboard_Data_Spec.pdf.
 *
 * These funds are WATCH-ONLY. They are carried in the screened universe so this
 * panel can show live borrow / delta / drawdown, but ls-algo hard-excludes them
 * from every sleeve — only a fraction are borrowable at all, and every one of
 * their underlyings already carries B1/B2/B4 exposure, so a hedge leg would
 * stack on a position we already hold in the same name.
 *
 * Two gates decide everything:
 *   GATE 1  TRADEABLE  enough borrow to short at size AND buy it back
 *   GATE 2  LIVE       the stock has fallen far enough that barrier delta is real
 *
 * Degrades quietly: a missing or corrupt artifact renders a note, never an error.
 */
(function () {
  const URL = 'data/bucket6_watch.json';
  const API = '/api/bucket6-watch';

  const pct = (v, nd = 1) => (v === null || v === undefined ? '—' : (100 * v).toFixed(nd) + '%');
  const num = (v, nd = 2) => (v === null || v === undefined ? '—' : Number(v).toFixed(nd));
  const int = (v) => (v === null || v === undefined ? '—' : Number(v).toLocaleString());
  const usd = (v) => (v === null || v === undefined ? '—' : '$' + Math.round(v).toLocaleString());

  const STATUS_CLASS = {
    'BOTH GATES': 'b6-both',
    'GATE 1': 'b6-g1',
    'GATE 2': 'b6-g2',
    'ASLEEP': 'b6-asleep',
  };

  async function load() {
    // Prefer the API (it reports why an artifact is absent); fall back to the
    // static file for Pages deploys where no backend is running.
    for (const src of [API, URL]) {
      try {
        const r = await fetch(src, { cache: 'no-store' });
        if (!r.ok) continue;
        const j = await r.json();
        if (j && (j.funds || j.available === false)) return j;
      } catch (e) { /* try the next source */ }
    }
    return null;
  }

  function renderAlerts(alerts) {
    if (!alerts || !alerts.length) {
      return '<div class="b6-quiet">No alerts. Nothing to do — the expected state.</div>';
    }
    const rows = alerts.map((a) => {
      const cls = a.priority <= 2 ? 'b6-alert-hi' : 'b6-alert-lo';
      return `<div class="${cls}"><b>P${a.priority}</b> ${escapeHtml(a.message)}</div>`;
    });
    return `<div class="b6-alerts">${rows.join('')}</div>`;
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
    ));
  }

  function renderRow(f) {
    const cls = STATUS_CLASS[f.status] || 'b6-asleep';
    // A positive convexity gap is the thesis-confirming sign; flag it, because
    // it is the field the abandon-or-continue decision keys off.
    const convexCls = f.convexity_gap === null || f.convexity_gap === undefined
      ? '' : (f.convexity_gap > 0 ? 'b6-good' : 'b6-bad');
    const bookCls = f.shared_underlying_flag ? 'b6-warn' : '';
    // Both barrier sides matter, for opposite reasons: the KI (put) side is where
    // the convexity goes live; the KO (call) side is where a tranche autocalls and
    // resets a live position out from under us.
    const ko = f.nearest_ko_distance === null || f.nearest_ko_distance === undefined
      ? '—' : pct(f.nearest_ko_distance) + (f.autocall_imminent ? ' !' : '');
    const ki = f.nearest_ki_distance === null || f.nearest_ki_distance === undefined
      ? '—' : pct(f.nearest_ki_distance);
    const koCls = f.autocall_imminent ? 'b6-warn' : '';
    const kiCls = f.gate2_live ? 'b6-bad' : '';
    const carryWarn = f.hedged_carry_confidence === 'low'
      ? ` <span class="b6-flag" title="${escapeHtml(f.hedged_carry_caveat || '')}">low-conf</span>` : '';
    return `<tr>
      <td class="b6-tick">${escapeHtml(f.etf)}</td>
      <td>${escapeHtml(f.underlying)}</td>
      <td><span class="b6-badge ${cls}">${escapeHtml(f.status)}</span></td>
      <td class="b6-r">${int(f.shares_available)}</td>
      <td class="b6-r">${pct(f.borrow_fee_annual, 2)}</td>
      <td class="b6-r ${koCls}">${ko}</td>
      <td class="b6-r ${kiCls}">${ki}</td>
      <td>${escapeHtml(f.nearest_barrier_side || '—')}</td>
      <td class="b6-r"><b>${num(f.delta_40d)}</b></td>
      <td class="b6-r">${num(f.delta_peak_to_date)}</td>
      <td class="b6-r ${convexCls}">${num(f.convexity_gap)}</td>
      <td class="b6-r">${pct(f.hedged_carry_ann)}${carryWarn}</td>
      <td class="b6-r">${int(f.adv_shares_20d)}</td>
      <td class="b6-r ${bookCls}">${usd(f.existing_book_gross_usd)}</td>
    </tr>`;
  }

  function render(el, panel) {
    // Two different absences, deliberately handled differently.
    //
    // panel === null means neither source answered: no backend AND no artifact
    // on disk. That is the public Pages build, where this panel is never meant
    // to appear — data/bucket6_watch.json carries per-underlying book exposure
    // and is not on ls-algo's public allowlist. Render nothing at all; a note
    // reading "unavailable" would advertise a panel the public site will never
    // have. The reason still goes to the console for anyone debugging locally.
    if (!panel) {
      el.innerHTML = '';
      el.style.display = 'none';
      console.debug('[bucket6] no panel source responded; hiding mount');
      return;
    }
    // available === false means the backend IS running and answered, it just has
    // no artifact synced yet. That is a real local diagnostic — keep the note.
    if (panel.available === false) {
      const why = panel.reason || 'artifact not available';
      el.style.display = '';
      el.innerHTML = `<div class="b6-quiet">Bucket 6 watch panel unavailable — ${escapeHtml(why)}.</div>`;
      return;
    }
    el.style.display = '';
    const c = panel.counts || {};
    const g = panel.gates || {};
    el.innerHTML = `
      <div class="b6-head">
        <div>
          <span class="b6-title">Bucket 6 — Single-Stock Autocallables</span>
          <span class="b6-sub">watch only · never sized</span>
        </div>
        <div class="b6-counts">
          <span>${c.gate1_open ?? 0}/${c.funds ?? 0} tradeable</span>
          <span>${c.gate2_open ?? 0}/${c.funds ?? 0} live</span>
          <span class="${(c.autocall_imminent ?? 0) > 0 ? 'b6-warn' : ''}">${c.autocall_imminent ?? 0} autocall imminent</span>
          <span class="${(c.actionable ?? 0) > 0 ? 'b6-good' : ''}">${c.actionable ?? 0} actionable</span>
        </div>
      </div>
      ${renderAlerts(panel.alerts)}
      <div class="b6-scroll"><table class="b6-table">
        <thead><tr>
          <th>ETF</th><th>Stock</th><th>Status</th>
          <th class="b6-r">Shares</th><th class="b6-r">Borrow</th>
          <th class="b6-r">&rarr;KO call</th><th class="b6-r">&rarr;KI put</th><th>Near</th>
          <th class="b6-r">Delta</th><th class="b6-r">Peak</th>
          <th class="b6-r">Convex</th><th class="b6-r">Carry</th>
          <th class="b6-r">ADV</th><th class="b6-r">In book</th>
        </tr></thead>
        <tbody>${(panel.funds || []).map(renderRow).join('')}</tbody>
      </table></div>
      <div class="b6-foot">
        GATE 1 &ge; ${int(g.gate1_min_shares)} shares and borrow &le; ${pct(g.gate1_max_borrow_annual, 0)} ·
        GATE 2 within ${pct(g.gate2_max_ki_distance, 0)} of a published knock-in ·
        <b>&rarr;KO</b> move up to the nearest autocall (<b>!</b> = inside ${pct(g.autocall_imminent_distance, 0)}, a live position gets reset) ·
        <b>&rarr;KI</b> move down to the nearest knock-in — the trade we want ·
        <b>In book</b> = gross we already hold in that underlying across every sleeve ·
        generated ${escapeHtml(panel.generated_utc || '')}
      </div>`;
  }

  async function mount(target) {
    const el = target || document.getElementById('bucket6-watch');
    if (!el) return false;
    render(el, await load());
    return true;
  }

  // The container is rendered by React, which mounts after DOMContentLoaded and
  // re-renders on tab changes. Watch for the node instead of racing it, and
  // re-fill it if React replaces it.
  function autoMount() {
    let filled = null;
    const tryMount = () => {
      const el = document.getElementById('bucket6-watch');
      if (el && el !== filled && !el.dataset.b6Filled) {
        el.dataset.b6Filled = '1';
        filled = el;
        mount(el);
      }
    };
    tryMount();
    const obs = new MutationObserver(tryMount);
    obs.observe(document.documentElement, { childList: true, subtree: true });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', autoMount);
  } else {
    autoMount();
  }

  window.Bucket6Watch = { load, render, mount };
})();
