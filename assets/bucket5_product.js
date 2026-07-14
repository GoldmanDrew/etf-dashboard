/**
 * Bucket 5 Product Dashboard — SPX-0DTE-style Overview / Regime / Daily.
 * Consumes snap.bucket5_product (schema bucket5_product_dashboard.v1).
 */
(function (root, factory) {
  if (typeof module === "object" && module.exports) {
    module.exports = factory();
  } else {
    root.Bucket5Product = factory();
  }
})(typeof self !== "undefined" ? self : this, function () {
  function fmtPct(n, d) {
    if (n == null || Number.isNaN(Number(n))) return "—";
    return (Number(n) * 100).toFixed(d == null ? 1 : d) + "%";
  }
  function fmtUsd(n) {
    if (n == null || Number.isNaN(Number(n))) return "—";
    const v = Number(n);
    const s = Math.abs(v).toLocaleString(undefined, { maximumFractionDigits: 0 });
    return (v < 0 ? "-$" : "$") + s;
  }
  function fmtNum(n, d) {
    if (n == null || Number.isNaN(Number(n))) return "—";
    return Number(n).toFixed(d == null ? 2 : d);
  }
  function cls(n) {
    if (n == null || Number.isNaN(Number(n))) return "";
    return Number(n) > 0 ? "pos" : Number(n) < 0 ? "neg" : "";
  }

  function seriesPath(series, w, h, pad, transform) {
    if (!series || series.length < 2) return "";
    const ys = series.map((pt) => transform(Number(pt[1])));
    const minY = Math.min(...ys);
    const maxY = Math.max(...ys);
    const span = maxY - minY || 1;
    return series
      .map((pt, i) => {
        const x = pad + (i / (series.length - 1)) * (w - 2 * pad);
        const y = h - pad - ((transform(Number(pt[1])) - minY) / span) * (h - 2 * pad);
        return `${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(" ");
  }

  function LineChart({ title, series, color, yLabel, transform }) {
    const w = 720;
    const h = 160;
    const pad = 12;
    const tf = transform || ((v) => v);
    const pts = seriesPath(series, w, h, pad, tf);
    return (
      '<div class="b5p-chart">' +
      `<h3>${title}</h3>` +
      `<svg viewBox="0 0 ${w} ${h}" width="100%" style="max-width:${w}px">` +
      (yLabel
        ? `<text x="${pad}" y="14" fill="var(--text-muted,#94a3b8)" font-size="11">${yLabel}</text>`
        : "") +
      `<polyline fill="none" stroke="${color || "#10b981"}" stroke-width="2" points="${pts}"></polyline>` +
      "</svg></div>"
    );
  }

  function MultiEquityChart(runs, primaryId) {
    const w = 720;
    const h = 180;
    const pad = 12;
    const colors = ["#10b981", "#3b82f6", "#f59e0b", "#a78bfa"];
    let paths = "";
    runs.forEach((run, i) => {
      const ser = (run.equity_series || []).map((pt) => [pt[0], Number(pt[1]) / 1e6]);
      const pts = seriesPath(ser, w, h, pad, (v) => v);
      const lw = run.id === primaryId ? 2.4 : 1.4;
      paths += `<polyline fill="none" stroke="${colors[i % colors.length]}" stroke-width="${lw}" points="${pts}"></polyline>`;
    });
    const legend = runs
      .map(
        (r, i) =>
          `<span class="b5p-leg"><i style="background:${colors[i % colors.length]}"></i>${r.label}</span>`
      )
      .join("");
    return (
      `<div class="b5p-chart"><h3>Equity overlay ($M)</h3><div class="b5p-legend">${legend}</div>` +
      `<svg viewBox="0 0 ${w} ${h}" width="100%" style="max-width:${w}px">${paths}</svg></div>`
    );
  }

  function StrategyGuide(guide, run) {
    if (!guide) return "";
    const sections = (guide.sections || [])
      .map((sec) => {
        const paras = (sec.paragraphs || []).map((p) => `<p>${p}</p>`).join("");
        const bullets = sec.bullets?.length
          ? `<ul>${sec.bullets.map((b) => `<li>${b}</li>`).join("")}</ul>`
          : "";
        return `<div class="b5p-guide-sec"><h3>${sec.title}</h3>${paras}${bullets}</div>`;
      })
      .join("");
    const results = guide.results?.length
      ? `<div class="strip b5p-results">${guide.results
          .map(
            (r) =>
              `<div class="stat"><div class="label">${r.label}</div><div class="value">${r.value}</div></div>`
          )
          .join("")}</div>`
      : "";
    return (
      `<div class="panel b5p-guide">` +
      `<div class="b5p-guide-head"><h2>${guide.title || run?.label || "Bucket 5"}</h2>` +
      (guide.subtitle ? `<p class="lead">${guide.subtitle}</p>` : "") +
      (run?.meta?.date_range
        ? `<p class="dim small">Backtest: <strong>${run.meta.date_range}</strong>${
            run.meta.synthetic_days ? ` · ${run.meta.synthetic_days} synthetic days` : ""
          }</p>`
        : "") +
      `</div>${sections}${results ? "<h3>Backtest results (this run)</h3>" + results : ""}</div>`
    );
  }

  function kpiStrip(summary) {
    const cards = [
      ["CAGR", fmtPct(summary.combined_CAGR)],
      ["Vol", fmtPct(summary.combined_Vol)],
      ["Sharpe", fmtNum(summary.combined_Sharpe)],
      ["Max DD", fmtPct(summary.combined_MaxDD)],
      ["Calmar", fmtNum(summary.combined_Calmar)],
      ["Harvested $", fmtUsd(summary["realized_$"])],
    ];
    return `<div class="strip">${cards
      .map(
        ([k, v]) =>
          `<div class="stat"><div class="label">${k}</div><div class="value">${v}</div></div>`
      )
      .join("")}</div>`;
  }

  function crashTable(crash) {
    if (!crash) return "";
    const rows = [
      ["Mild −20% SPX", crash["crash_mild_-20%"]],
      ["Severe −30% SPX", crash["crash_severe_-30%"]],
      ["Volmageddon −40%", crash["crash_volmageddon_-40%"]],
    ];
    return (
      `<div class="panel"><h3>Crash scenario payoff (fraction of equity)</h3>` +
      `<table class="tight"><thead><tr><th>Scenario</th><th>Payoff</th></tr></thead><tbody>` +
      rows
        .map(
          ([k, v]) =>
            `<tr><td>${k}</td><td class="${cls(v)}">${fmtPct(v, 1)}</td></tr>`
        )
        .join("") +
      `</tbody></table><p class="dim small">Stylized instantaneous carry + put payoff; not live fills.</p></div>`
    );
  }

  function renderOverview(data, run) {
    const guide = run.meta?.strategy_guide;
    const eq = run.equity_series || [];
    const dd = run.drawdown_series || [];
    const putMtm = (run.daily || []).map((d) => [d.date, d.put_mtm]);
    const harvested = (run.daily || []).map((d) => [d.date, d.put_cash_cum]);
    return (
      StrategyGuide(guide, run) +
      `<div class="panel"><h3>Key results — ${run.label}</h3>${kpiStrip(run.summary || {})}</div>` +
      MultiEquityChart(data.runs || [run], data.primary_run_id) +
      `<div class="two-col">` +
      LineChart({ title: "Drawdown", series: dd, color: "#ef4444", yLabel: "%", transform: (v) => v * 100 }) +
      LineChart({ title: "Put MTM ($)", series: putMtm, color: "#a78bfa", yLabel: "$" }) +
      `</div>` +
      LineChart({ title: "Cumulative harvested put cash ($)", series: harvested, color: "#22c55e", yLabel: "$" }) +
      crashTable(run.crash) +
      (data.notes?.live_vs_research
        ? `<p class="callout dim small">${data.notes.live_vs_research}</p>`
        : "")
    );
  }

  function renderRegime(run) {
    const rp = run.regime_panels || {};
    const bands =
      rp.r_lo != null
        ? `<p class="dim small">Contango band r_lo=${rp.r_lo}, backwardation r_hi=${rp.r_hi}</p>`
        : "";
    return (
      `<div class="panel"><h3>Regime — ${run.label}</h3>${bands}` +
      LineChart({ title: "VIX / VIX3M ratio", series: rp.ratio || [], color: "#6366f1" }) +
      `<div class="two-col">` +
      LineChart({ title: "Rho (SVIX/UVIX short)", series: rp.rho || [], color: "#ea580c" }) +
      LineChart({
        title: "Sleeve gross fraction",
        series: rp.gross_frac || [],
        color: "#059669",
        transform: (v) => v * 100,
        yLabel: "%",
      }) +
      `</div>` +
      `<div class="two-col">` +
      LineChart({ title: "VIX", series: rp.vix || [], color: "#b91c1c" }) +
      LineChart({
        title: "Put budget multiplier",
        series: rp.put_budget_mult || [],
        color: "#7c3aed",
      }) +
      `</div>` +
      (rp.cadence_interval_days?.length
        ? `<p class="dim small">Rebalances: ${rp.rebalance_dates?.length || 0} · median gap ${(
            rp.cadence_interval_days.reduce((a, b) => a + b[1], 0) / rp.cadence_interval_days.length
          ).toFixed(1)} calendar days</p>`
        : "") +
      `</div>`
    );
  }

  function renderDaily(data, run, state) {
    const daily = run.daily || [];
    const liveDays = Object.keys(data.live?.days || {}).sort();
    const dayOptions = Array.from(new Set([...daily.map((d) => d.date), ...liveDays])).sort();
    const day = state.day && dayOptions.includes(state.day) ? state.day : dayOptions[dayOptions.length - 1] || "";
    const dayRow = daily.find((d) => d.date === day);
    let marks = (run.marks_by_date || {})[day] || [];
    const events = (run.events_by_date || {})[day] || [];
    const liveDay = (data.live?.days || {})[day];
    // Fallback synthetic mark row when sparsified marks omit this day
    if (!marks.length && dayRow) {
      marks = [
        {
          kind: "put_overlay",
          name: "SPX put ladder",
          mtm_usd: dayRow.put_mtm,
          mtm_chg_usd: null,
          cash_flow_usd: dayRow.put_cash_flow,
        },
      ];
    }

    const runOpts = (data.runs || [])
      .map((r) => `<option value="${r.id}" ${r.id === run.id ? "selected" : ""}>${r.label}</option>`)
      .join("");
    const dayOpts = dayOptions
      .map(
        (d) =>
          `<option value="${d}" ${d === day ? "selected" : ""}>${d}${
            liveDays.includes(d) ? " · live" : ""
          }</option>`
      )
      .join("");

    const cards = dayRow
      ? `<div class="strip">
        <div class="stat"><div class="label">Combined ret</div><div class="value ${cls(dayRow.combined_ret)}">${fmtPct(dayRow.combined_ret, 2)}</div></div>
        <div class="stat"><div class="label">Equity</div><div class="value">${fmtUsd(dayRow.combined_equity)}</div></div>
        <div class="stat"><div class="label">Put MTM</div><div class="value">${fmtUsd(dayRow.put_mtm)}</div></div>
        <div class="stat"><div class="label">Put cashflow</div><div class="value ${cls(dayRow.put_cash_flow)}">${fmtUsd(dayRow.put_cash_flow)}</div></div>
        <div class="stat"><div class="label">Realized harvest</div><div class="value ${cls(dayRow.realized_day)}">${fmtUsd(dayRow.realized_day)}</div></div>
        <div class="stat"><div class="label">Rho / gross</div><div class="value">${fmtNum(dayRow.rho, 2)} / ${fmtPct(dayRow.gross_frac, 1)}</div></div>
        <div class="stat"><div class="label">Rebalance</div><div class="value">${dayRow.rebalance_flag ? "Yes" : "No"}</div></div>
      </div>`
      : `<p class="dim">No backtest row for ${day}.</p>`;

    const markRows = marks
      .map((m) => {
        if (m.kind === "carry_leg") {
          return `<tr><td>Carry</td><td>${m.name}</td><td>${fmtUsd(m.notional_usd)}</td><td>${fmtNum(m.price, 2)}</td><td>${fmtUsd(m.financing_pnl)}</td><td>${m.rebalance ? "rebal" : ""}</td></tr>`;
        }
        return `<tr><td>Puts</td><td>${m.name}</td><td>${fmtUsd(m.mtm_usd)}</td><td>Δ ${fmtUsd(m.mtm_chg_usd)}</td><td class="${cls(m.cash_flow_usd)}">${fmtUsd(m.cash_flow_usd)}</td><td></td></tr>`;
      })
      .join("");

    const eventRows = events
      .map(
        (e) =>
          `<tr><td>${e.kind || ""}</td><td class="${cls(e.usd)}">${fmtUsd(e.usd)}</td><td>${
            e.otm_pct != null ? (e.otm_pct * 100).toFixed(0) + "% OTM" : ""
          }</td><td>${e.mult != null ? e.mult + "×" : ""}</td><td>${e.vix != null ? "VIX " + e.vix : ""}</td></tr>`
      )
      .join("");

    let liveHtml = "";
    if (liveDay) {
      const pos = (liveDay.positions || [])
        .map(
          (p) =>
            `<tr><td>${p.etf}</td><td>${p.underlying}</td><td>${fmtUsd(p.proposed_gross_usd)}</td><td>${fmtPct(p.borrow_annual, 1)}</td></tr>`
        )
        .join("");
      liveHtml =
        `<div class="panel"><h3>Live sleeve — ${day}${liveDay.mode ? ` (${liveDay.mode})` : ""}</h3>` +
        (liveDay.note ? `<p class="dim small">${liveDay.note}</p>` : "") +
        `<div class="strip">
          <div class="stat"><div class="label">Proposed gross</div><div class="value">${fmtUsd(liveDay.proposed_gross_usd)}</div></div>
          <div class="stat"><div class="label">Marked PnL</div><div class="value ${cls(liveDay.marked_pnl)}">${fmtUsd(liveDay.marked_pnl)}</div></div>
        </div>` +
        (pos
          ? `<table class="tight"><thead><tr><th>ETF</th><th>Underlying</th><th>Gross</th><th>Borrow</th></tr></thead><tbody>${pos}</tbody></table>`
          : "") +
        `</div>`;
    }

    return (
      `<div class="panel">` +
      `<div class="row b5p-controls">` +
      `<label>Run <select id="b5p-run">${runOpts}</select></label>` +
      `<label>Day <select id="b5p-day">${dayOpts}</select></label>` +
      `</div>${cards}</div>` +
      `<div class="panel"><h3>Marks — ${day}</h3>` +
      `<table class="tight"><thead><tr><th>Layer</th><th>Name</th><th>Notional / MTM</th><th>Price / Δ</th><th>Financing / CF</th><th></th></tr></thead>` +
      `<tbody>${markRows || '<tr><td colspan="6" class="dim">No marks</td></tr>'}</tbody></table></div>` +
      `<div class="panel"><h3>Events — ${day}</h3>` +
      `<table class="tight"><thead><tr><th>Kind</th><th>USD</th><th>Strike</th><th>Mult</th><th>VIX</th></tr></thead>` +
      `<tbody>${eventRows || '<tr><td colspan="5" class="dim">No monetize / roll events</td></tr>'}</tbody></table></div>` +
      liveHtml
    );
  }

  function mount(container, data, opts) {
    if (!container) return;
    if (!data || data.schema !== "bucket5_product_dashboard.v1" || !(data.runs || []).length) {
      container.innerHTML =
        '<div class="callout dim">No Bucket 5 product dashboard. Run <code>python scripts/build_bucket5_product_dashboard.py</code> then rebuild the site snapshot.</div>';
      return;
    }
    const state = {
      sub: (opts && opts.sub) || "overview",
      runId: data.primary_run_id || data.runs[0].id,
      day: null,
    };

    function run() {
      return data.runs.find((r) => r.id === state.runId) || data.runs[0];
    }

    function paint() {
      const r = run();
      const subnav =
        `<nav class="dash-subnav b5p-subnav" aria-label="B5 product sections">` +
        ["overview", "regime", "daily"]
          .map(
            (s) =>
              `<a href="#" data-b5p-sub="${s}" class="${state.sub === s ? "active" : ""}">${
                s === "overview" ? "Overview" : s === "regime" ? "Regime" : "Daily"
              }</a>`
          )
          .join("") +
        `</nav>`;
      const body =
        state.sub === "regime"
          ? renderRegime(r)
          : state.sub === "daily"
            ? renderDaily(data, r, state)
            : renderOverview(data, r);
      container.innerHTML =
        `<div class="b5p-root">` +
        `<div class="panel-head"><h2>Bucket 5 Product — UVIX/SVIX insurance</h2>` +
        `<div class="quality-line dim">${data.generated_at_utc || ""} · primary ${data.primary_run_id || ""}</div></div>` +
        subnav +
        `<div id="b5p-body">${body}</div></div>`;

      container.querySelectorAll("[data-b5p-sub]").forEach((a) => {
        a.addEventListener("click", (e) => {
          e.preventDefault();
          state.sub = a.getAttribute("data-b5p-sub");
          paint();
        });
      });
      const runEl = container.querySelector("#b5p-run");
      const dayEl = container.querySelector("#b5p-day");
      if (runEl) {
        runEl.addEventListener("change", () => {
          state.runId = runEl.value;
          state.day = null;
          paint();
        });
      }
      if (dayEl) {
        dayEl.addEventListener("change", () => {
          state.day = dayEl.value;
          paint();
        });
      }
    }
    paint();
  }

  return { mount, fmtPct, fmtUsd };
});
