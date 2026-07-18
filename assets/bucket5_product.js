/**
 * Bucket 5 Product Dashboard — SPX-0DTE-style Overview / Regime / Daily.
 * Consumes schema bucket5_product_dashboard.v1.
 * Scoped chrome via bucket5_product.css (.b5p-*).
 */
(function (root, factory) {
  if (typeof module === "object" && module.exports) {
    module.exports = factory();
  } else {
    root.Bucket5Product = factory();
  }
})(typeof self !== "undefined" ? self : this, function () {
  const COLORS = ["#58a6ff", "#3fb950", "#bc8cff", "#d29922", "#f85149", "#56d4dd"];
  /** @type {{spec: object, el: HTMLElement, chart: object}[]} */
  let _liveCharts = [];
  /** @type {object[]} */
  let _pendingChartSpecs = [];

  function disposeLiveCharts() {
    _liveCharts.forEach((entry) => {
      try {
        if (entry.chart && typeof entry.chart.remove === "function") entry.chart.remove();
      } catch (_e) { /* ignore */ }
    });
    _liveCharts = [];
  }

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
  function escapeHtml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function yearAxisTicks(sortedDates) {
    const ticks = [];
    let lastYear = "";
    for (const d of sortedDates) {
      const y = String(d).slice(0, 4);
      if (y && y !== lastYear) {
        ticks.push({ x: d, label: y });
        lastYear = y;
      }
    }
    return ticks;
  }

  function niceYFormat(v, mode) {
    if (mode === "pct") return fmtPct(v / 100, Math.abs(v) >= 10 ? 0 : 1);
    if (mode === "pct100") return v.toFixed(Math.abs(v) >= 10 ? 0 : 1) + "%";
    if (mode === "ratio" || mode === "num") {
      const a = Math.abs(v);
      return a >= 100 ? v.toFixed(0) : a >= 10 ? v.toFixed(1) : v.toFixed(2);
    }
    if (mode === "usdM") return "$" + v.toFixed(v >= 100 ? 0 : 1) + "M";
    if (mode === "usd") {
      const a = Math.abs(v);
      if (a >= 1e6) return (v < 0 ? "-$" : "$") + (a / 1e6).toFixed(1) + "M";
      if (a >= 1e3) return (v < 0 ? "-$" : "$") + (a / 1e3).toFixed(0) + "k";
      return fmtUsd(v);
    }
    return String(v);
  }

  function pointsToLw(points) {
    const out = [];
    const seen = new Set();
    (points || []).forEach((p) => {
      const x = Array.isArray(p) ? p[0] : p.x;
      const y = Number(Array.isArray(p) ? p[1] : p.y);
      if (x == null || Number.isNaN(y)) return;
      const t = String(x).slice(0, 10);
      if (seen.has(t)) return;
      seen.add(t);
      out.push({ time: t, value: y });
    });
    out.sort((a, b) => (a.time < b.time ? -1 : a.time > b.time ? 1 : 0));
    return out;
  }

  function lwPriceFormatter(yMode) {
    return (v) => niceYFormat(v, yMode || "num");
  }

  /**
   * Interactive TradingView Lightweight Charts mount (crosshair / zoom / pan).
   * Falls back to static SVG if the library is unavailable.
   * series: [{ name, color, points, hidden?, primary?, id? }]
   */
  function LineChart({ title, series, height, yMode, variant, includeZero, yAxisLabel }) {
    const H = height || 300;
    const visible = (series || []).filter((s) => !s.hidden && (s.points || []).length);
    if (!visible.length) {
      return `<div class="b5p-chart b5p-chart--${variant || "secondary"}"><h3>${escapeHtml(
        title || ""
      )}</h3><p class="dim">No data.</p></div>`;
    }

    const hasLw =
      typeof window !== "undefined" &&
      window.LightweightCharts &&
      typeof window.LightweightCharts.createChart === "function";

    if (!hasLw) {
      return svgLineChart({ title, series: visible, height: H, yMode, variant, includeZero, yAxisLabel });
    }

    const idx = _pendingChartSpecs.length;
    _pendingChartSpecs.push({
      title: title || "",
      height: H,
      yMode: yMode || "num",
      includeZero: !!includeZero,
      yAxisLabel: yAxisLabel || "",
      series: visible.map((s) => ({
        name: s.name || s.id || "series",
        id: s.id || s.name,
        color: s.color || COLORS[0],
        primary: !!s.primary,
        data: pointsToLw(s.points),
      })),
    });

    return (
      `<div class="b5p-chart b5p-chart--${variant || "secondary"}">` +
      (title ? `<h3>${escapeHtml(title)}</h3>` : "") +
      `<div class="b5p-lw-wrap">` +
      `<div class="b5p-lw-toolbar">` +
      `<div class="b5p-lw-hud dim small" data-b5p-hud="${idx}"></div>` +
      `<div class="b5p-lw-actions" data-b5p-actions="${idx}">` +
      `<button type="button" class="b5p-lw-btn" data-b5p-range="1Y" title="Last 1 year">1Y</button>` +
      `<button type="button" class="b5p-lw-btn" data-b5p-range="3Y" title="Last 3 years">3Y</button>` +
      `<button type="button" class="b5p-lw-btn" data-b5p-range="5Y" title="Last 5 years">5Y</button>` +
      `<button type="button" class="b5p-lw-btn" data-b5p-range="all" title="Fit all">All</button>` +
      `<button type="button" class="b5p-lw-btn" data-b5p-zoom="in" title="Zoom in">+</button>` +
      `<button type="button" class="b5p-lw-btn" data-b5p-zoom="out" title="Zoom out">−</button>` +
      `<button type="button" class="b5p-lw-btn" data-b5p-zoom="reset" title="Reset view">Reset</button>` +
      `</div></div>` +
      `<div class="b5p-lw" data-b5p-idx="${idx}" style="height:${H}px" role="img" aria-label="${escapeHtml(
        title || "interactive chart"
      )}" tabindex="0"></div>` +
      `<p class="b5p-lw-hint dim small">Scroll = zoom at cursor · drag = pan · drag price/time axis to stretch · Shift+scroll = price zoom · 1Y/3Y/5Y = jump · double-click / Reset = fit</p>` +
      `</div></div>`
    );
  }

  /** Zoom time scale around a logical anchor (TradingView-style). */
  function zoomLogicalRange(range, anchor, factor) {
    if (!range || !Number.isFinite(range.from) || !Number.isFinite(range.to)) return null;
    const span = range.to - range.from;
    if (!(span > 0) || !(factor > 0)) return null;
    const a = Number.isFinite(anchor) ? anchor : (range.from + range.to) / 2;
    const nextSpan = Math.max(2, Math.min(span * factor, 1e6));
    const leftFrac = (a - range.from) / span;
    const from = a - nextSpan * leftFrac;
    return { from, to: from + nextSpan };
  }

  function hydrateInteractiveCharts(root) {
    disposeLiveCharts();
    const LW = typeof window !== "undefined" ? window.LightweightCharts : null;
    if (!LW || !root) {
      _pendingChartSpecs = [];
      return;
    }
    const LineSeries = LW.LineSeries;
    const mounts = root.querySelectorAll("[data-b5p-idx]");
    let syncing = false;

    mounts.forEach((el) => {
      const idx = Number(el.getAttribute("data-b5p-idx"));
      const spec = _pendingChartSpecs[idx];
      if (!spec || !spec.series.length) return;
      const hud = root.querySelector(`[data-b5p-hud="${idx}"]`);
      const actions = root.querySelector(`[data-b5p-actions="${idx}"]`);
      const chart = LW.createChart(el, {
        height: spec.height,
        autoSize: true,
        layout: {
          background: { color: "#111827" },
          textColor: "#d5e3f2",
          fontSize: 12,
          attributionLogo: false,
        },
        grid: {
          vertLines: { color: "#334155", visible: true },
          horzLines: { color: "#334155", visible: true },
        },
        crosshair: {
          mode: LW.CrosshairMode ? LW.CrosshairMode.Normal : 0,
          vertLine: { color: "#58a6ff", width: 1, style: 2, labelBackgroundColor: "#1e293b" },
          horzLine: { color: "#58a6ff", width: 1, style: 2, labelBackgroundColor: "#1e293b" },
        },
        rightPriceScale: {
          visible: true,
          borderVisible: true,
          borderColor: "#718096",
          scaleMargins: { top: 0.10, bottom: 0.12 },
          autoScale: true,
        },
        timeScale: {
          visible: true,
          borderVisible: true,
          borderColor: "#718096",
          timeVisible: true,
          secondsVisible: false,
          rightOffset: 6,
          barSpacing: 6,
          minBarSpacing: 0.5,
          lockVisibleTimeRangeOnResize: true,
          tickMarkFormatter: (time) => {
            const value = typeof time === "string"
              ? time
              : `${time.year}-${String(time.month).padStart(2, "0")}-${String(time.day).padStart(2, "0")}`;
            return value.slice(0, 7);
          },
        },
        // TradingView-like: wheel zooms time (at cursor); drag pans; wheel does NOT page-scroll the chart.
        handleScroll: {
          mouseWheel: false,
          pressedMouseMove: true,
          horzTouchDrag: true,
          vertTouchDrag: false,
        },
        handleScale: {
          axisPressedMouseMove: { time: true, price: true },
          axisDoubleClickReset: { time: true, price: true },
          mouseWheel: true,
          pinch: true,
        },
        kineticScroll: { mouse: true, touch: true },
      });

      const seriesApis = [];
      spec.series.forEach((s) => {
        if (!s.data.length) return;
        const api = LineSeries
          ? chart.addSeries(LineSeries, {
              color: s.color,
              lineWidth: s.primary ? 3 : 2,
              priceLineVisible: false,
              lastValueVisible: true,
              title: s.name,
            })
          : chart.addLineSeries({
              color: s.color,
              lineWidth: s.primary ? 3 : 2,
              priceLineVisible: false,
              lastValueVisible: true,
              title: s.name,
            });
        api.setData(s.data);
        api.applyOptions({
          priceFormat: {
            type: "custom",
            formatter: lwPriceFormatter(spec.yMode),
          },
        });
        seriesApis.push({ api, name: s.name });
      });

      try {
        chart.timeScale().fitContent();
      } catch (_e) { /* ignore */ }

      const setPriceAutoScale = (on) => {
        const apply = (ps) => {
          if (!ps) return;
          if (typeof ps.setAutoScale === "function") ps.setAutoScale(!!on);
          else ps.applyOptions({ autoScale: !!on });
        };
        try {
          apply(chart.priceScale("right"));
        } catch (_e) { /* ignore */ }
        seriesApis.forEach(({ api }) => {
          try {
            apply(api.priceScale());
          } catch (_e) { /* ignore */ }
        });
      };

      const fitAll = () => {
        try {
          chart.timeScale().fitContent();
          setPriceAutoScale(true);
        } catch (_e) { /* ignore */ }
      };

      const zoomAtClientX = (clientX, factor) => {
        const ts = chart.timeScale();
        const range = ts.getVisibleLogicalRange();
        if (!range) return;
        const rect = el.getBoundingClientRect();
        let anchor = (range.from + range.to) / 2;
        try {
          const logical = ts.coordinateToLogical(clientX - rect.left);
          if (Number.isFinite(logical)) anchor = logical;
        } catch (_e) { /* ignore */ }
        const next = zoomLogicalRange(range, anchor, factor);
        if (!next) return;
        try {
          ts.setVisibleLogicalRange(next);
        } catch (_e) { /* ignore */ }
      };

      const zoomPrice = (factor) => {
        const tryZoom = (ps) => {
          if (!ps || typeof ps.getVisibleRange !== "function" || typeof ps.setVisibleRange !== "function") {
            return false;
          }
          const vr = ps.getVisibleRange();
          if (!vr || !Number.isFinite(vr.from) || !Number.isFinite(vr.to)) return false;
          const mid = (vr.from + vr.to) / 2;
          const half = ((vr.to - vr.from) / 2) * factor;
          if (!(half > 0)) return false;
          ps.setVisibleRange({ from: mid - half, to: mid + half });
          return true;
        };
        let ok = false;
        seriesApis.forEach(({ api }) => {
          try {
            ok = tryZoom(api.priceScale()) || ok;
          } catch (_e) { /* ignore */ }
        });
        if (!ok) {
          try {
            tryZoom(chart.priceScale("right"));
          } catch (_e) { /* ignore */ }
        }
      };

      chart.subscribeCrosshairMove((param) => {
        if (!hud) return;
        if (!param || param.time === undefined || !param.seriesData) {
          hud.textContent = "";
          return;
        }
        const t = typeof param.time === "object"
          ? `${param.time.year}-${String(param.time.month).padStart(2, "0")}-${String(param.time.day).padStart(2, "0")}`
          : String(param.time);
        const parts = seriesApis
          .map(({ api, name }) => {
            const v = param.seriesData.get(api);
            if (!v || v.value == null) return null;
            return `${name} ${niceYFormat(v.value, spec.yMode)}`;
          })
          .filter(Boolean);
        hud.textContent = parts.length ? `${t}  ·  ${parts.join("  ·  ")}` : String(t);
      });

      // Capture wheel so the page does not scroll; Shift+wheel = price zoom (TV-like).
      el.addEventListener(
        "wheel",
        (e) => {
          if (e.shiftKey) {
            e.preventDefault();
            e.stopPropagation();
            const factor = e.deltaY < 0 ? 0.85 : 1.18;
            zoomPrice(factor);
            return;
          }
          // Library zooms time at cursor; still block page scroll while hovering the chart.
          if (e.cancelable) e.preventDefault();
        },
        { passive: false, capture: true }
      );

      el.addEventListener("dblclick", (e) => {
        e.preventDefault();
        fitAll();
      });

      if (actions) {
        actions.addEventListener("click", (e) => {
          const btn = e.target && e.target.closest ? e.target.closest("[data-b5p-zoom],[data-b5p-range]") : null;
          if (!btn) return;
          const rangeKind = btn.getAttribute("data-b5p-range");
          if (rangeKind) {
            if (rangeKind === "all") {
              fitAll();
              return;
            }
            const years = rangeKind === "1Y" ? 1 : rangeKind === "3Y" ? 3 : rangeKind === "5Y" ? 5 : 0;
            if (!years) return;
            const times = [];
            spec.series.forEach((s) => {
              (s.data || []).forEach((pt) => {
                if (pt && pt.time != null) times.push(String(pt.time).slice(0, 10));
              });
            });
            if (!times.length) return;
            times.sort();
            const to = times[times.length - 1];
            const toMs = Date.parse(to + "T00:00:00Z");
            if (!Number.isFinite(toMs)) return;
            const fromTarget = new Date(toMs);
            fromTarget.setUTCFullYear(fromTarget.getUTCFullYear() - years);
            const fromWant = fromTarget.toISOString().slice(0, 10);
            let from = times[0];
            for (let i = 0; i < times.length; i++) {
              if (times[i] >= fromWant) {
                from = times[i];
                break;
              }
            }
            try {
              chart.timeScale().setVisibleRange({ from, to });
              setPriceAutoScale(true);
            } catch (_e) { /* ignore */ }
            return;
          }
          const kind = btn.getAttribute("data-b5p-zoom");
          if (kind === "reset") {
            fitAll();
            return;
          }
          const rect = el.getBoundingClientRect();
          zoomAtClientX(rect.left + rect.width / 2, kind === "in" ? 0.7 : 1.4);
        });
      }

      _liveCharts.push({ chart, el, seriesApis, fitAll });
    });

    // Sync time range across all charts on the page (TradingView-like)
    _liveCharts.forEach((entry) => {
      entry.chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
        if (!range || syncing) return;
        syncing = true;
        _liveCharts.forEach((other) => {
          if (other.chart === entry.chart) return;
          try {
            other.chart.timeScale().setVisibleLogicalRange(range);
          } catch (_e) { /* ignore */ }
        });
        syncing = false;
      });
    });

    _pendingChartSpecs = [];
  }

  /** Static SVG fallback when Lightweight Charts CDN is blocked. */
  function svgLineChart({ title, series, height, yMode, variant, includeZero, yAxisLabel }) {
    const pad = { l: 72, r: 18, t: 18, b: 52 };
    const W = 900;
    const H = height || 300;
    const ink = "#94a3b8";
    const gridStroke = "#334155";
    const axisStroke = "#64748b";
    const visible = (series || []).filter((s) => !s.hidden && (s.points || []).length);
    const allPts = visible.flatMap((s) =>
      (s.points || []).map((p) => {
        if (Array.isArray(p)) return { x: p[0], y: Number(p[1]) };
        return { x: p.x, y: Number(p.y) };
      })
    );
    if (!allPts.length) {
      return `<div class="b5p-chart"><h3>${escapeHtml(title || "")}</h3><p class="dim">No data.</p></div>`;
    }
    const xs = [...new Set(allPts.map((p) => p.x))].sort();
    const xi = Object.fromEntries(xs.map((x, i) => [x, i]));
    const ys = allPts.map((p) => p.y);
    let ymin = Math.min(...ys);
    let ymax = Math.max(...ys);
    if (includeZero) {
      ymin = Math.min(0, ymin);
      ymax = Math.max(0, ymax);
    } else {
      const padY = (ymax - ymin) * 0.06 || Math.abs(ymax) * 0.05 || 0.05;
      ymin -= padY;
      ymax += padY;
    }
    if (ymin === ymax) ymax = ymin + 1;
    const xToPx = (x) => pad.l + (xs.length <= 1 ? 0 : (xi[x] / (xs.length - 1)) * (W - pad.l - pad.r));
    const yToPx = (y) => pad.t + (1 - (y - ymin) / (ymax - ymin)) * (H - pad.t - pad.b);
    const yticks = Array.from({ length: 6 }, (_, i) => ymin + (i / 5) * (ymax - ymin));
    let xticks = yearAxisTicks(xs);
    if (xticks.length > 14) xticks = xticks.filter((t, i) => i % 2 === 0 || i === xticks.length - 1);
    const grid = yticks
      .map((yt) => {
        const y = yToPx(yt);
        return (
          `<line x1="${pad.l}" x2="${W - pad.r}" y1="${y.toFixed(1)}" y2="${y.toFixed(1)}" stroke="${gridStroke}" stroke-width="1" stroke-dasharray="3,3"/>` +
          `<text x="${pad.l - 10}" y="${(y + 4).toFixed(1)}" fill="${ink}" font-size="11" text-anchor="end">${escapeHtml(
            niceYFormat(yt, yMode || "num")
          )}</text>`
        );
      })
      .join("");
    const xlabels = xticks
      .map((t) => {
        const x = xToPx(t.x);
        return (
          `<line x1="${x.toFixed(1)}" x2="${x.toFixed(1)}" y1="${H - pad.b}" y2="${H - pad.b + 5}" stroke="${axisStroke}" stroke-width="1"/>` +
          `<text x="${x.toFixed(1)}" y="${H - 10}" fill="${ink}" font-size="11" text-anchor="middle" font-weight="600">${escapeHtml(
            t.label
          )}</text>`
        );
      })
      .join("");
    const frame =
      `<line x1="${pad.l}" y1="${pad.t}" x2="${pad.l}" y2="${H - pad.b}" stroke="${axisStroke}" stroke-width="1.5"/>` +
      `<line x1="${pad.l}" y1="${H - pad.b}" x2="${W - pad.r}" y2="${H - pad.b}" stroke="${axisStroke}" stroke-width="1.5"/>`;
    const yTitle = yAxisLabel
      ? `<text x="15" y="${(H / 2).toFixed(1)}" fill="#d5e3f2" font-size="11" text-anchor="middle" font-weight="600" transform="rotate(-90 15 ${(H / 2).toFixed(
          1
        )})">${escapeHtml(yAxisLabel)}</text>`
      : "";
    const paths = visible
      .map((s) => {
        const pts = (s.points || []).map((p) => (Array.isArray(p) ? { x: p[0], y: Number(p[1]) } : p));
        const d = pts
          .map((p, i) => `${i === 0 ? "M" : "L"}${xToPx(p.x).toFixed(1)},${yToPx(p.y).toFixed(1)}`)
          .join(" ");
        return `<path d="${d}" fill="none" stroke="${s.color}" stroke-width="${s.primary ? 2.4 : 2}"/>`;
      })
      .join("");
    return (
      `<div class="b5p-chart b5p-chart--${variant || "secondary"}">` +
      (title ? `<h3>${escapeHtml(title)}</h3>` : "") +
      `<svg class="b5p-svg-chart" viewBox="0 0 ${W} ${H}" width="100%" preserveAspectRatio="xMidYMid meet" role="img" aria-label="${escapeHtml(
        title || "chart"
      )}">${grid}${frame}${yTitle}${xlabels}${paths}</svg>` +
      `<p class="dim small">Static chart (Lightweight Charts CDN not loaded).</p></div>`
    );
  }

  function MultiEquityChart(runs, primaryId, hiddenIds) {
    const hidden = hiddenIds || new Set();
    const series = (runs || []).map((run, i) => ({
      name: run.label || run.id,
      id: run.id,
      color: COLORS[i % COLORS.length],
      primary: run.id === primaryId,
      hidden: hidden.has(run.id),
      points: (run.equity_series || []).map((pt) => [pt[0], Number(pt[1]) / 1e6]),
    }));
    const legend = series
      .map(
        (s) =>
          `<span class="b5p-leg${s.hidden ? " off" : ""}" data-run-id="${escapeHtml(s.id)}" role="button" tabindex="0" title="Toggle series">` +
          `<i class="swatch" style="background:${s.color}"></i>${escapeHtml(s.name)}</span>`
      )
      .join("");
    return (
      `<div class="b5p-panel">` +
      `<h2>Equity overlay ($M)</h2>` +
      `<div class="b5p-legend">${legend}</div>` +
      LineChart({
        title: "",
        series,
        height: 400,
        yMode: "usdM",
        variant: "primary",
        includeZero: false,
        yAxisLabel: "Equity ($M)",
      }) +
      `</div>`
    );
  }

  function kpiCards(items, extraClass) {
    return (
      `<div class="b5p-cards ${extraClass || ""}">` +
      items
        .map(
          ([k, v, c]) =>
            `<div class="b5p-card"><div class="k">${escapeHtml(k)}</div><div class="v ${c || ""}">${v}</div></div>`
        )
        .join("") +
      `</div>`
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
        return `<div class="b5p-guide-sec"><h3>${escapeHtml(sec.title)}</h3>${paras}${bullets}</div>`;
      })
      .join("");
    return (
      `<div class="b5p-panel b5p-guide">` +
      `<div class="b5p-guide-head"><h2>${escapeHtml(guide.title || run?.label || "Bucket 5")}</h2>` +
      (guide.subtitle ? `<p class="lead">${guide.subtitle}</p>` : "") +
      (run?.meta?.date_range
        ? `<p class="dim small">Backtest: <strong>${escapeHtml(run.meta.date_range)}</strong>${
            run.meta.synthetic_days ? ` · ${run.meta.synthetic_days} synthetic days` : ""
          }</p>`
        : "") +
      `</div>${sections}` +
      `<details class="b5p-howto"><summary>How to read this dashboard</summary>` +
      `<p>Overview = strategy narrative + equity path. Regime = VIX term structure, rho, and sleeve gross. ` +
      `Daily = day-level marks and monetize events. Live tags are the GTP vol-ETP sleeve, not the full insurance book.</p>` +
      `</details></div>`
    );
  }

  function crashPanel(crash) {
    if (!crash) return "";
    const rows = [
      ["Mild −20% SPX", crash["crash_mild_-20%"]],
      ["Severe −30% SPX", crash["crash_severe_-30%"]],
      ["Volmageddon −40%", crash["crash_volmageddon_-40%"]],
    ];
    const maxAbs = Math.max(...rows.map(([, v]) => Math.abs(Number(v) || 0)), 0.01);
    const bars = rows
      .map(([k, v]) => {
        const n = Number(v) || 0;
        const pct = Math.min(100, (Math.abs(n) / maxAbs) * 100);
        return (
          `<div class="b5p-crash-row"><span>${escapeHtml(k)}</span>` +
          `<div class="b5p-crash-track"><div class="b5p-crash-fill ${cls(n)}" style="width:${pct.toFixed(
            1
          )}%;background:${n >= 0 ? "var(--b5p-pos)" : "var(--b5p-neg)"}"></div></div>` +
          `<span class="${cls(n)}">${fmtPct(n, 1)}</span></div>`
        );
      })
      .join("");
    return (
      `<div class="b5p-panel"><h2>Crash scenario payoff</h2>` +
      `<div class="b5p-crash-bars">${bars}</div>` +
      `<div class="b5p-table-wrap" style="margin-top:14px"><table class="b5p-table"><thead><tr><th>Scenario</th><th>Payoff</th></tr></thead><tbody>` +
      rows
        .map(([k, v]) => `<tr><td>${escapeHtml(k)}</td><td class="${cls(v)}">${fmtPct(v, 1)}</td></tr>`)
        .join("") +
      `</tbody></table></div>` +
      `<p class="dim small" style="margin-top:10px">Stylized instantaneous carry + put payoff; not live fills.</p></div>`
    );
  }

  function putSizingPanel(sizing) {
    if (!sizing || !(sizing.rungs || []).length) return "";
    const rows = sizing.rungs
      .map(
        (r) => `<tr><td>${fmtPct(r.otm_pct, 0)} OTM</td><td>${fmtNum(r.strike, 0)}</td>` +
          `<td>${fmtUsd(r.modeled_put_price)}</td><td>${fmtUsd(r.target_budget_usd)}</td>` +
          `<td>${r.baseline_contracts ?? "—"}</td><td><strong>${r.target_contracts ?? "—"}</strong></td>` +
          `<td>${fmtUsd(r.premium_used_usd)}</td><td>${fmtUsd(r.unspent_budget_usd)}</td></tr>`
      )
      .join("");
    return `<div class="b5p-panel"><h2>Next SPX put roll — reverse-solved quantity</h2>` +
      kpiCards([
        ["Account NAV", fmtUsd(sizing.account_nav_usd), ""],
        ["B5 pair gross", fmtUsd(sizing.b5_pair_gross_usd), ""],
        ["Effective B5 NAV", fmtUsd(sizing.effective_b5_nav_usd), ""],
        ["Baseline contracts", String(sizing.baseline_total_contracts ?? "—"), ""],
        ["2× target contracts", String(sizing.target_total_contracts ?? "—"), ""],
        ["Nominal 2× budget", fmtUsd(sizing.target_total_budget_usd), ""],
        ["Modeled premium used", fmtUsd(sizing.premium_used_usd), ""],
        ["Budget multiplier", `${fmtNum(sizing.dynamic_budget_multiplier, 2)}×`, ""],
      ]) +
      `<div class="b5p-table-wrap" style="margin-top:12px"><table class="b5p-table"><thead><tr>` +
      `<th>Rung</th><th>Strike</th><th>Modeled px</th><th>Budget</th><th>Old qty</th><th>2× qty</th><th>Used</th><th>Budget +/-</th>` +
      `</tr></thead><tbody>${rows}</tbody></table></div>` +
      (Number(sizing.premium_used_usd) > Number(sizing.target_total_budget_usd)
        ? `<p class="callout negative">Blocked for full rollout: exact 2× integer quantities exceed the nominal premium budget. Use the staged pilot and approve a smaller contract multiplier or an up-to-2× cap before live routing.</p>`
        : "") +
      `<p class="dim small" style="margin-top:10px">As of ${escapeHtml(sizing.as_of || "—")}. ` +
      `${escapeHtml(sizing.execution_formula || "")} ${escapeHtml(sizing.quote_note || "")}</p></div>`;
  }

  function renderOverview(data, run, state) {
    const guide = run.meta?.strategy_guide;
    const dd = run.drawdown_series || [];
    const putMtm = (run.daily || []).map((d) => [d.date, d.put_mtm]);
    const harvested = (run.daily || []).map((d) => [d.date, d.put_cash_cum]);
    const runOpts = (data.runs || [])
      .map(
        (r) =>
          `<option value="${escapeHtml(r.id)}" ${r.id === run.id ? "selected" : ""}>${escapeHtml(
            r.label
          )}</option>`
      )
      .join("");
    const kpi = `<div class="b5p-panel"><h2>Key results — ${escapeHtml(run.label)}</h2>${kpiCards([
      ["CAGR", fmtPct(run.summary?.combined_CAGR), ""],
      ["Vol", fmtPct(run.summary?.combined_Vol), ""],
      ["Sharpe", fmtNum(run.summary?.combined_Sharpe), ""],
      ["Max DD", fmtPct(run.summary?.combined_MaxDD), cls(run.summary?.combined_MaxDD)],
      ["Calmar", fmtNum(run.summary?.combined_Calmar), ""],
      ["Harvested $", fmtUsd(run.summary?.["realized_$"]), ""],
    ])}</div>`;

    return (
      (data.runs?.length > 1
        ? `<div class="b5p-run-row"><label class="dim">Primary run <select id="b5p-ov-run">${runOpts}</select></label></div>`
        : "") +
      StrategyGuide(guide, run) +
      kpi +
      putSizingPanel(run.put_sizing) +
      MultiEquityChart(data.runs || [run], data.primary_run_id, state.hiddenRuns) +
      `<div class="b5p-grid-2">` +
      `<div class="b5p-panel">${LineChart({
        title: "Drawdown",
        series: [{ name: "DD", color: "#f85149", points: dd.map((p) => [p[0], Number(p[1]) * 100]) }],
        height: 300,
        yMode: "pct100",
        variant: "secondary",
        includeZero: true,
        yAxisLabel: "Drawdown %",
      })}</div>` +
      `<div class="b5p-panel">${LineChart({
        title: "Put MTM ($)",
        series: [{ name: "Put MTM", color: "#bc8cff", points: putMtm }],
        height: 300,
        yMode: "usd",
        variant: "secondary",
        includeZero: true,
        yAxisLabel: "Put MTM ($)",
      })}</div>` +
      `</div>` +
      `<div class="b5p-panel">${LineChart({
        title: "Cumulative harvested put cash ($)",
        series: [{ name: "Harvested", color: "#3fb950", points: harvested }],
        height: 300,
        yMode: "usd",
        variant: "secondary",
        includeZero: false,
        yAxisLabel: "Harvested ($)",
      })}</div>` +
      crashPanel(run.crash) +
      (data.notes?.live_vs_research
        ? `<p class="callout dim small">${escapeHtml(data.notes.live_vs_research)}</p>`
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
      `<div class="b5p-panel"><h2>Regime — ${escapeHtml(run.label)}</h2>${bands}` +
      LineChart({
        title: "VIX / VIX3M ratio",
        series: [{ name: "ratio", color: "#58a6ff", points: rp.ratio || [] }],
        height: 280,
        yMode: "ratio",
        variant: "regime",
        includeZero: false,
        yAxisLabel: "VIX/VIX3M",
      }) +
      `</div>` +
      `<div class="b5p-grid-2">` +
      `<div class="b5p-panel">${LineChart({
        title: "Rho (SVIX/UVIX short)",
        series: [{ name: "rho", color: "#d29922", points: rp.rho || [] }],
        height: 240,
        yMode: "ratio",
        variant: "secondary",
        includeZero: false,
        yAxisLabel: "Rho",
      })}</div>` +
      `<div class="b5p-panel">${LineChart({
        title: "Sleeve gross fraction",
        series: [
          {
            name: "gross",
            color: "#3fb950",
            points: (rp.gross_frac || []).map((p) => [p[0], Number(p[1]) * 100]),
          },
        ],
        height: 240,
        yMode: "pct100",
        variant: "secondary",
        includeZero: false,
        yAxisLabel: "Gross %",
      })}</div>` +
      `</div>` +
      `<div class="b5p-grid-2">` +
      `<div class="b5p-panel">${LineChart({
        title: "VIX",
        series: [{ name: "VIX", color: "#f85149", points: rp.vix || [] }],
        height: 240,
        yMode: "num",
        variant: "secondary",
        includeZero: false,
        yAxisLabel: "VIX",
      })}</div>` +
      `<div class="b5p-panel">${LineChart({
        title: "Put budget multiplier",
        series: [{ name: "budget", color: "#bc8cff", points: rp.put_budget_mult || [] }],
        height: 240,
        yMode: "ratio",
        variant: "secondary",
        includeZero: false,
        yAxisLabel: "Budget ×",
      })}</div>` +
      `</div>` +
      (rp.cadence_interval_days?.length
        ? `<p class="dim small">Rebalances: ${rp.rebalance_dates?.length || 0} · median gap ${(
            rp.cadence_interval_days.reduce((a, b) => a + b[1], 0) / rp.cadence_interval_days.length
          ).toFixed(1)} calendar days</p>`
        : "")
    );
  }

  function buildDayOptions(data, run) {
    const daily = run.daily || [];
    const liveDays = new Set(Object.keys(data.live?.days || {}));
    const eventDays = new Set(Object.keys(run.events_by_date || {}));
    const rebalDays = new Set(daily.filter((d) => d.rebalance_flag).map((d) => d.date));
    const putDays = new Set(
      daily.filter((d) => Math.abs(Number(d.put_mtm) || 0) > 1 || Math.abs(Number(d.realized_day) || 0) > 1).map((d) => d.date)
    );
    const all = Array.from(new Set([...daily.map((d) => d.date), ...liveDays])).sort();
    // Prefer days with something to inspect — not quiet end-of-series zeros
    const eventful = all.filter(
      (d) => liveDays.has(d) || eventDays.has(d) || rebalDays.has(d) || putDays.has(d)
    );
    eventful.sort();
    return { all, eventful, liveDays, eventDays, rebalDays, putDays };
  }

  function hydrateMarks(dayRow, marks) {
    const hasCarry = (marks || []).some((m) => m.kind === "carry_leg");
    const hasPut = (marks || []).some((m) => m.kind === "put_overlay" || m.kind === "put_rung");
    const out = Array.isArray(marks) ? marks.slice() : [];
    if (!dayRow) return out;
    if (!hasCarry && (dayRow.u_notional != null || dayRow.s_notional != null)) {
      out.unshift(
        {
          kind: "carry_leg",
          name: "UVIX short",
          notional_usd: dayRow.u_notional,
          price: dayRow.uvix_px,
          financing_pnl: dayRow.u_financing_pnl != null ? dayRow.u_financing_pnl : dayRow.financing_pnl,
          rebalance: !!dayRow.rebalance_flag,
        },
        {
          kind: "carry_leg",
          name: "SVIX short",
          notional_usd: dayRow.s_notional,
          price: dayRow.svix_px,
          financing_pnl: dayRow.s_financing_pnl != null ? dayRow.s_financing_pnl : null,
          rebalance: !!dayRow.rebalance_flag,
        }
      );
    }
    if (!hasPut) {
      out.push({
        kind: "put_overlay",
        name: "SPX put ladder",
        mtm_usd: dayRow.put_mtm,
        mtm_chg_usd: null,
        cash_flow_usd: dayRow.put_cash_flow,
      });
    }
    return out;
  }

  function dayLabel(d, sets) {
    const tags = [];
    if (sets.liveDays.has(d)) tags.push("live");
    if (sets.eventDays.has(d)) tags.push("event");
    if (sets.rebalDays.has(d)) tags.push("rebal");
    return tags.length ? `${d} · ${tags.join("/")}` : d;
  }

  function renderDaily(data, run, state) {
    const sets = buildDayOptions(data, run);
    const mode = state.dayMode || "eventful";
    const pool = mode === "all" ? sets.all : sets.eventful;
    let filtered = pool;
    if (state.dayFilter) {
      const q = state.dayFilter.toLowerCase();
      filtered = pool.filter((d) => d.includes(q) || dayLabel(d, sets).toLowerCase().includes(q));
    }
    const day =
      state.day && filtered.includes(state.day)
        ? state.day
        : state.day && pool.includes(state.day)
          ? state.day
          : filtered[filtered.length - 1] || pool[pool.length - 1] || "";
    const daily = run.daily || [];
    const dayRow = daily.find((d) => d.date === day);
    let marks = hydrateMarks(dayRow, (run.marks_by_date || {})[day] || []);
    const events = (run.events_by_date || {})[day] || [];
    const liveDay = (data.live?.days || {})[day];

    const runOpts = (data.runs || [])
      .map(
        (r) =>
          `<option value="${escapeHtml(r.id)}" ${r.id === run.id ? "selected" : ""}>${escapeHtml(
            r.label
          )}</option>`
      )
      .join("");
    const dayOpts = filtered
      .map(
        (d) =>
          `<option value="${escapeHtml(d)}" ${d === day ? "selected" : ""}>${escapeHtml(
            dayLabel(d, sets)
          )}</option>`
      )
      .join("");

    const cards = dayRow
      ? kpiCards([
          ["Combined ret", fmtPct(dayRow.combined_ret, 2), cls(dayRow.combined_ret)],
          ["Equity", fmtUsd(dayRow.combined_equity), ""],
          ["Put MTM", fmtUsd(dayRow.put_mtm), ""],
          ["Put cashflow", fmtUsd(dayRow.put_cash_flow), cls(dayRow.put_cash_flow)],
          ["Realized harvest", fmtUsd(dayRow.realized_day), cls(dayRow.realized_day)],
          ["Rho / gross", `${fmtNum(dayRow.rho, 2)} / ${fmtPct(dayRow.gross_frac, 1)}`, ""],
          ["Rebalance", dayRow.rebalance_flag ? "Yes" : "No", ""],
        ])
      : `<p class="dim">No backtest row for ${escapeHtml(day)}.</p>`;

    // Daily PnL bars (sample last ~120 trading days ending at selected day)
    const idx = daily.findIndex((d) => d.date === day);
    const window = daily.slice(Math.max(0, idx - 119), idx + 1);
    const barSeries = [
      {
        name: "ret",
        color: "#58a6ff",
        points: window.map((d) => [d.date, Number(d.combined_ret || 0) * 100]),
      },
    ];
    const pnlChart =
      window.length > 2
        ? `<div class="b5p-panel">${LineChart({
            title: "Combined daily return (%) — trailing window",
            series: barSeries,
            height: 220,
            yMode: "pct100",
            variant: "secondary",
            includeZero: true,
          })}</div>`
        : "";

    const markRows = marks
      .map((m) => {
        if (m.kind === "carry_leg") {
          return `<tr><td>Carry</td><td>${escapeHtml(m.name)}</td><td>${fmtUsd(m.notional_usd)}</td><td>${fmtNum(
            m.price,
            2
          )}</td><td>${fmtUsd(m.financing_pnl)}</td><td>${m.rebalance ? "rebal" : ""}</td></tr>`;
        }
        return `<tr><td>Puts</td><td>${escapeHtml(m.name)}</td><td>${fmtUsd(m.mtm_usd)}</td><td>Δ ${fmtUsd(
          m.mtm_chg_usd
        )}</td><td class="${cls(m.cash_flow_usd)}">${fmtUsd(m.cash_flow_usd)}</td><td></td></tr>`;
      })
      .join("");

    const eventRows = events
      .map(
        (e) =>
          `<tr><td>${escapeHtml(e.kind || "")}</td><td class="${cls(e.usd)}">${fmtUsd(e.usd)}</td><td>${
            e.otm_pct != null ? (e.otm_pct * 100).toFixed(0) + "% OTM" : ""
          }</td><td>${e.mult != null ? e.mult + "×" : ""}</td><td>${
            e.vix != null ? "VIX " + e.vix : ""
          }</td></tr>`
      )
      .join("");

    let liveHtml = "";
    if (liveDay) {
      const pos = (liveDay.positions || [])
        .map(
          (p) =>
            `<tr><td>${escapeHtml(p.etf)}</td><td>${escapeHtml(p.underlying)}</td><td>${fmtUsd(
              p.proposed_gross_usd
            )}</td><td>${fmtPct(p.borrow_annual, 1)}</td></tr>`
        )
        .join("");
      liveHtml =
        `<div class="b5p-panel"><h2>Live sleeve — ${escapeHtml(day)}${
          liveDay.mode ? ` (${escapeHtml(liveDay.mode)})` : ""
        }</h2>` +
        (liveDay.note ? `<p class="dim small">${escapeHtml(liveDay.note)}</p>` : "") +
        kpiCards([
          ["Proposed gross", fmtUsd(liveDay.proposed_gross_usd), ""],
          ["Marked PnL", fmtUsd(liveDay.marked_pnl), cls(liveDay.marked_pnl)],
        ]) +
        (pos
          ? `<div class="b5p-table-wrap" style="margin-top:12px"><table class="b5p-table"><thead><tr><th>ETF</th><th>Underlying</th><th>Gross</th><th>Borrow</th></tr></thead><tbody>${pos}</tbody></table></div>`
          : "") +
        `</div>`;
    }

    return (
      `<div class="b5p-panel">` +
      `<div class="b5p-controls b5p-sticky">` +
      `<label>Run <select id="b5p-run">${runOpts}</select></label>` +
      `<label>Days <select id="b5p-day-mode">` +
      `<option value="eventful" ${mode === "eventful" ? "selected" : ""}>Eventful / live (${sets.eventful.length})</option>` +
      `<option value="all" ${mode === "all" ? "selected" : ""}>All days (${sets.all.length})</option>` +
      `</select></label>` +
      `<label>Filter <input type="search" id="b5p-day-filter" placeholder="YYYY-MM…" value="${escapeHtml(
        state.dayFilter || ""
      )}"/></label>` +
      `<label>Day <select id="b5p-day">${dayOpts || `<option value="">(none)</option>`}</select></label>` +
      `</div>${cards}</div>` +
      pnlChart +
      `<div class="b5p-panel"><h2>Marks — ${escapeHtml(day)}</h2>` +
      `<div class="b5p-table-wrap"><table class="b5p-table"><thead><tr><th>Layer</th><th>Name</th><th>Notional / MTM</th><th>Price / Δ</th><th>Financing / CF</th><th></th></tr></thead>` +
      `<tbody>${markRows || '<tr><td colspan="6" class="dim">No marks</td></tr>'}</tbody></table></div></div>` +
      `<div class="b5p-panel"><h2>Events — ${escapeHtml(day)}</h2>` +
      `<div class="b5p-table-wrap"><table class="b5p-table"><thead><tr><th>Kind</th><th>USD</th><th>Strike</th><th>Mult</th><th>VIX</th></tr></thead>` +
      `<tbody>${eventRows || '<tr><td colspan="5" class="dim">No monetize / roll events</td></tr>'}</tbody></table></div></div>` +
      liveHtml
    );
  }

  function readSubFromHash() {
    try {
      const raw = (location.hash || "").replace(/^#/, "");
      if (raw.startsWith("/bucket5")) {
        const parts = raw.split("?")[0].split("/").filter(Boolean);
        // bucket5 / regime
        if (parts[1] === "regime" || parts[1] === "daily" || parts[1] === "overview") return parts[1];
        const q = raw.includes("?") ? new URLSearchParams(raw.split("?")[1]) : null;
        const s = q && q.get("b5");
        if (s === "regime" || s === "daily" || s === "overview") return s;
        return null;
      }
      const params = new URLSearchParams(raw);
      const s = params.get("b5");
      if (s === "regime" || s === "daily" || s === "overview") return s;
    } catch (_e) { /* ignore */ }
    return null;
  }

  function writeSubToHash(sub) {
    try {
      const raw = (location.hash || "").replace(/^#/, "");
      if (raw.startsWith("/bucket5") || location.hash.includes("bucket5")) {
        const base = sub && sub !== "overview" ? `/bucket5/${sub}` : "/bucket5";
        if (location.hash !== "#" + base) location.hash = base;
        return;
      }
      const params = new URLSearchParams(raw.startsWith("tab=") || raw.includes("=") ? raw : "");
      if (!params.get("tab")) params.set("tab", "b5");
      else if (params.get("tab") !== "b5") params.set("tab", "b5");
      if (sub && sub !== "overview") params.set("b5", sub);
      else params.delete("b5");
      const next = params.toString();
      if (location.hash.replace(/^#/, "") !== next) location.hash = next;
    } catch (_e) { /* ignore */ }
  }

  function bindTooltips(root) {
    let tip = root.querySelector(".b5p-tip");
    if (!tip) {
      tip = document.createElement("div");
      tip.className = "b5p-tip";
      tip.hidden = true;
      root.style.position = root.style.position || "relative";
      root.appendChild(tip);
    }
    root.querySelectorAll(".b5p-hit").forEach((el) => {
      el.addEventListener("mouseenter", (e) => {
        tip.textContent = el.getAttribute("data-tip") || "";
        tip.hidden = false;
        const rect = root.getBoundingClientRect();
        tip.style.left = e.clientX - rect.left + 12 + "px";
        tip.style.top = e.clientY - rect.top + 12 + "px";
      });
      el.addEventListener("mousemove", (e) => {
        const rect = root.getBoundingClientRect();
        tip.style.left = e.clientX - rect.left + 12 + "px";
        tip.style.top = e.clientY - rect.top + 12 + "px";
      });
      el.addEventListener("mouseleave", () => {
        tip.hidden = true;
      });
    });
  }

  function mount(container, data, opts) {
    if (!container) return;
    if (!data || data.schema !== "bucket5_product_dashboard.v1" || !(data.runs || []).length) {
      container.innerHTML =
        '<div class="b5p-root"><div class="b5p-panel dim">No Bucket 5 product dashboard. Run <code>python scripts/build_bucket5_product_dashboard.py</code>.</div></div>';
      return;
    }
    const hashSub = readSubFromHash();
    const state = {
      sub: (opts && opts.sub) || hashSub || "overview",
      runId: data.primary_run_id || data.runs[0].id,
      day: null,
      dayMode: "eventful",
      dayFilter: "",
      hiddenRuns: new Set(),
    };

    function run() {
      return data.runs.find((r) => r.id === state.runId) || data.runs[0];
    }

    function paint() {
      disposeLiveCharts();
      _pendingChartSpecs = [];
      const r = run();
      writeSubToHash(state.sub);
      const tabs =
        `<nav class="b5p-tabs" role="tablist" aria-label="B5 product sections">` +
        ["overview", "regime", "daily"]
          .map((s) => {
            const label = s === "overview" ? "Overview" : s === "regime" ? "Regime" : "Daily";
            const active = state.sub === s;
            return (
              `<button type="button" role="tab" data-b5p-sub="${s}" class="${active ? "active" : ""}" ` +
              `aria-selected="${active ? "true" : "false"}">${label}</button>`
            );
          })
          .join("") +
        `</nav>`;
      const body =
        state.sub === "regime"
          ? renderRegime(r)
          : state.sub === "daily"
            ? renderDaily(data, r, state)
            : renderOverview(data, r, state);

      container.innerHTML =
        `<div class="b5p-root">` +
        `<header class="b5p-head"><h1>Bucket 5 Product</h1>` +
        `<div class="b5p-meta">${escapeHtml(data.generated_at_utc || "")} · primary ${escapeHtml(
          data.primary_run_id || ""
        )}</div></header>` +
        tabs +
        `<div id="b5p-body" class="b5p-body" role="tabpanel">${body}</div></div>`;

      const root = container.querySelector(".b5p-root");
      container.querySelectorAll("[data-b5p-sub]").forEach((a) => {
        a.addEventListener("click", (e) => {
          e.preventDefault();
          state.sub = a.getAttribute("data-b5p-sub");
          paint();
        });
      });

      const ovRun = container.querySelector("#b5p-ov-run");
      if (ovRun) {
        ovRun.addEventListener("change", () => {
          state.runId = ovRun.value;
          state.day = null;
          paint();
        });
      }

      container.querySelectorAll(".b5p-leg[data-run-id]").forEach((leg) => {
        const toggle = () => {
          const id = leg.getAttribute("data-run-id");
          if (state.hiddenRuns.has(id)) state.hiddenRuns.delete(id);
          else state.hiddenRuns.add(id);
          // keep at least one visible
          if (state.hiddenRuns.size >= (data.runs || []).length) state.hiddenRuns.clear();
          paint();
        };
        leg.addEventListener("click", toggle);
        leg.addEventListener("keydown", (e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            toggle();
          }
        });
      });

      const runEl = container.querySelector("#b5p-run");
      const dayEl = container.querySelector("#b5p-day");
      const dayModeEl = container.querySelector("#b5p-day-mode");
      const dayFilterEl = container.querySelector("#b5p-day-filter");
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
      if (dayModeEl) {
        dayModeEl.addEventListener("change", () => {
          state.dayMode = dayModeEl.value;
          state.day = null;
          paint();
        });
      }
      if (dayFilterEl) {
        let t = null;
        dayFilterEl.addEventListener("input", () => {
          clearTimeout(t);
          t = setTimeout(() => {
            state.dayFilter = dayFilterEl.value.trim();
            paint();
            const el = container.querySelector("#b5p-day-filter");
            if (el) {
              el.focus();
              const len = el.value.length;
              el.setSelectionRange(len, len);
            }
          }, 200);
        });
      }

      if (root) {
        bindTooltips(root);
        // Defer so layout has width for autoSize charts
        requestAnimationFrame(() => hydrateInteractiveCharts(root));
      }
    }
    paint();
  }

  return { mount, fmtPct, fmtUsd, readSubFromHash };
});
