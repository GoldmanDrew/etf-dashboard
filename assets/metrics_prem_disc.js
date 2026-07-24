/* global window, module */
/**
 * Premium/discount gating for Stats-tab metrics charts.
 *
 * Ingest stamps premium_discount_eligible=false on market-backed / carry-forward
 * / issuer_lag rows (stale issuer NAV + live close). The table already respected
 * that flag; charts must too — otherwise close−NAV looks like a huge tradeable
 * premium. Client also hard-caps |prem| so older JSON with loose eligibility
 * cannot spike the chart.
 */
(function initMetricsPremDisc(globalObj) {
  /** Matches scripts/ingest_etf_metrics.PREM_DISC_MAX_ABS_RATIO (fraction → percent). */
  const MAX_ABS_PREM_DISC_PCT = 10;

  const ISSUER_STALE_KINDS = new Set([
    "carry_forward",
    "market_backed_no_issuer_nav",
    "issuer_lag",
    "issuer_early",
    "anchor_lag",
  ]);

  function isCarryForwardMetricsRow(m) {
    return String(m?.source_url || "").startsWith("carry_forward://")
      || String(m?.source_provider || "").toLowerCase().startsWith("carry_forward")
      || String(m?.stale_kind || "").toLowerCase() === "carry_forward";
  }

  function isMarketBackedMetricsRow(m) {
    return String(m?.stale_kind || "").toLowerCase() === "market_backed_no_issuer_nav"
      || String(m?.source_provider || "").toLowerCase() === "market_backed"
      || String(m?.source_url || "").startsWith("market_backed://");
  }

  function isIssuerStaleMetricsRow(m) {
    if (isCarryForwardMetricsRow(m) || isMarketBackedMetricsRow(m)) return true;
    const kind = String(m?.stale_kind || "").toLowerCase();
    return ISSUER_STALE_KINDS.has(kind);
  }

  function isPremiumDiscountEligibleRow(m) {
    if (!m) return false;
    if (m.premium_discount_eligible === false) return false;
    if (isIssuerStaleMetricsRow(m)) return false;
    if (m.premium_discount_eligible === true) {
      // Defense for older builds that marked issuer_lag / ~49% gaps as valid.
      const n = Number(m.nav);
      const c = Number(m.close_price);
      if (Number.isFinite(n) && n > 0 && Number.isFinite(c)) {
        if (Math.abs((c - n) / n) * 100 > MAX_ABS_PREM_DISC_PCT) return false;
      }
      return true;
    }
    const issuerAsOf = String(m.issuer_asof_date || "");
    const marketAsOf = String(m.market_asof_date || "");
    if (issuerAsOf && marketAsOf && issuerAsOf !== marketAsOf) return false;
    return true;
  }

  function premiumDiscountPct(nav, close, metricsRow) {
    if (metricsRow && !isPremiumDiscountEligibleRow(metricsRow)) return null;
    const n = Number(nav);
    const c = Number(close);
    if (!Number.isFinite(n) || n <= 0 || !Number.isFinite(c)) return null;
    const pct = ((c - n) / n) * 100;
    if (Math.abs(pct) > MAX_ABS_PREM_DISC_PCT) return null;
    return pct;
  }

  /** Issuer NAV for the NAV-vs-close plot; null when NAV is stale vs session close. */
  function issuerNavForPlot(metricsRow) {
    if (!metricsRow || isIssuerStaleMetricsRow(metricsRow)) return null;
    const nav = Number(metricsRow.nav);
    if (!(Number.isFinite(nav) && nav > 0)) return null;
    // Frozen issuer NAV can still be eligible for a session or two while
    // |prem| stays under the 10% hard-cap — do not plot that as a live NAV.
    const close = Number(metricsRow.close_price);
    if (Number.isFinite(close) && Math.abs((close - nav) / nav) * 100 > MAX_ABS_PREM_DISC_PCT) {
      return null;
    }
    if (metricsRow.premium_discount_eligible === false) return null;
    return nav;
  }

  const api = {
    isCarryForwardMetricsRow,
    isMarketBackedMetricsRow,
    isIssuerStaleMetricsRow,
    isPremiumDiscountEligibleRow,
    premiumDiscountPct,
    issuerNavForPlot,
  };

  if (typeof module !== "undefined" && module.exports) {
    module.exports = api;
  }
  if (globalObj) {
    globalObj.MetricsPremDisc = api;
  }
})(typeof window !== "undefined" ? window : typeof globalThis !== "undefined" ? globalThis : null);
