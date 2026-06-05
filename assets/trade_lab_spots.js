/* global window, module */
(function initTradeLabSpots(globalObj) {
  const OPTIONS_STALE_AFTER_MINUTES = 180;

  function toNum(v) {
    if (typeof v === "number") return Number.isFinite(v) ? v : NaN;
    if (typeof v === "string") {
      const s = v.trim();
      if (!s) return NaN;
      const n = Number(s);
      return Number.isFinite(n) ? n : NaN;
    }
    return NaN;
  }

  function formatAgeLabel(ageSeconds) {
    const sec = toNum(ageSeconds);
    if (!Number.isFinite(sec) || sec < 0) return null;
    if (sec < 3600) return `${Math.round(sec / 60)}m`;
    if (sec < 86400) return `${Math.round(sec / 3600)}h`;
    return `${Math.round(sec / 86400)}d`;
  }

  function isFreshOptionsRow(row) {
    if (!row || row.stale === true) return false;
    const spot = toNum(row.spot);
    if (!Number.isFinite(spot) || spot <= 0) return false;
    const ageSec = toNum(row.cache_age_seconds);
    if (Number.isFinite(ageSec) && ageSec > OPTIONS_STALE_AFTER_MINUTES * 60) return false;
    return true;
  }

  function isFreshIntradayRow(row) {
    if (!row || row.stale === true) return false;
    const last = toNum(row.last);
    return Number.isFinite(last) && last > 0;
  }

  function resolveIntradayRow(symbol, underlying, intradaySpot) {
    if (!intradaySpot) return null;
    const bySymbol = intradaySpot.by_symbol || null;
    if (bySymbol && isFreshIntradayRow(bySymbol[symbol])) {
      return bySymbol[symbol];
    }
    if (symbol === underlying) {
      const byUnderlying = intradaySpot.by_underlying || null;
      if (byUnderlying && isFreshIntradayRow(byUnderlying[symbol])) {
        return byUnderlying[symbol];
      }
    }
    return null;
  }

  function resolveSymbolSpot(params) {
    const symbol = String((params && params.symbol) || "").toUpperCase();
    const underlying = String((params && params.underlyingSymbol) || "").toUpperCase();
    const optionsCache = (params && params.optionsCache) || null;
    const intradaySpot = (params && params.intradaySpot) || null;

    const cacheRow = optionsCache?.symbols?.[symbol] || null;
    if (isFreshOptionsRow(cacheRow)) {
      return {
        symbol,
        spot: toNum(cacheRow.spot),
        ok: true,
        stale: false,
        missing: false,
        source: cacheRow.source || "options_cache",
        updatedAt: cacheRow.updated_at || null,
        ageSeconds: toNum(cacheRow.cache_age_seconds),
        ageLabel: formatAgeLabel(cacheRow.cache_age_seconds),
      };
    }

    const intradayRow = resolveIntradayRow(symbol, underlying, intradaySpot);
    if (isFreshIntradayRow(intradayRow)) {
      return {
        symbol,
        spot: toNum(intradayRow.last),
        ok: true,
        stale: false,
        missing: false,
        source: intradayRow.source || "intraday_spot",
        updatedAt: intradaySpot?.build_time || null,
        ageSeconds: null,
        ageLabel: null,
      };
    }

    const staleSpot = toNum(cacheRow?.spot);
    if (Number.isFinite(staleSpot) && staleSpot > 0) {
      return {
        symbol,
        spot: staleSpot,
        ok: false,
        stale: true,
        missing: false,
        source: cacheRow?.source || "options_cache",
        updatedAt: cacheRow?.updated_at || null,
        ageSeconds: toNum(cacheRow?.cache_age_seconds),
        ageLabel: formatAgeLabel(cacheRow?.cache_age_seconds),
      };
    }

    return {
      symbol,
      spot: NaN,
      ok: false,
      stale: cacheRow?.stale === true,
      missing: true,
      source: cacheRow?.source || null,
      updatedAt: cacheRow?.updated_at || null,
      ageSeconds: toNum(cacheRow?.cache_age_seconds),
      ageLabel: formatAgeLabel(cacheRow?.cache_age_seconds),
    };
  }

  function resolveTradeSpots(params) {
    const symbols = Array.isArray(params && params.symbols) ? params.symbols : [];
    const underlyingSymbol = String((params && params.underlyingSymbol) || "").toUpperCase();
    const bySymbol = {};
    const metaBySymbol = {};
    for (const symRaw of symbols) {
      const sym = String(symRaw || "").toUpperCase();
      if (!sym) continue;
      const meta = resolveSymbolSpot({
        symbol: sym,
        underlyingSymbol,
        optionsCache: params && params.optionsCache,
        intradaySpot: params && params.intradaySpot,
      });
      metaBySymbol[sym] = meta;
      if (meta.ok && Number.isFinite(meta.spot)) bySymbol[sym] = meta.spot;
    }
    const hasStale = Object.values(metaBySymbol).some((m) => m && (m.stale || m.missing));
    return { bySymbol, metaBySymbol, hasStale };
  }

  const exported = {
    OPTIONS_STALE_AFTER_MINUTES,
    formatAgeLabel,
    isFreshOptionsRow,
    isFreshIntradayRow,
    resolveIntradayRow,
    resolveSymbolSpot,
    resolveTradeSpots,
  };

  if (typeof module !== "undefined" && module.exports) module.exports = exported;
  if (globalObj) globalObj.TradeLabSpots = exported;
})(typeof window !== "undefined" ? window : globalThis);
