/**
 * Quarantine manifest consumer — graceful degradation for held tickers/artifacts.
 *
 * data/quarantine.json is written by scripts/data_sentinel.py (gate + sweep) and
 * auto-ships with every Pages deploy (deploy-pages copies all sub-20MB data/ JSON).
 *
 * Contract with score rescaling: cross-sectional percentiles/scores in
 * dashboard_data.json (net_edge_p*, rv_pctile, borrow spike p_5d, ...) are computed
 * SERVER-SIDE in scripts/build_data.py over the full universe. Quarantine therefore
 * NEVER removes records — consumers badge/gray/null-display affected values, exactly
 * like the existing strategy_blacklisted "BL" badge and the VrpUi.blockSignal
 * field-nulling pattern. Removing rows client-side would not break rescaling, but
 * removing them upstream would silently shift every other ticker's scores, so the
 * manifest is the only quarantine channel end to end.
 *
 * Manifest shape (schema_v 1):
 *   { build_time, tickers:   { SYM:  { first_seen, last_seen, clean_streak, reasons: [{code, artifact, detail}] } },
 *     artifacts: { "data/x.json": { ...same shape... } } }
 */
(function () {
  const URL = 'data/quarantine.json';

  function isQuarantined(manifest, symbol) {
    if (!manifest || !manifest.tickers || !symbol) return false;
    return Object.prototype.hasOwnProperty.call(manifest.tickers, String(symbol).toUpperCase());
  }

  function reasons(manifest, symbol) {
    const ent = manifest && manifest.tickers
      ? manifest.tickers[String(symbol || '').toUpperCase()]
      : null;
    if (!ent || !Array.isArray(ent.reasons)) return [];
    return ent.reasons.map((r) => `${r.code}: ${r.detail}`);
  }

  /** Tooltip text for the QN badge. */
  function badgeTitle(manifest, symbol) {
    const rs = reasons(manifest, symbol);
    const head = 'Data quarantined by sentinel — values may be unreliable; ticker stays listed so cross-sectional scores are unaffected.';
    return rs.length ? `${head}\n${rs.join('\n')}` : head;
  }

  /** Artifact-level hold for a data file this panel consumes (or null). */
  function artifactHold(manifest, relPath) {
    if (!manifest || !manifest.artifacts) return null;
    return manifest.artifacts[relPath] || null;
  }

  /** Sorted [{symbol, entry}] for the Ops page table. */
  function entries(manifest) {
    if (!manifest || !manifest.tickers) return [];
    return Object.keys(manifest.tickers)
      .sort()
      .map((symbol) => ({ symbol, entry: manifest.tickers[symbol] }));
  }

  /** Sorted [{path, entry}] of artifact-level holds (whole-file integrity issues). */
  function artifactEntries(manifest) {
    if (!manifest || !manifest.artifacts) return [];
    return Object.keys(manifest.artifacts)
      .sort()
      .map((path) => ({ path, entry: manifest.artifacts[path] }));
  }

  /**
   * Annotate (never filter) records: adds _quarantined + _quarantine_reasons.
   * Callers that must hide actionable values should null display fields the way
   * VrpUi.blockSignal does — not drop the record.
   */
  function decorate(records, manifest) {
    if (!Array.isArray(records)) return records;
    return records.map((r) => {
      const held = isQuarantined(manifest, r && r.symbol);
      return held
        ? { ...r, _quarantined: true, _quarantine_reasons: reasons(manifest, r.symbol) }
        : r;
    });
  }

  window.Quarantine = { URL, isQuarantined, reasons, badgeTitle, artifactHold, entries, artifactEntries, decorate };
})();
