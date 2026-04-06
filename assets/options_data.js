/* global window, module, fetch */
(function initOptionsData(globalObj) {
  async function fetchOptionsCache(url) {
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) throw new Error(`options cache HTTP ${res.status}`);
    return res.json();
  }

  function getContractsForSymbol(cache, symbol) {
    const sym = String(symbol || "").toUpperCase();
    const rows = cache?.symbols?.[sym]?.options;
    return Array.isArray(rows) ? rows : [];
  }

  function getExpiries(cache, symbol) {
    return Array.from(new Set(getContractsForSymbol(cache, symbol).map((x) => x.expiration_date).filter(Boolean))).sort();
  }

  function getStrikes(cache, symbol, expiry, type) {
    return Array.from(
      new Set(
        getContractsForSymbol(cache, symbol)
          .filter((x) => x.expiration_date === expiry && String(x.contract_type || "").toLowerCase() === String(type || "").toLowerCase())
          .map((x) => Number(x.strike_price))
          .filter(Number.isFinite),
      ),
    ).sort((a, b) => a - b);
  }

  const exported = { fetchOptionsCache, getContractsForSymbol, getExpiries, getStrikes };
  if (typeof module !== "undefined" && module.exports) module.exports = exported;
  if (globalObj) globalObj.OptionsData = exported;
})(typeof window !== "undefined" ? window : globalThis);

