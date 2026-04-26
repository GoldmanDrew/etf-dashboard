/**
 * Compare data/etf_distributions.json cash amounts vs Yahoo Finance
 * chart API dividend events (same source many dashboards use).
 *
 * Usage: node scripts/vet_distributions_yahoo.js [SYMBOL]
 *   No arg: all YieldBOOST income symbols from index.html list that exist in JSON.
 */
const https = require("https");
const fs = require("fs");
const path = require("path");

const ROOT = path.join(__dirname, "..");
const DIST = JSON.parse(fs.readFileSync(path.join(ROOT, "data", "etf_distributions.json"), "utf8"));

const YB_SYMBOLS = [
  "AMYY", "AZYY", "BBYY", "COYY", "CWY", "HMYY", "HOYY", "IOYY", "MAAY", "FBYY",
  "MTYY", "MUYY", "NUGY", "NVYY", "PLYY", "QBY", "RGYY", "RTYY", "SEMY", "SMYY",
  "TMYY", "TQQY", "TSYY", "XBTY", "YSPY",
];

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    https
      .get(
        url,
        {
          headers: {
            "User-Agent": "Mozilla/5.0 (compatible; etf-dashboard-vet/1.0)",
            Accept: "application/json",
          },
        },
        (res) => {
          let b = "";
          res.on("data", (c) => (b += c));
          res.on("end", () => {
            if (res.statusCode !== 200) {
              reject(new Error(`HTTP ${res.statusCode} ${url.slice(0, 80)}`));
              return;
            }
            try {
              resolve(JSON.parse(b));
            } catch (e) {
              reject(e);
            }
          });
        },
      )
      .on("error", reject);
  });
}

async function yahooDividends(sym) {
  const u = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(sym)}?interval=1d&range=max&events=div`;
  const j = await fetchJson(u);
  const result = j?.chart?.result?.[0];
  if (!result) return { ok: false, error: "no chart result", rows: [] };
  const ev = result.events?.dividends;
  if (!ev) return { ok: true, rows: [] };
  const rows = Object.values(ev)
    .map((x) => ({
      ex: new Date(x.date * 1000).toISOString().slice(0, 10),
      amt: Number(x.amount),
    }))
    .sort((a, b) => a.ex.localeCompare(b.ex));
  return { ok: true, rows };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function diffOursYahoo(ours, yahooRows) {
  const om = Object.fromEntries(ours.map((r) => [r.ex_date, Number(r.amount)]));
  const ym = Object.fromEntries(yahooRows.map((r) => [r.ex, r.amt]));
  let mismatch = 0;
  let onlyOurs = 0;
  let onlyYahoo = 0;
  const problems = [];
  for (const r of ours) {
    const y = ym[r.ex_date];
    if (y === undefined) {
      onlyOurs++;
      problems.push(`only_ours ${r.ex_date} amt=${r.amount}`);
    } else if (Math.abs(Number(r.amount) - y) > 1e-6) {
      mismatch++;
      problems.push(`mismatch ${r.ex_date} ours=${r.amount} yahoo=${y}`);
    }
  }
  for (const r of yahooRows) {
    if (om[r.ex] === undefined) {
      onlyYahoo++;
      problems.push(`only_yahoo ${r.ex} amt=${r.amt}`);
    }
  }
  return { onlyOurs, onlyYahoo, mismatch, problems };
}

async function vetSymbol(sym) {
  const ours = DIST.by_symbol[sym] || [];
  const y = await yahooDividends(sym);
  if (!y.ok) return { sym, ok: false, error: y.error, ours: ours.length, yahoo: 0 };
  const d = diffOursYahoo(ours, y.rows);
  const clean = d.onlyOurs === 0 && d.onlyYahoo === 0 && d.mismatch === 0;
  return {
    sym,
    ok: clean,
    ours: ours.length,
    yahoo: y.rows.length,
    ...d,
  };
}

async function main() {
  const arg = process.argv[2];
  const symbols = arg ? [arg.toUpperCase()] : YB_SYMBOLS.filter((s) => (DIST.by_symbol[s] || []).length > 0);
  const results = [];
  for (const sym of symbols) {
    try {
      results.push(await vetSymbol(sym));
    } catch (e) {
      results.push({ sym, ok: false, error: String(e.message || e) });
    }
    await sleep(250);
  }
  const bad = results.filter((r) => !r.ok);
  console.log(JSON.stringify({ checked: results.length, failed: bad.length, results }, null, 2));
  if (bad.length) process.exitCode = 1;
}

main();
