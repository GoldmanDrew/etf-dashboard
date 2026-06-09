/**
 * Canonical split-aware total-return price series for ETF metrics rows.
 * Mirrors scripts/price_basis.py + build_data window logic.
 */
(function initPriceBasis(globalObj) {
  const INTEGER_SPLIT_FACTORS = [2, 3, 4, 5, 6, 10, 15, 20, 25, 50];
  const SPLIT_RATIOS = Array.from(
    new Set(INTEGER_SPLIT_FACTORS.concat(INTEGER_SPLIT_FACTORS.map((f) => 1 / f))),
  ).sort((a, b) => b - a);

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

  function parseDate(s) {
    const d = String(s || "").trim().slice(0, 10);
    return d.length === 10 ? d : "";
  }

  function nearestSplitRatio(observed, relTol = 0.075) {
    const x = toNum(observed);
    if (!Number.isFinite(x) || x <= 0) return null;
    let bestR = null;
    let bestErr = 1e9;
    for (const r of SPLIT_RATIOS) {
      const err = Math.abs(x / r - 1);
      if (err < bestErr) {
        bestErr = err;
        bestR = r;
      }
    }
    if (bestR == null || bestErr > relTol) return null;
    return bestR;
  }

  function matchSplitToPriceJump(jump, declaredMult, jumpTol = 0.18, relTol = 0.075) {
    const j = toNum(jump);
    const dm = toNum(declaredMult);
    if (!Number.isFinite(j) || j <= 0) return null;
    if (Number.isFinite(dm) && dm > 0 && Math.abs(j / dm - 1) <= jumpTol) return dm;
    return nearestSplitRatio(j, relTol);
  }

  function confirmSplitFromSharesOrNav(prev, curr, declaredMult, relTol = 0.15) {
    const dm = toNum(declaredMult);
    const candidates = [];
    const shP = toNum(prev && prev.shares_outstanding);
    const shC = toNum(curr && curr.shares_outstanding);
    if (shP > 0 && shC > 0) candidates.push(shP / shC);
    const navP = toNum(prev && prev.nav);
    const navC = toNum(curr && curr.nav);
    if (navP > 0 && navC > 0) candidates.push(navC / navP);
    for (const obs of candidates) {
      const matched = matchSplitToPriceJump(obs, dm, relTol, relTol);
      if (matched == null) continue;
      if (!Number.isFinite(dm) || Math.abs(matched - dm) <= Math.max(1e-6, relTol * Math.abs(dm))) {
        return Number.isFinite(dm) ? dm : matched;
      }
    }
    return null;
  }

  function etfAdjCloseNeedsSplitScaling(close, adj, eps = 1e-4) {
    const c = toNum(close);
    const a = toNum(adj);
    if (!(c > 0 && a > 0)) return false;
    return Math.abs(a / c - 1) <= eps;
  }

  function etfAdjOnBackAdjustedForwardBasis(close, adj, mult, relTol = 0.15) {
    const c = toNum(close);
    const a = toNum(adj);
    const m = toNum(mult);
    if (!(c > 0 && a > 0 && m > 0 && m < 1)) return false;
    return Math.abs((a / c) / m - 1) <= relTol;
  }

  function etfAdjOnPostSplitBasis(close, adj, mult, relTol = 0.15) {
    const c = toNum(close);
    const a = toNum(adj);
    const m = toNum(mult);
    if (!(c > 0 && a > 0 && m > 1.05)) return false;
    return Math.abs((a / c) / m - 1) <= relTol;
  }

  function yahooAdjLooksBackAdjusted(points, effectiveDate, mult, relTol = 0.15) {
    const eff = parseDate(effectiveDate);
    const m = toNum(mult);
    if (!eff || !(m > 0) || !Array.isArray(points) || !points.length) return false;
    const before = points.filter((p) => parseDate(p.date) < eff);
    const onAfter = points.filter((p) => parseDate(p.date) >= eff);
    if (!before.length || !onAfter.length) return false;
    const closeBefore = toNum(before[before.length - 1].close ?? before[before.length - 1][1]);
    const closeAfter = toNum(onAfter[0].close ?? onAfter[0][1]);
    const adjBefore = toNum(before[before.length - 1].adj ?? before[before.length - 1][2]);
    const adjAfter = toNum(onAfter[0].adj ?? onAfter[0][2]);
    if (!(closeBefore > 0 && closeAfter > 0 && adjBefore > 0 && adjAfter > 0)) return false;
    if (Math.abs(closeAfter / closeBefore - 1) > 0.08) return false;
    const ratio = adjAfter / adjBefore;
    return (
      Math.abs(ratio * m - 1) <= relTol
      || Math.abs(ratio / m - 1) <= relTol
      || Math.abs((adjBefore * m) / adjAfter - 1) <= relTol
    );
  }

  function adjCloseRatioMatchesPreBackAdjusted(ratio, mult, relTol = 0.15) {
    const r = toNum(ratio);
    const m = toNum(mult);
    if (!(r > 0 && m > 0)) return false;
    if (m < 1) {
      return (
        Math.abs(r * m - 1) <= relTol
        || Math.abs(r / m - 1) <= relTol
        || Math.abs(r - 1 / m) <= relTol
      );
    }
    return Math.abs(r / m - 1) <= relTol;
  }

  function adjCloseRatioMatchesPostSplit(ratio, mult, relTol = 0.08) {
    const r = toNum(ratio);
    const m = toNum(mult);
    if (!(r > 0 && m > 0)) return false;
    if (Math.abs(r - 1) <= relTol) return true;
    if (m < 1 && Math.abs(r / m - 1) <= relTol) return true;
    return false;
  }

  function detectAdjBoundary(points, effectiveDate, mult, relTol = 0.15, closeTol = 0.08) {
    const eff = parseDate(effectiveDate);
    const m = toNum(mult);
    if (!eff || !(m > 0) || !Array.isArray(points) || points.length < 2) return null;
    const sorted = [...points].sort((a, b) => parseDate(a.date).localeCompare(parseDate(b.date)));
    const candidates = [];
    for (let i = 1; i < sorted.length; i += 1) {
      const cPrev = toNum(sorted[i - 1].close ?? sorted[i - 1][1]);
      const cCur = toNum(sorted[i].close ?? sorted[i][1]);
      const aPrev = toNum(sorted[i - 1].adj ?? sorted[i - 1][2]);
      const aCur = toNum(sorted[i].adj ?? sorted[i][2]);
      const dCur = parseDate(sorted[i].date);
      if (!(cPrev > 0 && cCur > 0 && aPrev > 0 && aCur > 0 && dCur)) continue;
      const ratio = aCur / aPrev;
      if (Math.abs(cCur / cPrev - 1) <= closeTol) {
        if (
          Math.abs(ratio * m - 1) <= relTol
          || Math.abs(ratio / m - 1) <= relTol
          || Math.abs((aPrev * m) / aCur - 1) <= relTol
        ) {
          candidates.push(dCur);
          continue;
        }
      }
      const rPrev = aPrev / cPrev;
      const rCur = aCur / cCur;
      if (adjCloseRatioMatchesPostSplit(rCur, m, Math.min(relTol, 0.1))
        && adjCloseRatioMatchesPreBackAdjusted(rPrev, m, relTol)) {
        candidates.push(dCur);
      }
    }
    if (!candidates.length) return null;
    const near = candidates.filter((d) => {
      const dayDiff = Math.abs((new Date(d) - new Date(eff)) / 86400000);
      return dayDiff <= 7;
    });
    const pool = near.length ? near : candidates;
    return pool.sort((a, b) => a.localeCompare(b))[0];
  }

  function findCloseJumpBoundary(closePoints, mult, near, windowDays = 7, jumpTol = 0.18, relTol = 0.15) {
    const m = toNum(mult);
    const center = parseDate(near);
    if (!center || !(m > 0) || !Array.isArray(closePoints) || !closePoints.length) return null;
    const sorted = [...closePoints].sort((a, b) => parseDate(a.date ?? a[0]).localeCompare(parseDate(b.date ?? b[0])));
    const candidates = [];
    for (let i = 1; i < sorted.length; i += 1) {
      const dCur = parseDate(sorted[i].date ?? sorted[i][0]);
      const cCur = toNum(sorted[i].close ?? sorted[i][1]);
      const cPrev = toNum(sorted[i - 1].close ?? sorted[i - 1][1]);
      if (!dCur || !(cPrev > 0 && cCur > 0)) continue;
      if (Math.abs((new Date(dCur) - new Date(center)) / 86400000) > windowDays) continue;
      const jump = cCur / cPrev;
      const matched = matchSplitToPriceJump(jump, m, jumpTol, relTol);
      if (matched != null && Math.abs(matched - m) <= Math.max(1e-6, relTol * Math.abs(m))) {
        candidates.push(dCur);
      }
    }
    if (!candidates.length) return null;
    return candidates.sort(
      (a, b) => Math.abs(new Date(a) - new Date(center)) - Math.abs(new Date(b) - new Date(center)),
    )[0];
  }

  function closeJumpNearDate(closePoints, mult, center, windowDays = 7) {
    return findCloseJumpBoundary(closePoints, mult, center, windowDays) != null;
  }

  function detectStaggeredDiscreteSplits(points, closePoints, events, relTol = 0.15, windowDays = 7) {
    if (!Array.isArray(events) || !events.length) return [];
    const out = [];
    for (const ev of events) {
      const mult = toNum(ev.mult);
      if (!(mult >= 1.05)) continue;
      const boundary = findCloseJumpBoundary(closePoints, mult, ev.date, windowDays, 0.18, relTol);
      if (!boundary) continue;
      const adjBoundary = detectAdjBoundary(points, ev.date, mult, relTol) || parseDate(ev.date);
      if (boundary > adjBoundary) continue;
      out.push({ date: parseDate(ev.date), mult, boundary, variant: "staggered_reverse" });
    }
    return out;
  }

  function boundaryHasRawForwardAdj(points, boundary, fwdMult, relTol = 0.15) {
    const fm = toNum(fwdMult);
    const bnd = parseDate(boundary);
    if (!bnd || !(fm > 0 && fm < 1) || !Array.isArray(points) || !points.length) return false;
    const sorted = [...points].sort((a, b) => parseDate(a.date).localeCompare(parseDate(b.date)));
    for (let i = 1; i < sorted.length; i += 1) {
      if (parseDate(sorted[i].date) !== bnd) continue;
      const cPrev = toNum(sorted[i - 1].close ?? sorted[i - 1][1]);
      const aPrev = toNum(sorted[i - 1].adj ?? sorted[i - 1][2]);
      if (!(cPrev > 0 && aPrev > 0)) return false;
      const ratio = aPrev / cPrev;
      const inv = 1 / fm;
      if (Math.abs(ratio - inv) > relTol) return false;
      const closePts = sorted.map((p) => ({ date: p.date, close: p.close ?? p[1] }));
      const declaredDm = 1 / fm;
      if (closeJumpNearDate(closePts, declaredDm, bnd, 7)) return false;
      return true;
    }
    return false;
  }

  function detectAdjBasisSwitchSplits(points, events, relTol = 0.15, metricRows = null) {
    if (!Array.isArray(events) || !events.length) return [];
    const out = [];
    const seen = new Set();
    for (const ev of events) {
      const declared = toNum(ev.mult);
      if (!(declared > 0)) continue;
      const jump = splitCloseJumpRatio(points, ev.date);
      const closeContinuous = jump == null || Math.abs(jump - 1) <= 0.12;
      const trials = [];
      if (declared < 1) trials.push({ mult: declared, variant: "forward" });
      else {
        const closePts = points.map((p) => ({ date: p.date, close: p.close ?? p[1] }));
        const adjB = detectAdjBoundary(points, ev.date, declared, relTol) || parseDate(ev.date);
        const staggered = closeJumpNearDate(closePts, declared, adjB, 7);
        if (closeContinuous && !staggered) trials.push({ mult: declared, variant: "reverse_continuous" });
        const fwdMult = 1 / declared;
        const boundary = detectAdjBoundary(points, ev.date, fwdMult, relTol);
        if (boundary) {
          const dayDiff = Math.abs((new Date(boundary) - new Date(parseDate(ev.date))) / 86400000);
          if (dayDiff <= 7) {
            if (boundaryHasRawForwardAdj(points, boundary, fwdMult, relTol)) {
              trials.push({ mult: fwdMult, variant: "forward_continuous_close" });
            } else if (!staggered) {
              trials.push({ mult: fwdMult, variant: "forward" });
            }
          }
        }
      }
      for (const trial of trials) {
        const key = `${ev.date}:${trial.mult}:${trial.variant}`;
        if (seen.has(key)) continue;
        const boundary = detectAdjBoundary(points, ev.date, trial.mult, relTol);
        if (!boundary) continue;
        const dayDiff = Math.abs((new Date(boundary) - new Date(parseDate(ev.date))) / 86400000);
        if (dayDiff > 7) continue;
        if (trial.variant === "reverse_continuous") {
          const closePts = points.map((p) => ({ date: p.date, close: p.close ?? p[1] }));
          if (closeJumpNearDate(closePts, trial.mult, boundary, 7)) continue;
          const jumpAt = splitCloseJumpRatio(points, boundary);
          if (jumpAt != null && Math.abs(jumpAt - 1) > 0.12) continue;
        }
        seen.add(key);
        out.push({ date: parseDate(ev.date), mult: trial.mult, variant: trial.variant });
      }
    }
    return out;
  }

  function splitCloseJumpRatio(points, effectiveDate) {
    const eff = parseDate(effectiveDate);
    if (!eff || !Array.isArray(points) || !points.length) return null;
    const before = points.filter((p) => parseDate(p.date) < eff);
    const onAfter = points.filter((p) => parseDate(p.date) >= eff);
    if (!before.length || !onAfter.length) return null;
    const cBefore = toNum(before[before.length - 1].close);
    const cAfter = toNum(onAfter[0].close);
    if (!(cBefore > 0 && cAfter > 0)) return null;
    return cAfter / cBefore;
  }

  function filterSplitsNeedingCloseBasisFix(points, events, relTol = 0.15, metricRows = null) {
    if (!Array.isArray(events) || !events.length) return [];
    const out = [];
    const jumpTol = 0.18;
    for (const ev of events) {
      const mult = toNum(ev.mult);
      if (!(mult > 0)) continue;
      const jump = splitCloseJumpRatio(points, ev.date);
      if (jump != null && Math.abs(jump - 1) <= 0.08) {
        if (yahooAdjLooksBackAdjusted(points, ev.date, mult, relTol)) continue;
        continue;
      }

      let accepted = false;
      if (jump != null) {
        const matched = matchSplitToPriceJump(jump, mult, jumpTol, relTol);
        if (
          matched != null
          && Math.abs(matched - mult) <= Math.max(1e-6, relTol * Math.abs(mult))
          && !yahooAdjLooksBackAdjusted(points, ev.date, mult, relTol)
        ) {
          accepted = true;
        }
      }

      if (!accepted && jump == null && Array.isArray(metricRows) && metricRows.length) {
        const eff = parseDate(ev.date);
        const dated = metricRows
          .map((r) => ({ date: parseDate(r.date), row: r }))
          .filter((x) => x.date)
          .sort((a, b) => a.date.localeCompare(b.date));
        let prev = null;
        let curr = null;
        for (const item of dated) {
          if (item.date < eff) prev = item.row;
          else if (!curr) { curr = item.row; break; }
        }
        if (prev && curr && confirmSplitFromSharesOrNav(prev, curr, mult, relTol) != null) {
          accepted = true;
        }
      }

      if (accepted) out.push({ date: parseDate(ev.date), mult });
    }
    return out;
  }

  function detectSplitBoundary(points, splitMult, relTol = 0.15) {
    const target = toNum(splitMult);
    if (!Number.isFinite(target) || !Array.isArray(points) || points.length < 2) return null;
    for (let i = 1; i < points.length; i += 1) {
      const prev = toNum(points[i - 1].close);
      const cur = toNum(points[i].close);
      if (!(prev > 0 && cur > 0)) continue;
      const matched = matchSplitToPriceJump(cur / prev, target, 0.18, relTol);
      if (matched != null && Math.abs(matched - target) <= Math.max(1e-6, relTol * Math.abs(target))) {
        return parseDate(points[i].date);
      }
    }
    return null;
  }

  function parseSplitEventsFromCorp(corpPayload, ticker) {
    const sym = String(ticker || "").trim().toUpperCase();
    if (!sym || !corpPayload) return [];
    const events = [];
    for (const ev of corpPayload.events || []) {
      const t = String(ev?.ticker || "").trim().toUpperCase();
      if (t !== sym) continue;
      const typ = String(ev?.type || "");
      if (typ !== "reverse_split" && typ !== "forward_split") continue;
      const ed = parseDate(ev?.execution_date);
      const rf = toNum(ev?.ratio_from);
      const rt = toNum(ev?.ratio_to);
      if (!ed || !Number.isFinite(rf) || !Number.isFinite(rt) || rt === 0) continue;
      const mult = rf / rt;
      if (mult > 0) events.push({ date: ed, mult });
    }
    return events.sort((a, b) => a.date.localeCompare(b.date));
  }

  function cumSplitFactor(fromDate, toDate, events) {
    const from = parseDate(fromDate);
    const to = parseDate(toDate);
    if (!from || !to || from === to || !events?.length) return 1;
    let mult = 1;
    if (from < to) {
      for (const ev of events) {
        if (from < ev.date && ev.date <= to) mult *= ev.mult;
      }
    } else {
      for (const ev of events) {
        if (to < ev.date && ev.date <= from) mult /= ev.mult;
      }
    }
    return Number.isFinite(mult) && mult > 0 ? mult : 1;
  }

  /**
   * Resolve split context: discrete scaling only when raw close jumps at the event.
   */
  function resolveSplitContext(closePoints, splitEvents, metricRows = null) {
    const events = Array.isArray(splitEvents) ? splitEvents : [];
    if (!events.length || !closePoints.length) {
      return {
        mode: "continuous",
        boundary: null,
        mult: null,
        filtered: [],
      };
    }
    const filtered = filterSplitsNeedingCloseBasisFix(closePoints, events, 0.15, metricRows);
    let mult = null;
    let boundary = null;
    if (filtered.length) {
      mult = filtered[0].mult;
      boundary = detectSplitBoundary(closePoints, mult);
      if (!boundary) boundary = filtered[0].date;
      return { mode: "discrete_split", boundary, mult, filtered };
    }

    const staggered = detectStaggeredDiscreteSplits(closePoints, closePoints, events);
    if (staggered.length) {
      const sw = staggered[0];
      return {
        mode: "discrete_split",
        boundary: sw.boundary,
        mult: sw.mult,
        filtered: staggered,
        variant: sw.variant,
      };
    }

    const adjSwitch = detectAdjBasisSwitchSplits(closePoints, events, 0.15, metricRows);
    if (adjSwitch.length) {
      const sw = adjSwitch[0];
      const boundaryAdj = detectAdjBoundary(closePoints, sw.date, sw.mult)
        || sw.date;
      const mode = (sw.variant === "reverse_continuous" || sw.variant === "forward_continuous_close")
        ? "continuous_close_tr"
        : "adj_basis_switch";
      return {
        mode,
        boundary: boundaryAdj,
        mult: sw.mult,
        filtered: adjSwitch,
        variant: sw.variant,
      };
    }

    if (!boundary) {
      for (const ev of events) {
        const m = toNum(ev.mult);
        if (!(m > 0)) continue;
        const b = detectSplitBoundary(closePoints, m);
        if (b) {
          boundary = b;
          mult = m;
          break;
        }
      }
    }
    const mode = boundary && mult > 0 ? "discrete_split" : "continuous";
    return { mode, boundary, mult, filtered };
  }

  function isPreSplitDate(dateStr, ctx) {
    const ds = parseDate(dateStr);
    const boundary = parseDate(ctx?.boundary);
    const mult = toNum(ctx?.mult);
    const mode = ctx?.mode;
    return (
      (mode === "discrete_split" || mode === "adj_basis_switch" || mode === "continuous_close_tr")
      && boundary
      && mult > 0
      && ds
      && ds < boundary
    );
  }

  /**
   * ETF TR price on a common post-split (latest) basis when a discrete split applies.
   * When Yahoo adj is present it is authoritative; pre-split adj is mapped with mult.
   */
  function adjBasisSwitchTrPrice(close, mult) {
    const c = toNum(close);
    const m = toNum(mult);
    if (!(c > 0 && m > 0)) return NaN;
    return m <= 1 ? c * m : c / m;
  }

  function forwardPreSplitTrPrice(close, adj, mult, relTol = 0.15) {
    const c = toNum(close);
    const a = toNum(adj);
    const m = toNum(mult);
    if (!(c > 0 && a > 0 && m > 0 && m < 1)) return a;
    const ratio = a / c;
    if (Math.abs(ratio / m - 1) <= relTol) return a;
    if (Math.abs(ratio * m - 1) <= relTol || Math.abs(ratio - 1 / m) <= relTol) return c;
    return a;
  }

  function forwardPostSplitTrPrice(close, adj, mult, relTol = 0.15) {
    const c = toNum(close);
    const m = toNum(mult);
    if (!(c > 0 && m > 0 && m < 1)) return c;
    const a = toNum(adj);
    if (a > 0) {
      const ratio = a / c;
      if (Math.abs(ratio / m - 1) <= relTol) return a;
      if (Math.abs(ratio - 1) <= relTol) return adjBasisSwitchTrPrice(c, m);
    }
    return adjBasisSwitchTrPrice(c, m);
  }

  function adjBasisSwitchUsesFlatClose(sortedRows, ctx) {
    const boundary = parseDate(ctx?.boundary);
    if (!boundary || !Array.isArray(sortedRows)) return false;
    let preClose = null;
    let postClose = null;
    for (const row of sortedRows) {
      const ds = parseDate(row?.date);
      const close = toNum(row?.close_price) || toNum(row?.nav);
      if (!ds || !(close > 0)) continue;
      if (ds < boundary) preClose = close;
      else if (postClose == null) {
        postClose = close;
        break;
      }
    }
    if (!(preClose > 0 && postClose > 0)) return false;
    return Math.abs(postClose / preClose - 1) <= 0.02;
  }

  function etfTrPriceForPoint(point, ctx) {
    const close = toNum(point.close);
    if (!(close > 0)) return NaN;
    const adj = toNum(point.adj);
    const navTr = toNum(point.navTr);
    const preSplit = isPreSplitDate(point.date, ctx);
    const mult = toNum(ctx?.mult);
    const mode = ctx?.mode;

    if (mode === "adj_basis_switch") {
      if (ctx?._flatCloseTr) return close;
      if (preSplit) {
        if (adj > 0) return forwardPreSplitTrPrice(close, adj, mult);
        return adjBasisSwitchTrPrice(close, mult);
      }
      if (adj > 0) return forwardPostSplitTrPrice(close, adj, mult);
      return adjBasisSwitchTrPrice(close, mult);
    }

    if (mode === "continuous_close_tr") return close;

    if (preSplit && mult > 0) {
      if (adj > 0) {
        if (mult < 1 && etfAdjOnBackAdjustedForwardBasis(close, adj, mult)) return adj;
        if (etfAdjOnPostSplitBasis(close, adj, mult)) return adj;
        return adj * mult;
      }
      if (navTr > 0 && close > 0 && mult > 1.05) {
        if (navTr / close >= mult * 0.85 || navTr / close > 2.5) return close * mult;
        return navTr * mult;
      }
      if (navTr > 0) return navTr * mult;
      return close * mult;
    }
    if (
      mode === "discrete_split"
      && mult > 1.05
      && adj > 0
      && etfAdjOnPostSplitBasis(close, adj, mult)
    ) {
      return close;
    }
    if (
      mode === "discrete_split"
      && mult > 1.05
      && adj > 0
      && Math.abs((adj / close) * mult - 1) <= 0.15
    ) {
      return close;
    }
    if (adj > 0) return adj;
    if (navTr > 0) return navTr;
    return close;
  }

  function trModeForPoint(point, ctx) {
    const close = toNum(point.close);
    const adj = toNum(point.adj);
    const navTr = toNum(point.navTr);
    const preSplit = isPreSplitDate(point.date, ctx);
    const mult = toNum(ctx?.mult);
    const mode = ctx?.mode;

    if (mode === "adj_basis_switch") {
      return preSplit ? "pre_split_back_adj" : "post_split_back_adj_mapped";
    }
    if (mode === "continuous_close_tr") return "continuous_close_tr";

    if (preSplit && mult > 0) {
      if (adj > 0) {
        if (mult < 1 && etfAdjOnBackAdjustedForwardBasis(close, adj, mult)) return "pre_split_back_adj";
        if (etfAdjOnPostSplitBasis(close, adj, mult)) return "pre_split_adj_already_mapped";
        return "pre_split_adj_mapped";
      }
      if (navTr > 0 && close > 0 && mult > 1.05
        && (navTr / close >= mult * 0.85 || navTr / close > 2.5)) {
        return "pre_split_close_scaled";
      }
      if (navTr > 0) return "pre_split_nav_tr_scaled";
      return "pre_split_close_scaled";
    }
    if (adj > 0) return ctx?.mode === "discrete_split" ? "post_split_adj" : "continuous_adj";
    if (navTr > 0) return "nav_tr_fallback";
    return "close_fallback";
  }

  function metricsRowToPoint(row) {
    return {
      date: parseDate(row && row.date),
      close: toNum(row && row.close_price) || toNum(row && row.nav),
      adj: toNum(row && row.etf_adj_close),
      navTr: toNum(row && row.nav_total_return),
      row,
    };
  }

  function repairSplitOutlierBars(tr, ctx) {
    const boundary = parseDate(ctx?.boundary);
    if (!boundary || !Array.isArray(tr) || tr.length < 3) return tr;
    const repaired = tr.map((row) => ({ ...row }));
    for (let i = 1; i < repaired.length - 1; i += 1) {
      const d0 = parseDate(repaired[i].date);
      if (!d0 || d0 < boundary || Math.abs((new Date(d0) - new Date(boundary)) / 86400000) > 5) continue;
      const e0 = toNum(repaired[i - 1].trEtfPx);
      const e1 = toNum(repaired[i].trEtfPx);
      const u0 = toNum(repaired[i - 1].trUndPx);
      const u1 = toNum(repaired[i].trUndPx);
      if (!(e0 > 0 && e1 > 0 && u0 > 0 && u1 > 0)) continue;
      const lrE = Math.abs(Math.log(e1 / e0));
      const lrU = Math.abs(Math.log(u1 / u0));
      if (lrU >= 0.20 || lrE < 0.55 || lrE < lrU + 0.18) continue;
      const prevE = toNum(repaired[i - 1].trEtfPx);
      const nxtE = toNum(repaired[i + 1].trEtfPx);
      if (prevE > 0 && nxtE > 0) {
        repaired[i].trEtfPx = Math.sqrt(prevE * nxtE);
      }
    }
    return repaired;
  }

  /**
   * Build split-aware TR prices for decay, backtest, and charts.
   * @returns {Array<{date, trEtfPx, trUndPx, tradeClose, trMode, row}>}
   */
  function buildTrSeriesFromMetrics(rows, splitEvents) {
    const sorted = (Array.isArray(rows) ? rows : [])
      .filter((row) => parseDate(row && row.date))
      .sort((a, b) => parseDate(a.date).localeCompare(parseDate(b.date)));
    const points = sorted.map(metricsRowToPoint).filter((p) => p.date && p.close > 0);
    if (!points.length) return [];
    const closePoints = points.map((p) => ({ date: p.date, close: p.close, adj: p.adj }));
    const ctx = resolveSplitContext(closePoints, splitEvents || [], sorted);
    if (ctx.mode === "adj_basis_switch") {
      ctx._flatCloseTr = adjBasisSwitchUsesFlatClose(sorted, ctx);
    }
    const built = points.map((p) => {
      const trEtfPx = etfTrPriceForPoint(p, ctx);
      const trUndPx = toNum(p.row && p.row.underlying_adj_close);
      return {
        date: p.date,
        trEtfPx,
        trUndPx,
        tradeClose: p.close,
        trMode: trModeForPoint(p, ctx),
        row: p.row,
      };
    }).filter((x) => x.date && x.trEtfPx > 0 && x.trUndPx > 0);
    return repairSplitOutlierBars(built, ctx);
  }

  function maxAbsLogReturn(series, valueKey) {
    let maxJump = 0;
    let at = null;
    for (let i = 1; i < series.length; i += 1) {
      const a = toNum(series[i - 1][valueKey]);
      const b = toNum(series[i][valueKey]);
      if (!(a > 0 && b > 0)) continue;
      const lr = Math.abs(Math.log(b / a));
      if (lr > maxJump) {
        maxJump = lr;
        at = series[i].date;
      }
    }
    return { maxAbsLogReturn: maxJump, maxJumpDate: at };
  }

  /**
   * Coverage summary for Decay / Backtest TR badges.
   */
  function summarizeTrCoverage(rows, splitEvents) {
    const sorted = (Array.isArray(rows) ? rows : [])
      .filter((row) => parseDate(row && row.date));
    const rawInputDays = sorted.length;
    const etfAdjDays = sorted.filter((r) => toNum(r.etf_adj_close) > 0).length;
    const undAdjDays = sorted.filter((r) => toNum(r.underlying_adj_close) > 0).length;
    const bothLegDays = sorted.filter((r) => {
      const pl = toNum(r.close_price) || toNum(r.nav);
      const ps = toNum(r.underlying_adj_close);
      return pl > 0 && ps > 0;
    }).length;

    const closePoints = sorted
      .map(metricsRowToPoint)
      .filter((p) => p.date && p.close > 0)
      .map((p) => ({ date: p.date, close: p.close, adj: p.adj }));
    const ctx = resolveSplitContext(closePoints, splitEvents || [], sorted);
    const trSeries = buildTrSeriesFromMetrics(rows, splitEvents);
    const trJointDays = trSeries.length;
    const droppedDays = Math.max(0, bothLegDays - trJointDays);

    const etfTrModes = {};
    for (const row of trSeries) {
      const m = row.trMode || "unknown";
      etfTrModes[m] = (etfTrModes[m] || 0) + 1;
    }

    const etfJump = maxAbsLogReturn(trSeries, "trEtfPx");
    const undJump = maxAbsLogReturn(trSeries, "trUndPx");

    const warnings = [];
    if (droppedDays > 0) {
      warnings.push(`${droppedDays} day(s) dropped (missing ETF or underlying TR price).`);
    }
    if (etfAdjDays < rawInputDays * 0.85) {
      warnings.push(`ETF Yahoo adj close missing on ${rawInputDays - etfAdjDays} of ${rawInputDays} rows.`);
    }
    if (undAdjDays < rawInputDays * 0.85) {
      warnings.push(`Underlying adj close missing on ${rawInputDays - undAdjDays} of ${rawInputDays} rows.`);
    }
    if (etfJump.maxAbsLogReturn > 0.35) {
      warnings.push(`Large ETF TR daily move (${(Math.expm1(etfJump.maxAbsLogReturn) * 100).toFixed(0)}%) on ${etfJump.maxJumpDate || "?"}.`);
    }
    if (
      ctx.mode === "adj_basis_switch"
      && etfJump.maxAbsLogReturn <= 0.35
      && ctx.boundary
    ) {
      warnings.push(`Adj basis switch normalized at ${ctx.boundary} (×${ctx.mult}).`);
    }

    let quality = "good";
    if (etfJump.maxAbsLogReturn > 0.35 || droppedDays > bothLegDays * 0.1) quality = "degraded";
    if (trJointDays < 20 || etfJump.maxAbsLogReturn > 0.75) quality = "poor";

    const primaryEtfBasis = (() => {
      if (ctx.mode === "adj_basis_switch") return "adj_basis_switch";
      if (ctx.mode === "discrete_split") {
        const mapped = (etfTrModes.pre_split_adj_mapped || 0) + (etfTrModes.post_split_adj || 0);
        if (mapped >= trJointDays * 0.5) return "yahoo_adj_mapped";
        if ((etfTrModes.pre_split_close_scaled || 0) > 0) return "close_scaled";
        return "split_adjusted";
      }
      if ((etfTrModes.continuous_adj || 0) >= trJointDays * 0.5) return "yahoo_adj";
      if ((etfTrModes.nav_tr_fallback || 0) > 0) return "nav_tr";
      return "mixed";
    })();

    return {
      rawInputDays,
      bothLegDays,
      trJointDays,
      droppedDays,
      etfAdjDays,
      undAdjDays,
      etfAdjCoveragePct: rawInputDays ? etfAdjDays / rawInputDays : 0,
      undAdjCoveragePct: rawInputDays ? undAdjDays / rawInputDays : 0,
      jointCoveragePct: rawInputDays ? trJointDays / rawInputDays : 0,
      splitMode: ctx.mode,
      splitBoundary: ctx.boundary,
      splitMult: ctx.mult,
      etfTrModes,
      primaryEtfBasis,
      undBasis: "yahoo_adj",
      maxEtfDailyLogReturn: etfJump.maxAbsLogReturn,
      maxUndDailyLogReturn: undJump.maxAbsLogReturn,
      maxJumpDate: etfJump.maxJumpDate,
      quality,
      warnings,
    };
  }

  const exported = {
    SPLIT_RATIOS,
    nearestSplitRatio,
    matchSplitToPriceJump,
    confirmSplitFromSharesOrNav,
    etfAdjCloseNeedsSplitScaling,
    etfAdjOnPostSplitBasis,
    yahooAdjLooksBackAdjusted,
    detectAdjBasisSwitchSplits,
    splitCloseJumpRatio,
    filterSplitsNeedingCloseBasisFix,
    detectSplitBoundary,
    parseSplitEventsFromCorp,
    cumSplitFactor,
    resolveSplitContext,
    isPreSplitDate,
    etfTrPriceForPoint,
    trModeForPoint,
    buildTrSeriesFromMetrics,
    summarizeTrCoverage,
    metricsRowToPoint,
  };

  if (typeof module !== "undefined" && module.exports) {
    module.exports = exported;
  }
  if (globalObj) {
    globalObj.PriceBasis = exported;
  }
})(typeof window !== "undefined" ? window : globalThis);
