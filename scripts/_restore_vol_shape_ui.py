"""Restore vol-shape UI blocks removed by e310cf3 (Beta->Delta rename)."""
from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "index.html"
D = "div"


def git_show_index(rev: str) -> str:
    raw = subprocess.check_output(["git", "show", f"{rev}:index.html"], cwd=ROOT)
    if raw.startswith(b"\xff\xfe"):
        return raw.decode("utf-16")
    if raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16-be")
    return raw.decode("utf-8")


def slice_lines(text: str, start: int, end: int) -> str:
    lines = text.splitlines(keepends=True)
    return "".join(lines[start - 1 : end])


def main() -> None:
    text = INDEX.read_text(encoding="utf-8")
    if "buildUnderlyingVolShapeHistory" in text:
        print("already restored")
        return

    good = git_show_index("654acc7")
    detail = slice_lines(good, 3164, 3177)
    build_fn = slice_lines(good, 3387, 3443)
    chart_stat = slice_lines(good, 4795, 4800)
    history = slice_lines(good, 5005, 5048)
    use_memo = (
        "  const volShapeHistory = useMemo(\n"
        "    () => buildUnderlyingVolShapeHistory(etfBacktestSeries),\n"
        "    [etfBacktestSeries],\n"
        "  );\n"
    )

    und_anchor = (
        f'          <{D} className="detail-item">\n'
        f'            <{D} className="dlabel">Und. vol (12M realized)</{D}>\n'
    )
    if und_anchor not in text:
        raise SystemExit("missing Und. vol anchor")
    text = text.replace(und_anchor, detail + und_anchor, 1)

    text = text.replace(
        "function medianSharesTraded(rows, lookback = 60) {",
        build_fn + "function medianSharesTraded(rows, lookback = 60) {",
        1,
    )

    text = text.replace(
        "  }, [etfMetricsMap, etfSym, etfDistributionsSeries]);\n"
        "  const pairBacktestCoverageHint = useMemo(() => {",
        "  }, [etfMetricsMap, etfSym, etfDistributionsSeries]);\n"
        + use_memo
        + "  const pairBacktestCoverageHint = useMemo(() => {",
        1,
    )

    nav_anchor = (
        f'            <{D} className="chart-stat">\n'
        f'              <{D} className="cs-label">NAV (Daily)</{D}>\n'
    )
    nav_idx = text.find(nav_anchor)
    if nav_idx < 0:
        raise SystemExit("missing NAV anchor")
    text = text[:nav_idx] + chart_stat + text[nav_idx:]

    ladder_anchor = (
        "              </table>\n"
        f"            </{D}>\n"
        f"          </{D}>\n\n"
        "          {etfMetricsLoading &&"
    )
    if ladder_anchor not in text:
        raise SystemExit("missing ladder anchor")
    text = text.replace(
        ladder_anchor,
        "              </table>\n"
        f"            </{D}>\n\n"
        + history
        + f"          </{D}>\n\n"
        "          {etfMetricsLoading &&",
        1,
    )

    INDEX.write_text(text, encoding="utf-8")
    print("restored vol-shape UI in index.html")


if __name__ == "__main__":
    main()
