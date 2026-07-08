#!/usr/bin/env python3
"""Build and send the weekly ops email: B2/B4 earnings + recent delistings."""
from __future__ import annotations

import argparse
import json
import os
import smtplib
import sys
from datetime import UTC, date, datetime, timedelta
from email.message import EmailMessage
from email.utils import getaddresses
from html import escape
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from earnings_universe import (  # noqa: E402
    DEFAULT_EARNINGS_BUCKETS,
    load_bucket_underlying_etfs,
)

REPO_ROOT = SCRIPTS.parent
DATA_DIR = REPO_ROOT / "data"
KNOWN_CALENDAR_PATH = DATA_DIR / "event_calendar_known.json"
CORPORATE_ACTIONS_PATH = DATA_DIR / "corporate_actions.json"
PREVIEW_PATH = DATA_DIR / "weekly_ops_email_preview.json"

DEFAULT_FROM_EMAIL = "werdnamdlog01@gmail.com"
DEFAULT_SMTP_USER = DEFAULT_FROM_EMAIL
DEFAULT_SMTP_HOST = "smtp.gmail.com"
DEFAULT_SMTP_PORT = 587
EARNINGS_HORIZON_DAYS = 14
DELISTING_LOOKBACK_DAYS = 14


def parse_recipients(raw: str) -> list[str]:
    """Parse PNL_RECIPIENTS (same format as ls-algo run_eod_pnl_email.py)."""
    if not raw:
        return []
    normalized = raw.replace(";", ",").replace("\n", ",")
    pairs = getaddresses([normalized])
    emails = [addr.strip() for _, addr in pairs if addr and addr.strip()]
    return [e for e in emails if "@" in e and " " not in e]


def _parse_iso_date(raw: object) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _bucket_label(raw: object) -> str:
    s = str(raw or "").strip().lower()
    mapping = {
        "bucket_2": "B2",
        "bucket_2_low_beta": "B2",
        "bucket_2_low_delta": "B2",
        "bucket_4": "B4",
        "bucket_4_edge": "B4",
        "bucket_1": "B1",
        "bucket_1_high_beta": "B1",
        "bucket_1_high_delta": "B1",
        "bucket_3": "B3",
        "bucket_3_inverse": "B3",
    }
    return mapping.get(s, s.upper() if s else "—")


def collect_upcoming_earnings(
    *,
    today: date | None = None,
    horizon_days: int = EARNINGS_HORIZON_DAYS,
    calendar_path: Path = KNOWN_CALENDAR_PATH,
    etf_map: dict[str, list[str]] | None = None,
) -> list[dict]:
    """Upcoming earnings for B2/B4 underlyings within the forward window."""
    today = today or date.today()
    horizon = today + timedelta(days=horizon_days)
    etf_map = etf_map or load_bucket_underlying_etfs()
    cal = _load_json(calendar_path)
    rows: list[dict] = []

    for item in cal.get("items") or []:
        if str(item.get("event_type") or "").lower() != "earnings":
            continue
        und = str(item.get("underlying") or "").upper()
        if not und or und not in etf_map:
            continue
        ev_d = _parse_iso_date(item.get("event_date"))
        if ev_d is None or ev_d < today or ev_d > horizon:
            continue
        rows.append({
            "underlying": und,
            "event_date": ev_d.isoformat(),
            "days_until": (ev_d - today).days,
            "confirmation": str(item.get("confirmation") or "unknown"),
            "source": str(item.get("source") or "unknown"),
            "linked_etfs": etf_map.get(und, []),
            "historical_move_pct_mad": item.get("historical_move_pct_mad"),
        })

    rows.sort(key=lambda r: (r["event_date"], r["underlying"]))
    return rows


def collect_recent_delistings(
    *,
    today: date | None = None,
    lookback_days: int = DELISTING_LOOKBACK_DAYS,
    corporate_actions_path: Path = CORPORATE_ACTIONS_PATH,
) -> list[dict]:
    """Delisting announcements in the trailing lookback window."""
    today = today or date.today()
    cutoff = today - timedelta(days=lookback_days)
    payload = _load_json(corporate_actions_path)
    rows: list[dict] = []

    for ev in payload.get("events") or []:
        if str(ev.get("type") or "").lower() != "delisting":
            continue
        ref = _parse_iso_date(ev.get("announcement_date")) or _parse_iso_date(ev.get("execution_date"))
        if ref is None or ref < cutoff:
            continue
        rows.append({
            "ticker": str(ev.get("ticker") or "").upper(),
            "underlying": str(ev.get("underlying") or "").upper() or None,
            "announcement_date": ev.get("announcement_date"),
            "execution_date": ev.get("execution_date"),
            "status": str(ev.get("status") or "unknown"),
            "bucket": _bucket_label(ev.get("bucket")),
            "headline": str(ev.get("headline") or "").strip() or None,
            "source_url": ev.get("source_url"),
            "source": str(ev.get("source") or "unknown"),
        })

    rows.sort(
        key=lambda r: (
            _parse_iso_date(r.get("announcement_date")) or date.min,
            r.get("ticker") or "",
        ),
        reverse=True,
    )
    return rows


def build_email_payload(
    *,
    today: date | None = None,
    earnings_horizon_days: int = EARNINGS_HORIZON_DAYS,
    delisting_lookback_days: int = DELISTING_LOOKBACK_DAYS,
) -> dict:
    today = today or date.today()
    earnings = collect_upcoming_earnings(
        today=today,
        horizon_days=earnings_horizon_days,
    )
    delistings = collect_recent_delistings(
        today=today,
        lookback_days=delisting_lookback_days,
    )
    return {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "as_of_date": today.isoformat(),
        "earnings_horizon_days": earnings_horizon_days,
        "delisting_lookback_days": delisting_lookback_days,
        "universe_buckets": list(DEFAULT_EARNINGS_BUCKETS),
        "earnings": earnings,
        "delistings": delistings,
        "diagnostics": {
            "event_calendar_known": str(KNOWN_CALENDAR_PATH.relative_to(REPO_ROOT)).replace("\\", "/"),
            "corporate_actions": str(CORPORATE_ACTIONS_PATH.relative_to(REPO_ROOT)).replace("\\", "/"),
            "earnings_count": len(earnings),
            "delistings_count": len(delistings),
        },
    }


def _fmt_date(raw: object) -> str:
    d = _parse_iso_date(raw)
    return d.isoformat() if d else "—"


def _confirmation_badge(conf: str) -> str:
    c = (conf or "").lower()
    if c == "confirmed":
        return "confirmed"
    if c == "projected":
        return "projected"
    return conf or "unknown"


def render_plain_text(payload: dict) -> str:
    today = payload.get("as_of_date") or date.today().isoformat()
    lines = [
        f"ETF Dashboard Weekly Ops — {today}",
        "",
        f"Upcoming earnings (B2/B4 underlyings, next {payload.get('earnings_horizon_days', EARNINGS_HORIZON_DAYS)} days)",
        "—" * 72,
    ]
    earnings = payload.get("earnings") or []
    if not earnings:
        lines.append("No upcoming earnings in window.")
    else:
        for row in earnings:
            etfs = ", ".join(row.get("linked_etfs") or []) or "—"
            lines.append(
                f"{row['event_date']}  {row['underlying']:6s}  "
                f"{_confirmation_badge(row.get('confirmation', '')):10s}  "
                f"ETFs: {etfs}  ({row.get('source', '')})"
            )

    lines.extend([
        "",
        f"Delisting announcements (last {payload.get('delisting_lookback_days', DELISTING_LOOKBACK_DAYS)} days)",
        "—" * 72,
    ])
    delistings = payload.get("delistings") or []
    if not delistings:
        lines.append("No new delisting announcements in window.")
    else:
        for row in delistings:
            lines.append(
                f"Ann { _fmt_date(row.get('announcement_date')) }  "
                f"{row.get('ticker', '—'):6s}  "
                f"Eff { _fmt_date(row.get('execution_date')) }  "
                f"{row.get('status', '—'):8s}  "
                f"{row.get('bucket', '—'):3s}  "
                f"{(row.get('headline') or '')[:60]}"
            )
            if row.get("source_url"):
                lines.append(f"  {row['source_url']}")

    lines.extend([
        "",
        "Dashboard: https://goldmandrew.github.io/etf-dashboard/#/news",
        "",
        f"Generated {payload.get('generated_at', '')}",
    ])
    return "\n".join(lines)


def _html_table(headers: list[str], rows: list[list[str]]) -> str:
    if not rows:
        return "<p><em>None in window.</em></p>"
    head = "".join(f"<th>{escape(h)}</th>" for h in headers)
    body_rows = []
    for row in rows:
        cells = "".join(f"<td>{cell}</td>" for cell in row)
        body_rows.append(f"<tr>{cells}</tr>")
    return (
        '<table border="1" cellpadding="6" cellspacing="0" '
        'style="border-collapse:collapse;font-family:sans-serif;font-size:13px;">'
        f"<thead><tr>{head}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"
    )


def render_html(payload: dict) -> str:
    today = payload.get("as_of_date") or date.today().isoformat()
    earnings_rows = []
    for row in payload.get("earnings") or []:
        conf = _confirmation_badge(row.get("confirmation", ""))
        conf_html = (
            f'<span style="color:#b45309;">{escape(conf)}</span>'
            if conf == "projected"
            else escape(conf)
        )
        etfs = escape(", ".join(row.get("linked_etfs") or []) or "—")
        earnings_rows.append([
            escape(str(row.get("event_date") or "")),
            f"<strong>{escape(str(row.get('underlying') or ''))}</strong>",
            conf_html,
            etfs,
            escape(str(row.get("source") or "")),
        ])

    delisting_rows = []
    for row in payload.get("delistings") or []:
        headline = escape((row.get("headline") or "—")[:80])
        url = row.get("source_url")
        if url:
            headline = f'<a href="{escape(str(url))}">{headline}</a>'
        delisting_rows.append([
            escape(_fmt_date(row.get("announcement_date"))),
            f"<strong>{escape(str(row.get('ticker') or ''))}</strong>",
            escape(_fmt_date(row.get("execution_date"))),
            escape(str(row.get("status") or "")),
            escape(str(row.get("bucket") or "")),
            headline,
        ])

    return f"""<!DOCTYPE html>
<html>
<body style="font-family:sans-serif;color:#111;line-height:1.45;max-width:900px;">
  <h2>ETF Dashboard Weekly Ops — {escape(today)}</h2>
  <p>Upcoming <strong>bucket 2 + bucket 4</strong> underlying earnings (next {int(payload.get('earnings_horizon_days', EARNINGS_HORIZON_DAYS))} days).</p>
  {_html_table(["Date", "Underlying", "Confirmation", "B2/B4 ETFs", "Source"], earnings_rows)}
  <h3>Delisting announcements (last {int(payload.get('delisting_lookback_days', DELISTING_LOOKBACK_DAYS))} days)</h3>
  {_html_table(["Announced", "ETF", "Effective", "Status", "Bucket", "Headline"], delisting_rows)}
  <p style="margin-top:24px;font-size:12px;color:#555;">
    <a href="https://goldmandrew.github.io/etf-dashboard/#/news">News tab</a>
    · generated {escape(str(payload.get('generated_at') or ''))}
  </p>
</body>
</html>"""


def format_subject(payload: dict) -> str:
    today = payload.get("as_of_date") or date.today().isoformat()
    n_earn = len(payload.get("earnings") or [])
    n_delist = len(payload.get("delistings") or [])
    return f"ETF Dashboard Weekly — {n_earn} earnings / {n_delist} delistings — {today}"


def send_email(
    *,
    subject: str,
    plain_body: str,
    html_body: str,
    recipients: list[str],
) -> None:
    smtp_host = os.environ.get("SMTP_HOST", DEFAULT_SMTP_HOST)
    smtp_port = int(os.environ.get("SMTP_PORT", str(DEFAULT_SMTP_PORT)))
    smtp_user = os.environ.get("SMTP_USER", DEFAULT_SMTP_USER)
    smtp_pass = os.environ.get("SMTP_PASS", "")
    from_addr = os.environ.get("FROM_EMAIL", DEFAULT_FROM_EMAIL)

    if not recipients:
        raise ValueError("No valid recipients. Set PNL_RECIPIENTS.")
    if not smtp_pass:
        raise ValueError("SMTP_PASS is required to send email.")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(recipients)
    msg.set_content(plain_body)
    msg.add_alternative(html_body, subtype="html")

    with smtplib.SMTP(smtp_host, smtp_port, timeout=60) as smtp:
        smtp.starttls()
        smtp.login(smtp_user, smtp_pass)
        smtp.send_message(msg, to_addrs=recipients)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build and optionally send the weekly ops email")
    parser.add_argument("--send", action="store_true", help="Send via SMTP (requires secrets)")
    parser.add_argument("--dry-run", action="store_true", help="Print preview only; do not write or send")
    parser.add_argument("--earnings-horizon-days", type=int, default=EARNINGS_HORIZON_DAYS)
    parser.add_argument("--delisting-lookback-days", type=int, default=DELISTING_LOOKBACK_DAYS)
    parser.add_argument("--output", type=Path, default=PREVIEW_PATH)
    args = parser.parse_args()

    payload = build_email_payload(
        earnings_horizon_days=max(1, int(args.earnings_horizon_days)),
        delisting_lookback_days=max(1, int(args.delisting_lookback_days)),
    )
    subject = format_subject(payload)
    plain = render_plain_text(payload)
    html = render_html(payload)

    print(
        f"Weekly ops email: {payload['diagnostics']['earnings_count']} earnings, "
        f"{payload['diagnostics']['delistings_count']} delistings"
    )

    if args.dry_run:
        print(f"Subject: {subject}")
        print(plain[:3000])
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {args.output}")

    skip = os.environ.get("WEEKLY_OPS_SKIP_EMAIL", "").strip().lower() in ("1", "true", "yes")
    if args.send and not skip:
        recipients = parse_recipients(os.environ.get("PNL_RECIPIENTS", ""))
        send_email(subject=subject, plain_body=plain, html_body=html, recipients=recipients)
        print(f"Sent to {len(recipients)} recipient(s) from {os.environ.get('FROM_EMAIL', DEFAULT_FROM_EMAIL)}")
    elif args.send and skip:
        print("WEEKLY_OPS_SKIP_EMAIL=1 — email not sent")
    elif not args.send:
        print("Built preview only (pass --send to deliver)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
