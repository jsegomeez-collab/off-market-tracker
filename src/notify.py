"""
Notifier: sends top-scoring deals via Telegram bot and/or Gmail SMTP.

Required env vars (set via GitHub Actions secrets):
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID    - both required to enable Telegram
  GMAIL_USER, GMAIL_APP_PASSWORD, NOTIFY_EMAIL   - all three required for email

If env vars are missing, that channel is silently skipped.
"""

from __future__ import annotations

import os
import json
import smtplib
import sqlite3
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Sequence
import httpx


def _format_card(row: sqlite3.Row) -> str:
    reasons = []
    if row["score_reasons"]:
        try:
            reasons = json.loads(row["score_reasons"])
        except json.JSONDecodeError:
            reasons = []

    price = f"${row['listing_price']:,.0f}" if row["listing_price"] else "n/a"
    parts = [
        f"[{row['score']}] {row['address']}",
        f"  source: {row['source']} | city: {row['city'] or '?'} | price: {price}",
    ]
    if row["parcel_id"]:
        parts.append(f"  parcel: {row['parcel_id']}")
    if row["url"]:
        parts.append(f"  link: {row['url']}")
    if reasons:
        parts.append(f"  why: {' | '.join(reasons)}")
    return "\n".join(parts)


def _format_html(rows: Sequence[sqlite3.Row]) -> str:
    cards = []
    for row in rows:
        reasons_html = ""
        if row["score_reasons"]:
            try:
                rs = json.loads(row["score_reasons"])
                reasons_html = "<br><small style='color:#666'>" + " &middot; ".join(rs) + "</small>"
            except json.JSONDecodeError:
                pass
        price = f"${row['listing_price']:,.0f}" if row["listing_price"] else "n/a"
        url = row["url"] or "#"
        cards.append(
            f"""
            <div style="border-left:4px solid #2563eb;padding:12px 16px;margin:12px 0;background:#f8fafc">
              <div style="font-size:18px"><b>[{row['score']}]</b> {row['address']}</div>
              <div style="color:#475569;margin-top:4px">
                {row['source']} &middot; {row['city'] or '?'} &middot; <b>{price}</b>
                {f" &middot; parcel: <code>{row['parcel_id']}</code>" if row['parcel_id'] else ""}
              </div>
              <div style="margin-top:8px"><a href="{url}">View source</a></div>
              {reasons_html}
            </div>
            """
        )
    return f"<html><body style='font-family:system-ui,sans-serif;max-width:680px'>{''.join(cards)}</body></html>"


def send_telegram(rows: Sequence[sqlite3.Row]) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("[notify] Telegram skipped (no token/chat_id)")
        return False
    if not rows:
        return True

    header = f"DEAL RADAR — {len(rows)} new opportunities"
    body = "\n\n".join(_format_card(r) for r in rows)
    message = f"{header}\n\n{body}"

    for chunk_start in range(0, len(message), 4000):
        chunk = message[chunk_start : chunk_start + 4000]
        try:
            r = httpx.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": chunk, "disable_web_page_preview": True},
                timeout=20,
            )
            r.raise_for_status()
        except Exception as e:
            print(f"[notify] Telegram error: {e}")
            return False
    print(f"[notify] Telegram sent {len(rows)} deals")
    return True


def send_email(rows: Sequence[sqlite3.Row]) -> bool:
    user = os.getenv("GMAIL_USER")
    pwd = os.getenv("GMAIL_APP_PASSWORD")
    to_addr = os.getenv("NOTIFY_EMAIL", user)
    if not user or not pwd:
        print("[notify] Email skipped (no GMAIL_USER/GMAIL_APP_PASSWORD)")
        return False
    if not rows:
        return True

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Deal Radar: {len(rows)} new off-market opportunities"
    msg["From"] = user
    msg["To"] = to_addr

    text = "\n\n".join(_format_card(r) for r in rows)
    html = _format_html(rows)
    msg.attach(MIMEText(text, "plain"))
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
            server.login(user, pwd)
            server.sendmail(user, [to_addr], msg.as_string())
        print(f"[notify] Email sent to {to_addr} with {len(rows)} deals")
        return True
    except Exception as e:
        print(f"[notify] Email error: {e}")
        return False


def notify_all(rows: Sequence[sqlite3.Row]) -> bool:
    """Returns True if at least one channel successfully delivered."""
    if not rows:
        print("[notify] No new deals to notify")
        return False
    tg_ok = send_telegram(rows)
    em_ok = send_email(rows)
    return tg_ok or em_ok
