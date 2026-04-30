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


def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name, default)
    return v.strip() if isinstance(v, str) else v


SOURCE_LABELS = {
    "luzerne_tax_repo": "🏛️ TAX REPOSITORY",
    "luzerne_sheriff": "⚖️ SHERIFF SALE",
    "craigslist_scranton": "📰 FSBO (Craigslist)",
}

PRICE_NOTES = {
    "luzerne_tax_repo": (
        "Assessed value (county tax basis). Real cost to acquire: "
        "$500 (vacant lot) or $1,000 (with structure) + $100 fee + back taxes."
    ),
    "luzerne_sheriff": "Judgment amount on the foreclosure (not final sale price; auction starts at upset price).",
    "craigslist_scranton": "Owner's asking price.",
}


def _maps_url(address: str | None, city: str | None) -> str | None:
    if not address:
        return None
    parts = [p for p in [address, city, "PA"] if p]
    q = ", ".join(parts).replace(" ", "+")
    return f"https://www.google.com/maps/search/?api=1&query={q}"


def _street_view_url(address: str | None, city: str | None) -> str | None:
    if not address:
        return None
    parts = [p for p in [address, city, "PA"] if p]
    q = ", ".join(parts).replace(" ", "+")
    return f"https://www.google.com/maps/@?api=1&map_action=pano&pano_query={q}"


def _gis_url(parcel_id: str | None) -> str | None:
    if not parcel_id:
        return None
    return f"https://app.regrid.com/search?query={parcel_id.replace(' ', '+')}&context=us/pa/luzerne"


def _people_search_url(owner_name: str | None) -> str | None:
    if not owner_name:
        return None
    parts = owner_name.replace(",", " ").split()
    if len(parts) < 2:
        return None
    last, first = parts[0], parts[1]
    return f"https://www.truepeoplesearch.com/results?name={first}+{last}&citystatezip=PA"


def _format_card(row: sqlite3.Row) -> str:
    """Plain-text card for fallback/email. Telegram uses HTML."""
    reasons = []
    if row["score_reasons"]:
        try:
            reasons = json.loads(row["score_reasons"])
        except json.JSONDecodeError:
            reasons = []
    label = SOURCE_LABELS.get(row["source"], row["source"])
    price = f"${row['listing_price']:,.0f}" if row["listing_price"] else "n/a"
    parts = [
        f"[{row['score']}] {label} — {row['address']}",
        f"  city: {row['city'] or '?'} | price: {price}",
    ]
    if row["owner_name"]:
        parts.append(f"  owner: {row['owner_name']}")
    if row["parcel_id"]:
        parts.append(f"  parcel: {row['parcel_id']}")
    note = PRICE_NOTES.get(row["source"])
    if note:
        parts.append(f"  note: {note}")
    if row["url"]:
        parts.append(f"  source: {row['url']}")
    maps = _maps_url(row["address"], row["city"])
    if maps:
        parts.append(f"  maps: {maps}")
    if reasons:
        parts.append(f"  why: {' | '.join(reasons)}")
    return "\n".join(parts)


def _format_telegram_html(row: sqlite3.Row) -> str:
    label = SOURCE_LABELS.get(row["source"], row["source"])
    price = f"${row['listing_price']:,.0f}" if row["listing_price"] else "n/a"
    note = PRICE_NOTES.get(row["source"], "")

    raw_data = {}
    if row["raw"]:
        try:
            raw_data = json.loads(row["raw"]) if isinstance(row["raw"], str) else row["raw"]
        except (json.JSONDecodeError, TypeError):
            pass

    age_line = None
    jsd = raw_data.get("judicial_sale_date") if isinstance(raw_data, dict) else None
    if jsd:
        try:
            from datetime import datetime, date
            sale_dt = datetime.strptime(jsd, "%m/%d/%Y").date()
            years = (date.today() - sale_dt).days / 365.25
            if years <= 2:
                age_line = f"🟢 In repo since {jsd} ({years:.1f}y) — FRESH"
            elif years <= 5:
                age_line = f"🟡 In repo since {jsd} ({years:.1f}y)"
            else:
                age_line = f"🔴 In repo since {jsd} ({years:.0f}y) — likely problematic"
        except ValueError:
            pass

    from .classify import PROPERTY_TYPE_EMOJI, PROPERTY_TYPE_LABEL
    ptype = (row["property_type"] or "unknown").lower()
    type_emoji = PROPERTY_TYPE_EMOJI.get(ptype, "❓")
    type_label = PROPERTY_TYPE_LABEL.get(ptype, ptype)

    lines = [
        f"<b>[{row['score']}] {label}</b>",
        f"📍 <b>{row['address']}</b>",
        f"   {row['city'] or '?'} · PA · {type_emoji} {type_label}",
    ]
    if row["owner_name"]:
        lines.append(f"👤 Owner: <code>{row['owner_name']}</code>")
    if row["parcel_id"]:
        lines.append(f"🔢 Parcel: <code>{row['parcel_id']}</code>")
    lines.append(f"💵 {price}")
    if note:
        lines.append(f"   <i>{note}</i>")
    if age_line:
        lines.append(age_line)

    phones_raw = None
    try:
        phones_raw = row["st_phones"]
    except (IndexError, KeyError):
        pass
    if phones_raw:
        try:
            phones = json.loads(phones_raw) if isinstance(phones_raw, str) else phones_raw
        except (json.JSONDecodeError, TypeError):
            phones = []
        if phones:
            tel_links = " · ".join(f'<a href="tel:{p}">{p}</a>' for p in phones[:3])
            lines.append(f"📞 {tel_links}")

    links = []
    maps = _maps_url(row["address"], row["city"])
    if maps:
        links.append(f'<a href="{maps}">🗺️ Maps</a>')
    sv = _street_view_url(row["address"], row["city"])
    if sv:
        links.append(f'<a href="{sv}">📸 StreetView</a>')
    gis = _gis_url(row["parcel_id"])
    if gis:
        links.append(f'<a href="{gis}">🏠 GIS</a>')
    skip = _people_search_url(row["owner_name"])
    if skip:
        links.append(f'<a href="{skip}">📞 Skip-trace</a>')
    if row["url"]:
        links.append(f'<a href="{row["url"]}">📄 Source</a>')
    if links:
        lines.append("   " + " · ".join(links))
    return "\n".join(lines)


def _format_html(rows: Sequence[sqlite3.Row]) -> str:
    cards = []
    for row in rows:
        cards.append(f"<pre>{_format_telegram_html(row)}</pre>")
    return f"<html><body>{''.join(cards)}</body></html>"


def send_telegram(rows: Sequence[sqlite3.Row]) -> bool:
    token = _env("TELEGRAM_BOT_TOKEN")
    chat_id = _env("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("[notify] Telegram skipped (no token/chat_id)")
        return False
    if not rows:
        return True

    header = f"<b>🚨 DEAL RADAR</b> — {len(rows)} new opportunities\n"
    try:
        r = httpx.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": header,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=20,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"[notify] Telegram header error: {e}")
        return False

    sent = 0
    for row in rows:
        msg = _format_telegram_html(row)
        try:
            r = httpx.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": msg,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=20,
            )
            r.raise_for_status()
            sent += 1
        except Exception as e:
            print(f"[notify] Telegram error on row {row['id']}: {e}")

    print(f"[notify] Telegram sent {sent}/{len(rows)} deals")
    return sent > 0


def send_email(rows: Sequence[sqlite3.Row]) -> bool:
    user = _env("GMAIL_USER")
    pwd = _env("GMAIL_APP_PASSWORD")
    to_addr = _env("NOTIFY_EMAIL", user)
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
