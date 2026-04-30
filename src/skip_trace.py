"""
Skip-trace via TruePeopleSearch (free, no API).

Strategy:
- Cache results in `skip_trace` table keyed by (owner_name, city)
- Rate-limited: 1 lookup every 35 seconds
- Only run for properties above NOTIFY threshold to limit volume
- Graceful degradation: failures logged, deals still notified without phone

Owner name format from PDF is "LASTNAME, FIRSTNAME M." or "LASTNAME FIRSTNAME".
"""

from __future__ import annotations

import json
import re
import sqlite3
import time
import random
from typing import Sequence

import httpx
from selectolax.parser import HTMLParser

UA_POOL = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

RATE_LIMIT_SECONDS = 35


def _split_owner(owner_name: str) -> tuple[str, str] | None:
    """Returns (first_name, last_name) or None if cannot parse."""
    if not owner_name:
        return None
    s = owner_name.strip()
    if "," in s:
        parts = [p.strip() for p in s.split(",", 1)]
        if len(parts) == 2 and parts[0] and parts[1]:
            last = parts[0]
            first = parts[1].split()[0] if parts[1].split() else ""
            if first and len(last) >= 2:
                return first, last
    tokens = s.split()
    if len(tokens) >= 2:
        return tokens[1], tokens[0]
    return None


_PHONE_RE = re.compile(r"\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}")
_AGE_RE = re.compile(r"Age\s+(\d{1,3})", re.IGNORECASE)


def _extract_results(html: str) -> dict:
    tree = HTMLParser(html)
    phones: list[str] = []

    for node in tree.css("a[href^='tel:']"):
        href = node.attributes.get("href", "") or ""
        phone = href.replace("tel:", "").strip()
        if phone and phone not in phones:
            phones.append(phone)

    if not phones:
        for m in _PHONE_RE.finditer(html):
            p = m.group(0).strip()
            if p not in phones:
                phones.append(p)

    age = None
    age_m = _AGE_RE.search(html)
    if age_m:
        age = age_m.group(1)

    addr = None
    addr_node = tree.css_first(".content-value a[href*='address']")
    if addr_node:
        addr = (addr_node.text() or "").strip()

    return {
        "phones": phones[:5],
        "age": age,
        "current_address": addr,
    }


def _try_url(url: str) -> dict | None:
    ua = random.choice(UA_POOL)
    try:
        with httpx.Client(
            headers={
                "User-Agent": ua,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
            },
            follow_redirects=True,
            timeout=30,
        ) as c:
            r = c.get(url)
            if r.status_code != 200:
                return {"error": f"HTTP {r.status_code}", "url": url}
            return {**_extract_results(r.text), "url": url, "html_len": len(r.text)}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}", "url": url}


def lookup(owner_name: str, city: str | None = None) -> dict | None:
    parts = _split_owner(owner_name)
    if not parts:
        return None
    first, last = parts

    qs_name = f"{first}+{last}".replace(" ", "+")
    loc_suffix = ""
    if city:
        loc_suffix = f"&citystatezip={city.replace(' ', '+')}+PA"

    primary = f"https://www.truepeoplesearch.com/results?name={qs_name}{loc_suffix}"
    result = _try_url(primary)
    if result and (result.get("phones") or not result.get("error")):
        return result

    fallback = (
        f"https://www.fastpeoplesearch.com/name/{first.lower()}-{last.lower()}_pa"
        if not city
        else f"https://www.fastpeoplesearch.com/name/{first.lower()}-{last.lower()}_{city.lower().replace(' ', '-')}-pa"
    )
    fb_result = _try_url(fallback)
    if fb_result and fb_result.get("phones"):
        fb_result["fallback"] = "fastpeoplesearch"
        return fb_result

    return result or fb_result


def get_cached(conn: sqlite3.Connection, owner_name: str, city: str | None) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM skip_trace WHERE owner_name = ? AND COALESCE(city,'') = COALESCE(?,'')",
        (owner_name, city),
    ).fetchone()


def save(conn: sqlite3.Connection, owner_name: str, city: str | None, result: dict) -> None:
    phones = json.dumps(result.get("phones") or [])
    conn.execute(
        """INSERT INTO skip_trace (owner_name, city, phones, age, current_address, raw)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(owner_name, city) DO UPDATE SET
             phones = excluded.phones,
             age = excluded.age,
             current_address = excluded.current_address,
             raw = excluded.raw,
             looked_up_at = CURRENT_TIMESTAMP""",
        (owner_name, city, phones, result.get("age"), result.get("current_address"), json.dumps(result, default=str)),
    )
    conn.commit()


def enrich_rows(conn: sqlite3.Connection, rows: Sequence[sqlite3.Row], max_lookups: int = 20) -> int:
    """For each row in the notify queue, ensure skip_trace cache is populated.
    Returns count of NEW lookups performed (excluding cache hits)."""
    new_lookups = 0
    for row in rows:
        if new_lookups >= max_lookups:
            break
        owner = row["owner_name"]
        city = row["city"]
        if not owner:
            continue
        if get_cached(conn, owner, city):
            continue

        result = lookup(owner, city)
        if not result:
            continue
        save(conn, owner, city, result)
        new_lookups += 1
        phones = result.get("phones") or []
        print(
            f"[skip-trace] {owner} ({city or '?'}) -> "
            f"{len(phones)} phone(s){' err='+result['error'] if result.get('error') else ''}"
        )
        time.sleep(RATE_LIMIT_SECONDS + random.uniform(0, 5))
    return new_lookups
