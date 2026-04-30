"""
Property enrichment lookups.

Currently implements:
- Luzerne back-tax lookup via Elite Revenue Tri-Search.
  Returns total amount owed, real address, and years owed.
  IMPORTANT: when the property is in repository, back taxes are exonerated
  upon deed recording — so for `luzerne_tax_repo` the value is informational
  (signals distress depth) rather than a cost the buyer absorbs.

For judicial-sale lots (luzerne_sheriff, lackawanna_judicial), the
listing_price is already the upset bid; this lookup adds context.
"""

from __future__ import annotations

import json
import re
import sqlite3
from typing import Sequence

import httpx

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 OffmarketBot/1.0"

ELITE_URL_TPL = "https://eliterevenue.rba.com/taxes/luzerne/trirsp2pp.asp?parcel={parcel}&currentlist=0"

_TOTAL_DUE_RE = re.compile(
    r"Taxes of\s*\$([\d,]+\.\d{2})", re.IGNORECASE
)
_ADDRESS_RE = re.compile(
    r"<td[^>]*>\s*(\d{1,5}\s+[A-Z][^<]{4,80})\s*</td>", re.IGNORECASE
)
_YEAR_RE = re.compile(r"Year\s+(\d{4})", re.IGNORECASE)


def lookup_luzerne(parcel_id: str, timeout: int = 25) -> dict | None:
    if not parcel_id:
        return None
    url = ELITE_URL_TPL.format(parcel=parcel_id)
    try:
        r = httpx.get(url, headers={"User-Agent": UA}, follow_redirects=True, timeout=timeout)
        r.raise_for_status()
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}", "url": url}

    html = r.text
    total_due = None
    m = _TOTAL_DUE_RE.search(html)
    if m:
        try:
            total_due = float(m.group(1).replace(",", ""))
        except ValueError:
            pass

    real_address = None
    addr_m = _ADDRESS_RE.search(html)
    if addr_m:
        real_address = addr_m.group(1).strip()

    years = sorted({int(y) for y in _YEAR_RE.findall(html)})

    return {
        "back_taxes": total_due,
        "real_address": real_address,
        "years_owed": years,
        "url": url,
    }


def enrich_rows(conn: sqlite3.Connection, rows: Sequence[sqlite3.Row], max_lookups: int = 30) -> int:
    """Enrich properties with back-tax + real-address. Returns count of new lookups."""
    new = 0
    for row in rows:
        if new >= max_lookups:
            break
        if row["county"] != "luzerne" or not row["parcel_id"]:
            continue
        ptype = (row["property_type"] or "").lower()
        if ptype in {"mobile_home", "unbuildable"}:
            continue

        existing = {}
        if row["raw"]:
            try:
                existing = json.loads(row["raw"]) if isinstance(row["raw"], str) else dict(row["raw"])
            except (json.JSONDecodeError, TypeError):
                existing = {}
        if "back_taxes" in existing or existing.get("enrich_attempted"):
            continue

        result = lookup_luzerne(row["parcel_id"])
        if not result:
            continue

        existing["enrich_attempted"] = True
        existing["back_taxes"] = result.get("back_taxes")
        existing["real_address"] = result.get("real_address")
        existing["years_owed"] = result.get("years_owed")
        existing["enrich_url"] = result.get("url")
        if result.get("error"):
            existing["enrich_error"] = result["error"]

        conn.execute(
            "UPDATE properties SET raw = ? WHERE id = ?",
            (json.dumps(existing, default=str), row["id"]),
        )
        new += 1
        bt = result.get("back_taxes")
        addr = result.get("real_address") or "?"
        print(
            f"[enrich] {row['parcel_id']} -> "
            f"back_taxes={'$%.2f' % bt if bt else 'n/a'} "
            f"addr={addr[:50]}"
        )
    conn.commit()
    return new
