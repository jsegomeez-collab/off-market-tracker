"""
Deal scoring engine. Pure rules in Sprint 1; LLM scoring added in Sprint 2.

Score 0-100. Hard filter: returns -1 if outside target counties.
Notification threshold (in main.py): score >= 40.
"""

from __future__ import annotations

import sqlite3

TARGET_COUNTIES = {"luzerne"}

TOP_CITIES_LUZERNE = {
    "wilkes-barre", "wilkes barre", "hazleton", "nanticoke", "kingston", "pittston",
}

DISTRESS_KEYWORDS = [
    "estate", "as-is", "as is", "motivated", "must sell", "executor",
    "inherited", "fixer", "handyman", "tax sale", "cash only",
    "investor special", "tlc", "needs work", "distressed", "foreclosure",
    "vacant", "abandoned", "behind on taxes", "out of state",
    "seller financing", "owner financing", "rent to own",
]

MULTI_UNIT_KEYWORDS = [
    "multi", "duplex", "triplex", "fourplex", "2 unit", "3 unit", "4 unit",
    "2-unit", "3-unit", "4-unit", "two unit", "three unit", "two-family",
    "three-family", "multi-family", "multifamily", "mixed use", "mixed-use",
]

SOURCE_BASE_SCORE = {
    "luzerne_tax_repo": 35,
    "luzerne_sheriff": 20,
    "craigslist_scranton": 10,
}


def score_property(row: sqlite3.Row | dict) -> tuple[int, list[str]]:
    """Return (score, list_of_reasons). Score = -1 means filtered out (don't notify)."""
    get = row.get if isinstance(row, dict) else (lambda k, d=None: row[k] if k in row.keys() else d)

    county = (get("county", "") or "").lower()
    if county not in TARGET_COUNTIES:
        return -1, [f"outside target counties (county={county!r})"]

    score = 0
    reasons: list[str] = []

    source = get("source", "") or ""
    if source in SOURCE_BASE_SCORE:
        s = SOURCE_BASE_SCORE[source]
        score += s
        reasons.append(f"source:{source} +{s}")

    price = get("listing_price")
    if price is not None:
        try:
            price = float(price)
        except (TypeError, ValueError):
            price = None
    if price is not None:
        if price <= 5_000:
            score += 30
            reasons.append(f"price ${price:.0f} <= $5k +30")
        elif price <= 30_000:
            score += 20
            reasons.append(f"price ${price:.0f} <= $30k +20")
        elif price <= 70_000:
            score += 10
            reasons.append(f"price ${price:.0f} <= $70k +10")
        elif price > 300_000:
            score -= 10
            reasons.append(f"price ${price:.0f} > $300k -10")

    text_blob = " ".join(
        str(get(k) or "") for k in ("description", "address", "property_type")
    ).lower()

    distress_hits = [k for k in DISTRESS_KEYWORDS if k in text_blob]
    if distress_hits:
        bonus = min(20, 5 * len(distress_hits))
        score += bonus
        reasons.append(f"distress({','.join(distress_hits[:3])}) +{bonus}")

    multi_hits = [k for k in MULTI_UNIT_KEYWORDS if k in text_blob]
    if multi_hits:
        score += 15
        reasons.append(f"multi-unit({multi_hits[0]}) +15")

    city = (get("city", "") or "").lower()
    if any(tc in city for tc in TOP_CITIES_LUZERNE):
        score += 10
        reasons.append(f"top-city:{city} +10")

    score = max(-1, min(100, score))
    return score, reasons


def score_all_unscored(conn: sqlite3.Connection) -> int:
    """Score every property where score_reasons IS NULL. Returns count scored."""
    import json

    rows = conn.execute(
        "SELECT * FROM properties WHERE score_reasons IS NULL"
    ).fetchall()
    n = 0
    for row in rows:
        s, reasons = score_property(row)
        conn.execute(
            "UPDATE properties SET score = ?, score_reasons = ? WHERE id = ?",
            (s, json.dumps(reasons), row["id"]),
        )
        n += 1
    conn.commit()
    return n
