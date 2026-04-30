"""
Deal scoring engine. Pure rules in Sprint 1; LLM scoring added in Sprint 2.

Score 0-100. Hard filter: returns -1 if outside target counties.
Notification threshold (in main.py): score >= 40.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, date

TARGET_COUNTIES = {"luzerne", "lackawanna"}

TOP_CITIES = {
    "wilkes-barre", "wilkes barre", "hazleton", "nanticoke", "kingston", "pittston",
    "scranton",
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
    "luzerne_delinquent": 50,
    "luzerne_tax_repo": 35,
    "luzerne_sheriff": 20,
    "lackawanna_judicial": 30,
    "craigslist_scranton": 10,
}

OWNER_BLACKLIST_PATTERNS = [
    "utilities", "water co", "water company", "electric co", "gas co",
    "telephone", "communications", "verizon", "ppl", "ppl electric",
    "school district", "borough of", "township of", "city of",
    "county of", "commonwealth of pa", "authority", "redevelopment",
    "conservancy", "land trust", "foundation",
    "church", "diocese", "parish", "cemetery",
    "post office", "u s postal", "usps",
    "railroad", "rail co", "rr co",
    "department of", "state of pennsylvania",
    "homeowner association", "hoa", "community association",
    "resort co", "resort association",
]


def score_property(row: sqlite3.Row | dict) -> tuple[int, list[str]]:
    """Return (score, list_of_reasons). Score = -1 means filtered out (don't notify)."""
    get = row.get if isinstance(row, dict) else (lambda k, d=None: row[k] if k in row.keys() else d)

    county = (get("county", "") or "").lower()
    if county not in TARGET_COUNTIES:
        return -1, [f"outside target counties (county={county!r})"]

    owner = (get("owner_name", "") or "").lower()
    for pat in OWNER_BLACKLIST_PATTERNS:
        if pat in owner:
            return -1, [f"owner blacklist match: {pat!r}"]

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

    ptype = (get("property_type") or "").lower()
    if ptype == "unbuildable":
        return -1, reasons + [f"property_type=unbuildable (filter)"]
    if ptype == "structure_multi":
        score += 20
        reasons.append("type:multi-family +20")
    elif ptype == "structure_sfh":
        score += 12
        reasons.append("type:single-family +12")
    elif ptype == "mobile_home":
        score -= 5
        reasons.append("type:mobile_home -5")
    elif ptype == "vacant_lot":
        score -= 8
        reasons.append("type:vacant_lot -8")

    city = (get("city", "") or "").lower()
    if any(tc in city for tc in TOP_CITIES):
        score += 10
        reasons.append(f"top-city:{city} +10")

    raw_str = get("raw") or ""
    raw = {}
    if raw_str:
        try:
            raw = json.loads(raw_str) if isinstance(raw_str, str) else raw_str
        except (json.JSONDecodeError, TypeError):
            raw = {}

    if isinstance(raw, dict):
        status = (raw.get("status") or "").upper()
        if "NO BID" in status:
            score += 12
            reasons.append("status:NO BID (next-up for repo) +12")
        elif "CONTINUED" in status:
            score += 5
            reasons.append("status:CONTINUED (live) +5")

        if raw.get("is_out_of_state"):
            score += 18
            reasons.append("owner OUT-OF-STATE +18")
        elif raw.get("is_absentee"):
            score += 10
            reasons.append("owner ABSENTEE +10")

    if isinstance(raw, dict):
        jsd = raw.get("judicial_sale_date")
        if jsd:
            try:
                sale_dt = datetime.strptime(jsd, "%m/%d/%Y").date()
                years_in_repo = (date.today() - sale_dt).days / 365.25
                if years_in_repo <= 2:
                    score += 15
                    reasons.append(f"fresh repo entry ({years_in_repo:.1f}y) +15")
                elif years_in_repo <= 5:
                    score += 5
                    reasons.append(f"recent repo entry ({years_in_repo:.1f}y) +5")
                elif years_in_repo > 10:
                    score -= 15
                    reasons.append(f"stale repo entry ({years_in_repo:.0f}y) -15")
                elif years_in_repo > 5:
                    score -= 8
                    reasons.append(f"older repo entry ({years_in_repo:.0f}y) -8")
            except ValueError:
                pass

    score = max(-1, min(100, score))
    return score, reasons


def score_all_unscored(conn: sqlite3.Connection) -> int:
    """Score every property where score_reasons IS NULL. Returns count scored."""
    conn.row_factory = sqlite3.Row
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
