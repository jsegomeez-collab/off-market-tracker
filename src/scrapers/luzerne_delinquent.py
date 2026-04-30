"""
Luzerne County — currently tax-delinquent properties (Elite Revenue Tri-Search).

This is the GOLDMINE source: properties whose owners are STILL in tax-claim
process (BEFORE judicial sale). Owner still owns the property and has time
pressure — perfect motivated-seller targets for direct outreach.

Approach:
- Loop surnames A-Z (Tri-Search requires a surname filter)
- Each search returns hundreds of rows: parcel | owner | property address
- Each property is upserted; enrich phase later adds back-taxes + mailing
"""

from __future__ import annotations

import re
import time
import httpx
from selectolax.parser import HTMLParser
from ..models import Property
from ..classify import classify

BASE = "https://eliterevenue.rba.com/taxes/luzerne"
SEARCH_URL_TPL = f"{BASE}/trirsp1.asp?surname={{letter}}"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 OffmarketBot/1.0"

_PARCEL_RE = re.compile(r"^\d{2}-[A-Z0-9]+\s*-[A-Z0-9]+-[A-Z0-9]+(?:-\d+)?$")

LUZERNE_MUNICIPALITIES = [
    "wilkes-barre city", "wilkes-barre", "hazleton city", "hazleton",
    "nanticoke", "kingston", "pittston city", "pittston",
    "ashley borough", "plymouth borough", "plymouth township",
    "edwardsville", "luzerne borough", "duryea", "swoyersville",
    "wyoming", "exeter", "west pittston", "forty fort", "courtdale",
    "larksville", "white haven", "freeland", "shickshinny",
    "harveys lake", "mountain top", "dallas", "back mountain",
    "sugarloaf", "drums", "weatherly", "conyngham",
    "hanover township", "wright township", "fairview township",
    "butler township", "avoca borough", "avoca", "dupont",
    "jenkins township", "jeddo borough", "laflin",
    "newport township", "rice township", "salem township",
    "sugar notch", "warrior run", "yatesville",
    "hazle township", "west hazleton",
]


def _classify_city(text: str) -> str | None:
    if not text:
        return None
    t = text.lower()
    for m in LUZERNE_MUNICIPALITIES:
        if m in t:
            return m.title()
    return None


def _parse_results_html(html: str) -> list[Property]:
    out: list[Property] = []
    seen: set[str] = set()
    tree = HTMLParser(html)

    for tr in tree.css("tr"):
        text = tr.text(separator="|", strip=True) or ""
        if not text:
            continue
        cells = [c.strip() for c in text.split("|") if c.strip()]
        if len(cells) < 2:
            continue

        parcel = None
        for c in cells:
            if _PARCEL_RE.match(c):
                parcel = c
                break
        if not parcel or parcel in seen:
            continue
        seen.add(parcel)

        idx = cells.index(parcel)
        owner = cells[idx + 1] if idx + 1 < len(cells) else None
        addr = cells[idx + 2] if idx + 2 < len(cells) else None
        if not owner:
            continue
        if owner.upper().startswith(("OWNER", "PARCEL", "ADDRESS", "LOCATION")):
            continue

        ptype = classify(addr or "", addr, parcel)
        city = _classify_city(addr or "")

        out.append(
            Property(
                source="luzerne_delinquent",
                source_id=parcel,
                parcel_id=parcel,
                address=addr or parcel,
                city=city,
                county="luzerne",
                owner_name=owner[:200],
                listing_price=None,
                property_type=ptype,
                description=f"Currently tax-delinquent. Owner: {owner}. Property: {addr or '?'}",
                url=f"{BASE}/trirsp2pp.asp?parcel={parcel}&currentlist=0",
                raw={
                    "parcel": parcel,
                    "owner_pdf": owner,
                    "addr_raw": addr,
                    "list_source": "tri_search_delinquent",
                },
            )
        )
    return out


def scrape() -> list[Property]:
    all_props: dict[str, Property] = {}
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    with httpx.Client(
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
        timeout=60,
    ) as c:
        for letter in letters:
            url = SEARCH_URL_TPL.format(letter=letter)
            try:
                r = c.get(url)
                r.raise_for_status()
                props = _parse_results_html(r.text)
                for p in props:
                    if p.source_id not in all_props:
                        all_props[p.source_id] = p
                print(f"[luzerne_delinquent] surname={letter}: {len(props)} parcels (total uniq: {len(all_props)})")
            except Exception as e:
                print(f"[luzerne_delinquent] surname={letter} failed: {e}")
            time.sleep(1.0)
    return list(all_props.values())


if __name__ == "__main__":
    props = scrape()
    print(f"\nTotal unique delinquent parcels: {len(props)}")
    for p in props[:5]:
        print(f"  {p.parcel_id} | {p.owner_name[:40]:<40} | {p.address[:40]}")
