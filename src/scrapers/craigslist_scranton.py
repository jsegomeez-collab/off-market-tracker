"""
Craigslist Scranton (covers all NEPA) - Real Estate by Owner.

Filter only listings whose location matches Luzerne County municipalities.
Session warming required: Craigslist 403s without prior cookie set.
"""

from __future__ import annotations

import re
import httpx
from selectolax.parser import HTMLParser
from ..models import Property

BASE = "https://scranton.craigslist.org"
SEARCH_URL = f"{BASE}/search/rea?purveyor=owner"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15"
)

LUZERNE_CITIES = {
    "wilkes-barre", "wilkes barre", "hazleton", "nanticoke", "kingston",
    "pittston", "plymouth", "ashley", "edwardsville", "luzerne",
    "duryea", "swoyersville", "wyoming", "exeter", "west pittston",
    "forty fort", "courtdale", "larksville", "white haven", "freeland",
    "shickshinny", "harveys lake", "mountain top", "dallas", "back mountain",
    "sugarloaf", "drums", "weatherly", "conyngham", "hanover township",
    "wright township", "fairview township", "butler township",
}

LACKAWANNA_CITIES = {
    "scranton", "carbondale", "archbald", "blakely", "dickson city",
    "dunmore", "old forge", "taylor", "throop", "moosic",
    "mayfield", "olyphant", "jermyn", "jessup", "vandling",
    "moscow", "dalton", "clarks summit", "clarks green",
    "waverly", "factoryville",
}


def _city_match(location_text: str) -> tuple[str, str] | None:
    if not location_text:
        return None
    t = location_text.lower().strip()
    for city in LUZERNE_CITIES:
        if city in t:
            return city.title(), "luzerne"
    for city in LACKAWANNA_CITIES:
        if city in t:
            return city.title(), "lackawanna"
    return None


def _parse_price(text: str | None) -> float | None:
    if not text:
        return None
    m = re.search(r"\$?([\d,]+)", text)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def scrape() -> list[Property]:
    out: list[Property] = []
    with httpx.Client(
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
        timeout=30,
    ) as c:
        c.get(f"{BASE}/")
        r = c.get(SEARCH_URL, headers={"Referer": f"{BASE}/"})
        r.raise_for_status()

    tree = HTMLParser(r.text)
    for li in tree.css("li.cl-static-search-result"):
        title = li.attributes.get("title", "") or ""
        a = li.css_first("a")
        href = a.attributes.get("href", "") if a else ""
        price_node = li.css_first(".price")
        loc_node = li.css_first(".location")
        price = _parse_price(price_node.text()) if price_node else None
        location = (loc_node.text() or "").strip() if loc_node else ""

        match = _city_match(location)
        if not match:
            continue
        city_name, county_name = match

        post_id_m = re.search(r"/(\d+)\.html", href)
        post_id = post_id_m.group(1) if post_id_m else href

        out.append(
            Property(
                source="craigslist_scranton",
                source_id=f"cl-{post_id}",
                address=title[:200],
                city=city_name,
                county=county_name,
                listing_price=price,
                property_type="fsbo",
                description=title,
                url=href,
                raw={"title": title, "location": location, "price_raw": price_node.text() if price_node else None},
            )
        )
    return out


if __name__ == "__main__":
    props = scrape()
    print(f"Found {len(props)} Luzerne FSBO listings on Craigslist")
    for p in props[:10]:
        print(f"  ${p.listing_price} | {p.city} | {p.address[:70]}")
