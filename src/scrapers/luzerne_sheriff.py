"""
Luzerne County Sheriff Sales scraper.

Source: https://sheriffsale.luzernecounty.org/Sheriff.Salelisting/
Sales: 1st Friday of every other month at 10:30am.
The portal can be flaky; this scraper retries and degrades gracefully.
"""

from __future__ import annotations

import re
import httpx
from selectolax.parser import HTMLParser
from ..models import Property

PORTAL = "https://sheriffsale.luzernecounty.org/Sheriff.Salelisting/"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15"
)


def _fetch(url: str, retries: int = 3) -> str | None:
    last_err = None
    for attempt in range(retries):
        try:
            with httpx.Client(
                headers={"User-Agent": USER_AGENT},
                follow_redirects=True,
                timeout=45,
                verify=False,
            ) as c:
                r = c.get(url)
                r.raise_for_status()
                return r.text
        except Exception as e:
            last_err = e
    print(f"[luzerne_sheriff] failed after {retries} attempts: {last_err}")
    return None


_ADDRESS_RE = re.compile(
    r"\d+\s+[A-Z][\w\s\.\-]+(?:STREET|ST|AVENUE|AVE|ROAD|RD|LANE|LN|DRIVE|DR|BOULEVARD|BLVD|COURT|CT|PLACE|PL|WAY|TERRACE|TER)\b",
    re.IGNORECASE,
)


def scrape() -> list[Property]:
    html = _fetch(PORTAL)
    if not html:
        return []

    out: list[Property] = []
    tree = HTMLParser(html)

    rows = tree.css("table tr") or tree.css("div.sale-listing") or tree.css(".property-row")
    for idx, row in enumerate(rows):
        text = row.text(separator=" ", strip=True)
        if not text or len(text) < 20:
            continue

        addr_m = _ADDRESS_RE.search(text)
        if not addr_m:
            continue
        address = addr_m.group(0)

        link = row.css_first("a")
        href = link.attributes.get("href", "") if link else ""
        if href and not href.startswith("http"):
            href = PORTAL.rstrip("/") + "/" + href.lstrip("/")

        sale_id_m = re.search(r"(?:sale|case|judgment)[\s#:]*([\w\-]+)", text, re.IGNORECASE)
        sale_id = sale_id_m.group(1) if sale_id_m else f"row-{idx}"

        judgment = None
        money_m = re.search(r"\$([\d,]+\.\d{2})", text)
        if money_m:
            try:
                judgment = float(money_m.group(1).replace(",", ""))
            except ValueError:
                pass

        out.append(
            Property(
                source="luzerne_sheriff",
                source_id=f"sh-{sale_id}",
                address=address,
                county="luzerne",
                listing_price=judgment,
                property_type="sheriff_sale",
                description=text[:500],
                url=href or PORTAL,
                raw={"text": text[:1000]},
            )
        )
    return out


if __name__ == "__main__":
    props = scrape()
    print(f"Found {len(props)} sheriff sale properties")
    for p in props[:5]:
        print(f"  {p.address} | judgment: ${p.listing_price}")
