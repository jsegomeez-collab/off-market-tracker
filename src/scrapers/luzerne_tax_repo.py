"""
Luzerne County Tax Claim - Repository List scraper.

Source: https://luzernecountytaxclaim.com/repository/
Format: PDF, updated periodically. Repository = properties unsold at Judicial Sale,
purchasable for the minimum bid (usually $800-$3,000) without notice.
"""

from __future__ import annotations

import io
import re
import httpx
import pdfplumber
from selectolax.parser import HTMLParser
from ..models import Property

REPO_PAGE = "https://luzernecountytaxclaim.com/repository/"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 OffmarketBot/1.0"


def _find_pdf_url() -> str | None:
    r = httpx.get(REPO_PAGE, headers={"User-Agent": USER_AGENT}, follow_redirects=True, timeout=30)
    r.raise_for_status()
    tree = HTMLParser(r.text)
    candidates: list[tuple[int, str]] = []
    for a in tree.css("a"):
        href = (a.attributes.get("href", "") or "").strip()
        text = (a.text() or "").strip().lower()
        if not href.lower().endswith(".pdf"):
            continue
        h_lower = href.lower()
        score = 0
        if "list" in h_lower or "list" in text or "listing" in text:
            score += 10
        if any(bad in h_lower for bad in ("form", "policy", "salesform", "bid_form", "bidform")):
            score -= 20
        if any(bad in text for bad in ("policy", "bid form")):
            score -= 20
        if "repository" in h_lower or "repository" in text:
            score += 5
        if score > 0:
            full = href if href.startswith("http") else (
                "https://luzernecountytaxclaim.com" + href if href.startswith("/") else REPO_PAGE + href
            )
            candidates.append((score, full))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


_PARCEL_RE = re.compile(r"\b\d{2}-[A-Z0-9]+-[A-Z0-9]+-[A-Z0-9]+(?:-\d+)?\b")
_MONEY_RE = re.compile(r"\$\s*([\d,]+(?:\.\d{2})?)")
_DATE_RE = re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b")

_MUNICIPALITIES = [
    "wilkes-barre city", "wilkes-barre township", "wilkes-barre",
    "hazleton city", "hazleton",
    "hazle township", "west hazleton",
    "nanticoke", "kingston", "pittston city", "pittston township", "pittston",
    "ashley borough", "ashley",
    "plymouth borough", "plymouth township", "plymouth",
    "edwardsville", "luzerne borough", "duryea", "swoyersville",
    "wyoming borough", "wyoming", "exeter borough", "exeter",
    "west pittston", "forty fort", "courtdale", "larksville",
    "white haven", "freeland", "shickshinny", "harveys lake",
    "mountain top", "dallas borough", "dallas township", "dallas",
    "back mountain", "sugarloaf", "drums", "weatherly", "conyngham",
    "hanover township", "wright township", "fairview township", "butler township",
    "bear creek township", "buck township", "black creek township",
    "avoca borough", "avoca", "dupont", "duryea",
    "jenkins township", "jeddo borough", "laflin",
    "newport township", "rice township", "salem township",
    "sugar notch", "warrior run", "yatesville",
]


def _classify_city(text: str) -> str | None:
    t = text.lower()
    for m in _MUNICIPALITIES:
        if m in t:
            return m.title()
    return None


def _parse_line(line: str, source_url: str) -> Property | None:
    m = _PARCEL_RE.search(line)
    if not m:
        return None
    parcel = m.group(0)

    p_start, p_end = m.span()
    owner = line[:p_start].strip(" \t|-")

    after = line[p_end:].strip()
    money_match = _MONEY_RE.search(after)
    assessed_value: float | None = None
    if money_match:
        try:
            assessed_value = float(money_match.group(1).replace(",", ""))
        except ValueError:
            assessed_value = None
        after_money = after[money_match.end():].strip()
    else:
        after_money = after

    after_no_date = _DATE_RE.sub("", after_money).strip()

    city = _classify_city(after_no_date)
    if city:
        idx = after_no_date.lower().rfind(city.lower())
        address = after_no_date[:idx].strip(" \t,-") if idx > 0 else after_no_date
    else:
        address = after_no_date

    if not owner or len(owner) < 2:
        return None

    return Property(
        source="luzerne_tax_repo",
        source_id=parcel,
        parcel_id=parcel,
        address=address[:200] or parcel,
        city=city,
        county="luzerne",
        owner_name=owner[:200],
        listing_price=assessed_value,
        property_type="repository",
        description=line.strip(),
        url=source_url,
        raw={"line": line.strip(), "pdf": source_url, "assessed_value": assessed_value},
    )


def _parse_pdf_lines(pdf_bytes: bytes, source_url: str) -> list[Property]:
    out: list[Property] = []
    seen: set[str] = set()
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for line in text.split("\n"):
                prop = _parse_line(line, source_url)
                if not prop or prop.parcel_id in seen:
                    continue
                seen.add(prop.parcel_id)
                out.append(prop)
    return out


def scrape() -> list[Property]:
    pdf_url = _find_pdf_url()
    if not pdf_url:
        return []
    r = httpx.get(pdf_url, headers={"User-Agent": USER_AGENT}, follow_redirects=True, timeout=60)
    r.raise_for_status()
    return _parse_pdf_lines(r.content, pdf_url)


if __name__ == "__main__":
    props = scrape()
    print(f"Found {len(props)} repository properties")
    for p in props[:5]:
        print(f"  {p.parcel_id} | {p.city} | ${p.listing_price} | {p.address[:60]}")
