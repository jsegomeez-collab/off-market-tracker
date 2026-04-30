"""
Lackawanna County Tax Claim - Judicial Sale Advertising List.

Format: PDF on cms8.revize.com (Lackawanna's CMS).
Each row: #N | CV# | OWNER | MAP_NUMBER | ADDRESS | $AMOUNT | *STATUS*

Status semantics:
  *NO BID*       -> went unsold at sale, headed to repository (BUYABLE next sale)
  *CONTINUED*    -> rolled to a future sale date (LIVE)
  *SOLD AT X.Y*  -> already sold (FILTER OUT)
  no marker      -> still scheduled / open

The county's main page is firewall-protected from scrapers, so we hardcode
the PDF URL and let it 404 gracefully if it rotates — the orchestrator
keeps running the other sources.
"""

from __future__ import annotations

import io
import re
import httpx
import pdfplumber
from ..models import Property
from ..classify import classify

PDF_URL = (
    "https://cms8.revize.com/revize/lackawanna/Document_center/Department/"
    "Tax%20Claim/Copy%20of%202025%20Judicial%20list%20(WEBSITE)%205.14.pdf"
)

USER_AGENT = "Mozilla/5.0 OffmarketBot/1.0"

LACKAWANNA_MUNICIPALITIES = [
    "scranton city", "scranton",
    "carbondale city", "carbondale",
    "archbald borough", "archbald",
    "blakely borough", "blakely",
    "dickson city",
    "dunmore borough", "dunmore",
    "old forge borough", "old forge",
    "taylor borough", "taylor",
    "throop borough", "throop",
    "moosic borough", "moosic",
    "mayfield borough", "mayfield",
    "olyphant borough", "olyphant",
    "jermyn borough", "jermyn",
    "jessup borough", "jessup",
    "vandling borough", "vandling",
    "moscow borough", "moscow",
    "clifton township", "covington township", "elmhurst township",
    "fell township", "glenburn township", "greenfield township",
    "jefferson township", "lackawanna township", "lehigh township",
    "madison township", "newton township", "north abington township",
    "ransom township", "roaring brook township", "scott township",
    "south abington township", "spring brook township", "thornhurst township",
    "waverly township", "west abington township",
    "dalton borough", "dalton", "clarks summit borough", "clarks summit",
    "clarks green borough",
]


_PARCEL_RE = re.compile(r"\b\d{5}-\d{3}-\d{3,5}\b")
_MONEY_RE = re.compile(r"\$\s*([\d,]+\.\d{2})")
_STATUS_RE = re.compile(r"\*([^*]+)\*")
_CV_RE = re.compile(r"\b(\d{2}-CV-\d{3,5})\b")
_SALE_NUM_RE = re.compile(r"^#(\d+)\s+")


def _classify_city(text: str) -> str | None:
    t = text.lower()
    for m in LACKAWANNA_MUNICIPALITIES:
        if m in t:
            return m.title()
    return None


def _parse_pdf_lines(pdf_bytes: bytes, source_url: str) -> list[Property]:
    out: list[Property] = []
    seen: set[str] = set()
    current_municipality: str | None = None
    municipality_header_re = re.compile(
        r"^\s*(" + "|".join(re.escape(m.upper()) for m in LACKAWANNA_MUNICIPALITIES) + r")\s*$",
        re.IGNORECASE,
    )

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for raw_line in text.split("\n"):
                line = raw_line.strip()
                if not line:
                    continue

                muni_m = municipality_header_re.match(line)
                if muni_m:
                    current_municipality = muni_m.group(1).title()
                    continue

                parcel_m = _PARCEL_RE.search(line)
                if not parcel_m:
                    continue

                parcel = parcel_m.group(0)
                if parcel in seen:
                    continue

                status_m = _STATUS_RE.search(line)
                status = status_m.group(1).strip() if status_m else "OPEN"
                if "SOLD AT" in status.upper():
                    continue

                cv_m = _CV_RE.search(line)
                cv_id = cv_m.group(1) if cv_m else parcel

                sale_num_m = _SALE_NUM_RE.match(line)
                sale_num = sale_num_m.group(1) if sale_num_m else ""

                between = line[: parcel_m.start()]
                between = _SALE_NUM_RE.sub("", between)
                between = _CV_RE.sub("", between)
                owner = between.strip(" \t|-")

                after_parcel = line[parcel_m.end():]
                money_match = _MONEY_RE.search(after_parcel)
                bid: float | None = None
                if money_match:
                    try:
                        bid = float(money_match.group(1).replace(",", ""))
                    except ValueError:
                        pass
                    addr_part = after_parcel[: money_match.start()].strip()
                else:
                    addr_part = after_parcel.strip()
                addr_part = _STATUS_RE.sub("", addr_part).strip(" \t|-")

                seen.add(parcel)
                ptype = classify(line, addr_part, parcel)

                out.append(
                    Property(
                        source="lackawanna_judicial",
                        source_id=f"lack-{cv_id}",
                        parcel_id=parcel,
                        address=addr_part[:200] or parcel,
                        city=current_municipality,
                        county="lackawanna",
                        owner_name=owner[:200] if owner else None,
                        listing_price=bid,
                        property_type=ptype,
                        description=line,
                        url=source_url,
                        raw={
                            "line": line,
                            "pdf": source_url,
                            "cv_number": cv_id,
                            "sale_number": sale_num,
                            "status": status,
                            "list_source": "judicial_sale",
                        },
                    )
                )
    return out


def scrape() -> list[Property]:
    try:
        r = httpx.get(
            PDF_URL,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
            timeout=60,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"[lackawanna_judicial] fetch failed: {e}")
        return []
    return _parse_pdf_lines(r.content, PDF_URL)


if __name__ == "__main__":
    props = scrape()
    print(f"Found {len(props)} Lackawanna judicial sale properties")
    for p in props[:8]:
        st = p.raw.get("status", "?")
        print(f"  [{st[:18]:<18}] {p.parcel_id} | {p.city} | ${p.listing_price} | {p.owner_name[:30] if p.owner_name else '-':<30} | {p.address[:40]}")
