"""
Heuristic property-type classifier from text descriptions.

Used to filter out unbuildable lots and prioritize structures
(single-family / duplex / multi-family) that wholesalers actually want.

Codes:
  unbuildable   - DO NOT NOTIFY (common driveway, landlocked, deeded easement)
  vacant_lot    - lot only, possibly buildable
  mobile_home   - mobile home or trailer in MHP
  structure_sfh - has structure, single-family likely
  structure_multi - duplex / triplex / multi-unit
  unknown       - cannot classify
"""

from __future__ import annotations

import re

UNBUILDABLE_TOKENS = [
    "unbuildable", "common driveway", "(driveway)", "landlocked",
    "right of way", "easement", "rear strip", "alley",
]

MOBILE_HOME_TOKENS = [
    "mhpk", "mhp", "mobile home", "trailer park", "mobile hm", "mh park",
]

MULTI_UNIT_TOKENS = [
    "duplex", "triplex", "fourplex", "multi-family", "multifamily",
    "2 unit", "3 unit", "4 unit", "2-unit", "3-unit", "4-unit",
    "two-family", "three-family", "two unit", "three unit",
    "mixed use", "mixed-use", "apartment", "apt building",
]

VACANT_LOT_HINTS = [
    "vacant", "lot only", "lot ", "rear ", "lot/garage", "garage only",
    "parcel", "tract", "subdivision lot",
]

STREET_SUFFIX = re.compile(
    r"\b(st|street|ave|avenue|rd|road|ln|lane|dr|drive|blvd|boulevard|"
    r"ct|court|pl|place|way|ter|terrace|hwy|highway|cir|circle|pkwy|parkway)\b\.?",
    re.IGNORECASE,
)
LEADING_NUMBER = re.compile(r"^\s*\d{1,6}\s+")


def classify(description: str | None, address: str | None = None, parcel_id: str | None = None) -> str:
    blob = " ".join(filter(None, [description, address])).lower()
    if not blob:
        return "unknown"

    if parcel_id and re.search(r"-T0?\d+-", parcel_id):
        return "mobile_home"

    for tok in UNBUILDABLE_TOKENS:
        if tok in blob:
            return "unbuildable"

    for tok in MOBILE_HOME_TOKENS:
        if tok in blob:
            return "mobile_home"

    for tok in MULTI_UNIT_TOKENS:
        if tok in blob:
            return "structure_multi"

    addr_for_check = (address or description or "").strip()
    has_street_suffix = bool(STREET_SUFFIX.search(addr_for_check))
    has_leading_number = bool(LEADING_NUMBER.match(addr_for_check))

    for tok in VACANT_LOT_HINTS:
        if tok in blob:
            return "vacant_lot"

    if has_street_suffix and has_leading_number:
        return "structure_sfh"
    if has_street_suffix and not has_leading_number:
        return "vacant_lot"

    return "unknown"


PROPERTY_TYPE_EMOJI = {
    "structure_sfh": "🏠",
    "structure_multi": "🏢",
    "mobile_home": "🚐",
    "vacant_lot": "🟫",
    "unbuildable": "🚫",
    "unknown": "❓",
}

PROPERTY_TYPE_LABEL = {
    "structure_sfh": "Single-family",
    "structure_multi": "Multi-family",
    "mobile_home": "Mobile home / lot",
    "vacant_lot": "Vacant lot",
    "unbuildable": "Unbuildable",
    "unknown": "Unknown",
}
