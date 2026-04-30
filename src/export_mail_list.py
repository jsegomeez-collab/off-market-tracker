"""
Export a CSV of absentee/long-term/high-equity owners ready for direct mail.

Run: python -m src.export_mail_list [--out path]

Filters:
  - has owner_name and parcel_id
  - has owner_mailing_city (i.e., enriched)
  - is_absentee = true (mailing != property city)

Output columns:
  parcel_id, property_address, property_city, owner_first, owner_last,
  mailing_address_line, mailing_city, mailing_state, mailing_zip,
  is_out_of_state, back_taxes, years_owed, judicial_sale_date, score, source
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from pathlib import Path

from .db import DB_PATH


def export(output_path: Path, only_absentee: bool = True) -> int:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT * FROM properties
        WHERE owner_name IS NOT NULL
          AND parcel_id IS NOT NULL
          AND raw LIKE '%owner_mailing_city%'
        ORDER BY score DESC
        """
    ).fetchall()

    written = 0
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "parcel_id", "property_address", "property_city", "county",
            "owner_first_name", "owner_last_name", "owner_full",
            "mailing_city", "mailing_state", "mailing_zip",
            "is_absentee", "is_out_of_state",
            "back_taxes", "years_owed", "judicial_sale_date",
            "property_type", "score", "source",
        ])
        for r in rows:
            try:
                raw = json.loads(r["raw"]) if r["raw"] else {}
            except (json.JSONDecodeError, TypeError):
                raw = {}
            if only_absentee and not raw.get("is_absentee"):
                continue
            mc = raw.get("owner_mailing_city")
            if not mc:
                continue

            years = raw.get("years_owed") or []
            years_str = f"{min(years)}-{max(years)}" if years else ""

            w.writerow([
                r["parcel_id"],
                raw.get("real_address") or r["address"],
                r["city"] or "",
                r["county"] or "",
                raw.get("owner_first_name") or "",
                raw.get("owner_last_name") or "",
                r["owner_name"],
                mc,
                raw.get("owner_mailing_state") or "",
                raw.get("owner_mailing_zip") or "",
                "Y" if raw.get("is_absentee") else "N",
                "Y" if raw.get("is_out_of_state") else "N",
                raw.get("back_taxes") or "",
                years_str,
                raw.get("judicial_sale_date") or "",
                r["property_type"] or "",
                r["score"],
                r["source"],
            ])
            written += 1
    conn.close()
    return written


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data/mail_list.csv", type=Path)
    p.add_argument("--all", action="store_true", help="Include non-absentee too")
    args = p.parse_args()

    n = export(args.out, only_absentee=not args.all)
    print(f"Wrote {n} rows -> {args.out}")


if __name__ == "__main__":
    main()
