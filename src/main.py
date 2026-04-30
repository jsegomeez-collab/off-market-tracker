"""
Deal Radar orchestrator.

Runs all scrapers, dedups + persists to SQLite, scores everything new,
then notifies the top unnotified deals via Telegram + email.

Run locally:  python -m src.main
GH Actions:   see .github/workflows/scrape.yml
"""

from __future__ import annotations

import os
import traceback
from datetime import datetime

from .db import (
    get_conn,
    upsert_properties,
    log_run,
    top_unnotified,
    mark_notified,
)
from .scoring import score_all_unscored
from .notify import notify_all
from . import skip_trace
from . import enrich

from .scrapers import (
    luzerne_tax_repo,
    luzerne_sheriff,
    craigslist_scranton,
    lackawanna_judicial,
)

SCRAPERS = [
    ("luzerne_tax_repo", luzerne_tax_repo.scrape),
    ("luzerne_sheriff", luzerne_sheriff.scrape),
    ("lackawanna_judicial", lackawanna_judicial.scrape),
    ("craigslist_scranton", craigslist_scranton.scrape),
]

MIN_NOTIFY_SCORE = int(os.getenv("MIN_NOTIFY_SCORE", "40"))
MAX_NOTIFY_PER_RUN = int(os.getenv("MAX_NOTIFY_PER_RUN", "20"))


def run() -> int:
    conn = get_conn()
    total_found = 0
    total_new = 0

    for name, scrape_fn in SCRAPERS:
        started = datetime.utcnow()
        try:
            print(f"\n[scrape] {name} starting...")
            props = scrape_fn()
            found, new = upsert_properties(conn, props)
            log_run(conn, name, started, found, new)
            print(f"[scrape] {name}: found={found} new={new}")
            total_found += found
            total_new += new
        except Exception as e:
            err = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            print(f"[scrape] {name} ERROR: {err}")
            log_run(conn, name, started, 0, 0, error=err[:1000])

    n_scored = score_all_unscored(conn)
    print(f"\n[score] scored {n_scored} new properties")

    rows = top_unnotified(conn, min_score=MIN_NOTIFY_SCORE, limit=MAX_NOTIFY_PER_RUN)
    print(f"\n[notify] {len(rows)} deals at score >= {MIN_NOTIFY_SCORE}")
    delivered = False
    if rows:
        max_enrich = int(os.getenv("MAX_ENRICH_PER_RUN", "20"))
        if max_enrich > 0:
            done = enrich.enrich_rows(conn, rows, max_lookups=max_enrich)
            print(f"[enrich] performed {done} back-tax lookups (cap={max_enrich})")

        max_skip = int(os.getenv("MAX_SKIP_TRACE_PER_RUN", "10"))
        if max_skip > 0:
            looked = skip_trace.enrich_rows(conn, rows, max_lookups=max_skip)
            print(f"[skip-trace] performed {looked} new lookups (cap={max_skip})")

        rows = top_unnotified(conn, min_score=MIN_NOTIFY_SCORE, limit=MAX_NOTIFY_PER_RUN)
        delivered = notify_all(rows)
        if delivered:
            mark_notified(conn, [r["id"] for r in rows])
        else:
            print("[notify] no channel delivered — keeping deals unnotified for next run")

    print(f"\n[done] total_found={total_found} total_new={total_new} queued={len(rows)} delivered={delivered}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
