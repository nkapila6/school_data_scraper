"""
Entry point — orchestrates scraping with a thread pool and writes CSV output.

Run as:
    python -m scraper khda
    python -m scraper spea
    python -m scraper both
    scraper khda  (if installed via uv / pip)
"""

import argparse
import csv
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from . import khda, spea


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_done(output_csv: str) -> set[str]:
    """Return the set of school names already written to *output_csv*."""
    p = Path(output_csv)
    if not p.exists():
        return set()
    with open(p, newline="", encoding="utf-8-sig") as f:
        return {row["school_name"] for row in csv.DictReader(f)}


def _open_writer(output_csv: str, fieldnames: list, resume: bool):
    """Open the output CSV for writing (or appending) and return (file, writer)."""
    done = _load_done(output_csv) if resume else set()
    mode = "a" if done else "w"
    f = open(output_csv, mode, newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    if not done:
        writer.writeheader()
    return f, writer, done


def _print_summary(output_csv: str, fee_col: str, transport_col: str) -> None:
    rows = list(csv.DictReader(open(output_csv, encoding="utf-8-sig")))
    with_fees      = sum(1 for r in rows if r.get(fee_col))
    with_transport = sum(1 for r in rows if r.get(transport_col))
    print(f"\n{'=' * 60}")
    print(f"Done → {output_csv}")
    print(f"  {len(rows)} schools | {with_fees} with fees | {with_transport} with transport")


# ---------------------------------------------------------------------------
# KHDA run
# ---------------------------------------------------------------------------

def run_khda(args) -> None:
    print("Fetching KHDA school list…")
    schools = khda.build_school_list()
    print(f"  → {len(schools)} schools found")

    # Filters
    if args.school:
        sf = args.school.lower()
        schools = [
            s for s in schools
            if sf in s["name"].lower()
            or args.school in (s["center_id"], s["khda_id"])
        ]
        print(f"  → School filter {args.school!r}: {len(schools)} match(es)")
        if not schools:
            print("No matching schools found.")
            return

    if args.curriculum:
        cf = args.curriculum.lower()
        # We can't filter before fetching because curriculum is on the detail page,
        # so we fetch everything and skip mismatches post-parse.
        print(f"  → Curriculum filter: {args.curriculum!r} (applied after fetch)")

    out_csv = args.output or "output_khda.csv"
    f, writer, done = _open_writer(out_csv, khda.FIELDNAMES, args.resume)
    lock = threading.Lock()
    total = len(schools)
    counter = {"done": 0}

    def process(school):
        name = school["name"]
        if name in done:
            return None, name, "skipped"

        row = khda.fetch_school(school, include_transport=args.transport)

        # Post-fetch curriculum filter
        if args.curriculum and args.curriculum.lower() not in row.get("curriculum", "").lower():
            return None, name, f"filtered ({row.get('curriculum', '?')})"

        return row, name, row.get("curriculum", "?")

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process, s): s for s in schools}
        for future in as_completed(futures):
            row, name, status = future.result()
            with lock:
                counter["done"] += 1
                idx = counter["done"]
                if status == "skipped":
                    print(f"[{idx}/{total}] {name[:45]}  → already done")
                    continue
                if row is None:
                    print(f"[{idx}/{total}] {name[:45]}  → {status}")
                    continue
                writer.writerow(row)
                f.flush()
                curr   = row.get("curriculum", "")
                dsib   = row.get("dsib_rating", "")
                avg    = row.get("khda_average_fee", "–")
                t_min  = row.get("transport_fee_min_aed", "–")
                t_max  = row.get("transport_fee_max_aed", "–")
                print(
                    f"[{idx}/{total}] {name[:45]:<46}"
                    f"  {curr:<12}  DSIB:{dsib:<14}"
                    f"  avg:{avg}  transport:{t_min}–{t_max}"
                )

    f.close()
    _print_summary(out_csv, "khda_lowest_fee", "transport_fee_min_aed")


# ---------------------------------------------------------------------------
# SPEA run
# ---------------------------------------------------------------------------

def run_spea(args) -> None:
    # Resolve curriculum filter to SPEA param IDs
    curriculum_ids = None
    if args.curriculum:
        cf = args.curriculum.lower()
        curriculum_ids = [cid for cid, label in spea.CURRICULA.items()
                          if cf in label.lower()]
        if not curriculum_ids:
            print(f"Unknown curriculum {args.curriculum!r}.")
            print(f"Options: {', '.join(spea.CURRICULA.values())}")
            return
        print(f"  Curriculum filter: {[spea.CURRICULA[c] for c in curriculum_ids]}")

    print("Collecting SPEA school IDs…")
    school_list = spea.collect_school_ids(curriculum_ids)
    print(f"  → {len(school_list)} schools found")

    # Pre-filter: only possible when the filter looks like a numeric SPEA ID.
    # Name-based filtering is done post-fetch (names aren't in the listing).
    if args.school and args.school.isdigit():
        school_list = [(sid, hint) for sid, hint in school_list if sid == args.school]
        print(f"  → ID filter {args.school!r}: {len(school_list)} match(es)")

    out_csv = args.output or "output_spea.csv"
    f, writer, done = _open_writer(out_csv, spea.FIELDNAMES, args.resume)
    lock = threading.Lock()
    total = len(school_list)
    counter = {"done": 0}

    def process(sid_hint):
        sid, hint = sid_hint

        # Fetch first so we have the name for done-check and school filter
        row = spea.fetch_school(sid, hint, include_transport=args.transport)
        name = row.get("school_name", f"School {sid}")

        # School name filter (applied post-fetch since names aren't in the listing)
        if args.school and args.school.lower() not in name.lower() and args.school != sid:
            return None, name, "filtered"

        if name in done:
            return None, name, "skipped"

        return row, name, row.get("curriculum", "?")

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process, sh): sh for sh in school_list}
        for future in as_completed(futures):
            row, name, status = future.result()
            with lock:
                counter["done"] += 1
                idx = counter["done"]
                if status in ("filtered", "skipped"):
                    if status == "skipped":
                        print(f"[{idx}/{total}] {name[:45]}  → already done")
                    continue
                writer.writerow(row)
                f.flush()
                curr   = row.get("curriculum", "")
                rating = row.get("spea_rating", "")
                low    = row.get("spea_lowest_fee", "–")
                high   = row.get("spea_highest_fee", "–")
                t_min  = row.get("transport_fee_min_aed", "–")
                print(
                    f"[{idx}/{total}] {name[:45]:<46}"
                    f"  {curr:<12}  rating:{rating:<14}"
                    f"  fees:{low}–{high}  transport:{t_min}"
                )

    f.close()
    _print_summary(out_csv, "spea_lowest_fee", "transport_fee_min_aed")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        prog="scraper",
        description="UAE school fee scraper — KHDA (Dubai) and SPEA (Sharjah)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  scraper khda                              all 230 Dubai schools
  scraper spea                              all 96 Sharjah schools
  scraper both                              both authorities
  scraper khda --curriculum British
  scraper spea --curriculum Indian
  scraper khda --school "GEMS Wellington"
  scraper spea --school 384
  scraper khda --output dubai.csv --resume
  scraper spea --workers 10 --no-transport

SPEA curricula: MoE, American, British, Indian, Pakistani,
                SABIS, Australian, Pilipinas, French, German
        """,
    )

    parser.add_argument(
        "authority",
        choices=["khda", "spea", "both"],
        help="Authority to scrape (khda / spea / both)",
    )
    parser.add_argument(
        "--school",
        metavar="NAME_OR_ID",
        default=None,
        help="Partial school name or numeric ID",
    )
    parser.add_argument(
        "--curriculum",
        metavar="LABEL",
        default=None,
        help="Filter by curriculum, e.g. British, Indian, American",
    )
    parser.add_argument(
        "--output",
        metavar="FILE",
        default=None,
        help="Output CSV path (default: output_khda.csv / output_spea.csv)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip schools already present in the output CSV",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=5,
        metavar="N",
        help="Number of parallel workers (default: 5)",
    )
    parser.add_argument(
        "--no-transport",
        dest="transport",
        action="store_false",
        default=True,
        help="Skip transport fee collection",
    )

    args = parser.parse_args()

    if args.authority in ("khda", "both"):
        run_khda(args)

    if args.authority in ("spea", "both"):
        # For 'both', use separate output files unless user specified one
        if args.authority == "both" and args.output:
            print("\nNote: --output ignored for 'both'; using output_khda.csv + output_spea.csv")
            args.output = None
        run_spea(args)


if __name__ == "__main__":
    main()
