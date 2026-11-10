"""
Try a single careers link and print the job postings it yields — read-only.

Runs only worker_handler (scrape + keyword/location filter) for one
company. Nothing is read from or written to DynamoDB and no email is
sent, so every job printed here will still count as "new" on the next
real scheduled run.

Usage:
    export CONFIG_CSV_URL="https://docs.google.com/spreadsheets/d/e/.../pub?output=csv"
    python3 test_link.py "Amazon" "https://www.amazon.jobs/en/search?base_query=software+engineer&loc_query=India"

    # show every scraped posting, before the keyword/location filter
    python3 test_link.py "Amazon" "<url>" --all

Keywords come from the same Google Sheet the real run uses, so the
filtered output matches what the pipeline would pick up. Override with
--include / --exclude (repeatable) to try keywords without editing the sheet.
"""

import argparse
import os
import sys

import lambda_function as lf


# Belt-and-braces: the worker never touches DynamoDB, but make sure
# nothing in this script can ever mark a job as seen.
def _no_ddb():
    raise RuntimeError("test_link.py must not access DynamoDB")


lf.get_ddb_table = _no_ddb


def _print_jobs(jobs):
    for i, j in enumerate(jobs, start=1):
        print(f"{i:3}. {j['title']}")
        print(f"     location: {j.get('location') or 'unknown'}"
              f"   posted: {j.get('posted_date') or 'unknown'}")
        print(f"     {j['url']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("name", help="company name, as it would appear in the sheet")
    parser.add_argument("url", help="careers page URL to test")
    parser.add_argument("--all", action="store_true",
                        help="print every scraped posting, ignoring keyword/location filters")
    parser.add_argument("--include", action="append",
                        help="include keyword (repeatable); overrides the sheet's include list")
    parser.add_argument("--exclude", action="append",
                        help="exclude keyword (repeatable); overrides the sheet's exclude list")
    args = parser.parse_args()

    if args.all:
        renderer = lf.PlaywrightRenderer()
        try:
            jobs, tier = lf.scrape_company({"name": args.name, "url": args.url}, renderer)
        finally:
            renderer.close()
        jobs = lf._dedupe_by_job_id(jobs)
        print(f"{args.name}: {len(jobs)} scraped (tier={tier}), unfiltered\n")
        _print_jobs(jobs)
        return

    include = args.include
    exclude = args.exclude
    if include is None or exclude is None:
        config = lf.load_config(os.environ.get("CONFIG_CSV_URL", lf.CONFIG_CSV_URL))
        if include is None:
            include = config["include_keywords"]
        if exclude is None:
            exclude = config["exclude_keywords"]
    include = [k.lower() for k in include]
    exclude = [k.lower() for k in exclude]

    if not include:
        print("WARNING: no include keywords — nothing will match. "
              "Set CONFIG_CSV_URL or pass --include.", file=sys.stderr)

    result = lf.worker_handler({
        "name": args.name,
        "url": args.url,
        "include_keywords": include,
        "exclude_keywords": exclude,
    }, None)

    print(f"{result['company']}: {result['scraped']} scraped, "
          f"{len(result['jobs'])} relevant (status={result['status']})")
    if result.get("detail"):
        print(f"note: {result['detail']}")
    print()
    _print_jobs(result["jobs"])


if __name__ == "__main__":
    main()
