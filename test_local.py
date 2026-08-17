"""
Local dry-run harness for lambda_function.py — no AWS resources required.

Simulates the Step Functions pipeline sequentially in-process:
    config_loader_handler -> worker_handler (once per company, in a
    plain loop instead of a parallel Map state) -> aggregator_handler
Stubs DynamoDB with an in-memory dict and SES with a print statement, so
you can verify config parsing, scraping, and filtering against your real
published Google Sheet before creating any AWS infrastructure.

Usage:
    export CONFIG_CSV_URL="https://docs.google.com/spreadsheets/d/e/.../pub?output=csv"
    python3 test_local.py
"""

import os
import sys

import lambda_function as lf

# --- Stub DynamoDB with an in-memory dict -----------------------------
_fake_seen_table = {}


class FakeTable:
    def get_item(self, Key):
        job_id = Key["job_id"]
        if job_id in _fake_seen_table:
            return {"Item": _fake_seen_table[job_id]}
        return {}

    def put_item(self, Item):
        _fake_seen_table[Item["job_id"]] = Item


lf.get_ddb_table = lambda: FakeTable()

# --- Stub SES: print the digest instead of sending ---------------------
def fake_send_digest_email(new_jobs, company_results):
    if not new_jobs and not lf.has_scrape_issues(company_results):
        print("\n[SES STUB] No new jobs and no scrape issues — would skip sending.\n")
        return
    text_body, _html_body = lf.build_email_body(new_jobs, company_results)
    print("\n" + "=" * 70)
    print(f"[SES STUB] Would send digest email to {lf.RECIPIENT_EMAIL or '(RECIPIENT_EMAIL not set)'}")
    print(f"[SES STUB] From: {lf.SENDER_EMAIL or '(SENDER_EMAIL not set)'}")
    print("=" * 70)
    print(text_body)
    print("=" * 70 + "\n")


lf.send_digest_email = fake_send_digest_email

# --- Run it: simulate LoadConfig -> Map(worker) -> SendDigest ------------

if __name__ == "__main__":
    csv_url = os.environ.get("CONFIG_CSV_URL", lf.CONFIG_CSV_URL)
    if "PLACEHOLDER" in csv_url or "/edit" in csv_url:
        print(
            "WARNING: CONFIG_CSV_URL doesn't look like a published CSV link "
            f"(got: {csv_url}).\nSet a real one first, e.g.:\n"
            '  export CONFIG_CSV_URL="https://docs.google.com/spreadsheets/d/e/.../pub?output=csv"\n',
            file=sys.stderr,
        )

    lf.CONFIG_CSV_URL = csv_url

    # LoadConfig
    config = lf.config_loader_handler({}, None)
    print(f"Loaded {len(config['companies'])} companies, "
          f"{len(config['include_keywords'])} include keywords, "
          f"{len(config['exclude_keywords'])} exclude keywords")

    # ScrapeAllCompanies (Map) — sequential here; real deploy runs these
    # in parallel across separate Lambda invocations
    worker_results = []
    for company in config["companies"]:
        event = {
            "name": company["name"],
            "url": company["url"],
            "include_keywords": config["include_keywords"],
            "exclude_keywords": config["exclude_keywords"],
        }
        result = lf.worker_handler(event, None)
        print(f"  {result['company']}: {result['scraped']} scraped, "
              f"{len(result['jobs'])} relevant (status={result['status']})")
        worker_results.append(result)

    # SendDigest
    final_result = lf.aggregator_handler(worker_results, None)
    print("\nFinal result:", final_result)
    print("Fake 'seen jobs' table now contains:", len(_fake_seen_table), "entries")
