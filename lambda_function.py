"""
SDE1 Job Scraper — AWS Lambda function.

Scrapes career pages of MNCs configured in a published Google Sheet CSV,
filters titles for SDE1-relevant roles using include/exclude keywords
(also from the sheet), deduplicates against a DynamoDB table, and emails
a digest of newly found jobs via SES.

=====================================================================
ONE-TIME MANUAL SETUP (read this before deploying)
=====================================================================

1. Google Sheet config
   - Create a Google Sheet with three columns: type, value1, value2
     (see README.md for the exact row format).
   - File -> Share -> Publish to web -> select the sheet/tab -> CSV.
   - Copy the published link and replace CONFIG_CSV_URL below (or, better,
     set it via the CONFIG_CSV_URL environment variable so you don't have
     to edit code).

2. SES sandbox verification (REQUIRED or email sending will fail)
   - By default, new SES accounts are in "sandbox mode": you can only
     send email to/from addresses you have individually verified.
   - In the AWS Console: SES -> Verified identities -> Create identity
     -> Email address. Do this for BOTH the sender address (SENDER_EMAIL)
     and the recipient address (RECIPIENT_EMAIL). Each will receive a
     confirmation email with a link you must click.
   - Until both are verified, SendEmail calls will fail with
     "Email address is not verified".
   - To send to arbitrary recipients without per-address verification,
     you'd need to request production access (moves SES out of sandbox).

3. DynamoDB table
   - Table name: job_scraper_seen_jobs (or set SEEN_JOBS_TABLE env var).
   - Partition key: job_id (String).
   - Enable TTL on attribute "ttl" (Number, epoch seconds) so old rows
     auto-expire (this script writes ttl = now + SEEN_JOB_TTL_DAYS days).
   - See the seed schema note near write_seen_job() below.

4. Lambda environment variables
   - CONFIG_CSV_URL      published Google Sheet CSV URL
   - SENDER_EMAIL        SES-verified "from" address
   - RECIPIENT_EMAIL     SES-verified "to" address (comma-separated for multiple)
   - SEEN_JOBS_TABLE      (optional, default "job_scraper_seen_jobs")
   - SEEN_JOB_TTL_DAYS    (optional, default "75")
   - AWS_REGION is provided automatically by Lambda; SES/DynamoDB clients
     use it unless SES_REGION / DDB_REGION are set explicitly.

5. IAM permissions required by the Lambda execution role
   - ses:SendEmail, ses:SendRawEmail
   - dynamodb:GetItem, dynamodb:PutItem on the seen-jobs table
   - logs:CreateLogGroup, logs:CreateLogStream, logs:PutLogEvents
     (standard CloudWatch Logs access, usually via the
     AWSLambdaBasicExecutionRole managed policy)

6. EventBridge schedule (twice daily)
   - Rate expression:  rate(12 hours)
   - or Cron expression (e.g. 9am and 9pm UTC): cron(0 9,21 * * ? *)
   - Target: this Lambda function. No special input payload required.

=====================================================================
"""

import csv
import hashlib
import io
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import boto3
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# REPLACE WITH YOUR PUBLISHED GOOGLE SHEET CSV LINK
# (File -> Share -> Publish to web -> CSV). Overridable via env var so the
# code itself never needs to be edited after deployment.
CONFIG_CSV_URL = os.environ.get(
    "CONFIG_CSV_URL",
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ2U2AkRZVE9ZjB2ZLn46kDQ3jlqPeTEWuKkUM9IVuKtEgsMFQelDKbmpPeuuw17fBMHO2t3T_cjPZW/pub?gid=1791749391&single=true&output=csv",
)

SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "")
RECIPIENT_EMAIL = os.environ.get("RECIPIENT_EMAIL", "")

SEEN_JOBS_TABLE = os.environ.get("SEEN_JOBS_TABLE", "job_scraper_seen_jobs")
SEEN_JOB_TTL_DAYS = int(os.environ.get("SEEN_JOB_TTL_DAYS", "75"))

SES_REGION = os.environ.get("SES_REGION") or os.environ.get("AWS_REGION")
DDB_REGION = os.environ.get("DDB_REGION") or os.environ.get("AWS_REGION")

HTTP_TIMEOUT = 15  # seconds, per outbound HTTP request
USER_AGENT = (
    "Mozilla/5.0 (compatible; SDE1JobScraper/1.0; "
    "+https://github.com/) job-alert-bot"
)

# ---------------------------------------------------------------------------
# AWS clients (created lazily so unit tests can import this module without
# valid AWS credentials configured)
# ---------------------------------------------------------------------------

_ddb_resource = None
_ses_client = None


def get_ddb_table():
    global _ddb_resource
    if _ddb_resource is None:
        _ddb_resource = boto3.resource("dynamodb", region_name=DDB_REGION)
    return _ddb_resource.Table(SEEN_JOBS_TABLE)


def get_ses_client():
    global _ses_client
    if _ses_client is None:
        _ses_client = boto3.client("ses", region_name=SES_REGION)
    return _ses_client


# ---------------------------------------------------------------------------
# 1. Configuration loading (Google Sheet published-CSV)
# ---------------------------------------------------------------------------

def load_config(csv_url):
    """
    Fetch and parse the config CSV. Returns a dict:
        {"companies": [{"name": str, "url": str}, ...],
         "include_keywords": [str, ...],
         "exclude_keywords": [str, ...]}

    Fails gracefully: on any network/parse error, logs and returns an
    empty config rather than raising, so the whole run doesn't crash.
    """
    empty_config = {"companies": [], "include_keywords": [], "exclude_keywords": []}

    try:
        resp = requests.get(csv_url, timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT})
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Failed to fetch config CSV from %s: %s", csv_url, exc)
        return empty_config

    try:
        reader = csv.DictReader(io.StringIO(resp.text))
        fieldnames = [f.strip().lower() for f in (reader.fieldnames or [])]
        if not {"type", "value1"}.issubset(set(fieldnames)):
            logger.error(
                "Config CSV is missing required columns (need at least "
                "'type' and 'value1'); found columns: %s", reader.fieldnames
            )
            return empty_config
    except Exception as exc:  # noqa: BLE001 - defensive, must never crash the run
        logger.error("Failed to parse config CSV header: %s", exc)
        return empty_config

    companies = []
    include_keywords = []
    exclude_keywords = []

    # Re-read with normalized (lowercased) field access since DictReader
    # keys off the raw header text.
    reader = csv.DictReader(io.StringIO(resp.text))
    for row_num, raw_row in enumerate(reader, start=2):  # header is row 1
        try:
            row = {(k or "").strip().lower(): (v or "").strip() for k, v in raw_row.items()}
            row_type = row.get("type", "").lower()

            if row_type == "company":
                name = row.get("value1", "")
                url = row.get("value2", "")
                if not name or not url:
                    logger.warning(
                        "Skipping malformed 'company' row %d (missing name or URL): %s",
                        row_num, raw_row,
                    )
                    continue
                companies.append({"name": name, "url": url})

            elif row_type == "include":
                keyword = row.get("value1", "")
                if not keyword:
                    logger.warning("Skipping empty 'include' row %d", row_num)
                    continue
                include_keywords.append(keyword.lower())

            elif row_type == "exclude":
                keyword = row.get("value1", "")
                if not keyword:
                    logger.warning("Skipping empty 'exclude' row %d", row_num)
                    continue
                exclude_keywords.append(keyword.lower())

            elif row_type == "":
                continue  # blank line
            else:
                logger.warning("Skipping row %d with unknown type '%s'", row_num, row_type)

        except Exception as exc:  # noqa: BLE001 - one bad row shouldn't kill config load
            logger.warning("Skipping malformed row %d (%s): %s", row_num, raw_row, exc)
            continue

    if not companies:
        logger.warning("Config parsed but no valid 'company' rows were found.")
    if not include_keywords:
        logger.warning("Config parsed but no 'include' keywords were found — "
                        "no jobs will ever match.")

    logger.info(
        "Loaded config: %d companies, %d include keywords, %d exclude keywords",
        len(companies), len(include_keywords), len(exclude_keywords),
    )

    return {
        "companies": companies,
        "include_keywords": include_keywords,
        "exclude_keywords": exclude_keywords,
    }


# ---------------------------------------------------------------------------
# 2. Scraping logic
# ---------------------------------------------------------------------------

def make_job_id(company, title, url, ats_id=None):
    """Stable identifier for a job posting."""
    if ats_id:
        return f"{company.lower()}:{ats_id}"
    if url:
        return f"{company.lower()}:{url}"
    raw = f"{company}|{title}|{url}".encode("utf-8")
    return f"{company.lower()}:{hashlib.sha256(raw).hexdigest()[:16]}"


def normalize_job(company, title, url, posted_date=None, ats_id=None):
    return {
        "company": company,
        "title": (title or "").strip(),
        "url": url,
        "posted_date": posted_date,
        "job_id": make_job_id(company, title or "", url or "", ats_id),
    }


def detect_ats(url):
    """Return one of 'greenhouse', 'lever', 'workday', 'smartrecruiters', or None."""
    host = urlparse(url).netloc.lower()
    if "greenhouse.io" in host:
        return "greenhouse"
    if "lever.co" in host:
        return "lever"
    if "myworkdayjobs.com" in host:
        return "workday"
    if "smartrecruiters.com" in host:
        return "smartrecruiters"
    return None


def _extract_board_token(url, marker_segments):
    """
    Pull the short board/company token out of a career-page URL, e.g.
    https://boards.greenhouse.io/stripe -> "stripe"
    https://jobs.lever.co/stripe -> "stripe"
    """
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    if parts:
        return parts[0]
    # token sometimes lives in the subdomain instead (e.g. Workday)
    host_parts = parsed.netloc.split(".")
    return host_parts[0] if host_parts else None


def scrape_greenhouse(company_name, url):
    token = _extract_board_token(url, [])
    if not token:
        raise ValueError(f"Could not determine Greenhouse board token from {url}")

    api_url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=false"
    resp = requests.get(api_url, timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    data = resp.json()

    jobs = []
    for item in data.get("jobs", []):
        title = item.get("title")
        job_url = item.get("absolute_url")
        posted = item.get("updated_at") or item.get("created_at")
        ats_id = item.get("id")
        jobs.append(normalize_job(company_name, title, job_url, posted, ats_id))
    return jobs


def scrape_lever(company_name, url):
    token = _extract_board_token(url, [])
    if not token:
        raise ValueError(f"Could not determine Lever site token from {url}")

    api_url = f"https://api.lever.co/v0/postings/{token}?mode=json"
    resp = requests.get(api_url, timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    data = resp.json()

    jobs = []
    for item in data:
        title = item.get("text")
        job_url = item.get("hostedUrl") or item.get("applyUrl")
        created = item.get("createdAt")
        posted = None
        if created:
            try:
                posted = datetime.fromtimestamp(int(created) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                posted = None
        ats_id = item.get("id")
        jobs.append(normalize_job(company_name, title, job_url, posted, ats_id))
    return jobs


def scrape_workday(company_name, url):
    """
    Workday career sites are JS-rendered, but expose a JSON search API at
    <tenant>.myworkdayjobs.com/wday/cxs/<tenant>/<site>/jobs (POST).
    We derive tenant/site from the career page URL; this is best-effort
    since Workday URL structure varies by tenant configuration.
    """
    parsed = urlparse(url)
    host_parts = parsed.netloc.split(".")
    tenant = host_parts[0] if host_parts else None
    path_parts = [p for p in parsed.path.split("/") if p]
    site = path_parts[0] if path_parts else "External"

    if not tenant:
        raise ValueError(f"Could not determine Workday tenant from {url}")

    api_url = f"https://{parsed.netloc}/wday/cxs/{tenant}/{site}/jobs"
    payload = {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": ""}
    resp = requests.post(
        api_url, json=payload, timeout=HTTP_TIMEOUT,
        headers={"User-Agent": USER_AGENT, "Content-Type": "application/json"},
    )
    resp.raise_for_status()
    data = resp.json()

    jobs = []
    for item in data.get("jobPostings", []):
        title = item.get("title")
        path = item.get("externalPath", "")
        job_url = f"https://{parsed.netloc}/{site}{path}" if path else None
        posted = item.get("postedOn")
        ats_id = item.get("bulletFields", [None])[0] if item.get("bulletFields") else None
        jobs.append(normalize_job(company_name, title, job_url, posted, ats_id))
    return jobs


def scrape_smartrecruiters(company_name, url):
    token = _extract_board_token(url, [])
    if not token:
        raise ValueError(f"Could not determine SmartRecruiters company identifier from {url}")

    api_url = f"https://api.smartrecruiters.com/v1/companies/{token}/postings"
    resp = requests.get(api_url, timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    data = resp.json()

    jobs = []
    for item in data.get("content", []):
        title = item.get("name")
        job_url = item.get("applyUrl") or item.get("ref")
        posted = item.get("releasedDate")
        ats_id = item.get("id")
        jobs.append(normalize_job(company_name, title, job_url, posted, ats_id))
    return jobs


def scrape_generic_html(company_name, url):
    """
    Best-effort fallback for career pages that don't match a known ATS.
    Uses common markup patterns (anchor tags whose text looks like a job
    title, or elements with job/posting/position-ish class names).
    Likely needs custom selector tuning per company — this is a floor,
    not a reliable scraper.
    """
    logger.warning(
        "Company '%s' does not match a known ATS; using generic HTML scraper. "
        "This company may need custom selector tuning.", company_name
    )

    resp = requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    jobs = []
    seen_urls = set()

    candidates = soup.select(
        "a[href*='job'], a[href*='career'], a[href*='posting'], "
        "a[class*='job'], a[class*='posting'], li[class*='job'] a"
    )

    for anchor in candidates:
        title = anchor.get_text(strip=True)
        href = anchor.get("href")
        if not title or not href or len(title) < 4:
            continue

        job_url = href if href.startswith("http") else _urljoin(url, href)
        if job_url in seen_urls:
            continue
        seen_urls.add(job_url)

        # best-effort posted date: look for a sibling/parent date-ish element
        posted_date = None
        parent = anchor.find_parent()
        if parent:
            date_el = parent.find(class_=re.compile(r"date|posted", re.I))
            if date_el:
                posted_date = date_el.get_text(strip=True)

        jobs.append(normalize_job(company_name, title, job_url, posted_date))

    return jobs


def _urljoin(base_url, href):
    parsed = urlparse(base_url)
    if href.startswith("/"):
        return f"{parsed.scheme}://{parsed.netloc}{href}"
    return f"{parsed.scheme}://{parsed.netloc}/{href}"


ATS_SCRAPERS = {
    "greenhouse": scrape_greenhouse,
    "lever": scrape_lever,
    "workday": scrape_workday,
    "smartrecruiters": scrape_smartrecruiters,
}


def scrape_company(company):
    """
    Scrape a single company's career page. Never raises — logs and
    returns an empty list on failure so one broken site doesn't kill
    the whole run.
    """
    name = company["name"]
    url = company["url"]

    try:
        ats = detect_ats(url)
        if ats:
            logger.info("Scraping '%s' via %s API (%s)", name, ats, url)
            return ATS_SCRAPERS[ats](name, url)
        else:
            return scrape_generic_html(name, url)
    except Exception as exc:  # noqa: BLE001 - isolate per-company failures
        logger.error("Failed to scrape company '%s' (%s): %s", name, url, exc, exc_info=True)
        return []


# ---------------------------------------------------------------------------
# 3. Relevance filtering
# ---------------------------------------------------------------------------

def is_relevant(title, include_keywords, exclude_keywords):
    if not title:
        return False
    title_lower = title.lower()

    if any(bad in title_lower for bad in exclude_keywords):
        return False

    return any(good in title_lower for good in include_keywords)


def filter_jobs(jobs, include_keywords, exclude_keywords):
    return [j for j in jobs if is_relevant(j["title"], include_keywords, exclude_keywords)]


# ---------------------------------------------------------------------------
# 4. Deduplication (DynamoDB)
# ---------------------------------------------------------------------------
#
# Expected table schema for `job_scraper_seen_jobs`:
#   Partition key: job_id (String)
#   Other attributes written on each new notification:
#     company     (String)
#     title       (String)
#     notified_at (String, ISO-8601 UTC timestamp)
#     ttl         (Number, epoch seconds — enable DynamoDB TTL on this
#                  attribute so rows auto-expire after SEEN_JOB_TTL_DAYS,
#                  keeping the table small)
#
# Enable TTL via: DynamoDB console -> table -> Additional settings ->
# Time to Live -> attribute name "ttl".

def is_job_seen(table, job_id):
    try:
        resp = table.get_item(Key={"job_id": job_id})
        return "Item" in resp
    except Exception as exc:  # noqa: BLE001
        logger.error("DynamoDB get_item failed for job_id=%s: %s", job_id, exc)
        # Fail open on read errors: better to risk a duplicate email than
        # to silently drop a real job because Dynamo hiccuped.
        return False


def write_seen_job(table, job):
    now = datetime.now(timezone.utc)
    ttl_epoch = int((now + timedelta(days=SEEN_JOB_TTL_DAYS)).timestamp())
    try:
        table.put_item(
            Item={
                "job_id": job["job_id"],
                "company": job["company"],
                "title": job["title"],
                "notified_at": now.isoformat(),
                "ttl": ttl_epoch,
            }
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("DynamoDB put_item failed for job_id=%s: %s", job["job_id"], exc)


def dedupe_against_dynamodb(table, jobs):
    new_jobs = []
    for job in jobs:
        if is_job_seen(table, job["job_id"]):
            continue
        new_jobs.append(job)
    return new_jobs


# ---------------------------------------------------------------------------
# 5. Notification (SES)
# ---------------------------------------------------------------------------

def build_email_body(new_jobs):
    lines_text = [f"{len(new_jobs)} new SDE1-relevant job(s) found:\n"]
    rows_html = []

    for job in sorted(new_jobs, key=lambda j: j["company"].lower()):
        posted = job.get("posted_date") or "N/A"
        lines_text.append(
            f"- [{job['company']}] {job['title']} (posted: {posted})\n  {job['url']}\n"
        )
        rows_html.append(
            "<tr>"
            f"<td style='padding:6px 10px;border-bottom:1px solid #eee;'>{_esc(job['company'])}</td>"
            f"<td style='padding:6px 10px;border-bottom:1px solid #eee;'>{_esc(job['title'])}</td>"
            f"<td style='padding:6px 10px;border-bottom:1px solid #eee;'>{_esc(posted)}</td>"
            "<td style='padding:6px 10px;border-bottom:1px solid #eee;'>"
            f"<a href='{_esc(job['url'])}'>Apply</a></td>"
            "</tr>"
        )

    text_body = "\n".join(lines_text)

    html_body = f"""
    <html>
      <body style="font-family:Arial,sans-serif;">
        <h2>{len(new_jobs)} new SDE1-relevant job(s)</h2>
        <table style="border-collapse:collapse;width:100%;">
          <thead>
            <tr style="text-align:left;background:#f5f5f5;">
              <th style="padding:6px 10px;">Company</th>
              <th style="padding:6px 10px;">Title</th>
              <th style="padding:6px 10px;">Posted</th>
              <th style="padding:6px 10px;">Link</th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows_html)}
          </tbody>
        </table>
      </body>
    </html>
    """

    return text_body, html_body


def _esc(value):
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def send_digest_email(new_jobs):
    if not new_jobs:
        logger.info("No new relevant jobs — skipping email send.")
        return

    if not SENDER_EMAIL or not RECIPIENT_EMAIL:
        logger.error(
            "SENDER_EMAIL and/or RECIPIENT_EMAIL environment variables are not "
            "set — cannot send digest email. (Remember both must be SES-"
            "verified while SES is in sandbox mode.)"
        )
        return

    recipients = [addr.strip() for addr in RECIPIENT_EMAIL.split(",") if addr.strip()]
    text_body, html_body = build_email_body(new_jobs)

    ses = get_ses_client()
    try:
        ses.send_email(
            Source=SENDER_EMAIL,
            Destination={"ToAddresses": recipients},
            Message={
                "Subject": {"Data": f"SDE1 Job Digest — {len(new_jobs)} new posting(s)", "Charset": "UTF-8"},
                "Body": {
                    "Text": {"Data": text_body, "Charset": "UTF-8"},
                    "Html": {"Data": html_body, "Charset": "UTF-8"},
                },
            },
        )
        logger.info("Sent digest email with %d new job(s) to %s", len(new_jobs), recipients)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to send SES digest email: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# 6. Lambda handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    logger.info("SDE1 job scraper run starting.")

    config = load_config(CONFIG_CSV_URL)
    companies = config["companies"]
    include_keywords = config["include_keywords"]
    exclude_keywords = config["exclude_keywords"]

    if not companies or not include_keywords:
        logger.error("Config incomplete (companies=%d, include_keywords=%d) — aborting run.",
                      len(companies), len(include_keywords))
        return {"statusCode": 200, "body": json.dumps({"new_jobs": 0, "reason": "incomplete_config"})}

    all_jobs = []
    for company in companies:
        try:
            company_jobs = scrape_company(company)
            logger.info("Scraped %d job(s) from '%s'", len(company_jobs), company["name"])
            all_jobs.extend(company_jobs)
        except Exception as exc:  # noqa: BLE001 - belt-and-suspenders; scrape_company already isolates
            logger.error("Unexpected error scraping '%s': %s", company["name"], exc, exc_info=True)
            continue

    relevant_jobs = filter_jobs(all_jobs, include_keywords, exclude_keywords)
    logger.info("%d of %d scraped jobs matched keyword filters", len(relevant_jobs), len(all_jobs))

    table = get_ddb_table()
    new_jobs = dedupe_against_dynamodb(table, relevant_jobs)
    logger.info("%d of %d relevant jobs are new (not previously notified)", len(new_jobs), len(relevant_jobs))

    send_digest_email(new_jobs)

    for job in new_jobs:
        write_seen_job(table, job)

    result = {"scraped": len(all_jobs), "relevant": len(relevant_jobs), "new_jobs": len(new_jobs)}
    logger.info("SDE1 job scraper run complete: %s", result)
    return {"statusCode": 200, "body": json.dumps(result)}
