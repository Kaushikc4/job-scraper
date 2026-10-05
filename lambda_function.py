"""
SDE1 Job Scraper.

Scrapes career pages of MNCs configured in a published Google Sheet CSV,
filters titles for SDE1-relevant roles using include/exclude keywords
(also from the sheet) plus an India/Remote location filter, deduplicates
against a DynamoDB table, and emails a digest of newly found jobs via SES.

Orchestrated by AWS Step Functions as three separate Lambda functions —
all built from this same container image, differentiated only by which
handler in this file each one's --image-config Command points at:

    config_loader_handler  -> Lambda "sde1-config-loader"
    worker_handler          -> Lambda "sde1-worker" (one invocation per
                                company, run in parallel by a Map state)
    aggregator_handler      -> Lambda "sde1-aggregator"

See state_machine.asl.json for the state machine definition and
README.md for the full setup/deploy walkthrough (Google Sheet, SES
verification, DynamoDB table, IAM roles, EventBridge schedule).
"""

import csv
import hashlib
import io
import json
import logging
import os
import re
import signal
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

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
# Safety cap on pagination depth per company — without this, a company
# with thousands of open roles (e.g. Amazon) would page through all of
# them every run, ballooning runtime and email size.
MAX_JOBS_PER_COMPANY = int(os.environ.get("MAX_JOBS_PER_COMPANY", "100"))
USER_AGENT = (
    "Mozilla/5.0 (compatible; SDE1JobScraper/1.0; "
    "+https://github.com/) job-alert-bot"
)

# Hard wall-clock cap on scraping a single company (seconds). Needed
# because Playwright's own per-call timeouts (page.goto, click) don't
# cover every operation — page.close() in particular has been observed
# to hang indefinitely against an unresponsive/zombie browser process.
# Without this, one stuck company (bot-blocked sites are the usual
# culprit) can silently consume the entire Lambda invocation and lose
# every result — including from companies that already scraped fine.
PER_COMPANY_TIMEOUT_SECONDS = int(os.environ.get("PER_COMPANY_TIMEOUT_SECONDS", "90"))

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


def normalize_job(company, title, url, posted_date=None, ats_id=None, location=None):
    return {
        "company": company,
        "title": (title or "").strip(),
        "url": url,
        "posted_date": posted_date,
        "location": location,
        "job_id": make_job_id(company, title or "", url or "", ats_id),
    }


def detect_ats(url):
    """
    Return a platform identifier for URL-pattern-based dispatch. Most of
    these are real third-party ATS platforms (Greenhouse/Lever/Workday/
    SmartRecruiters); 'amazon', 'google', and 'microsoft' are grouped in
    here too since they're also detectable purely from the URL host and
    have a dedicated scraper, even though each is a company's own
    in-house platform rather than a third-party ATS.
    """
    host = urlparse(url).netloc.lower()
    if "greenhouse.io" in host:
        return "greenhouse"
    if "lever.co" in host:
        return "lever"
    if "myworkdayjobs.com" in host or "myworkdaysite.com" in host:
        return "workday"
    if "smartrecruiters.com" in host:
        return "smartrecruiters"
    if "amazon.jobs" in host:
        return "amazon"
    if host == "careers.google.com" or host.endswith(".careers.google.com"):
        return "google"
    if host.endswith("careers.microsoft.com"):
        return "microsoft"
    return None


def _extract_board_token(url, marker_segments):
    """
    Pull the short board/company token out of a career-page URL, e.g.
    https://boards.greenhouse.io/stripe -> "stripe"
    https://jobs.lever.co/stripe -> "stripe"
    """
    parsed = urlparse(url)
    # API URLs carry the token after a fixed segment (e.g.
    # boards-api.greenhouse.io/v1/boards/<token>/jobs, api.lever.co/v0/postings/<token>)
    api_match = re.search(r"/(?:boards|postings|companies)/([^/?#]+)", parsed.path)
    if api_match:
        return api_match.group(1)
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
        location = (item.get("location") or {}).get("name")
        jobs.append(normalize_job(company_name, title, job_url, posted, ats_id, location))
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
        location = (item.get("categories") or {}).get("location")
        jobs.append(normalize_job(company_name, title, job_url, posted, ats_id, location))
    return jobs


def _workday_tenant_and_site(parsed_url):
    """
    Workday career sites come in two different public URL shapes that
    both proxy to the same underlying /wday/cxs/{tenant}/{site}/jobs
    API (confirmed by testing — identical results either way):

      *.myworkdayjobs.com/{site}          -> tenant is the subdomain
      *.myworkdaysite.com/.../recruiting/{tenant}/{site}/...
                                           -> tenant/site are the two
                                              path segments right after
                                              "recruiting" (there can be
                                              a locale prefix before it,
                                              e.g. /en-US/recruiting/...,
                                              so search for the segment
                                              rather than assume a fixed
                                              position)
    """
    path_parts = [p for p in parsed_url.path.split("/") if p]

    if "myworkdaysite.com" in parsed_url.netloc.lower():
        if "recruiting" in path_parts:
            idx = path_parts.index("recruiting")
            if idx + 2 < len(path_parts):
                return path_parts[idx + 1], path_parts[idx + 2]
        return None, None

    host_parts = parsed_url.netloc.split(".")
    tenant = host_parts[0] if host_parts else None
    # Skip an optional locale segment (e.g. /en-US/<site>)
    if path_parts and re.fullmatch(r"[a-z]{2}-[A-Z]{2}", path_parts[0]):
        path_parts = path_parts[1:]
    site = path_parts[0] if path_parts else "External"
    return tenant, site


def scrape_workday(company_name, url):
    """
    Workday career sites are JS-rendered, but expose a JSON search API
    at /wday/cxs/<tenant>/<site>/jobs (POST) — see
    _workday_tenant_and_site() for how tenant/site are derived from
    either of Workday's two public URL shapes. Paginates via
    limit/offset up to MAX_JOBS_PER_COMPANY (per the API's own `total`
    field — e.g. Wells Fargo reports 1500+ openings globally).
    """
    parsed = urlparse(url)
    tenant, site = _workday_tenant_and_site(parsed)

    if not tenant or not site:
        raise ValueError(f"Could not determine Workday tenant/site from {url}")

    # Job detail URLs need the full career-page path as their prefix on
    # myworkdaysite.com (e.g. /recruiting/wf/WellsFargoJobs/job/... —
    # just /{site}/job/... 404s), but only /{site}/job/... on
    # myworkdayjobs.com — confirmed by testing both against real job
    # links, not assumed.
    if "myworkdaysite.com" in parsed.netloc.lower():
        job_url_prefix = parsed.path.rstrip("/")
    else:
        job_url_prefix = f"/{site}"

    api_url = f"https://{parsed.netloc}/wday/cxs/{tenant}/{site}/jobs"
    page_size = 20

    jobs = []
    offset = 0
    total = None
    while total is None or (len(jobs) < min(total, MAX_JOBS_PER_COMPANY) and offset < total):
        payload = {"appliedFacets": {}, "limit": page_size, "offset": offset, "searchText": ""}
        resp = requests.post(
            api_url, json=payload, timeout=HTTP_TIMEOUT,
            headers={"User-Agent": USER_AGENT, "Content-Type": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()

        postings = data.get("jobPostings", [])
        if not postings:
            break
        total = data.get("total", len(postings))

        for item in postings:
            title = item.get("title")
            path = item.get("externalPath", "")
            job_url = f"https://{parsed.netloc}{job_url_prefix}{path}" if path else None
            posted = item.get("postedOn")
            ats_id = item.get("bulletFields", [None])[0] if item.get("bulletFields") else None
            location = item.get("locationsText")
            jobs.append(normalize_job(company_name, title, job_url, posted, ats_id, location))

        offset += page_size

    return jobs[:MAX_JOBS_PER_COMPANY]


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
        loc = item.get("location") or {}
        location = ", ".join(filter(None, [loc.get("city"), loc.get("region"), loc.get("country")])) or None
        jobs.append(normalize_job(company_name, title, job_url, posted, ats_id, location))
    return jobs


def scrape_microsoft(company_name, url):
    """
    jobs.careers.microsoft.com looks like a JS-only SPA, but it's backed
    by a public, unauthenticated JSON API (found via network-request
    inspection, not documented anywhere): apply.careers.microsoft.com/
    api/pcsx/search. Query text and location are pulled from the
    configured URL's own `q`/`lc` params. Paginates via `start` (fixed
    page size of 10, per the API's own `data.count` total-hits field).
    """
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    search_query = query.get("q", [""])[0]
    search_location = query.get("lc", [""])[0]

    api_url = "https://apply.careers.microsoft.com/api/pcsx/search"
    page_size = 10

    jobs = []
    start = 0
    total = None
    while total is None or (len(jobs) < min(total, MAX_JOBS_PER_COMPANY) and start < total):
        params = {"domain": "microsoft.com", "query": search_query, "location": search_location, "start": start}
        data = _get_json(api_url, params=params).get("data", {})

        positions = data.get("positions", [])
        if not positions:
            break
        total = data.get("count", len(positions))

        for item in positions:
            title = item.get("name")
            path = item.get("positionUrl")
            job_url = f"https://jobs.careers.microsoft.com{path}" if path else None
            posted_ts = item.get("postedTs")
            posted = datetime.fromtimestamp(posted_ts, tz=timezone.utc).strftime("%Y-%m-%d") if posted_ts else None
            ats_id = item.get("id") or item.get("atsJobId")
            job_location = ", ".join(item.get("locations", [])) or None
            jobs.append(normalize_job(company_name, title, job_url, posted, ats_id, job_location))

        start += page_size

    return jobs[:MAX_JOBS_PER_COMPANY]


def scrape_amazon(company_name, url):
    """
    amazon.jobs exposes a public JSON search API mirroring the search page's
    own query params (base_query, loc_query), so we forward whatever the
    configured career-page URL already has and just switch the path to
    /search.json. Paginates via offset/result_limit up to
    MAX_JOBS_PER_COMPANY (Amazon's own `hits` total is often in the
    thousands).

    Note: `loc_query` (the param the search page's own URL uses) turns
    out to NOT actually filter results server-side — confirmed by
    testing, it returns the same global result set with or without it.
    The parameter that actually works is `country` (ISO alpha-3, e.g.
    "IND"). So if the configured loc_query mentions India, we translate
    it to the real filter param instead of trusting the ignored one.
    """
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    base_query = query.get("base_query", [""])[0]
    loc_query = query.get("loc_query", [""])[0]

    api_url = "https://www.amazon.jobs/en/search.json"
    page_size = 50

    jobs = []
    offset = 0
    while len(jobs) < MAX_JOBS_PER_COMPANY:
        params = {
            "base_query": base_query,
            "result_limit": page_size,
            "offset": offset,
        }
        if "india" in loc_query.lower():
            params["country"] = "IND"
        else:
            params["loc_query"] = loc_query
        resp = requests.get(api_url, params=params, timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT})
        resp.raise_for_status()
        data = resp.json()

        page_jobs = data.get("jobs", [])
        if not page_jobs:
            break

        for item in page_jobs:
            title = item.get("title")
            path = item.get("job_path")
            job_url = f"https://www.amazon.jobs{path}" if path else None
            posted = item.get("posted_date")
            ats_id = item.get("id_icims") or item.get("id")
            location = item.get("normalized_location") or item.get("location")
            jobs.append(normalize_job(company_name, title, job_url, posted, ats_id, location))

        if len(page_jobs) < page_size or offset + page_size >= data.get("hits", 0):
            break
        offset += page_size

    return jobs[:MAX_JOBS_PER_COMPANY]


def _extract_js_object(content, marker):
    """
    Pull a brace-balanced JS object literal out of raw page text, starting
    right after `marker` (e.g. "phApp.ddo = "). Returns a parsed dict, or
    None if the marker isn't present or the object can't be parsed.
    Tracks string state so braces inside quoted strings don't throw off
    the balance count.
    """
    start = content.find(marker)
    if start == -1:
        return None
    obj_start = content.find("{", start)
    if obj_start == -1:
        return None

    depth = 0
    in_str = False
    esc = False
    str_char = ""
    i = obj_start
    while i < len(content):
        c = content[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == str_char:
                in_str = False
        else:
            if c in "\"'":
                in_str = True
                str_char = c
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    break
        i += 1
    else:
        return None

    raw = content[obj_start:i + 1]
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _fetch_html_or_none(url):
    """Detection-only fetch: a failed request means 'not this platform' and
    must fall through to the next tier, not abort the whole company."""
    try:
        resp = requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT})
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as exc:
        logger.info("Detection fetch failed for %s (%s); trying next tier.", url, exc)
        return None


def _get_json(url, params=None, attempts=3):
    """GET JSON, backing off on rate limiting (429) and transient 5xx errors."""
    for i in range(attempts):
        resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT})
        if resp.status_code in (429, 500, 502, 503, 504) and i < attempts - 1:
            time.sleep(2 ** (i + 1))
            continue
        resp.raise_for_status()
        return resp.json()


def _phenom_jobs_from_raw(company_name, jobs_raw):
    jobs = []
    for item in jobs_raw:
        title = item.get("title")
        job_url = item.get("applyUrl")
        posted = item.get("postedDate") or item.get("dateCreated")
        ats_id = item.get("reqId") or item.get("jobId")
        location = item.get("location") or ", ".join(item.get("multi_location", [])) or None
        jobs.append(normalize_job(company_name, title, job_url, posted, ats_id, location))
    return jobs


def try_scrape_phenom(company_name, url):
    """
    Detect and parse Phenom People-powered career pages (used by Adobe and
    various other large companies). These pages look JS-rendered but
    actually server-render the full job list as an embedded JSON object
    (`phApp.ddo = {...}`), so a plain HTTP GET is enough — no browser
    needed. Returns None (not a list) if this page isn't Phenom-powered,
    so the caller can fall through to the next scraping tier.

    Paginates by re-fetching the same URL with `&from={offset}&s=1`
    appended — this is the query param the site's own "next page" links
    use (confirmed against Adobe's pagination controls). Assumed to be a
    platform-wide Phenom convention rather than Adobe-specific; if a
    given page doesn't yield more embedded data, pagination just stops
    rather than erroring.
    """
    html = _fetch_html_or_none(url)
    if html is None:
        return None

    if "phApp.ddo" not in html:
        return None

    ddo = _extract_js_object(html, "phApp.ddo = ")
    if ddo is None:
        return None

    erf = ddo.get("eagerLoadRefineSearch", {})
    jobs_raw = erf.get("data", {}).get("jobs") or ddo.get("jobs") or []
    if not jobs_raw:
        return None

    jobs = _phenom_jobs_from_raw(company_name, jobs_raw)
    page_size = len(jobs_raw)
    total_hits = erf.get("totalHits") or page_size

    separator = "&" if "?" in url else "?"
    offset = page_size
    while len(jobs) < min(total_hits, MAX_JOBS_PER_COMPANY) and offset < total_hits:
        page_url = f"{url}{separator}from={offset}&s=1"
        try:
            page_resp = requests.get(page_url, timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT})
            page_resp.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("Phenom pagination request failed for '%s' at offset %d: %s", company_name, offset, exc)
            break

        page_ddo = _extract_js_object(page_resp.text, "phApp.ddo = ")
        page_jobs_raw = (
            page_ddo.get("eagerLoadRefineSearch", {}).get("data", {}).get("jobs")
            if page_ddo else None
        )
        if not page_jobs_raw:
            break

        jobs.extend(_phenom_jobs_from_raw(company_name, page_jobs_raw))
        offset += page_size

    return jobs[:MAX_JOBS_PER_COMPANY]


_EIGHTFOLD_MARKER_RE = re.compile(r"pcsx", re.I)
_DOMAIN_RE = re.compile(r"\b([a-z0-9-]+\.(?:com|io|net|org|co|in|ai))\b", re.I)


def _brand_domain(host, html):
    """
    Eightfold's API takes the company's registrable domain (e.g. netflix.com)
    even when the careers host is a subdomain (explore.jobs.netflix.net).
    Pick the most frequent domain in the page whose name matches a host label.
    """
    labels = host.split(".")
    counts = {}
    for m in _DOMAIN_RE.finditer(html):
        dom = m.group(1).lower()
        counts[dom] = counts.get(dom, 0) + 1
    for dom, _ in sorted(counts.items(), key=lambda kv: -kv[1]):
        sld = dom.split(".")[0]
        if len(sld) > 2 and sld in labels:
            return dom
    return None


def _eightfold_page(base, domain, start, location):
    """One page of an Eightfold job search. Tries the PCS endpoint first,
    then the apply/v2 endpoint (both seen in production sites), and returns
    (endpoint_name, positions) or (None, []) if neither responds with jobs."""
    attempts = [
        ("pcsx", f"{base}/api/pcsx/search",
         {"domain": domain, "query": "", "location": location, "start": start}),
        ("apply_v2", f"{base}/api/apply/v2/jobs",
         {"domain": domain, "query": "", "location": location, "start": start, "num": 10}),
    ]
    for name, api_url, params in attempts:
        try:
            data = _get_json(api_url, params=params)
        except (requests.RequestException, ValueError):
            continue
        positions = (data.get("data") or {}).get("positions") or data.get("positions") or []
        if positions:
            return name, positions
    return None, []


def try_scrape_eightfold(company_name, url):
    """
    Eightfold-powered career sites (e.g. Qualcomm, Netflix, Microsoft's
    own board uses the same family) render from a JSON search API whose
    marker — "pcsx" CSS/API naming — is present in the raw page HTML. Plain
    HTTP, no browser. Returns None if the page isn't Eightfold-powered.
    """
    html = _fetch_html_or_none(url)
    if html is None:
        return None
    if not _EIGHTFOLD_MARKER_RE.search(html):
        return None

    host = urlparse(url).netloc.lower()
    domain = _brand_domain(host, html)
    if not domain:
        return None
    base = f"https://{host}"

    jobs = []
    endpoint = None
    start = 0
    while len(jobs) < MAX_JOBS_PER_COMPANY:
        name, positions = _eightfold_page(base, domain, start, "India")
        if not positions:
            break
        endpoint = endpoint or name
        for item in positions:
            title = item.get("name") or item.get("posting_name")
            path = item.get("positionUrl") or item.get("canonicalPositionUrl") or ""
            job_url = path if path.startswith("http") else f"{base}{path}" if path else None
            posted_ts = item.get("postedTs")
            posted = (datetime.fromtimestamp(posted_ts, tz=timezone.utc).strftime("%Y-%m-%d")
                      if isinstance(posted_ts, (int, float)) else None)
            location = ", ".join(item.get("locations") or []) or None
            ats_id = item.get("id") or item.get("atsJobId")
            jobs.append(normalize_job(company_name, title, job_url, posted, ats_id, location))
        start += len(positions)

    if endpoint is None:
        return None
    return jobs[:MAX_JOBS_PER_COMPANY]


def _slug_candidates(company_name):
    slug = re.sub(r"[^a-z0-9]", "", company_name.lower())
    return [slug] if slug else []


def try_probe_public_board(company_name):
    """
    Some companies host a public Lever or Greenhouse board under their
    exact company-name slug (e.g. Meesho -> jobs.lever.co/meesho), even
    when their own career page is a custom site. Probe those public APIs
    directly — no browser — and accept only an exact slug with real
    postings to avoid matching an unrelated company with a similar name.
    Returns (jobs, tier) or None.
    """
    for slug in _slug_candidates(company_name):
        try:
            resp = requests.get(f"https://api.lever.co/v0/postings/{slug}?mode=json",
                                timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT})
            if resp.status_code == 200 and resp.json():
                return scrape_lever(company_name, f"https://jobs.lever.co/{slug}"), "board_probe_lever"
        except (requests.RequestException, ValueError):
            pass
        try:
            resp = requests.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
                                timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT})
            if resp.status_code == 200 and resp.json().get("jobs"):
                return scrape_greenhouse(company_name, f"https://boards.greenhouse.io/{slug}"), "board_probe_greenhouse"
        except (requests.RequestException, ValueError):
            pass
    return None


_ARIA_LABEL_PREFIX_RE = re.compile(r"^(view job|apply for|job)\s*[:\-]\s*", re.I)
_POSTED_TEXT_RE = re.compile(r"(posted\s|days?\s+ago|hours?\s+ago|weeks?\s+ago|\d{4}-\d{2}-\d{2})", re.I)


def _anchor_title(anchor):
    """
    Job cards on SPA career sites often jam title+location+posted-date
    (and, on some ATS platforms, job-id/job-family) into one text block
    with no separators — raw anchor text alone is usually unreadable in
    a digest email. Two cleaner sources, tried in order, before falling
    back to raw text:
      1. aria-label (common accessibility pattern: "View job: Title")
      2. a heading element (h1-h6) nested in the anchor — several ATS
         platforms wrap just the title in one (e.g. Oracle Taleo's
         `<a><h2>Title</h2><span>job id: ...</span>...</a>`), separate
         from the surrounding metadata text.
    """
    aria_label = (anchor.get("aria-label") or "").strip()
    if aria_label:
        cleaned = _ARIA_LABEL_PREFIX_RE.sub("", aria_label).strip()
        if len(cleaned) >= 4:
            return cleaned

    heading = anchor.find(re.compile(r"^h[1-6]$"))
    if heading:
        heading_text = heading.get_text(strip=True)
        if len(heading_text) >= 4:
            return heading_text

    return anchor.get_text(strip=True)


def _find_posted_date(anchor):
    """Best-effort: a nearby element whose class or text looks date-ish."""
    parent = anchor.find_parent()
    if not parent:
        return None
    date_el = parent.find(class_=re.compile(r"date|posted", re.I))
    if date_el:
        return date_el.get_text(strip=True)
    text_node = parent.find(string=_POSTED_TEXT_RE)
    if text_node:
        return text_node.strip()
    return None


_LOCATION_TEXT_RE = re.compile(r"\bremote\b|\bindia\b|[A-Za-z]+,\s*[A-Za-z]+", re.I)
# A link only counts as a job in the generic tier if its URL looks like a
# job detail page (path segment, id, or requisition), not a nav/menu page.
_JOB_DETAIL_URL_RE = re.compile(
    r"/jobs?/|/position|/requisition|/opening|/posting|job[_-]?id=|\d{5,}", re.I
)


def _find_location(anchor):
    """
    Best-effort: a nearby element whose class or text looks location-ish.
    Unreliable compared to the structured-API tiers (Amazon/Adobe/
    Microsoft/Google) — this is the generic fallback used for companies
    with no known data source, so there's no consistent markup to key
    off. Prefer a class hint (`class*=location`), fall back to a loose
    text pattern ("Remote", "India", or a "City, Country"-shaped string).
    """
    parent = anchor.find_parent()
    if not parent:
        return None
    loc_el = parent.find(class_=re.compile(r"location", re.I))
    if loc_el:
        return loc_el.get_text(strip=True)
    text_node = parent.find(string=_LOCATION_TEXT_RE)
    if text_node:
        return text_node.strip()
    return None


def _extract_generic_jobs(company_name, base_url, soup):
    """
    Shared best-effort extraction: anchor tags whose href/class look
    job-related. Used both for the plain-HTTP generic fallback and for
    Playwright-rendered pages.
    """
    jobs = []
    seen_urls = set()

    candidates = soup.select(
        "a[href*='job'], a[href*='career'], a[href*='posting'], "
        "a[class*='job'], a[class*='posting'], li[class*='job'] a"
    )

    for anchor in candidates:
        title = _anchor_title(anchor)
        href = anchor.get("href")
        if not title or not href or len(title) < 4:
            continue
        if href.startswith(("mailto:", "tel:", "javascript:", "#")):
            continue

        job_url = href if href.startswith("http") else _urljoin(base_url, href)
        if job_url in seen_urls or not _JOB_DETAIL_URL_RE.search(job_url):
            continue
        seen_urls.add(job_url)

        posted_date = _find_posted_date(anchor)
        location = _find_location(anchor)
        jobs.append(normalize_job(company_name, title, job_url, posted_date, location=location))

    return jobs


def scrape_generic_html(company_name, url):
    """
    Plain-HTTP best-effort fallback — only useful for career pages that
    render their job list server-side without JS. Most modern SPA career
    sites will return zero jobs here; scrape_generic_playwright is the
    real fallback used for those (see scrape_company).
    """
    resp = requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    return _extract_generic_jobs(company_name, url, soup)


class PlaywrightRenderer:
    """
    Lazily launches a headless Chromium instance on first use. One
    instance is created per worker_handler invocation (one company each
    — see Step Functions handlers below) and closed at the end of that
    same invocation; there's no cross-company reuse to manage since each
    company already gets its own Lambda invocation.

    If the browser process crashes mid-scrape (observed in practice on
    heavy pages — a single "--single-process" Chromium instance can die
    from resource pressure and take the whole tab-context with it),
    render()/open_page() detect the dead browser and relaunch once
    rather than failing outright.

    Requires the `playwright` package AND its Chromium browser binary to
    be present in the runtime — see README.md for the container-image
    deployment this requires (a plain zip package is too large/won't
    have the browser installed).
    """

    LAUNCH_ARGS = [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--single-process",
        "--no-zygote",
    ]

    def __init__(self):
        self._playwright = None
        self._browser = None

    def _launch(self):
        from playwright.sync_api import sync_playwright  # noqa: PLC0415 - lazy so non-JS-only deployments don't need playwright installed

        if self._playwright is None:
            self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(args=self.LAUNCH_ARGS)

    def _new_page(self):
        if self._browser is None or not self._browser.is_connected():
            self._launch()
        try:
            return self._browser.new_page(user_agent=USER_AGENT)
        except Exception:  # noqa: BLE001 - browser died since last use; relaunch once and retry
            logger.warning("Playwright browser was unresponsive; relaunching.")
            self._launch()
            return self._browser.new_page(user_agent=USER_AGENT)

    def render(self, url, wait_ms=4000):
        """One-shot: navigate, return the rendered HTML, close the page."""
        page = self._new_page()
        try:
            page.goto(url, timeout=30000, wait_until="domcontentloaded")
            page.wait_for_timeout(wait_ms)
            return page.content()
        finally:
            page.close()

    def open_page(self, url, wait_ms=4000):
        """
        Navigate and return the live Page object for multi-step
        interaction (e.g. clicking through pagination) — caller is
        responsible for calling page.close() when done.
        """
        page = self._new_page()
        page.goto(url, timeout=30000, wait_until="domcontentloaded")
        page.wait_for_timeout(wait_ms)
        return page

    def close(self):
        if self._browser is not None:
            self._browser.close()
        if self._playwright is not None:
            self._playwright.stop()
        self._browser = None
        self._playwright = None


def _google_cards_from_soup(company_name, soup):
    jobs = []
    for card in soup.select("li.lLd3Je"):
        title_el = card.select_one("h3.QJPWVe")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)

        jsdata_el = card.select_one("[jsdata]")
        jsdata = jsdata_el.get("jsdata", "") if jsdata_el else ""
        parts = jsdata.split(";")
        job_id = parts[1] if len(parts) > 1 else None
        if not job_id:
            continue

        job_url = f"https://www.google.com/about/careers/applications/jobs/results/{job_id}"
        location_el = card.select_one("span.r0wTof")
        location = location_el.get_text(strip=True) if location_el else None
        jobs.append(normalize_job(company_name, title, job_url, None, job_id, location))
    return jobs


def scrape_google(company_name, url, renderer):
    """
    Google's career site has no public API and no useful data in raw
    HTML (job data is populated client-side via Google's internal
    AF_initDataCallback format, not plain JSON). After rendering, each
    job card is an <li class="lLd3Je"> containing an <h3 class="QJPWVe">
    title and a nested [jsdata] attribute whose second semicolon-
    separated field is the numeric job ID — confirmed by cross-checking
    that ID against the real job page
    (google.com/about/careers/applications/jobs/results/{id}).

    Paginates by clicking the results table's "Go to next page" button
    (confirmed via testing that this changes the underlying job set,
    unlike scrolling which does nothing — Google's results list isn't
    infinite-scroll, it's a paged table) until MAX_JOBS_PER_COMPANY is
    reached or the button stops producing new results.
    """
    page = renderer.open_page(url)
    jobs = []
    seen_ids = set()
    try:
        while len(jobs) < MAX_JOBS_PER_COMPANY:
            soup = BeautifulSoup(page.content(), "html.parser")
            page_jobs = _google_cards_from_soup(company_name, soup)
            new_jobs = [j for j in page_jobs if j["job_id"] not in seen_ids]
            if not new_jobs:
                break
            for j in new_jobs:
                seen_ids.add(j["job_id"])
            jobs.extend(new_jobs)

            if len(jobs) >= MAX_JOBS_PER_COMPANY:
                break

            next_button = page.get_by_label("Go to next page", exact=False)
            if next_button.count() == 0:
                break
            try:
                next_button.click(timeout=5000)
            except Exception:  # noqa: BLE001 - likely disabled (last page reached)
                break
            page.wait_for_timeout(2500)
    finally:
        page.close()

    return jobs[:MAX_JOBS_PER_COMPANY]


def scrape_generic_playwright(company_name, url, renderer):
    """
    Last-resort fallback for career pages that are fully client-rendered
    (no useful data in the raw HTML and not a recognized ATS/Phenom
    site). Renders the page with headless Chromium, then applies the
    same best-effort anchor-tag heuristics as scrape_generic_html.
    Likely needs custom selector tuning per company — this is a floor,
    not a reliable scraper.
    """
    logger.warning(
        "Company '%s' requires JS rendering (no known ATS or embedded "
        "data found); using Playwright headless browser. This company "
        "may need custom selector tuning.", company_name
    )

    html = renderer.render(url)
    soup = BeautifulSoup(html, "html.parser")
    return _extract_generic_jobs(company_name, url, soup)


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
    "amazon": scrape_amazon,
    "microsoft": scrape_microsoft,
}

# Known platforms that need a rendered DOM (not just an HTTP GET) but have
# a dedicated, verified extraction path rather than the generic heuristic.
PLAYWRIGHT_KNOWN_SCRAPERS = {
    "google": scrape_google,
}


def scrape_company(company, renderer):
    """
    Scrape a single company's career page, in four tiers:
      1. Known ATS URL pattern (Greenhouse/Lever/Workday/SmartRecruiters/
         Amazon/Microsoft) -> direct API call. Fast, no browser.
      2. Not a known ATS -> try detecting a Phenom People-powered page
         (plain HTTP GET, checks for embedded JSON). Fast, no browser.
      3. Known platform with a dedicated Playwright extractor (currently
         just Google) -> browser required, but pagination behavior is
         known and verified.
      4. Neither -> render with headless Chromium (Playwright) and apply
         generic best-effort selectors. Slow, last resort, single page
         only (no generic pagination handling — this is also where Uber
         lands: its pagination was investigated but found unreliable,
         see README).

    Never raises — logs and returns an empty list on failure so one
    broken site doesn't kill the whole run.

    Returns (jobs, tier) — tier is one of "ats", "phenom",
    "known_playwright", "generic", or "error". Callers use this to flag
    "generic" results as best-effort/unverified in the email, since that
    tier's output can be junk (nav links, language switchers, etc.) on
    bot-protected sites — the scrape can succeed with 0 errors while
    still returning nothing but noise (e.g. Atlassian).
    """
    name = company["name"]
    url = company["url"]

    try:
        platform = detect_ats(url)
        if platform in ATS_SCRAPERS:
            logger.info("Scraping '%s' via %s API (%s)", name, platform, url)
            return ATS_SCRAPERS[platform](name, url), "ats"

        phenom_jobs = try_scrape_phenom(name, url)
        if phenom_jobs is not None:
            logger.info("Scraping '%s' via detected Phenom People embedded data (%s)", name, url)
            return phenom_jobs, "phenom"

        eightfold_jobs = try_scrape_eightfold(name, url)
        if eightfold_jobs is not None:
            logger.info("Scraping '%s' via detected Eightfold JSON API (%s)", name, url)
            return eightfold_jobs, "eightfold"

        probed = try_probe_public_board(name)
        if probed is not None:
            logger.info("Scraping '%s' via public board probe (%s)", name, probed[1])
            return probed

        if platform in PLAYWRIGHT_KNOWN_SCRAPERS:
            logger.info("Scraping '%s' via known-site Playwright extraction (%s)", name, platform)
            return PLAYWRIGHT_KNOWN_SCRAPERS[platform](name, url, renderer), "known_playwright"

        return scrape_generic_playwright(name, url, renderer), "generic"
    except Exception as exc:  # noqa: BLE001 - isolate per-company failures
        logger.error("Failed to scrape company '%s' (%s): %s", name, url, exc, exc_info=True)
        return [], "error"


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


_INDIA_LOCATION_RE = re.compile(r"\bindia\b|\bind\b", re.I)
_REMOTE_LOCATION_RE = re.compile(r"\bremote\b", re.I)


def is_india_or_remote(location):
    """
    True if the job's location mentions India or is remote. Jobs with
    no location info are excluded rather than assumed-in-scope — since
    the whole point is narrowing to India/remote roles, silently
    keeping unknown-location jobs would defeat that.

    Matches the whole word "India" or the ISO alpha-3 code "IND" (some
    sources, e.g. Amazon, use the country code rather than the name) —
    word-boundary matching, not substring, so "Indiana, USA" doesn't
    false-positive on "india".
    """
    if not location:
        return False
    return bool(_INDIA_LOCATION_RE.search(location) or _REMOTE_LOCATION_RE.search(location))


def filter_jobs(jobs, include_keywords, exclude_keywords):
    return [
        j for j in jobs
        if is_relevant(j["title"], include_keywords, exclude_keywords)
        and is_india_or_remote(j.get("location"))
    ]


def _dedupe_by_job_id(jobs):
    """
    Collapse duplicate job_ids from this run's scrape before they ever
    reach DynamoDB dedup. Needed because some paginated sources (e.g.
    Adobe's search results, observed in testing) can return overlapping
    results across pages if the underlying result ordering shifts
    between page fetches — without this, the same posting could appear
    twice in a single digest email.
    """
    seen = set()
    deduped = []
    for job in jobs:
        if job["job_id"] in seen:
            continue
        seen.add(job["job_id"])
        deduped.append(job)
    return deduped


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

def has_scrape_issues(company_results):
    return any(r["status"] != "ok" for r in company_results)


def build_scrape_status_text(company_results):
    issues = [r for r in company_results if r["status"] != "ok"]
    ok_count = len(company_results) - len(issues)

    lines = [f"Scrape status: {ok_count} of {len(company_results)} companies scraped cleanly.\n"]

    if issues:
        lines.append("Issues:")
        for r in issues:
            label = r["status"].upper()
            detail = f" — {r['detail']}" if r.get("detail") else ""
            lines.append(f"  - {r['name']}: {label}{detail}")
        lines.append("")

    lines.append("All companies:")
    for r in company_results:
        suffix = f" ({r['status'].upper()})" if r["status"] != "ok" else ""
        line = f"  {r['name']}: {r['scraped']} job(s){suffix}"
        if r.get("detail") and r["status"] == "ok":
            line += f" — {r['detail']}"
        lines.append(line)

    return "\n".join(lines)


def build_scrape_status_html(company_results):
    issues = [r for r in company_results if r["status"] != "ok"]
    ok_count = len(company_results) - len(issues)

    rows = []
    for r in company_results:
        color = "#c0392b" if r["status"] != "ok" else "#2d2d2d"
        detail = _esc(r.get("detail") or "")
        rows.append(
            "<tr>"
            f"<td style='padding:4px 10px;border-bottom:1px solid #eee;color:{color};'>{_esc(r['name'])}</td>"
            f"<td style='padding:4px 10px;border-bottom:1px solid #eee;color:{color};'>{_esc(r['status'].upper())}</td>"
            f"<td style='padding:4px 10px;border-bottom:1px solid #eee;'>{r['scraped']}</td>"
            f"<td style='padding:4px 10px;border-bottom:1px solid #eee;color:{color};'>{detail}</td>"
            "</tr>"
        )

    return f"""
    <h3>Scrape status: {ok_count} of {len(company_results)} companies scraped cleanly</h3>
    <table style="border-collapse:collapse;width:100%;margin-bottom:20px;">
      <thead>
        <tr style="text-align:left;background:#f5f5f5;">
          <th style="padding:4px 10px;">Company</th>
          <th style="padding:4px 10px;">Status</th>
          <th style="padding:4px 10px;">Jobs found</th>
          <th style="padding:4px 10px;">Detail</th>
        </tr>
      </thead>
      <tbody>
        {''.join(rows)}
      </tbody>
    </table>
    """


def build_email_body(new_jobs, company_results):
    lines_text = [f"{len(new_jobs)} new SDE1-relevant job(s) found:\n"]
    rows_html = []

    for job in sorted(new_jobs, key=lambda j: j["company"].lower()):
        posted = job.get("posted_date") or "N/A"
        location = job.get("location") or "N/A"
        lines_text.append(
            f"- [{job['company']}] {job['title']} ({location}, posted: {posted})\n  {job['url']}\n"
        )
        rows_html.append(
            "<tr>"
            f"<td style='padding:6px 10px;border-bottom:1px solid #eee;'>{_esc(job['company'])}</td>"
            f"<td style='padding:6px 10px;border-bottom:1px solid #eee;'>{_esc(job['title'])}</td>"
            f"<td style='padding:6px 10px;border-bottom:1px solid #eee;'>{_esc(location)}</td>"
            f"<td style='padding:6px 10px;border-bottom:1px solid #eee;'>{_esc(posted)}</td>"
            "<td style='padding:6px 10px;border-bottom:1px solid #eee;'>"
            f"<a href='{_esc(job['url'])}'>Apply</a></td>"
            "</tr>"
        )

    lines_text.append("\n" + build_scrape_status_text(company_results))
    text_body = "\n".join(lines_text)

    html_body = f"""
    <html>
      <body style="font-family:Arial,sans-serif;">
        <h2>{len(new_jobs)} new SDE1-relevant job(s)</h2>
        <table style="border-collapse:collapse;width:100%;margin-bottom:24px;">
          <thead>
            <tr style="text-align:left;background:#f5f5f5;">
              <th style="padding:6px 10px;">Company</th>
              <th style="padding:6px 10px;">Title</th>
              <th style="padding:6px 10px;">Location</th>
              <th style="padding:6px 10px;">Posted</th>
              <th style="padding:6px 10px;">Link</th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows_html)}
          </tbody>
        </table>
        {build_scrape_status_html(company_results)}
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


def send_digest_email(new_jobs, company_results):
    """
    Sends whenever there's something worth telling the user about: new
    jobs, OR at least one company that didn't scrape cleanly (timeout,
    error, or skipped for time budget). A silent run — nothing new and
    no scrape issues — is skipped, same as before. Without the issues
    check, a run that hit real errors but happened to match 0 new jobs
    would go completely unreported.
    """
    if not new_jobs and not has_scrape_issues(company_results):
        logger.info("No new relevant jobs and no scrape issues — skipping email send.")
        return

    if not SENDER_EMAIL or not RECIPIENT_EMAIL:
        logger.error(
            "SENDER_EMAIL and/or RECIPIENT_EMAIL environment variables are not "
            "set — cannot send digest email. (Remember both must be SES-"
            "verified while SES is in sandbox mode.)"
        )
        return

    recipients = [addr.strip() for addr in RECIPIENT_EMAIL.split(",") if addr.strip()]
    text_body, html_body = build_email_body(new_jobs, company_results)

    issues_count = sum(1 for r in company_results if r["status"] != "ok")
    subject = f"SDE1 Job Digest — {len(new_jobs)} new posting(s)"
    if issues_count:
        subject += f" ({issues_count} company issue{'s' if issues_count != 1 else ''})"

    ses = get_ses_client()
    try:
        ses.send_email(
            Source=SENDER_EMAIL,
            Destination={"ToAddresses": recipients},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
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
# 6. Step Functions handlers
# ---------------------------------------------------------------------------
#
# Three Lambda functions, all pointing at this same module, orchestrated
# by a Step Functions state machine (state_machine.asl.json):
#
#   LoadConfig (config_loader_handler)
#         │  output: {"companies": [...], "include_keywords": [...],
#         │           "exclude_keywords": [...]}
#         ▼
#   ScrapeAllCompanies — Map state, one worker_handler invocation per
#   company, run in parallel (MaxConcurrency 10)
#         │  output: [worker_handler result, ...] — one per company
#         ▼
#   SendDigest (aggregator_handler)
#
# Splitting scraping into one Lambda invocation per company is what
# keeps each invocation short regardless of how many companies are
# configured — the old single-Lambda version looped over every company
# sequentially in one invocation, so total runtime (and risk of hitting
# Lambda's 15-minute hard ceiling) grew with the company count.

class ScrapeTimeoutError(BaseException):
    """
    Deliberately extends BaseException, not Exception — like
    KeyboardInterrupt/SystemExit. Several places in the scraping code
    (e.g. PlaywrightRenderer._new_page's "browser died, relaunch and
    retry" logic) have broad `except Exception:` handlers that would
    otherwise silently swallow this as just another scrape failure,
    defeating the whole point of a hard timeout: the retry they'd fall
    into has no timeout of its own, so the hang just continues instead
    of actually being interrupted.
    """


def _run_with_timeout(func, timeout_seconds, *args, **kwargs):
    """
    Hard wall-clock timeout via SIGALRM — interrupts blocking native
    calls (socket reads, subprocess waits) that a plain Python-level
    timeout can't reach, which is what a hung Playwright/Chromium call
    actually needs. Only safe in the main thread on Unix, which is how
    the Lambda runtime invokes the handler.
    """
    def _handler(signum, frame):
        raise ScrapeTimeoutError(f"Timed out after {timeout_seconds}s")

    old_handler = signal.signal(signal.SIGALRM, _handler)
    signal.alarm(timeout_seconds)
    try:
        return func(*args, **kwargs)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def config_loader_handler(event, context):
    """
    Step Functions first state (LoadConfig). Fetches and parses the
    Google Sheet CSV — thin wrapper around the existing load_config().
    Its return value becomes the Map state's input, so ItemsPath
    ("$.companies") and the per-item keyword params in the state
    machine definition both read directly off this shape.
    """
    return load_config(CONFIG_CSV_URL)


def worker_handler(event, context):
    """
    Step Functions Map-state worker (ScrapeAllCompanies). Scrapes and
    filters exactly one company per invocation.

    event: {"name": str, "url": str, "include_keywords": [str],
             "exclude_keywords": [str]}

    Returns a uniformly-shaped result — this is also what
    aggregator_handler uses to reconstruct the email's per-company
    "Scrape status" section, so the shape mirrors the old lambda_handler
    loop's company_results entries:
        {"company": str, "status": "ok"|"timeout"|"error",
         "detail": str|None, "scraped": int, "jobs": [...]}

    "scraped" is the raw pre-filter count, deliberately distinct from
    len(jobs) (the post-filter/relevant count) — this is what lets the
    digest email distinguish "this site is bot-blocked and returned
    nothing but nav-link noise" (scraped > 0, jobs == []) from "this
    site genuinely has zero matching openings right now".

    A fresh PlaywrightRenderer is created and torn down within this one
    invocation — no cross-company reuse to manage, since each company
    already gets its own invocation. A stuck company can no longer
    affect any other company's scrape or consume shared run time.
    """
    name = event["name"]
    url = event["url"]
    include_keywords = event.get("include_keywords", [])
    exclude_keywords = event.get("exclude_keywords", [])

    renderer = PlaywrightRenderer()
    try:
        company_jobs, tier = _run_with_timeout(
            scrape_company, PER_COMPANY_TIMEOUT_SECONDS, {"name": name, "url": url}, renderer
        )
        company_jobs = _dedupe_by_job_id(company_jobs)
        relevant_jobs = filter_jobs(company_jobs, include_keywords, exclude_keywords)
        detail = (
            "generic fallback (best-effort selectors, no known data "
            "source for this site) — may include non-job links (nav, "
            "language switchers) rather than real postings, especially "
            "on bot-protected sites"
        ) if tier == "generic" else None
        logger.info(
            "Scraped %d job(s) from '%s' (tier=%s), %d relevant",
            len(company_jobs), name, tier, len(relevant_jobs),
        )
        return {
            "company": name, "status": "ok", "detail": detail,
            "scraped": len(company_jobs), "jobs": relevant_jobs,
        }
    except ScrapeTimeoutError:
        logger.error(
            "Company '%s' exceeded the %ds per-company timeout.", name, PER_COMPANY_TIMEOUT_SECONDS,
        )
        return {
            "company": name, "status": "timeout",
            "detail": f"exceeded {PER_COMPANY_TIMEOUT_SECONDS}s per-company timeout",
            "scraped": 0, "jobs": [],
        }
    except Exception as exc:  # noqa: BLE001 - never let one company's failure crash this invocation
        logger.error("Worker failed scraping '%s': %s", name, exc, exc_info=True)
        return {
            "company": name, "status": "error", "detail": str(exc)[:200],
            "scraped": 0, "jobs": [],
        }
    finally:
        try:
            _run_with_timeout(renderer.close, 30)
        except BaseException:  # noqa: BLE001 - best-effort cleanup, must never fail the invocation
            pass


def aggregator_handler(event, context):
    """
    Step Functions final state (SendDigest). `event` is the Map state's
    collected output — a list of worker_handler results, one per
    company. This includes both application-level failures
    worker_handler already handled gracefully (status "timeout"/"error")
    and, if the state machine's Map-level Catch fired (a company whose
    Lambda invocation itself failed outright — e.g. an unhandled crash),
    a same-shaped fallback entry produced by the state machine itself.

    Flattens the already-filtered per-company job lists, dedupes against
    DynamoDB, sends the digest email, and records newly notified jobs —
    identical logic to the old lambda_handler's tail end, just now
    consuming pre-scraped results instead of scraping itself.
    """
    worker_results = event if isinstance(event, list) else event.get("results", [])

    all_relevant_jobs = []
    company_results = []
    for r in worker_results:
        company_results.append({
            "name": r.get("company", "unknown"),
            "scraped": r.get("scraped", 0),
            "status": r.get("status", "error"),
            "detail": r.get("detail"),
        })
        all_relevant_jobs.extend(r.get("jobs", []))

    table = get_ddb_table()
    new_jobs = dedupe_against_dynamodb(table, all_relevant_jobs)
    logger.info("%d of %d relevant jobs are new (not previously notified)", len(new_jobs), len(all_relevant_jobs))

    send_digest_email(new_jobs, company_results)

    for job in new_jobs:
        write_seen_job(table, job)

    result = {
        "scraped": sum(r["scraped"] for r in company_results),
        "relevant": len(all_relevant_jobs),
        "new_jobs": len(new_jobs),
    }
    logger.info("SDE1 job scraper run complete: %s", result)
    return result
