# SDE1 Job Scraper (AWS Lambda)

Scrapes configured company career pages, filters titles for SDE1-relevant
roles, deduplicates against DynamoDB, and emails a digest via SES.
Triggered on a schedule by EventBridge (twice daily).

## Files

- `lambda_function.py` — the Lambda handler and all logic.
- `requirements.txt` — third-party dependencies to bundle (`requests`,
  `beautifulsoup4`, `playwright`). `boto3` is not listed because it's
  included in the Lambda Python runtime by default.
- `Dockerfile` — builds the container image this function now requires
  (see "Packaging & deploying" — Playwright's Chromium binary is too
  large for a plain zip package).

## Scraping strategy (four tiers, cheapest first)

For each company row, `scrape_company()` tries, in order:

1. **Known ATS API** — Greenhouse, Lever, Workday, SmartRecruiters,
   Amazon, or Microsoft, detected from the URL. A single lightweight
   HTTP call, paginated (see below).
2. **Phenom People detection** — some career pages (e.g. Adobe) look
   JS-rendered but actually embed the full job list as JSON server-side
   (`phApp.ddo = {...}`). Detected with a plain HTTP GET, no browser,
   paginated by re-fetching with `&from={offset}&s=1`.
3. **Known-site Playwright extractor** — currently just Google, whose
   job cards have no real `<a href>` at all (title is JS-populated into
   an `<h3>`, and the job ID lives in a `jsdata` attribute used to build
   a direct URL). Paginated by clicking the results table's "next page"
   button (Google's results are a paged table, not infinite scroll).
4. **Generic Playwright fallback** — last resort: render with headless
   Chromium, then apply best-effort anchor-tag heuristics (preferring
   `aria-label` text over concatenated visible text, since many SPA job
   cards jam title+location+date into one text block with no
   separators). One Chromium instance is launched per Lambda invocation
   and reused across every company that needs it. **Single page only —
   no pagination in this tier** (see Uber note below for why).

### Pagination

Every tier-1/2/3 scraper now pages through results up to
`MAX_JOBS_PER_COMPANY` (env var, default 100) instead of returning only
the first page — this matters because e.g. Amazon reported **2,037**
total hits and Adobe **384** for a broad "software engineer" query, so
a single unpaginated page was a tiny, arbitrary slice. Each company's
own scraped-list is deduped by `job_id` before filtering
(`_dedupe_by_job_id`), since some sources (Adobe's search API,
observed in testing) can return overlapping results across pages if
result ordering shifts between fetches — without that, the same
posting could show up twice in one digest email.

**Uber pagination was investigated and found infeasible to do
reliably:** its "next page" link only works via a genuine click within
an already-loaded session — direct navigation to the page-N URL (even
via `page.goto()` in the same browser context) gets redirected to a
bot-detection challenge (`def.uber.com/en/challenge`). Clicking the
link itself is also unreliable: an intermittent dialog overlay
(promotional popup) blocks the click, and the button's locator wasn't
consistently findable across otherwise-identical runs. Since a broken
pagination attempt risked returning *fewer* jobs than the working
single-page baseline, Uber was left on the generic single-page
fallback rather than shipping something flaky.

**Verified against live sites during development** (see git history /
dev notes, not re-run automatically): Amazon and Microsoft (tier 1),
Adobe (tier 2), and Google (tier 3) all paginate cleanly and return
real job titles, direct links, and (where available) posted dates —
each hit the `MAX_JOBS_PER_COMPANY` cap with 100% unique job IDs in
testing (Adobe needed the dedup fix to get there; Amazon/Microsoft/
Google didn't). Uber (tier 4, single page) returns ~10-13 real jobs
per run but rendering was observed to be flaky in constrained/
resource-limited environments — see the Dockerfile/memory notes below.

**Confirmed dead ends, not fixable by this architecture:**
- **Atlassian** — serves an invisible reCAPTCHA that blocks headless
  Chromium from ever loading real job data. Watch out: the generic
  fallback can occasionally produce a *false-positive* match here too —
  it once picked up a URL that looked job-shaped
  (`/company/careers/details/{id}`) but actually led to an empty
  generic page, not a real posting. Sanity-check any Atlassian entries
  that do show up in the digest.
- **Salesforce** — Akamai WAF returns "Access Denied" to the headless
  browser outright.
- Both would need deliberate anti-bot-detection techniques, which are
  intentionally not included here.

**Unresolved:** Flipkart (TurboHire) — the real job list lives one
click deeper than the configured URL (a "View All Jobs" link), and even
after navigating there directly, jobs never appeared within a
reasonable wait (tried up to ~20s and multiple wait strategies). May
need a longer wait, a different navigation path, or interacting with
on-page filters — needs further investigation.

Companies that fall through all tiers unresolved will keep logging the
"may need custom selector tuning" warning and returning 0 jobs until
addressed with a dedicated fix (following the same pattern as
`scrape_google`).

## Location filtering (India + Remote only)

Every scraper now captures a `location` field alongside title/URL/posted
date, and `filter_jobs()` requires a job to match **both** the keyword
filter **and** `is_india_or_remote(location)` — jobs with no location
mentioning "India" (or the ISO code "IND") or "Remote" are dropped, and
jobs with no location data at all are dropped too (rather than assumed
in-scope, since silently keeping unknowns would defeat the point).
Matching is whole-word (`\bindia\b|\bind\b`), not substring, so e.g.
"Indiana, USA" doesn't false-positive.

Location extraction quality varies by tier:
- **Amazon, Adobe, Microsoft, Google** — clean, structured location
  data straight from their APIs/embedded JSON. Reliable.
- **Generic Playwright fallback** (Uber, and anything else on that
  tier) — best-effort only (`_find_location`), same caveat as the
  fallback tier's title/date extraction: there's no consistent markup
  to key off across arbitrary companies, so it can miss real India/
  remote jobs whose location text doesn't match the fallback's loose
  pattern.

One real bug found and fixed while wiring this up: Amazon's own search
page URL param (`loc_query=India`) turns out to **not** filter results
server-side at all (confirmed by testing — same global result set with
or without it). The actual working param is `country=IND` (ISO alpha-3
code), which `scrape_amazon()` now uses instead whenever the configured
URL's `loc_query` mentions India. Without this fix, the location filter
would still produce correct *output* (bad matches get filtered out
downstream either way) but would waste most of the `MAX_JOBS_PER_COMPANY`
pagination budget fetching non-India jobs that just get discarded.

## 1. Google Sheet config format

Create a sheet with exactly these three columns: `type`, `value1`, `value2`.

| type    | value1              | value2                          |
|---------|---------------------|----------------------------------|
| company | Google              | https://boards.greenhouse.io/... |
| company | Amazon              | https://amazon.jobs/...          |
| include | software engineer   |                                  |
| include | sde                 |                                  |
| exclude | senior              |                                  |
| exclude | staff               |                                  |

Then: **File → Share → Publish to web → select the sheet/tab → CSV**.
Copy the resulting link into the `CONFIG_CSV_URL` Lambda environment
variable (there's a placeholder constant at the top of
`lambda_function.py` too, but the env var takes priority and means you
never have to touch code after deploying).

The script re-fetches this CSV on every run — no caching — so you can
add/remove companies or tune keywords at any time without redeploying.

## Scrape status in the email

Every email includes a "Scrape status" section showing, per company:
whether it scraped cleanly (`ok`), hit the per-company timeout
(`timeout`), raised an unexpected error (`error`), or wasn't attempted
because the run was low on remaining time (`skipped`) — plus a detail
message for anything that wasn't `ok` (e.g. "exceeded 90s per-company
timeout", or the exception message for an `error`).

This is also why an email can now go out with **zero new jobs**: if any
company had a non-`ok` status, an email still sends so the issue is
visible without having to check CloudWatch Logs. A fully clean run with
nothing new still sends nothing, same as before.

**A `status: ok` company can still be unreliable.** The status only
means "the scrape function didn't error or time out" — it says nothing
about whether the data was real. Companies on the generic Playwright
fallback tier (no known ATS/API for that site) get an explicit caveat
in their detail column even when `status: ok`, e.g. Atlassian's
reCAPTCHA block means the fallback's best-effort selectors pick up nav
links and the language switcher instead of real postings — the scrape
"succeeds" (no error), returns a non-zero count, and the keyword filter
correctly rejects all of it before it reaches the digest. Without this
caveat that combination reads as a mystery ("27 jobs found, 0 in the
digest — is something broken?"); with it, the email explains itself.

## 2. SES sandbox setup (required before email will work)

New AWS accounts have SES in **sandbox mode**: you can only send to/from
addresses you've explicitly verified.

1. AWS Console → SES → Verified identities → Create identity → Email address.
2. Do this for **both** `SENDER_EMAIL` and `RECIPIENT_EMAIL`.
3. Click the confirmation link AWS emails to each address.
4. Until both are verified, `send_email` calls fail with
   "Email address is not verified".

(To email arbitrary recipients without per-address verification, request
production access to move SES out of the sandbox — out of scope here.)

## 3. DynamoDB table

- Name: `job_scraper_seen_jobs` (override via `SEEN_JOBS_TABLE` env var).
- Partition key: `job_id` (String).
- Enable TTL on the `ttl` attribute (Number, epoch seconds) so old
  entries auto-expire — this script writes `ttl = now + SEEN_JOB_TTL_DAYS`
  (default 75 days) on every new notification.

Create it via CLI:

```bash
aws dynamodb create-table \
  --table-name job_scraper_seen_jobs \
  --attribute-definitions AttributeName=job_id,AttributeType=S \
  --key-schema AttributeName=job_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

aws dynamodb update-time-to-live \
  --table-name job_scraper_seen_jobs \
  --time-to-live-specification "Enabled=true, AttributeName=ttl"
```

## 4. Lambda environment variables

| Variable            | Required | Default                  | Notes                                   |
|----------------------|----------|---------------------------|------------------------------------------|
| `CONFIG_CSV_URL`     | yes      | placeholder in code       | published Google Sheet CSV link          |
| `SENDER_EMAIL`       | yes      | —                          | must be SES-verified                     |
| `RECIPIENT_EMAIL`    | yes      | —                          | must be SES-verified; comma-separate for multiple |
| `SEEN_JOBS_TABLE`    | no       | `job_scraper_seen_jobs`   |                                           |
| `SEEN_JOB_TTL_DAYS`  | no       | `75`                       | how long dedup entries live              |
| `MAX_JOBS_PER_COMPANY` | no     | `100`                      | pagination cap per company per run       |
| `PER_COMPANY_TIMEOUT_SECONDS` | no | `90`                    | hard wall-clock cap per company (see below) |
| `REMAINING_TIME_BUFFER_MS` | no  | `60000`                    | stop starting new companies once less than this remains, so there's time to send whatever was found |

## 5. IAM permissions for the Lambda execution role

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["ses:SendEmail", "ses:SendRawEmail"],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["dynamodb:GetItem", "dynamodb:PutItem"],
      "Resource": "arn:aws:dynamodb:REGION:ACCOUNT_ID:table/job_scraper_seen_jobs"
    },
    {
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:REGION:ACCOUNT_ID:*"
    }
  ]
}
```

(The logs permissions are also covered by attaching the AWS-managed
`AWSLambdaBasicExecutionRole` policy instead of writing them by hand.)

## 6. EventBridge schedule (twice daily)

Rate expression:

```
rate(12 hours)
```

or, to pin specific UTC times (e.g. 9am and 9pm UTC):

```
cron(0 9,21 * * ? *)
```

Point the rule's target at this Lambda function; no custom input payload
is needed — `event` is unused by the handler.

## 7. Packaging & deploying (container image — required now)

Playwright's Chromium binary plus its OS-level shared libraries are far
past the 250MB zip/layer limit for Lambda, and Playwright's own
`install-deps` helper only supports Debian/Ubuntu — not AWS's Amazon
Linux Lambda base. So this function now deploys as a **container image**
built from Playwright's own (Ubuntu-based) image, per the included
`Dockerfile`.

```bash
# 1. Build the image
docker build -t sde1-job-scraper .

# 2. Create an ECR repo (one-time) and push
aws ecr create-repository --repository-name sde1-job-scraper
aws ecr get-login-password --region REGION | docker login --username AWS \
  --password-stdin ACCOUNT_ID.dkr.ecr.REGION.amazonaws.com

docker tag sde1-job-scraper:latest ACCOUNT_ID.dkr.ecr.REGION.amazonaws.com/sde1-job-scraper:latest
docker push ACCOUNT_ID.dkr.ecr.REGION.amazonaws.com/sde1-job-scraper:latest

# 3. Create the function from the pushed image
aws lambda create-function \
  --function-name sde1-job-scraper \
  --package-type Image \
  --code ImageUri=ACCOUNT_ID.dkr.ecr.REGION.amazonaws.com/sde1-job-scraper:latest \
  --role arn:aws:iam::ACCOUNT_ID:role/sde1-job-scraper-role \
  --timeout 120 \
  --memory-size 2048 \
  --environment "Variables={CONFIG_CSV_URL=...,SENDER_EMAIL=...,RECIPIENT_EMAIL=...}"
```

Notes on the numbers above:
- **Timeout 120s / memory 2048MB**: rendering several JS-heavy pages
  sequentially in one run is much slower and more memory-hungry than
  plain HTTP calls were. Tune based on how many companies actually fall
  through to the Playwright tier — pure-API companies (Amazon, ATS
  boards) don't need this headroom, but if even one company needs
  Playwright, the whole invocation does. In production this hit the
  full function timeout (600s, at whatever value was configured) with
  **zero output** — see the per-company timeout note below for the fix;
  after that fix, 600s (10 min) is a comfortable configured timeout,
  since no single company can now consume more than
  `PER_COMPANY_TIMEOUT_SECONDS` (default 90s).
- Cold starts will be noticeably slower (multi-second) than the old zip
  deployment, since the image is much larger (~1-2GB with Chromium
  bundled in). This is a real cost/latency trade-off versus a leaner
  company list that only uses ATS/Phenom-tier scraping.

**Per-company timeout (important reliability fix):** a stuck company
used to be able to hang the *entire* invocation until the Lambda's
configured timeout killed it — and since email-sending and DynamoDB
writes only happen after the whole scraping loop finishes, that meant
losing every result, even from companies that scraped successfully
before the hang. `lambda_handler` now wraps each company's scrape in a
hard `SIGALRM`-based wall-clock timeout (`PER_COMPANY_TIMEOUT_SECONDS`,
default 90s) — chosen over Playwright's own per-call timeouts because
some operations (notably `page.close()`/`new_page()` against an
unresponsive/zombie browser process) were observed to hang indefinitely
with no timeout of their own.

**A subtle first attempt at this fix didn't actually work** — confirmed
in production logs: the timeout fired correctly after ~90s, but
`PlaywrightRenderer._new_page()` has a broad `except Exception:` (its
own "browser died, relaunch and retry" logic) that silently caught the
timeout exception too, treated it as just another failure, and retried
— with **no timeout on the retry**, so the hang continued anyway until
the outer Lambda timeout eventually killed everything ~470s later.
Fixed by making `ScrapeTimeoutError` extend `BaseException` instead of
`Exception` (the same reason `KeyboardInterrupt`/`SystemExit` do this in
Python itself) so it can't be swallowed by any broad `except Exception:`
in the call chain — verified by reproducing the exact broad-except/
retry pattern in a test and confirming the timeout now propagates
through it correctly. On a timeout, the renderer is also discarded and
recreated before moving to the next company, since forcibly interrupting
a blocking Chromium IPC call (rather than cleanly cancelling it) could
leave it in a corrupted state.

It also checks `context.get_remaining_time_in_millis()`
before starting each company and stops early if there isn't enough
runway left (`REMAINING_TIME_BUFFER_MS`, default 60s buffer) — so a run
that's running low on time still sends whatever it found instead of
timing out with nothing.

For subsequent code updates: rebuild, re-push with a new tag (or
`:latest`), then:

```bash
aws lambda update-function-code \
  --function-name sde1-job-scraper \
  --image-uri ACCOUNT_ID.dkr.ecr.REGION.amazonaws.com/sde1-job-scraper:latest
```

## Notes / known limitations

- **Playwright fallback tier** (`scrape_generic_playwright`) is
  best-effort, same as the old plain-HTML fallback was — it applies
  anchor/class heuristics against the rendered DOM and will likely need
  per-company selector tuning. A warning is logged whenever it's used.
- **Bot-protected career pages** (e.g. Atlassian's, which serves an
  invisible reCAPTCHA that blocks stock headless Chromium from ever
  loading real job data) will not work through this fallback — that
  needs deliberate anti-detection work not included here.
- **Workday** career sites are normally JS-rendered; this script instead
  calls the underlying `cxs/.../jobs` JSON search endpoint directly,
  which works for most tenants but URL structure (tenant/site names)
  can vary — if a Workday company returns zero jobs, double-check the
  derived `tenant`/`site` values against the real career page URL.
- **Amazon and Microsoft** are handled without any browser via direct
  (undocumented, but public and unauthenticated) JSON APIs — Microsoft's
  was found by inspecting network requests, not any published docs, so
  it could change without notice. **Phenom-People-powered sites** (e.g.
  Adobe) are also browser-free — an embedded-JSON parse off the raw
  HTML. None of these need the Playwright tier at all.
- Relevance filtering is keyword include/exclude matching on job titles
  (no LLM classification, no description parsing — intentional for this
  version), combined with the India/Remote location filter described
  above.
