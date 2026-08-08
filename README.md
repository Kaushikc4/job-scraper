# SDE1 Job Scraper

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Building and Deploying](#building-and-deploying)
- [Scheduling (EventBridge)](#scheduling-eventbridge)
- [Manual Testing](#manual-testing)
- [Monitoring and Logs](#monitoring-and-logs)
- [Known Limitations and Troubleshooting](#known-limitations-and-troubleshooting)
- [Costs](#costs)

## Overview

Scrapes career pages for a configurable list of companies, filters postings down to SDE1-relevant roles located in India or Remote, and emails a digest of newly found matches. Runs on a schedule via EventBridge; already-notified postings are deduplicated in DynamoDB so nothing gets emailed twice.

## Architecture

```
Google Sheet (config, published as CSV)
        │
        ▼
Lambda: scrape → filter (keyword + India/Remote) → dedupe
        │                                      │
        ▼                                      ▼
   DynamoDB (seen-job dedup, TTL)          SES (digest email)
```

Deploys as a **container image**, not a zip package. Playwright's headless Chromium binary and its OS-level dependencies exceed Lambda's 250MB zip/layer limit, and Playwright's own dependency installer only supports Debian/Ubuntu — not Amazon Linux, which the zip runtime uses. The image is built on Playwright's own Ubuntu-based base image instead, with the AWS Lambda Runtime Interface Client (`awslambdaric`) layered on top.

Scraping uses four tiers per company, cheapest/most reliable first:

1. **Known ATS/company API** (Greenhouse, Lever, Workday, SmartRecruiters, Amazon, Microsoft) — direct JSON API call, no browser.
2. **Phenom People-powered pages** (e.g. Adobe) — job data is embedded as JSON in the raw HTML; parsed with a plain HTTP GET, no browser.
3. **Known-site Playwright extractor** (currently just Google) — headless browser required, but the extraction and pagination logic is site-specific and verified.
4. **Generic Playwright fallback** — last resort for any company not covered above; best-effort selectors, single page only, output quality varies (see [Known Limitations](#known-limitations-and-troubleshooting)).

## Prerequisites

One-time setup, before the function will work:

- [ ] Google Sheet published to the web as CSV (File → Share → Publish to web → CSV).
- [ ] SES: `SENDER_EMAIL` and `RECIPIENT_EMAIL` addresses verified as identities in SES, **in the same region as the Lambda function** (SES is in sandbox mode by default — both ends must be verified or sending fails).
- [ ] DynamoDB table `job_scraper_seen_jobs` — partition key `job_id` (String), TTL enabled on attribute `ttl`.
- [ ] IAM role for the Lambda with:
  - `AWSLambdaBasicExecutionRole` (managed policy — CloudWatch Logs)
  - Inline policy granting `ses:SendEmail`, `ses:SendRawEmail` (`Resource: "*"`) and `dynamodb:GetItem`, `dynamodb:PutItem` scoped to the table's ARN

## Configuration

### Google Sheet CSV

Three columns: `type`, `value1`, `value2`.

| type | value1 | value2 |
|---|---|---|
| `company` | Company name | Career page URL |
| `include` | Keyword that must appear in a job title | *(unused)* |
| `exclude` | Keyword that disqualifies a job title | *(unused)* |

Matching is case-insensitive **substring** matching, not exact/whole-word — e.g. excluding `lead` would also reject a title merely containing "lead" anywhere in it. The sheet is re-fetched on every run; no redeploy needed to add companies or tune keywords.

### Lambda environment variables

| Variable | Required | Default | Notes |
|---|---|---|---|
| `CONFIG_CSV_URL` | Yes | — | Published Google Sheet CSV link |
| `SENDER_EMAIL` | Yes | — | Must be SES-verified |
| `RECIPIENT_EMAIL` | Yes | — | Must be SES-verified; comma-separate for multiple |
| `SEEN_JOBS_TABLE` | No | `job_scraper_seen_jobs` | DynamoDB table name |
| `SEEN_JOB_TTL_DAYS` | No | `75` | Days before a dedup entry expires |
| `MAX_JOBS_PER_COMPANY` | No | `100` | Pagination cap per company per run |
| `PER_COMPANY_TIMEOUT_SECONDS` | No | `90` | Hard wall-clock cap per company; a stuck company is force-skipped rather than hanging the whole run |
| `REMAINING_TIME_BUFFER_MS` | No | `60000` | Stop starting new companies once less than this much invocation time remains |
| `SES_REGION` / `DDB_REGION` | No | Lambda's own region | Override if SES/DynamoDB live in a different region |

## Building and Deploying

```bash
export ACCOUNT_ID=<your-account-id>
export REGION=<your-region>
export REPO=$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/sde1-job-scraper

# One-time: create the ECR repo
aws ecr create-repository --repository-name sde1-job-scraper --region $REGION

# Authenticate Docker to ECR (token expires ~12h, repeat as needed)
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $REPO

# Build and push
docker buildx build \
  --platform linux/amd64 \
  --provenance=false \
  --sbom=false \
  -t $REPO:latest \
  --push .

# Point the Lambda function at the new image
aws lambda update-function-code \
  --function-name sde1-job-scraper \
  --image-uri $REPO:latest \
  --region $REGION
```

`--platform linux/amd64 --provenance=false --sbom=false` are required, not optional — a plain `docker build`/`docker push` produces an OCI manifest list with attestations that Lambda rejects outright (`InvalidParameterValueException: ... image manifest ... is not supported`).

Lambda picks up a new `:latest` push automatically on the *next* invocation after `update-function-code` completes — there's no separate "activate" step. For anything beyond personal use, pin to a specific image digest (`$REPO@sha256:...`) instead of `:latest`, so a bad push can't silently change behavior on the next scheduled run.

## Scheduling (EventBridge)

Configured via **EventBridge Scheduler** (not classic EventBridge Rules), currently `rate(4 hours)`.

To change frequency: EventBridge console → Scheduler → select the schedule → edit the schedule expression → Save. No redeploy needed. CLI equivalent:

```bash
aws scheduler update-schedule \
  --name sde1-job-scraper-schedule \
  --schedule-expression "rate(4 hours)" \
  --region $REGION
```

## Manual Testing

```bash
aws lambda invoke \
  --function-name sde1-job-scraper \
  --region $REGION \
  --payload '{}' \
  --cli-binary-format raw-in-base64-out \
  response.json

cat response.json
```

The function ignores its input payload, so `{}` is fine. Check results in CloudWatch Logs (see below) — for a quick pass/fail read on a single recent invocation, searching/filtering the log group directly is faster and more reliable than CloudWatch Logs Insights.

## Monitoring and Logs

- **Lambda console** → function → **Monitor** tab — invocation count, error count, duration graphs.
- **CloudWatch Logs** → log group `/aws/lambda/sde1-job-scraper` → most recent log stream.
- Useful filter patterns within a log stream:
  - `run complete` — successful completion summary (`scraped`/`relevant`/`new_jobs` counts)
  - `ERROR` — any per-company or system-level failure
  - `Task timed out` — hit the Lambda's own configured timeout (not the per-company one)

## Known Limitations and Troubleshooting

- **Bot-protected sites**: Atlassian (reCAPTCHA) and Salesforce (Akamai WAF, "Access Denied") are confirmed blocked. Both fall through to the generic fallback tier, which can return a non-zero job count that's actually nav links/noise, not real postings — check the per-company detail in the digest email before assuming a "0 in digest, N scraped" mismatch is a bug.
- **Uber and Flipkart** have no dedicated scraper tier and rely on the generic fallback — results are inconsistent by nature of that tier, not a specific known defect.
- **Per-company timeout**: a company that hangs (headless Chromium against an unresponsive page) is force-skipped after `PER_COMPANY_TIMEOUT_SECONDS` (default 90s) rather than consuming the whole invocation. Lambda's own function timeout is 600s (10 min), memory 2048MB.
- **Docker push fails with "image manifest ... not supported"**: missing the `--platform linux/amd64 --provenance=false --sbom=false` buildx flags — see [Building and Deploying](#building-and-deploying).
- **`ModuleNotFoundError: playwright`** at runtime: the Dockerfile installs `playwright` explicitly via pip — the base image's Chromium binaries alone aren't enough, the Python package must be installed too. `requirements.txt` is for local development (`test_local.py`) only; the Dockerfile has its own explicit `pip install` line and does not read `requirements.txt`.

## Costs

At this run frequency and job volume, Lambda, DynamoDB (on-demand billing), EventBridge Scheduler, and SES all fall within AWS free-tier or negligible pay-as-you-go cost. If the schedule frequency, company list, or `MAX_JOBS_PER_COMPANY` increase significantly, check AWS Budgets to confirm costs stay where expected.
