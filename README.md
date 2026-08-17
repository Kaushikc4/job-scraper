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

Scrapes career pages for a configurable list of companies, filters postings down to SDE1-relevant roles located in India or Remote, and emails a digest of newly found matches. Runs on a schedule via EventBridge, orchestrated by a Step Functions state machine that scrapes each company in its own parallel Lambda invocation; already-notified postings are deduplicated in DynamoDB so nothing gets emailed twice.

## Architecture

```
                    Google Sheet (config, published as CSV)
                              │
                              ▼
                   ┌─ Lambda: sde1-config-loader ─┐
                   │  fetch + parse CSV            │
                   └────────────┬───────────────────┘
                                 │ {companies, include_keywords, exclude_keywords}
                                 ▼
        Step Functions Map state (MaxConcurrency 10)
        ┌──────────────┬──────────────┬──────────────┐
        ▼              ▼              ▼              ▼
   Lambda: sde1-worker (one invocation per company)
   scrape → keyword + India/Remote filter → return matches
        │              │              │              │
        └──────────────┴──────────────┴──────────────┘
                                 │ [worker result, ...]
                                 ▼
                   ┌─ Lambda: sde1-aggregator ─────┐
                   │  flatten → dedupe (DynamoDB)   │
                   │  → send digest (SES)           │
                   └─────────────────────────────────┘
```

Three Lambda functions, all built from **one container image** (differentiated only by which handler each function's `--image-config Command` points at — no separate Dockerfiles), orchestrated by Step Functions instead of one Lambda looping over every company sequentially. This is what keeps each invocation short regardless of company count: the old single-Lambda version's total runtime scaled with the number of companies and risked the 15-minute Lambda hard ceiling as more got added; now each company scrapes independently and in parallel.

Deploys as a **container image**, not a zip package — unchanged from before. Playwright's headless Chromium binary and its OS-level dependencies exceed Lambda's 250MB zip/layer limit, and Playwright's own dependency installer only supports Debian/Ubuntu, not Amazon Linux (the zip runtime's base). The image is built on Playwright's own Ubuntu-based base image, with the AWS Lambda Runtime Interface Client (`awslambdaric`) layered on top.

Per-company scraping (inside `sde1-worker`) uses four tiers, cheapest/most reliable first:

1. **Known ATS/company API** (Greenhouse, Lever, Workday, SmartRecruiters, Amazon, Microsoft) — direct JSON API call, no browser.
2. **Phenom People-powered pages** (e.g. Adobe) — job data is embedded as JSON in the raw HTML; parsed with a plain HTTP GET, no browser.
3. **Known-site Playwright extractor** (currently just Google) — headless browser required, but the extraction and pagination logic is site-specific and verified.
4. **Generic Playwright fallback** — last resort for any company not covered above; best-effort selectors, single page only, output quality varies (see [Known Limitations](#known-limitations-and-troubleshooting)).

## Prerequisites

One-time setup, before the pipeline will work:

- [ ] Google Sheet published to the web as CSV (File → Share → Publish to web → CSV).
- [ ] SES: `SENDER_EMAIL` and `RECIPIENT_EMAIL` addresses verified as identities in SES, **in the same region as the Lambda functions** (SES is in sandbox mode by default — both ends must be verified or sending fails).
- [ ] DynamoDB table `job_scraper_seen_jobs` — partition key `job_id` (String), TTL enabled on attribute `ttl`.
- [ ] Three Lambda functions (`sde1-config-loader`, `sde1-worker`, `sde1-aggregator`) built from the same container image — see [Building and Deploying](#building-and-deploying).
- [ ] IAM roles:
  - `sde1-config-loader` and `sde1-worker`: `AWSLambdaBasicExecutionRole` only (neither touches DynamoDB or SES — scraping and config-loading don't need those permissions).
  - `sde1-aggregator`: `AWSLambdaBasicExecutionRole` + inline policy granting `ses:SendEmail`/`ses:SendRawEmail` (`Resource: "*"`) and `dynamodb:GetItem`/`dynamodb:PutItem` scoped to the table's ARN.
- [ ] A Step Functions state machine (`state_machine.asl.json`) with an execution role granting `lambda:InvokeFunction` on all three function ARNs.
- [ ] EventBridge Scheduler targeting the state machine (`states:StartExecution`), not the Lambda directly.

## Configuration

### Google Sheet CSV

Three columns: `type`, `value1`, `value2`. Unchanged from before — the refactor only touches orchestration, not this format.

| type | value1 | value2 |
|---|---|---|
| `company` | Company name | Career page URL |
| `include` | Keyword that must appear in a job title | *(unused)* |
| `exclude` | Keyword that disqualifies a job title | *(unused)* |

Matching is case-insensitive **substring** matching, not exact/whole-word — e.g. excluding `lead` would also reject a title merely containing "lead" anywhere in it. The sheet is re-fetched on every run; no redeploy needed to add companies or tune keywords.

### Lambda environment variables

Split across the three functions by what each actually uses:

| Variable | Function(s) | Required | Default | Notes |
|---|---|---|---|---|
| `CONFIG_CSV_URL` | config-loader | Yes | — | Published Google Sheet CSV link |
| `MAX_JOBS_PER_COMPANY` | worker | No | `100` | Pagination cap per company per run |
| `PER_COMPANY_TIMEOUT_SECONDS` | worker | No | `90` | Hard wall-clock cap on that company's scrape; it's force-returned as a `timeout` result rather than hanging the invocation |
| `SENDER_EMAIL` | aggregator | Yes | — | Must be SES-verified |
| `RECIPIENT_EMAIL` | aggregator | Yes | — | Must be SES-verified; comma-separate for multiple |
| `SEEN_JOBS_TABLE` | aggregator | No | `job_scraper_seen_jobs` | DynamoDB table name |
| `SEEN_JOB_TTL_DAYS` | aggregator | No | `75` | Days before a dedup entry expires |
| `SES_REGION` / `DDB_REGION` | aggregator | No | Lambda's own region | Override if SES/DynamoDB live in a different region |

## Building and Deploying

One image, three functions — build/push once, then create (or update) each function pointing at the same image with a different handler override.

```bash
export ACCOUNT_ID=<your-account-id>
export REGION=<your-region>
export REPO=$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/sde1-job-scraper

# --- One-time: build and push the shared image -----------------------

aws ecr create-repository --repository-name sde1-job-scraper --region $REGION

aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $REPO

docker buildx build \
  --platform linux/amd64 \
  --provenance=false \
  --sbom=false \
  -t $REPO:latest \
  --push .
```

`--platform linux/amd64 --provenance=false --sbom=false` are required, not optional — a plain `docker build`/`docker push` produces an OCI manifest list with attestations that Lambda rejects outright (`InvalidParameterValueException: ... image manifest ... is not supported`).

```bash
# --- One-time: minimal IAM role shared by config-loader + worker ------

aws iam create-role --role-name sde1-scraper-worker-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

aws iam attach-role-policy --role-name sde1-scraper-worker-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# --- Create the three functions ----------------------------------------

aws lambda create-function --function-name sde1-config-loader \
  --package-type Image \
  --code ImageUri=$REPO:latest \
  --image-config '{"Command":["lambda_function.config_loader_handler"]}' \
  --role arn:aws:iam::$ACCOUNT_ID:role/sde1-scraper-worker-role \
  --timeout 30 --memory-size 256 --region $REGION \
  --environment "Variables={CONFIG_CSV_URL=<your-published-csv-url>}"

aws lambda create-function --function-name sde1-worker \
  --package-type Image \
  --code ImageUri=$REPO:latest \
  --image-config '{"Command":["lambda_function.worker_handler"]}' \
  --role arn:aws:iam::$ACCOUNT_ID:role/sde1-scraper-worker-role \
  --timeout 150 --memory-size 2048 --region $REGION \
  --environment "Variables={MAX_JOBS_PER_COMPANY=100,PER_COMPANY_TIMEOUT_SECONDS=90}"

# Aggregator reuses the existing sde1-job-scraper-role (already has the
# right SES + DynamoDB permissions from the pre-refactor single Lambda)
aws lambda create-function --function-name sde1-aggregator \
  --package-type Image \
  --code ImageUri=$REPO:latest \
  --image-config '{"Command":["lambda_function.aggregator_handler"]}' \
  --role arn:aws:iam::$ACCOUNT_ID:role/sde1-job-scraper-role \
  --timeout 60 --memory-size 256 --region $REGION \
  --environment "Variables={SENDER_EMAIL=<verified-sender>,RECIPIENT_EMAIL=<verified-recipient>,SEEN_JOBS_TABLE=job_scraper_seen_jobs,SEEN_JOB_TTL_DAYS=75}"
```

`sde1-worker`'s 150s timeout is deliberately generous headroom above `PER_COMPANY_TIMEOUT_SECONDS` (90s) plus renderer-cleanup time — still a small fraction of the old single-Lambda's 600s, since this function now only ever handles one company.

```bash
# --- Create the Step Functions execution role and state machine --------

aws iam create-role --role-name sde1-stepfunctions-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"states.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

aws iam put-role-policy --role-name sde1-stepfunctions-role \
  --policy-name invoke-scraper-lambdas \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": [
        "arn:aws:lambda:'"$REGION"':'"$ACCOUNT_ID"':function:sde1-config-loader",
        "arn:aws:lambda:'"$REGION"':'"$ACCOUNT_ID"':function:sde1-worker",
        "arn:aws:lambda:'"$REGION"':'"$ACCOUNT_ID"':function:sde1-aggregator"
      ]
    }]
  }'

# Substitute REGION/ACCOUNT_ID into state_machine.asl.json before this step
sed -e "s/REGION/$REGION/g" -e "s/ACCOUNT_ID/$ACCOUNT_ID/g" \
  state_machine.asl.json > /tmp/state_machine.json

aws stepfunctions create-state-machine \
  --name sde1-job-scraper-workflow \
  --definition file:///tmp/state_machine.json \
  --role-arn arn:aws:iam::$ACCOUNT_ID:role/sde1-stepfunctions-role \
  --type STANDARD \
  --region $REGION
```

For subsequent code updates: rebuild/push the image once (as above), then for each function:

```bash
aws lambda update-function-code --function-name sde1-config-loader --image-uri $REPO:latest --region $REGION
aws lambda update-function-code --function-name sde1-worker --image-uri $REPO:latest --region $REGION
aws lambda update-function-code --function-name sde1-aggregator --image-uri $REPO:latest --region $REGION
```

Lambda picks up a new `:latest` push automatically on each function's *next* invocation after `update-function-code` completes — no separate "activate" step. For anything beyond personal use, pin to a specific image digest (`$REPO@sha256:...`) instead of `:latest` on all three functions, so a bad push can't silently change behavior on the next scheduled run.

## Scheduling (EventBridge)

Configured via **EventBridge Scheduler** (not classic EventBridge Rules), currently `rate(4 hours)`. Post-refactor, its target changes from "invoke the Lambda directly" to "start a Step Functions execution":

```bash
# One-time: role EventBridge Scheduler assumes to start executions
aws iam create-role --role-name sde1-scheduler-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

aws iam put-role-policy --role-name sde1-scheduler-role \
  --policy-name start-scraper-workflow \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": "states:StartExecution",
      "Resource": "arn:aws:states:'"$REGION"':'"$ACCOUNT_ID"':stateMachine:sde1-job-scraper-workflow"
    }]
  }'

# Repoint the existing schedule at the state machine instead of the Lambda
aws scheduler update-schedule \
  --name sde1-job-scraper-schedule \
  --schedule-expression "rate(4 hours)" \
  --target '{
    "Arn": "arn:aws:states:'"$REGION"':'"$ACCOUNT_ID"':stateMachine:sde1-job-scraper-workflow",
    "RoleArn": "arn:aws:iam::'"$ACCOUNT_ID"':role/sde1-scheduler-role"
  }' \
  --region $REGION
```

To change frequency later: EventBridge console → Scheduler → select the schedule → edit the schedule expression → Save. No redeploy needed.

## Manual Testing

Start a Step Functions execution directly (replaces the old `aws lambda invoke` against a single function):

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:$REGION:$ACCOUNT_ID:stateMachine:sde1-job-scraper-workflow \
  --region $REGION

# check status / output
aws stepfunctions describe-execution \
  --execution-arn <ExecutionArn from the start-execution output> \
  --region $REGION
```

To test a single Lambda in isolation instead of the whole workflow (useful when debugging one stage), invoke it directly with a hand-built event matching what that function expects — e.g. for the worker:

```bash
aws lambda invoke --function-name sde1-worker --region $REGION \
  --payload '{"name":"Amazon","url":"https://www.amazon.jobs/en/search?base_query=software+engineer&loc_query=India","include_keywords":["software engineer"],"exclude_keywords":["senior"]}' \
  --cli-binary-format raw-in-base64-out response.json
cat response.json
```

For a full pipeline test without touching any AWS resources at all, run `python3 test_local.py` locally — it simulates `config_loader_handler` → `worker_handler` (once per company, sequentially) → `aggregator_handler` in-process, with DynamoDB and SES stubbed out.

## Monitoring and Logs

- **Step Functions console** → state machine → **Executions** tab — visual graph of each run, which state failed if any, and per-state input/output (including each Map iteration's worker result).
- **Lambda console** → each function → **Monitor** tab — invocation count, error count, duration graphs, now per-function instead of one combined view.
- **CloudWatch Logs** — one log group per function:
  - `/aws/lambda/sde1-config-loader`
  - `/aws/lambda/sde1-worker` (all company scrapes interleave here — filter by request ID or company name to isolate one)
  - `/aws/lambda/sde1-aggregator`
- Useful filter patterns:
  - In `sde1-aggregator`: `run complete` — successful completion summary (`scraped`/`relevant`/`new_jobs` counts)
  - In `sde1-worker`: `ERROR` — a company that timed out or errored; `tier=generic` — a company using the unreliable fallback scraper
  - `Task timed out` in any log group — that function hit its own configured Lambda timeout (distinct from `sde1-worker`'s internal `PER_COMPANY_TIMEOUT_SECONDS`, which returns a graceful result instead of actually timing out the invocation)

## Known Limitations and Troubleshooting

- **Bot-protected sites**: Atlassian (reCAPTCHA) and Salesforce (Akamai WAF, "Access Denied") are confirmed blocked. Both fall through to the generic fallback tier, which can return a non-zero `scraped` count that's actually nav links/noise, not real postings — check the per-company detail in the digest email before assuming a "0 relevant, N scraped" mismatch is a bug.
- **Uber and Flipkart** have no dedicated scraper tier and rely on the generic fallback — results are inconsistent by nature of that tier, not a specific known defect.
- **Per-company timeout**: a company that hangs (headless Chromium against an unresponsive page) returns a `status: "timeout"` result after `PER_COMPANY_TIMEOUT_SECONDS` (default 90s) rather than consuming the whole `sde1-worker` invocation. Since each company is now its own invocation, a stuck company can no longer affect any other company's scrape or the total pipeline runtime the way it could in the old single-Lambda design.
- **One Map iteration failing outright** (not a graceful `worker_handler` timeout/error result, but the Lambda invocation itself crashing) is caught at the state machine level (`Catch` on `ScrapeCompany`) and replaced with a same-shaped error entry, so it doesn't block the aggregator from running with everyone else's results.
- **Docker push fails with "image manifest ... not supported"**: missing the `--platform linux/amd64 --provenance=false --sbom=false` buildx flags — see [Building and Deploying](#building-and-deploying).
- **`ModuleNotFoundError: playwright`** at runtime: the Dockerfile installs `playwright` explicitly via pip — the base image's Chromium binaries alone aren't enough, the Python package must be installed too. `requirements.txt` is for local development (`test_local.py`) only; the Dockerfile has its own explicit `pip install` line and does not read `requirements.txt`.

## Costs

At this run frequency and job volume, the three Lambda functions, DynamoDB (on-demand billing), Step Functions (charged per state transition — a handful per execution, four times a day), EventBridge Scheduler, and SES all fall within AWS free-tier or negligible pay-as-you-go cost. If the schedule frequency, company list, or `MAX_JOBS_PER_COMPANY` increase significantly, check AWS Budgets to confirm costs stay where expected.
