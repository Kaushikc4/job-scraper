# SDE1 Job Scraper (AWS Lambda)

Scrapes configured company career pages, filters titles for SDE1-relevant
roles, deduplicates against DynamoDB, and emails a digest via SES.
Triggered on a schedule by EventBridge (twice daily).

## Files

- `lambda_function.py` — the Lambda handler and all logic.
- `requirements.txt` — third-party dependencies to bundle (`requests`,
  `beautifulsoup4`). `boto3` is not listed because it's included in the
  Lambda Python runtime by default.

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

## 7. Packaging & deploying

```bash
mkdir -p build
pip install -r requirements.txt -t build/
cp lambda_function.py build/
cd build && zip -r ../function.zip . && cd ..

aws lambda create-function \
  --function-name sde1-job-scraper \
  --runtime python3.12 \
  --role arn:aws:iam::ACCOUNT_ID:role/sde1-job-scraper-role \
  --handler lambda_function.lambda_handler \
  --timeout 60 \
  --memory-size 256 \
  --zip-file fileb://function.zip \
  --environment "Variables={CONFIG_CSV_URL=...,SENDER_EMAIL=...,RECIPIENT_EMAIL=...}"
```

For subsequent updates:

```bash
aws lambda update-function-code --function-name sde1-job-scraper --zip-file fileb://function.zip
```

## Notes / known limitations

- **Generic HTML fallback** (`scrape_generic_html`) is best-effort for
  companies not on Greenhouse/Lever/Workday/SmartRecruiters. It uses
  common anchor/class heuristics but will likely need per-company
  selector tuning — a warning is logged whenever it's used.
- **Workday** career sites are normally JS-rendered; this script instead
  calls the underlying `cxs/.../jobs` JSON search endpoint directly,
  which works for most tenants but URL structure (tenant/site names)
  can vary — if a Workday company returns zero jobs, double-check the
  derived `tenant`/`site` values against the real career page URL.
- **No headless browser** (Playwright/Puppeteer) is used or required by
  default — if a specific company genuinely needs JS rendering beyond
  what a JSON API fallback can provide, that would need to be added
  separately and would increase the Lambda package size/cold start
  significantly.
- Relevance filtering is pure keyword include/exclude matching on job
  titles only (no LLM classification, no description parsing) — this is
  intentional for this version.
