# Container image for the SDE1 job scraper Lambda.
#
# A plain zip-based Lambda package can't be used once Playwright is in the
# mix: the Chromium binary plus its OS-level shared libraries are far past
# the 250MB unzipped Lambda package/layer limit, and Playwright's own
# dependency installer (`playwright install-deps`) only supports Debian/
# Ubuntu apt-based systems — not AWS's Amazon Linux Lambda base images.
#
# So instead we build FROM Playwright's own (Ubuntu-based) image, which
# already has Chromium and all its OS dependencies correctly installed,
# and lay the AWS Lambda Runtime Interface Client on top of it. This is
# the standard community pattern for running Playwright on Lambda.
#
# Build:
#   docker build -t sde1-job-scraper .
#
# Push to ECR and deploy — see README.md "Packaging & deploying" section
# for the full aws ecr / lambda create-function commands.

FROM mcr.microsoft.com/playwright/python:v1.62.0-noble

# awslambdaric = AWS Lambda Runtime Interface Client, lets a non-Amazon-Linux
# container image act as a Lambda runtime.
#
# playwright is installed explicitly here even though the base image
# already bundles its Chromium browser binaries — the `playwright` pip
# package itself was NOT reliably importable via the default `python` on
# PATH in testing (ModuleNotFoundError at runtime), so don't rely on the
# base image for it.
RUN pip install --no-cache-dir awslambdaric boto3 requests beautifulsoup4 playwright==1.62.0

WORKDIR /var/task
COPY lambda_function.py .

# Playwright's browsers are already installed as part of the base image
# (under /ms-playwright by default in this image), so no extra
# `playwright install` step is needed here — only the pip package itself.

ENTRYPOINT ["python", "-m", "awslambdaric"]
CMD ["lambda_function.lambda_handler"]
