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
# This single image serves THREE Lambda functions (config loader,
# per-company worker, aggregator — see lambda_function.py's module
# docstring and README.md for the full architecture). They're
# differentiated purely by each function's --image-config Command
# override at create-function time, not by separate Dockerfiles/images
# — the CMD below is only the default used if a function is invoked
# without that override.
#
# Build:
#   docker build -t sde1-job-scraper .
#
# Push to ECR and deploy — see README.md "Building and Deploying"
# section for the full aws ecr / lambda create-function commands,
# including the --image-config Command override per function.

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
# Default handler if a function is created without overriding Command —
# in practice every function created from this image sets its own
# --image-config Command (config_loader_handler / worker_handler /
# aggregator_handler), so this value itself is never relied on.
CMD ["lambda_function.worker_handler"]
