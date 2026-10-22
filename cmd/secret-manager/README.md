# secret-manager

Python-based CLI tool for managing OpenShift CI secrets in Google Secret Manager.

## Overview

This tool provides a user-friendly interface for DPTP team members and authorized users 
to manage secrets stored in Google Secret Manager for the OpenShift CI infrastructure. 
Access is controlled through Rover groups.

## Architecture

**Containerized Deployment:**

- Source code lives in `ci-tools-standalone/cmd/secret-manager/`
- Built and published to `quay.io/openshift/ci-public:ci_secret-manager_latest`
- Users run via wrapper script in `openshift/release` repo: `hack/secret-manager.sh`

## Usage

Users in the `openshift/release` repository run:

```bash
# Authenticate (first time only)
./hack/secret-manager.sh login

# List collections (no SSO login needed)
./hack/secret-manager.sh list

# List secrets in a collection
./hack/secret-manager.sh list -c my-collection

# Show a secret's metadata (creation date, ownership info; never the value)
./hack/secret-manager.sh describe -c my-collection aws/access-key-id

# Create a secret
./hack/secret-manager.sh create

# Update a secret
./hack/secret-manager.sh update

# Delete a secret
./hack/secret-manager.sh delete

# Get service account information
./hack/secret-manager.sh get-sa

# Clean cached credentials
./hack/secret-manager.sh clean
```

## Development

### Local Testing

Build and test the container locally:

```bash
# Build the image (from ci-tools repo root)
podman build -t localhost/secret-manager:dev \
  --file - . <<'EOF'
FROM python:3.13-slim
RUN apt-get update && apt-get install -y curl gnupg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | \
    tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
    apt-get update && apt-get install -y google-cloud-cli && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY cmd/secret-manager/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY cmd/secret-manager/ .
ENTRYPOINT ["python3", "/app/main.py"]
EOF

# Test it
export SECRET_MANAGER_IMAGE=localhost/secret-manager:dev
cd /path/to/openshift/release
./hack/secret-manager.sh login
```
