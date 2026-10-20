# ci-tools-standalone

Standalone CI tools extracted from [openshift/ci-tools](https://github.com/openshift/ci-tools). Each tool has minimal dependencies and runs independently.

## Tools

| Tool | Description |
|------|-------------|
| `backport-verifier` | Prow plugin that verifies backport PRs carry the correct labels and approvals |
| `cluster-manifest-verifier` | Validates openshift/release cluster manifest PRs against Argo CD (generate + dry-run sync) |
| `ci-scheduling-webhook` | Kubernetes mutating admission webhook for CI workload scheduling and prioritization |
| `determinize-peribolos` | Deterministically formats Peribolos org configuration YAML |
| [`dispatcher-dashboard`](cmd/dispatcher-dashboard/README.md) | Read-only web dashboard for dispatcher policy, placement, overrides, and optional Kubernetes pod-log diagnostics |
| `gpu-scheduling-webhook` | Kubernetes mutating admission webhook for GPU/KVM workload scheduling |
| `helpdesk-faq` | Web service that serves helpdesk FAQ items from Kubernetes ConfigMaps |
| `pipeline-controller` | Kubernetes controller that manages CI pipeline resources |
| `pr-reminder` | Sends Slack reminders to team members about PRs awaiting review |
| `publicize` | Prow plugin that mirrors private PR merges to public repositories |
| `retester` | Periodically retests GitHub PRs based on configurable policies |

## Building

```bash
# Build all tools
make build-all

# Build a single tool
make build-backport-verifier

# Install all binaries to $GOPATH/bin
make production-install
```

## Testing

```bash
# Run all unit tests
make test

# Check Go source formatting
make format

# Run linter (uses golangci-lint via container locally, directly in CI)
make lint

# Verify Go modules are tidy
make validate-modules
```

## Container images

```bash
# Build a container image for a specific tool
make image-cluster-manifest-verifier
```

## Repository layout

```
cmd/                    One subdirectory per tool
internal/               Repo-private shared packages
  gzip/                 Gzip decompression utility
  helpdeskfaq/          Helpdesk FAQ client and types
  prreminder/           PR reminder Rover types
  retester/             Retester logic and caches
images/                 Dockerfiles per tool
hack/                   Build and CI scripts
```
