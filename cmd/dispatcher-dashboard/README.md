# Dispatcher dashboard

A standalone, read-only UI for the Prow job dispatcher's observation API. The Go
binary embeds its HTML, CSS, and JavaScript. No frontend build or external assets
are required.

- Overview: serving-policy readiness and identity, cluster inventory, effective
  job counts and estimated demand, and reconciliation health.
- Cluster policy: inventory and configured weights alongside published temporary
  rules, their capability scope, reason, current serving counts, and expiry.
  Upcoming expirations summarize the next published rules whose recorded end
  times are in the future.
- Job lookup: explicitly search by job name or exact group, optionally within an
  effective cluster. Results are limited to 25 per page and explain baseline
  placement, the current decision, and override expiry.
- Overrides: filter records by cluster, kind, recorded status, capability, expiry,
  and lifecycle; inspect publication, serving counts, approvals, and audit history.
- Diagnostics: separate scheduler communication signals and dispatcher runtime
  warnings, grouped by repeated issue with pod identity and collection coverage.

## Run locally

```sh
make build-dispatcher-dashboard
./_output/dispatcher-dashboard --demo
```

Open <http://127.0.0.1:8080>. Demo mode is explicitly marked in the UI and uses
sample data. A failed live connection never falls back to demo data.

For an in-cluster dispatcher built with the ServiceAccount-authenticated
HTTPS observation listener:

```sh
./_output/dispatcher-dashboard \
  --dispatcher-url=https://prowjob-dispatcher.ci.svc:8443 \
  --dispatcher-token-file=/var/run/dispatcher-observation/token \
  --dispatcher-ca-file=/etc/dispatcher-ca/service-ca.crt
```

Live mode uses the in-cluster HTTPS observation Service. Both credential-file
flags are required. Project a bound token for ServiceAccount `ci/dispatcher-dashboard`
with audience `prowjob-dispatcher-observation` into the token path. The dispatcher
validates the token through Kubernetes TokenReview and permits only that identity.
The default Kubernetes API token has a different audience and cannot be used.
There is no manually shared observation-token Secret.

Mount the OpenShift-injected service CA bundle at the CA path. The client verifies
the dispatcher certificate and hostname with TLS 1.2 or newer, rejects plaintext
origins, and refuses redirects. Token files are reread on every request. CA changes
replace the pooled TLS transport before the next request, so projected token and
service CA rotation do not require a dashboard restart. Credentials remain on the
server and are redacted from upstream responses before reaching the browser.

## Deploy

`make image-dispatcher-dashboard` uses the repository's image-build convention.
The image includes the embedded UI and runs as a non-root user. Configure the
HTTPS upstream URL, projected ServiceAccount token, and injected service CA bundle.
Keep the application listener on loopback behind the OAuth sidecar.

Expose it behind the platform's OpenShift OAuth proxy with TLS, restricted to
Test Platform administrators. Do not expose its application port directly. This
service does not implement its own login flow. Use `/healthz` and `/readyz` for process
probes. Readiness checks local token and CA availability; upstream availability
is reported separately in the UI.

The application forwards only observation GET requests and reads the dispatcher's
`/metrics` endpoint for three reconciliation-health values. It provides no control
API adapter or write actions. Observation views use only the projected
ServiceAccount identity, without dispatcher control or Slack credentials. Optional
log diagnostics use read-only Kubernetes access.

## Optional log diagnostics

Diagnostics starts empty. Only **Collect logs** fetches diagnostics; opening the
view, changing filters, returning to the tab, and automatic refresh do not read
logs. Other observation views retain their refresh behavior. Choose **Last 30
minutes** or **As much as available within the limit**. The latter applies no
time cutoff and reads retained logs within the same collection budget.

`GET /api/v1/diagnostics?window=30m` and `?window=available` select these two modes.
Repeated clicks can reuse a collection cached for 30 seconds; its observation
time remains visible. Text search, category filters, and Clear filters operate on
the current collection without requests. Text search ignores case and includes
messages, details, job and cluster identities, pods, components, and levels. Each
component's issue list scrolls independently, and Show 10 more preserves its
scroll position.

The dashboard reads logs directly through the Kubernetes API. It discovers the
scheduler and dispatcher using fixed selectors and reads their matching main
containers. It includes current logs and available previous-container logs when
a container has restarted. The namespace and component containers are trusted
server configuration, never browser parameters. Credentials remain on the
server. Demo mode uses marked sample diagnostics and rejects live log collection.

In a pod, the dashboard uses its ServiceAccount. The example
[`rbac.yaml`](rbac.yaml) grants only pod discovery and pod-log reads in `ci`;
configure the deployment to use that ServiceAccount. It grants no dispatcher
control permissions. Kubernetes RBAC permits namespace-wide log reads, while the
application restricts discovery to the two component selectors and their main
containers. Enable collection with `--enable-log-diagnostics`. For local access
with existing credentials, also set:

```sh
  --log-kubeconfig="$HOME/.kube/config" \
  --log-namespace=ci
```

`--log-namespace` defaults to `ci`. Without an explicit `--log-kubeconfig`, only
in-cluster credentials are used. `--log-max-bytes` sets the single total read
budget, defaulting to 8 MiB (`8388608` bytes), split equally between the two
components (4 MiB each by default). The accepted range is 1 MiB through 64 MiB.
Collection has an 8-second deadline and no line-count cap. Budgets apply before
filtering normal messages, so a busy controller can exhaust them while producing
few matching issues. Selecting all available logs does not increase the budget.
Kubernetes reads forward from the beginning of each requested log range. When the
byte limit is reached, the sample contains the earliest eligible logs and newer
events may be missing. In available mode, collection starts with the oldest logs
that Kubernetes still retains for that container instance, rather than its newest
events. The UI reports this limitation prominently when the byte limit is hit.

The view reports bytes read, byte limits, observed first and last timestamps,
and known truncation reasons. These timestamps do not prove uninterrupted
coverage: a component can be quiet, containers can restart, and logs can rotate.
Deleted pods and older rotated log files are unavailable through this API.
Previous-container logs depend on what Kubernetes still retains. Byte limits,
the deadline, and other reported limits can leave partial coverage. See the
[Kubernetes log-rotation documentation](https://kubernetes.io/docs/concepts/cluster-administration/logging/#log-rotation).
An empty issue list is not a health guarantee.

Scheduler filtering excludes normal assignments and Plank activity. It includes
explicit communication errors, dispatcher fallback warnings, and API-client
throttling. The fallback warning does not log its underlying cause, so it is not
proof of a network failure. Throttling is a delay signal, not a failed request.
Generated-job fallback warnings are collapsed by default and can be shown;
explicit communication errors remain visible. Dispatcher diagnostics include
runtime warnings and errors. Only selected, credential-redacted fields are sent
to the browser, rather than full log records.

## Interpreting the data

Configured capacity is a scheduling weight, not physical CPU or memory capacity.
Estimated demand and normalized demand describe placement, not measured resource
utilization. A policy's change time is not a heartbeat. The UI shows successful
refresh time and reconciliation timestamps separately; missing metrics are
reported as unavailable or not recorded.

An override's durable status, inclusion in the serving policy, and effective job
count are distinct. Expiry can return decisions to baseline without a generation
change. Each jobs page is live at its observation time; a changed generation
restarts pagination. Repeated pages across an expiry boundary are not an atomic
snapshot.

Cluster policy combines observations only when their ready policy generation and
digest agree. Unavailable, stale, or mismatched observations are identified rather
than treated as an absence of overrides. Configured weights and requested temporary
rules remain separate, since overlapping or capability-scoped rules cannot be
represented by one effective cluster weight. Expiry summaries use the API's
observation time, and serving-job counts describe that observation. Newer policy
publications can change placement before an expiry is reached.

Override filters start with all records. Ended or terminal records are historical
unless they are still serving jobs or remain published without a confirmed elapsed
expiry. Missing timing information is identified explicitly.

Job lookup starts empty and sends a request only when Search is submitted or a
results page is selected. A job name or exact group is required; selecting a
cluster alone does not load its inventory. Editing or clearing filters clears
the previous results. Automatic refresh and the toolbar's Refresh button update
aggregate observations only. Submit Search again to refresh job results.

## Check

```sh
go test -race ./cmd/dispatcher-dashboard
go vet ./cmd/dispatcher-dashboard
```
