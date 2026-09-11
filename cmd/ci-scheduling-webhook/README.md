# ci-scheduling-webhook

Admission webhook and background controller for OpenShift CI build clusters. It classifies prow/ci-operator pods into workload pools (builds, tests, longtests, prowjobs), applies scheduling constraints, and scales **worker MachineSets down** when nodes go idle.

The cluster autoscaler still handles scale-up. This component owns the scale-down path.

## Graceful cluster DNS during scale-down

### Why this matters

Build-farm jobs resolve test-cluster API servers through the build cluster's DNS (`172.30.0.10`). When that DNS blips, logs show errors like:

```text
dial tcp: lookup api.ci-op-… on 172.30.0.10:53: read udp …: i/o timeout
```

Investigation ([OCPBUGS-82081](https://issues.redhat.com/browse/OCPBUGS-82081), `#forum-ocp-release-oversight` Apr 2026) found several causes. One we control: **this webhook removing a node without giving cluster DNS pods time to shut down cleanly**.

OpenShift cluster DNS (`openshift-dns/dns-default`) uses CoreDNS `health.lameduck` (20 seconds). CI worker nodes run `openshift-dns/node-resolver` instead of `dns-default`; both must be evicted gracefully before machine delete.

That graceful path works when the **cluster autoscaler** removes a node — `dns-default` pods carry `cluster-autoscaler.kubernetes.io/enable-ds-eviction: true` for that reason.

It does **not** run when **this webhook** deletes a machine via the Machine API. The machine controller drains the node but does not evict DaemonSet pods the way the autoscaler does. A hard drain can drop DNS while clients still have the pod in their endpoint list.

### What the webhook does now

During `scaleDown()` in `prioritization.go`, after CI pods are gone and before the machine delete annotation:

1. **NoSchedule taint** (`ci-scheduling.ci.openshift.io/graceful-dns-drain`) — stops the DaemonSet controller from immediately scheduling replacement DNS pods on the dying node. Cordoning alone is not enough for DaemonSets.
2. **Evict `dns-default` and `node-resolver` pods** on that node only (not every DaemonSet on the node).
3. **Poll up to 25 seconds** for those DNS pods to leave Ready (lameduck window) before machine delete.

Then the existing flow continues: machine delete annotation and MachineSet replica decrement.

RBAC needs `create` on `pods/eviction` (see `common_ci_scheduling_webhook/rbac.yaml` in openshift/release).
