# Deploying and monitoring sync

## Files

| File | What it is |
|---|---|
| `kubernetes/sync-headless-service.yaml` | The Service that makes sync's metrics reachable from the monitoring cluster |
| `prometheus/sync-scrape-job.yaml` | The Prometheus scrape job, as a fragment to merge |
| `grafana/sync-dashboard.json` | A local copy of the dashboard. **Not tracked** — the authoritative one is in the grafana chart (`helm/grafana`), and a second tracked copy would only drift from it |

## How the monitoring is wired

sync runs in the **staging-mongo** cluster; Prometheus and Grafana run in the
**staging-manju** cluster. They share a VPC, and the `.svc.mongo` suffix is the
multicluster DNS alias for the database cluster.

A ClusterIP is only routable inside the cluster that owns it, so a scrape job
pointed at one from outside connects to nothing — and the failure is quiet: the
deployment is healthy, `/metrics` answers when you curl it from inside, and the
dashboard is simply empty. `sync-headless` has no ClusterIP, so it resolves to
the pod addresses, which are routable across the peering. That is the only
reason it exists.

## Known drift, 2026-09-05

**The live scrape job is not in the ConfigMap that is supposed to define it.**

- `monitoring/prometheus-config` in staging-manju has 294 lines and **no
  `sync-replication` job**.
- The running pod's `/etc/prometheus/prometheus.yml` **does** have it, and it is
  a plain file dated 2026-09-04 04:44 rather than a ConfigMap symlink — the
  config was written into the container, not through the ConfigMap.
- The deployment does mount `prometheus-config`, so **the next restart of that
  pod drops the job and every panel in the Replication folder goes blank.**

Nothing here has been applied. Merging `prometheus/sync-scrape-job.yaml` into
that ConfigMap is what closes it.

## Do not apply `helm/prometheus/prometheus-config-new.yaml`

That file is a whole `prometheus.yml`, not a fragment, so applying it replaces
everything. It does carry a correct `sync-replication` job — which is what makes
it dangerous, because it looks like the file to use. Against the live config it
would also:

- **delete the `redis-cluster` job** (6 targets, all currently up)
- **point `mongodb-sharded-cluster` at the wrong cluster**: its targets end
  `.svc.locust`, and the live ones end `.svc.mongo`. All 6 MongoDB targets would
  go down.

So it fixes one job and breaks twelve targets. Take the job fragment from
`prometheus/sync-scrape-job.yaml` and merge it instead.

`helm/prometheus` is not a git repository, which is why a stale file there looks
as authoritative as a current one. The live configuration was captured to
`helm/prometheus/prometheus-config-live-backup-20260904T043244Z.yaml` before any
of today's changes; that backup also predates the sync job, so it is a record of
what was there and not a file to restore from either.
