# imposm-operator

Keeps PostGIS databases in sync with OpenStreetMap using
[imposm](https://imposm.org): it imports a snapshot, deploys it and applies
replication diffs on a schedule, and re-imports when the mapping or source
changes while production keeps serving and syncing. It runs as a Kubernetes
operator, with or without a CRD.

## Getting started

Install the CRD (once per cluster) and the operator:

```sh
helm install imposm-operator-crds charts/imposm-operator-crds
helm install imposm-operator charts/imposm-operator -n imposm-system --create-namespace
```

A sync needs PostGIS and an imposm mapping. The examples use CloudNativePG
(PostgreSQL 18, PostGIS as an image volume extension; Kubernetes 1.35+) and
a split example mapping:

```sh
kubectl apply -f examples/cnpg-cluster.yaml
kubectl apply -k examples/mapping
```

Then create a sync. It imports Monaco from Geofabrik and from then on applies
the daily diffs:

```sh
kubectl apply -f - <<'EOF'
apiVersion: imposm.johanjk.github.io/v1alpha1
kind: ImposmSync
metadata:
  name: monaco
spec:
  source:
    baseURL: https://download.geofabrik.de/europe/
    pbfPath: monaco-latest.osm.pbf
    replicationPath: monaco-updates/
  schedule: "17 * * * *"  # every hour at minute 17
  database:
    host: osm-rw          # the CloudNativePG cluster's read-write service
    database: osm
    user: osm
    pgpassSecretRef:      # libpq password file; CNPG generates it
      name: osm-app
      key: pgpass
  mapping:
    sources:
      - configMapRef:
          name: osm-mapping
EOF
kubectl get imposmsyncs   # -w to follow the import live
# NAME     PHASE     LAG   ACTIVITY                   LAST RUN   AGE
# monaco   Syncing   12m   idle, next run 21:17 UTC   1m         2m
```

Actions are annotations, e.g. `kubectl annotate imposmsync monaco
imposm.johanjk.github.io/action=sync`. No cluster rights for a CRD? The same
spec works as a labelled ConfigMap, with an operator that needs only a Role
in its namespace: see [deployment options](docs/deployment.md).

## Changing a sync

Edit the spec and the operator does the least work that gets there:

- **New mapping, SRID or limitTo:** a new import is built next to production,
  which keeps serving *and keeps receiving diffs* meanwhile, so its lag does
  not grow; the new import is deployed once it has caught up. This is
  `deploy.syncDuringReimport: auto` (the default), which needs room for a
  second imposm cache on the volume; without it production pauses until the
  deploy, and the status says why.
- **Renamed schemas, table prefix or tables:** the existing tables are moved
  and renamed in place, no re-import.
- **Tables that left the mapping:** dropped after the deploy (only this
  sync's, never with CASCADE).

## Documentation

[docs/](docs/README.md): deployment options, features, design, development.

Map data in `test/fixtures` © OpenStreetMap contributors, ODbL.
