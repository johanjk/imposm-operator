# Deployment options

The chart installs the operator; syncs are separate resources you create
with kubectl or GitOps (see [examples](../examples)). The same sync spec
(`ImposmSyncSpec`) works in two modes, picked by what you are allowed to
install. The actual work happens in short-lived runner pods (imposm + the
sync engine) on a persistent volume, created by the controller.

|                               | `crd`                                        | `configmap`                                        |
|-------------------------------|----------------------------------------------|----------------------------------------------------|
| Sync defined as               | `ImposmSync` resource                        | ConfigMap labelled `imposm.johanjk.github.io/sync=true`, key `spec.yaml` |
| Needs                         | the CRD (cluster admin, once) + a Role or ClusterRole | a namespaced Role (namespace `admin` can install it) |
| Spec validation               | apiserver (CRD schema) + controller          | controller (`SpecValid` condition), JSON Schema for CI |
| Status                        | `kubectl get imposmsync`: phase, lag, live activity, conditions | ConfigMap `<name>-status`              |
| Actions (`deploy`, `revert-deploy`, …) | annotation on the resource          | annotation on the ConfigMap                        |
| Mapping / limitTo ConfigMap changes | picked up and re-rendered               | picked up and re-rendered                          |

**`crd` — the operator (recommended).**
A cluster admin installs the CRD once; the operator itself can still be
namespace-scoped (`rbac.scope=namespace`). Syncs are `ImposmSync` resources
([minimal](../examples/imposmsync.yaml),
[with expire tiles](../examples/imposmsync-expiretiles.yaml),
[two regions](../examples/imposmsync-regions.yaml),
[minutely diffs](../examples/imposmsync-osmfr.yaml),
[the planet](../examples/imposmsync-planet.yaml)).

```sh
helm install imposm-operator-crds charts/imposm-operator-crds           # once per cluster, by an admin
helm install imposm-operator charts/imposm-operator -n imposm-system --create-namespace
kubectl apply -f examples/imposmsync.yaml
kubectl get imposmsyncs
# NAME     PHASE     LAG    ACTIVITY                                          LAST RUN   AGE
# monaco   Syncing   12m    idle, next run 21:17 UTC                          43m        2d
# norway   Syncing   6d0h   download norway-latest.osm.pbf 42% (553Mi/1.3Gi)  1m         2d
kubectl annotate imposmsync monaco imposm.johanjk.github.io/action=revert-deploy
```

**`configmap` — the operator without CRDs (tenants without cluster rights).**
Same controller and behaviour, but syncs are labelled ConfigMaps
([example](../examples/configmap-sync.yaml)), found in the namespaces the
controller watches, and status lands in a sibling `<name>-status` ConfigMap,
so GitOps tools never see the controller edit your ConfigMap. With
`rbac.scope=namespace` the operator needs only a Role in its own namespace.

```sh
helm install imposm-operator charts/imposm-operator --set mode=configmap --set rbac.scope=namespace
kubectl apply -f examples/configmap-sync.yaml
kubectl get configmap monaco-status -o jsonpath='{.data.status\.yaml}'
kubectl annotate configmap monaco imposm.johanjk.github.io/action=sync
```

In both modes you need PostGIS and an imposm mapping, for example:

```sh
kubectl apply -f examples/cnpg-cluster.yaml   # PostgreSQL 18 + PostGIS image volume extension via CloudNativePG (optional; Kubernetes 1.35+)
kubectl apply -k examples/mapping             # imposm mapping, split into parts
```
