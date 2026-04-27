# Vitess operator deployment

This directory contains Kubernetes manifests for Vitess:

- `operator.yaml`: Vitess operator CRDs, RBAC, and controller deployment.
- `vitess-single.yaml`: one unsharded `local` keyspace in namespace
  `vitess-single`.
- `vitess-sharded.yaml`: a `sharded` keyspace with four shards plus a `local`
  keyspace in namespace `vitess-sharded`.
- `pf-single.sh` and `pf-sharded.sh`: local port-forward helpers.

## Start

Vitess deployment depends on `kubectl` and a working Kubernetes cluster.

Install the operator first, then create one namespace for the single cluster and
one namespace for the sharded cluster:

```bash
kubectl apply -f operator.yaml
kubectl rollout status deployment/vitess-operator

kubectl create namespace vitess-single
kubectl create namespace vitess-sharded
```

Apply the single and sharded cluster manifests to their matching namespaces:

```bash
kubectl apply -f vitess-single.yaml
kubectl apply -f vitess-sharded.yaml
```

Wait for the clusters to become ready:

```bash
kubectl get pods -n vitess-single
kubectl get pods -n vitess-sharded
```

## Port forwards

Run these in separate terminals:

```bash
sh pf-single.sh
sh pf-sharded.sh
```

Forwarded MySQL-compatible endpoints:

- Single cluster vtgate: `127.0.0.1:15306`
- Sharded cluster vtgate: `127.0.0.1:25306`

DistRanger uses `main/vitess/vitess_ddc_config.txt`, which expects the `local`
keyspace at `host.docker.internal:15306` and the `sharded` keyspace at
`host.docker.internal:25306`.

Those defaults line up with the port-forward scripts:

```text
pf-single.sh   -> local vtgate on 127.0.0.1:15306
pf-sharded.sh  -> sharded vtgate on 127.0.0.1:25306
```

## Notes

- These files are Kubernetes manifests, not Docker Compose files.
- The manifests reference `vitess/lite:latest`, `vitess/vtadmin:latest`, and
  `prom/mysqld-exporter:v0.14.0`.
- Each tablet requests a `10Gi` persistent volume claim, so the Kubernetes
  cluster must have a default storage class or another compatible PVC setup.

## Stop

```bash
kubectl delete -f vitess-sharded.yaml
kubectl delete -f vitess-single.yaml
kubectl delete -f operator.yaml
kubectl delete namespace vitess-sharded
kubectl delete namespace vitess-single
```
