# Distribution-Aware Distributed Database Testing

Distribution-Aware Testing (DAT) is a novel automated testing approach for distributed database management systems (DDBMSs) that systematically leverages distribution-aware information throughout the testing pipeline.

DAT builds on a set of techniques that capture diverse combinations of logical schemas and data distribution strategies, 
and performs guided query mutation to trigger a wide range of distributed query execution behaviors and optimizations (e.g., shard routing and co-located joins), while improving query executability via historical feedback.

We implement DAT in a tool called DistRanger, and evaluate it on four widely used production DDBMSs: Citus, Vitess, Apache ShardingSphere, and ClickHouse.
So far, DistRanger has uncovered 31 previously unknown bugs, including 28 distribution-specific ones whose manifestations or root
causes are tied to distributed query processing and optimization. These bugs are of different types, including logic bugs (incorrect query results), crashes, errors, unexpected exceptions, and timeouts.

This repository provides instructions on how to use Distranger. For technical details of DAT and Distranger, please refer to our technical report [tech-rpt.pdf](https://github.com/duerwuyi/DAT/blob/main/tech-rpt.pdf)


## Repository layout

- `docker/distranger/deploy.sh`: builds the DistRanger Docker image and starts
  an interactive container with the source tree mounted as a volume.
- `compile.sh`: builds `distranger` from the existing `build` directory and
  copies the binary to the repository root.
- `main/<dbms>/dat_test.sh`: DBMS-specific DAT test entry points. Each script calls
  `compile.sh`, creates the DBMS `saved` directory, and runs `./distranger` with
  the matching DAT config.
- `main/<dbms>/*_ddc_config.txt`: connection settings for the reference database
  and the distributed database under test.
- `<dbms>/*_config`: extra runtime configuration consumed by DistRanger for
  worker nodes, Docker container names, or DBMS control clients.
- `docker/<dbms>/README.md`: DBMS-specific deployment notes.

## Build and run DistRanger

Build locally (not recommended):

```bash
bash compile.sh
```

Build and enter the DistRanger Docker environment (recommended):

```bash
./docker/distranger/deploy.sh
```

Run a DBMS test from the repository root:

```bash
bash main/citus/dat_test.sh
```

The test scripts follow this pattern:

```bash
bash main/<dbms>/dat_test.sh
```

Available test scripts:

- `main/citus/dat_test.sh`
- `main/clickhouse/dat_test.sh`
- `main/shardingsphere/dat_test.sh`
- `main/vitess/dat_test.sh`

## Common prerequisites

- Docker and Docker Compose v2 for the Docker-based deployments.
- `kubectl` and a working Kubernetes cluster for Vitess.
- A reference database reachable from the DistRanger container through
  `host.docker.internal`, usually on the port listed as `*-port0` in the
  relevant `main/<dbms>/*_ddc_config.txt` file.
- The distributed DBMS deployment reachable through `host.docker.internal` on
  the `*-port1` port.

## Citus

Prerequisites:

- Docker and Docker Compose v2.
- A reference PostgreSQL database at `host.docker.internal:5432` with database
  `testdb`.

Start Citus:

```bash
cd docker/citus
docker compose -f docker_compose.yml up -d
```

Run DistRanger:

```bash
bash main/citus/dat_test.sh
```

Main DAT config:

- `main/citus/citus_ddc_config.txt`
- Reference PostgreSQL: `host.docker.internal:5432`
- Citus coordinator: `host.docker.internal:5433`

Extra config:

- `citus/citus_config`: worker addresses, expected to be
  `host.docker.internal:5434` through `host.docker.internal:5438`.
- `citus/citus_docker_config`: worker container names, expected to be
  `docker-worker1-1` through `docker-worker5-1`.

See `docker/citus/README.md` for deployment details.

## ClickHouse

Prerequisites:

- Docker and Docker Compose v2.
- A standalone reference ClickHouse server and a distributed ClickHouse cluster.

Start the standalone reference node:

```bash
cd docker/clickhouse/single_node
sh a.sh
```

Start the distributed cluster:

```bash
cd docker/clickhouse/cluster_2S_1R
docker compose up -d
```

Run DistRanger:

```bash
bash main/clickhouse/dat_test.sh
```

Main DAT config:

- `main/clickhouse/clickhouse_ddc_config.txt`
- Reference ClickHouse: `host.docker.internal:9007`
- Distributed ClickHouse entry point: `host.docker.internal:9000`

Extra config:

- `clickhouse/cluster_config`: cluster name used by DistRanger, currently
  `cluster_2S_1R`.

See `docker/clickhouse/README.md` for deployment details.

## ShardingSphere

Prerequisites:

- Docker and Docker Compose v2.
- A reference PostgreSQL database at `host.docker.internal:5432` with database
  `testdb`.

Start ShardingSphere Proxy and its PostgreSQL backend nodes:

```bash
cd docker/shardingsphere
docker compose up -d
```

Run DistRanger:

```bash
bash main/shardingsphere/dat_test.sh
```

Main DAT config:

- `main/shardingsphere/ss_ddc_config.txt`
- Reference PostgreSQL: `host.docker.internal:5432`
- ShardingSphere Proxy: `host.docker.internal:5440`

Extra config:

- `shardingsphere/ss_pg_config`: PostgreSQL backend nodes at
  `host.docker.internal:5443` through `host.docker.internal:5447`, with user
  `postgres` and password `123abc`.
- `shardingsphere/ss_mysql_config`: MySQL-style backend node config for
  ShardingSphere MySQL deployments, if used.

See `docker/shardingsphere/README.md` for deployment details.

## Vitess

Prerequisites:

- `kubectl`.
- A working Kubernetes cluster with a default storage class or another
  compatible persistent volume setup.
- Vitess operator installed from `docker/vitess-operator/operator.yaml`.
- Two namespaces: `vitess-single` and `vitess-sharded`.

Start Vitess:

```bash
cd docker/vitess-operator
kubectl apply -f operator.yaml
kubectl rollout status deployment/vitess-operator
kubectl create namespace vitess-single
kubectl create namespace vitess-sharded
kubectl apply -f vitess-single.yaml
kubectl apply -f vitess-sharded.yaml
```

Start port forwards in separate terminals:

```bash
sh pf-single.sh
sh pf-sharded.sh
```

Run DistRanger:

```bash
bash main/vitess/dat_test.sh
```

Main DAT config:

- `main/vitess/vitess_ddc_config.txt`
- Single Vitess keyspace `local`: `host.docker.internal:15306`
- Sharded Vitess keyspace `sharded`: `host.docker.internal:25306`

Extra config:

- `vitess/vtctldclient_config`: vtctld endpoints used by DistRanger,
  `single host.docker.internal 15999` and
  `distributed host.docker.internal 25999`.

See `docker/vitess-operator/README.md` for deployment details.


## Stop deployments

Use the DBMS-specific README under `docker/<dbms>/README.md` for shutdown
commands. Docker Compose deployments generally use `docker compose down` from
their deployment directory. Vitess resources are removed with `kubectl delete`
commands listed in `docker/vitess-operator/README.md`.
