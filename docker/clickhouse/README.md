# ClickHouse deployment

This directory contains two ClickHouse setups:

- `single_node`: one standalone ClickHouse server.
- `cluster_2S_1R`: a two-shard, one-replica-per-shard cluster with three
  ClickHouse Keeper nodes.

## Start the standalone server

```bash
cd docker/clickhouse/single_node
sh a.sh
```

Standalone endpoints:

- HTTP: `127.0.0.1:8127`
- Native TCP: `127.0.0.1:9007`

## Start the cluster

Run Compose from the cluster directory. The compose file uses `${PWD}` in bind
mounts, so running it from another directory will point the mounts at the wrong
path.

```bash
cd docker/clickhouse/cluster_2S_1R
docker compose up -d
```

Cluster endpoints:

- Shard 1 native TCP: `127.0.0.1:9000`
- Shard 2 native TCP: `127.0.0.1:9001`
- Shard 1 HTTP: `127.0.0.1:8123`
- Shard 2 HTTP: `127.0.0.1:8124`

DistRanger uses `main/clickhouse/clickhouse_ddc_config.txt`, which expects the
standalone reference node at `host.docker.internal:9007` and the distributed
cluster entry point at `host.docker.internal:9000`.

## Notes

- `docker compose config` parses the cluster file successfully when run from
  `docker/clickhouse/cluster_2S_1R`.
- The XML files under `cluster_2S_1R/fs` define the two ClickHouse shards and
  three Keeper nodes.
- The cluster compose file binds ports to `127.0.0.1`, so it is intended for
  local testing on the host.

## Stop

```bash
cd docker/clickhouse/cluster_2S_1R
docker compose down
docker rm -f clickhouse-single
```
