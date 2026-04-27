# ShardingSphere PostgreSQL deployment

This directory starts a ShardingSphere Proxy backed by five PostgreSQL
containers and one ZooKeeper container.

## Start

```bash
cd docker/shardingsphere
docker compose up -d
```

## Endpoints

- ShardingSphere Proxy: `127.0.0.1:5440`
- PostgreSQL backends: `127.0.0.1:5443` through `127.0.0.1:5447`
- ZooKeeper: `127.0.0.1:2181`

DistRanger uses `main/shardingsphere/ss_ddc_config.txt`, which expects the
proxy at `host.docker.internal:5440` and the reference PostgreSQL database at
`host.docker.internal:5432`.

DistRanger also uses `shardingsphere/ss_pg_config` for the PostgreSQL backend
nodes:

```text
host.docker.internal 5443 postgres 123abc
host.docker.internal 5444 postgres 123abc
host.docker.internal 5445 postgres 123abc
host.docker.internal 5446 postgres 123abc
host.docker.internal 5447 postgres 123abc
```

## Notes

- `docker compose config` parses this file successfully.
- `conf/database-sharding.yaml` contains entries for `testdb`, which matches the
  DistRanger configuration.
- `initdb/pg1` through `initdb/pg5` create `testdb` on the PostgreSQL backend
  containers during first initialization.

## Stop

```bash
docker compose down
```
