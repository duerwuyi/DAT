# Citus deployment

This directory starts one Citus coordinator, five Citus workers, and the Citus
membership manager.

## Start

```bash
cd docker/citus
docker compose -f docker_compose.yml up -d
```

## Endpoints

- Coordinator: `127.0.0.1:5433`
- Workers: `127.0.0.1:5434` through `127.0.0.1:5438`

DistRanger uses `main/citus/citus_ddc_config.txt`, which expects Citus at
`host.docker.internal:5433` and the reference PostgreSQL database at
`host.docker.internal:5432`.

The worker addresses used by DistRanger are listed in `citus/citus_config`:

```text
host.docker.internal 5434
host.docker.internal 5435
host.docker.internal 5436
host.docker.internal 5437
host.docker.internal 5438
```

The worker container names used by Docker-based cluster operations are listed in
`citus/citus_docker_config`:

```text
docker-worker1-1
docker-worker2-1
docker-worker3-1
docker-worker4-1
docker-worker5-1
```

These values match the default ports and `container_name` values in
`docker_compose.yml`, so the default DistRanger configuration should connect to
this stack without edits once the reference PostgreSQL service is available on
`host.docker.internal:5432`.

## Notes

- `docker compose config` parses this file successfully.
- Compose warns when `POSTGRES_PASSWORD` is unset. The services also set
  `POSTGRES_HOST_AUTH_METHOD=trust`, so the blank password is intentional for a
  local test deployment.
- The membership manager needs access to `/var/run/docker.sock`.
- The worker container names match `citus/citus_docker_config`.

## Stop

```bash
docker compose -f docker_compose.yml down
```
