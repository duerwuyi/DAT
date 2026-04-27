#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  docker/distranger/deploy.sh [command...]

Environment variables:
  DISTRANGER_REPO_URL     Git repository to clone if the source directory is missing.
  DISTRANGER_BRANCH       Optional branch or tag for git clone.
  DISTRANGER_SOURCE_DIR   Source directory. Defaults to the current repository.
  DISTRANGER_IMAGE        Image tag. Defaults to distranger:latest.
  DISTRANGER_OUTPUT_DIR   Host output directory. Defaults to ./docker-output.
  DISTRANGER_NO_BUILD=1   Skip docker build.

Examples:
  ./docker/distranger/deploy.sh
  ./docker/distranger/deploy.sh bash main/citus/ddc_test.sh
  DISTRANGER_REPO_URL=https://github.com/USER/DistRanger.git \
    DISTRANGER_SOURCE_DIR=/tmp/DistRanger \
    ./docker/distranger/deploy.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"

repo_url="${DISTRANGER_REPO_URL:-}"
source_dir="${DISTRANGER_SOURCE_DIR:-${repo_root}}"
branch="${DISTRANGER_BRANCH:-}"
image="${DISTRANGER_IMAGE:-distranger:latest}"
workdir="${DISTRANGER_WORKDIR:-/workspace/DistRanger}"
output_dir="${DISTRANGER_OUTPUT_DIR:-${source_dir}/docker-output}"

if [[ $# -eq 0 ]]; then
    set -- bash
fi

if [[ ! -d "${source_dir}" ]]; then
    if [[ -z "${repo_url}" ]]; then
        echo "Source directory does not exist and DISTRANGER_REPO_URL is not set: ${source_dir}" >&2
        exit 1
    fi

    mkdir -p "$(dirname "${source_dir}")"
    clone_args=()
    if [[ -n "${branch}" ]]; then
        clone_args+=(--branch "${branch}")
    fi
    git clone "${clone_args[@]}" "${repo_url}" "${source_dir}"
fi

source_dir="$(cd "${source_dir}" && pwd)"
compose_dir="${source_dir}/docker/distranger"
compose_file="${compose_dir}/docker-compose.yml"
dockerfile="${compose_dir}/dockerfile"

if [[ ! -f "${compose_file}" || ! -f "${dockerfile}" ]]; then
    echo "Expected Docker files were not found under: ${compose_dir}" >&2
    exit 1
fi

mkdir -p "${output_dir}"
output_dir="$(cd "${output_dir}" && pwd)"

if [[ "${DISTRANGER_NO_BUILD:-0}" != "1" ]]; then
    docker build \
        --tag "${image}" \
        --file "${dockerfile}" \
        "${compose_dir}"
fi

override_file="$(mktemp)"
cleanup() {
    rm -f "${override_file}"
}
trap cleanup EXIT

cat > "${override_file}" <<EOF
services:
  fuzzer:
    image: ${image}
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ${source_dir}:${workdir}
      - ${output_dir}:/workspace/output
    working_dir: ${workdir}
    extra_hosts:
      - "host.docker.internal:host-gateway"
EOF

docker compose \
    --file "${compose_file}" \
    --file "${override_file}" \
    run --rm \
    fuzzer \
    "$@"
