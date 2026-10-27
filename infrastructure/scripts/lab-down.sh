#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$ROOT"
# Keep all data volumes; also stop optional final-profile brokers.
docker compose -f infrastructure/docker-compose.yml --profile final down --remove-orphans
