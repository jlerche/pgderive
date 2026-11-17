#!/usr/bin/env bash
set -euo pipefail
exec "$(dirname "$0")/check_worker.sh" --replication-only "$@"
