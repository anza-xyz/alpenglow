#!/usr/bin/env bash

set -e

# limit jobs to 4gb/thread
if [[ -f "/proc/meminfo" ]]; then
  JOBS=$(grep MemTotal /proc/meminfo | awk '{printf "%.0f", ($2 / (4 * 1024 * 1024))}')
  NPROC=$(nproc)
else
  JOBS=$(sysctl hw.memsize | awk '{printf "%.0f", ($2 / (4 * 1024**3))}')
  NPROC=$(sysctl -n hw.ncpu)
fi

JOBS=$((JOBS > NPROC ? NPROC : JOBS))

export NPROC
export JOBS
