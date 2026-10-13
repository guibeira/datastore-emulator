#!/usr/bin/env bash
# Waits up to two minutes for the emulator on localhost:$1 to answer HTTP.
# A bound socket is not enough: the Java emulator listens before it is ready.
set -euo pipefail
port="$1"
for attempt in $(seq 1 60); do
  if curl -sf -o /dev/null "http://localhost:${port}/"; then
    echo "Port ${port} answers."
    exit 0
  fi
  sleep 2
done
echo "::error::Emulator on port ${port} did not answer in time."
docker compose -f benchmark/compose.yml logs || true
exit 1
