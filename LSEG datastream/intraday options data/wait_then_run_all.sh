#!/bin/bash
# Wait for AAPL to finish, then launch run_all_om_bars.sh
# Usage: nohup ./wait_then_run_all.sh PID [WORKERS] &

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

WAIT_PID="${1:-3143951}"
WORKERS="${2:-8}"

echo "[$(date)] Waiting for PID $WAIT_PID (AAPL) to finish..."

while kill -0 "$WAIT_PID" 2>/dev/null; do
    sleep 30
done

echo "[$(date)] PID $WAIT_PID finished. Launching run_all_om_bars.sh..."
./run_all_om_bars.sh "$WORKERS"
