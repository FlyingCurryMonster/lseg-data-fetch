#!/bin/bash
# Run download_minute_bars.py for all tickers sequentially.
# Waits for a given PID to finish first if provided.
#
# Usage:
#   ./run_minute_bars_chain.sh [WAIT_PID]
#
# Each ticker resumes from bars_log.jsonl — safe to kill and restart.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

WAIT_PID="$1"
WORKERS=8

if [ -n "$WAIT_PID" ]; then
    echo "Waiting for PID $WAIT_PID to finish..."
    while kill -0 "$WAIT_PID" 2>/dev/null; do
        sleep 30
    done
    echo "PID $WAIT_PID finished. Starting minute bars chain."
fi

for TICKER in NVDA AMD TSLA SPY; do
    echo ""
    echo "============================================================"
    echo "  STARTING $TICKER minute bars  $(date)"
    echo "============================================================"
    echo ""

    PYTHONUNBUFFERED=1 python download_minute_bars.py "$TICKER" "$WORKERS"

    EXIT_CODE=$?
    echo ""
    echo "  FINISHED $TICKER (exit code $EXIT_CODE)  $(date)"
    echo ""

    if [ $EXIT_CODE -ne 0 ]; then
        echo "  ERROR: $TICKER failed, stopping chain."
        exit $EXIT_CODE
    fi
done

echo "ALL TICKERS COMPLETE  $(date)"
