#!/bin/bash
# Download option tick data for all tickers sequentially.
# Each ticker: discover contracts, download trades.
# Safe to kill and restart — each ticker resumes from its log.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

for TICKER in SPY NVDA AMD TSLA; do
    echo ""
    echo "============================================================"
    echo "  STARTING $TICKER  $(date)"
    echo "============================================================"
    echo ""

    # PYTHONUNBUFFERED ensures output isn't block-buffered through tee
    PYTHONUNBUFFERED=1 python download_option_ticks.py "$TICKER" discover

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
