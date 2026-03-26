#!/bin/bash
# Run download_om_minute_bars.py for all tickers in all_tickers.csv,
# ordered by contract count (most contracts first).
# Contracts for each ticker come from data/{TICKER}/contracts.csv
# (built by build_ticker_contracts.py — covers OM, CBOE, and gap sources).
#
# Safe to kill and restart — completed tickers are skipped automatically.
# A ticker is considered complete if its om_run.log contains "COMPLETE".
#
# Usage:
#   ./run_all_om_bars.sh [WORKERS]
#
# WORKERS: parallel workers per ticker (default: 8)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

WORKERS="${1:-8}"
TICKERS_CSV="all_tickers.csv"
MASTER_LOG="om_all_run.log"

if [ ! -f "$TICKERS_CSV" ]; then
    echo "ERROR: $TICKERS_CSV not found. Run build_ticker_contracts.py first."
    exit 1
fi

# Count total tickers (minus header)
TOTAL=$(tail -n +2 "$TICKERS_CSV" | wc -l)
DONE=0
SKIPPED=0

echo "============================================================" | tee -a "$MASTER_LOG"
echo "START: $(date)" | tee -a "$MASTER_LOG"
echo "Total tickers: $TOTAL  Workers per ticker: $WORKERS" | tee -a "$MASTER_LOG"
echo "============================================================" | tee -a "$MASTER_LOG"

# Read tickers in order (skip header)
while IFS=',' read -r ticker contracts; do
    # Strip quotes
    ticker="${ticker//\"/}"
    contracts="${contracts//\"/}"

    # Skip header row
    [ "$ticker" = "ticker" ] && continue

    DONE=$((DONE + 1))

    # Check if already complete
    run_log="data/$ticker/om_run.log"
    if [ -f "$run_log" ] && grep -q "COMPLETE" "$run_log"; then
        echo "[SKIP $DONE/$TOTAL] $ticker — already complete" | tee -a "$MASTER_LOG"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    echo "" | tee -a "$MASTER_LOG"
    echo "------------------------------------------------------------" | tee -a "$MASTER_LOG"
    echo "[RUN $DONE/$TOTAL] $ticker  (~$contracts contracts)  $(date)" | tee -a "$MASTER_LOG"
    echo "------------------------------------------------------------" | tee -a "$MASTER_LOG"

    PYTHONUNBUFFERED=1 python download_om_minute_bars.py "$ticker" "$WORKERS" \
        >> "data/$ticker/om_run.log" 2>&1

    EXIT_CODE=$?

    if grep -q "COMPLETE" "data/$ticker/om_run.log" 2>/dev/null; then
        BARS=$(grep "Total bars:" "data/$ticker/om_run.log" | tail -1 | awk '{print $NF}')
        CSV_SIZE=$(grep "CSV size:" "data/$ticker/om_run.log" | tail -1 | awk '{print $NF, $(NF-1)}')
        echo "[DONE $DONE/$TOTAL] $ticker — bars: $BARS  csv: $CSV_SIZE  $(date)" | tee -a "$MASTER_LOG"
    else
        echo "[ERROR $DONE/$TOTAL] $ticker — exit code $EXIT_CODE  $(date)" | tee -a "$MASTER_LOG"
    fi

done < "$TICKERS_CSV"

echo "" | tee -a "$MASTER_LOG"
echo "============================================================" | tee -a "$MASTER_LOG"
echo "ALL DONE: $(date)" | tee -a "$MASTER_LOG"
echo "Tickers processed: $TOTAL  Skipped (already done): $SKIPPED" | tee -a "$MASTER_LOG"
echo "============================================================" | tee -a "$MASTER_LOG"
