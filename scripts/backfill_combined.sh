#!/bin/bash
# Combined backfill runner for token_holders AND token_wallet_connections
# Runs both backfills in parallel so you only wait once
#
# Usage:
#   ./backfill_combined.sh              # Run both from start
#   ./backfill_combined.sh resume       # Resume both from where they left off

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/backfill_work/combined_logs"
mkdir -p "$LOG_DIR"

echo "=============================================="
echo "Combined Backfill: Holders + Transfers"
echo "=============================================="
echo "Running both backfills in parallel"
echo "Logs: $LOG_DIR"
echo ""

# Check if we should resume or start fresh
if [ "$1" == "resume" ]; then
    echo "Resuming from saved progress..."
    HOLDER_ARGS=""
    TRANSFER_ARGS=""
else
    echo "Starting fresh backfill..."
    # Clear progress files
    rm -f "$SCRIPT_DIR/backfill_work/progress.txt"
    rm -f "/tmp/token_transfer_backfill_progress"
    HOLDER_ARGS=""
    TRANSFER_ARGS="0"
fi

echo ""
echo "Starting holder backfill (by policy)..."
# Using 4 workers - TiKV is stable at 12GB
$SCRIPT_DIR/backfill_holders.sh 4 > "$LOG_DIR/holders.log" 2>&1 &
HOLDER_PID=$!

echo "Starting transfer backfill (by slot)..."
$SCRIPT_DIR/../backfill_token_transfers.sh $TRANSFER_ARGS > "$LOG_DIR/transfers.log" 2>&1 &
TRANSFER_PID=$!

echo ""
echo "Backfills running:"
echo "  Holders:   PID $HOLDER_PID (log: $LOG_DIR/holders.log)"
echo "  Transfers: PID $TRANSFER_PID (log: $LOG_DIR/transfers.log)"
echo ""
echo "Monitor progress with:"
echo "  tail -f $LOG_DIR/holders.log"
echo "  tail -f $LOG_DIR/transfers.log"
echo ""
echo "To stop: kill $HOLDER_PID $TRANSFER_PID"
echo ""

# Function to check if process is running
check_running() {
    kill -0 $1 2>/dev/null
    return $?
}

# Wait and show progress
echo "Waiting for completion (Ctrl+C to detach, backfills continue)..."
echo ""

trap "echo ''; echo 'Detached. Backfills still running in background.'; exit 0" INT

while true; do
    HOLDER_RUNNING=$(check_running $HOLDER_PID && echo "yes" || echo "no")
    TRANSFER_RUNNING=$(check_running $TRANSFER_PID && echo "yes" || echo "no")

    # Get progress if available
    HOLDER_PROGRESS=""
    if [ -f "$SCRIPT_DIR/backfill_work/progress.txt" ]; then
        HOLDER_DONE=$(cat "$SCRIPT_DIR/backfill_work/progress.txt" 2>/dev/null || echo "0")
        HOLDER_TOTAL=$(wc -l < "$SCRIPT_DIR/backfill_work/policies.txt" 2>/dev/null || echo "?")
        HOLDER_PROGRESS="$HOLDER_DONE/$HOLDER_TOTAL policies"
    fi

    TRANSFER_PROGRESS=""
    if [ -f "/tmp/token_transfer_backfill_progress" ]; then
        TRANSFER_SLOT=$(cat "/tmp/token_transfer_backfill_progress" 2>/dev/null || echo "0")
        TRANSFER_PROGRESS="slot $TRANSFER_SLOT"
    fi

    printf "\r[$(date '+%H:%M:%S')] Holders: %-8s %-20s | Transfers: %-8s %-20s" \
        "$HOLDER_RUNNING" "$HOLDER_PROGRESS" \
        "$TRANSFER_RUNNING" "$TRANSFER_PROGRESS"

    if [ "$HOLDER_RUNNING" == "no" ] && [ "$TRANSFER_RUNNING" == "no" ]; then
        echo ""
        echo ""
        echo "=============================================="
        echo "Both backfills complete!"

        # Get final counts
        MYSQL_CMD="mysql -h 127.0.0.1 -P 4000 -u root nectar -N"
        HOLDER_COUNT=$($MYSQL_CMD -e "SELECT COUNT(*) FROM token_holders" 2>/dev/null || echo "?")
        TRANSFER_COUNT=$($MYSQL_CMD -e "SELECT COUNT(*) FROM token_wallet_connections" 2>/dev/null || echo "?")

        echo "Total holders:   $HOLDER_COUNT"
        echo "Total transfers: $TRANSFER_COUNT"
        echo "=============================================="
        break
    fi

    sleep 5
done
