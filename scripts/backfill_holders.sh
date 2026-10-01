#!/bin/bash
#
# Backfill token_holders table from existing Nectar data
# Parallelized for speed, safe for RAM
#
# Usage: ./backfill_holders.sh [num_workers]
# Default: 4 workers
#

set -e

# Configuration
DB_HOST="127.0.0.1"
DB_PORT="4000"
DB_USER="root"
DB_NAME="nectar"
WORKERS=${1:-4}  # Default 4 workers
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORK_DIR="$SCRIPT_DIR/backfill_work"
PROGRESS_FILE="$WORK_DIR/progress.txt"
POLICIES_FILE="$WORK_DIR/policies.txt"
LOG_DIR="$WORK_DIR/logs"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=== Token Holders Backfill Script ===${NC}"
echo "Workers: $WORKERS"
echo ""

# Create work directory
mkdir -p "$WORK_DIR"
mkdir -p "$LOG_DIR"

# MySQL command helper
mysql_cmd() {
    mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" "$DB_NAME" -N "$@" 2>/dev/null
}

mysql_exec() {
    mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" "$DB_NAME" -e "$@" 2>/dev/null
}

# Step 1: Create table if not exists
echo -e "${YELLOW}Step 1: Creating token_holders table...${NC}"
mysql_exec "
CREATE TABLE IF NOT EXISTS token_holders (
    policy VARBINARY(28) NOT NULL,
    name VARBINARY(32) NOT NULL,
    address VARCHAR(256) NOT NULL,
    amount BIGINT UNSIGNED NOT NULL,
    tx_hash VARBINARY(32),
    output_index INT UNSIGNED,
    stake_address VARCHAR(63),
    last_updated_slot BIGINT UNSIGNED,
    PRIMARY KEY (policy, name, address),
    INDEX idx_token_holders_amount (policy, name, amount DESC),
    INDEX idx_token_holders_stake_address (stake_address),
    INDEX idx_token_holders_last_updated (last_updated_slot)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"
echo -e "${GREEN}Table ready.${NC}"

# Step 2: Get list of policies (if not already done)
if [ ! -f "$POLICIES_FILE" ]; then
    echo -e "${YELLOW}Step 2: Fetching policy list...${NC}"
    mysql_cmd -e "SELECT DISTINCT HEX(policy) FROM multi_assets;" > "$POLICIES_FILE"
    TOTAL=$(wc -l < "$POLICIES_FILE")
    echo -e "${GREEN}Found $TOTAL policies.${NC}"
else
    TOTAL=$(wc -l < "$POLICIES_FILE")
    echo -e "${GREEN}Using existing policy list: $TOTAL policies.${NC}"
fi

# Step 3: Initialize progress tracking
if [ ! -f "$PROGRESS_FILE" ]; then
    echo "0" > "$PROGRESS_FILE"
fi
COMPLETED=$(cat "$PROGRESS_FILE")
echo "Already completed: $COMPLETED / $TOTAL"

# Step 4: Process function for each worker
process_policy() {
    local policy=$1
    local worker_id=$2
    local log_file="$LOG_DIR/worker_${worker_id}.log"

    # Skip if empty
    [ -z "$policy" ] && return 0

    # Run the backfill query for this policy
    # Uses ON DUPLICATE KEY UPDATE to overwrite any existing stale data
    mysql_exec "
        INSERT INTO token_holders (policy, name, address, amount, tx_hash, output_index)
        SELECT
            mao.policy,
            mao.name,
            txo.address,
            SUM(mao.quantity),
            MAX(mao.tx_hash),
            MAX(mao.tx_index)
        FROM ma_tx_outs mao
        JOIN tx_outs txo ON mao.tx_hash = txo.tx_hash AND mao.tx_index = txo.index
        LEFT JOIN tx_ins ti ON txo.tx_hash = ti.tx_out_hash AND txo.index = ti.tx_out_index
        WHERE mao.policy = UNHEX('$policy')
          AND ti.tx_out_hash IS NULL
        GROUP BY mao.policy, mao.name, txo.address
        ON DUPLICATE KEY UPDATE
            amount = VALUES(amount),
            tx_hash = VALUES(tx_hash),
            output_index = VALUES(output_index);
    " >> "$log_file" 2>&1

    return $?
}

# Step 5: Parallel processing
echo -e "${YELLOW}Step 3: Processing policies with $WORKERS workers...${NC}"
echo "Progress will be saved. You can stop (Ctrl+C) and resume anytime."
echo ""

# Export functions for parallel
export -f process_policy
export -f mysql_cmd
export -f mysql_exec
export DB_HOST DB_PORT DB_USER DB_NAME LOG_DIR

# Track start time
START_TIME=$(date +%s)

# Process policies in parallel
PROCESSED=0
SKIPPED=$COMPLETED

# Read policies and process in parallel
tail -n +$((COMPLETED + 1)) "$POLICIES_FILE" | \
while IFS= read -r policy; do
    # Wait if we have too many background jobs
    while [ $(jobs -r | wc -l) -ge $WORKERS ]; do
        sleep 0.1
    done

    # Start background job
    (
        process_policy "$policy" "$$"
    ) &

    PROCESSED=$((PROCESSED + 1))
    CURRENT=$((SKIPPED + PROCESSED))

    # Update progress every 100 policies
    if [ $((PROCESSED % 100)) -eq 0 ]; then
        echo "$CURRENT" > "$PROGRESS_FILE"

        # Calculate ETA
        ELAPSED=$(($(date +%s) - START_TIME))
        if [ $PROCESSED -gt 0 ] && [ $ELAPSED -gt 0 ]; then
            RATE=$(echo "scale=2; $PROCESSED / $ELAPSED" | bc)
            REMAINING=$((TOTAL - CURRENT))
            if [ $(echo "$RATE > 0" | bc) -eq 1 ]; then
                ETA_SECS=$(echo "scale=0; $REMAINING / $RATE" | bc)
                ETA_MINS=$((ETA_SECS / 60))
                ETA_HOURS=$((ETA_MINS / 60))
                ETA_MINS=$((ETA_MINS % 60))
            fi
        fi

        printf "\r${GREEN}Progress: %d / %d (%.1f%%) | Rate: %.1f/sec | ETA: %dh %dm${NC}    " \
            "$CURRENT" "$TOTAL" \
            "$(echo "scale=1; $CURRENT * 100 / $TOTAL" | bc)" \
            "$RATE" \
            "$ETA_HOURS" "$ETA_MINS"
    fi
done

# Wait for remaining jobs
wait

# Final progress update
echo "$TOTAL" > "$PROGRESS_FILE"

# Calculate final stats
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
HOURS=$((DURATION / 3600))
MINS=$(((DURATION % 3600) / 60))
SECS=$((DURATION % 60))

echo ""
echo ""
echo -e "${GREEN}=== Backfill Complete ===${NC}"
echo "Total policies processed: $TOTAL"
echo "Time taken: ${HOURS}h ${MINS}m ${SECS}s"

# Step 6: Verify results
echo ""
echo -e "${YELLOW}Verifying results...${NC}"
HOLDER_COUNT=$(mysql_cmd -e "SELECT COUNT(*) FROM token_holders;")
POLICY_COUNT=$(mysql_cmd -e "SELECT COUNT(DISTINCT policy) FROM token_holders;")
echo -e "${GREEN}Total holders indexed: $HOLDER_COUNT${NC}"
echo -e "${GREEN}Total policies with holders: $POLICY_COUNT${NC}"

echo ""
echo "Done! You can now query token_holders for instant holder lookups."
