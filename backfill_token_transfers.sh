#!/bin/bash
# Backfill token_wallet_connections in batches
#
# This script populates the token_wallet_connections table by processing
# historical blockchain data in slot batches. It finds all token transfers
# (sender -> receiver) by joining ma_tx_outs with tx_ins.
#
# WARNING: This is a SLOW process due to the complex joins required.
# Estimated time: Several days to weeks depending on hardware.
#
# Usage:
#   ./backfill_token_transfers.sh                    # Full backfill
#   ./backfill_token_transfers.sh 100000000          # Start from specific slot
#   ./backfill_token_transfers.sh 100000000 110000000 # Process range

# Configuration
BATCH_SIZE=10000   # Reduced from 50k to stay within memory quota
MAX_SLOT=180000000 # Current mainnet tip (update as needed)
START_SLOT=${1:-0}
END_SLOT=${2:-$MAX_SLOT}
SLEEP_BETWEEN_BATCHES=5  # Seconds to let TiKV recover between batches

# TiDB connection
MYSQL_CMD="mysql -h 127.0.0.1 -P 4000 -u root nectar"

echo "=============================================="
echo "Token Transfer Backfill"
echo "=============================================="
echo "Start slot:  $START_SLOT"
echo "End slot:    $END_SLOT"
echo "Batch size:  $BATCH_SIZE slots"
echo ""

# Create table if not exists
echo "Ensuring table exists..."
$MYSQL_CMD -e "
CREATE TABLE IF NOT EXISTS token_wallet_connections (
    tx_hash VARBINARY(32) NOT NULL,
    tx_index INT UNSIGNED NOT NULL,
    policy_id VARBINARY(28) NOT NULL,
    asset_name VARBINARY(32) NOT NULL,
    from_address VARCHAR(256) NOT NULL,
    to_address VARCHAR(256) NOT NULL,
    from_stake_address VARCHAR(63),
    to_stake_address VARCHAR(63),
    quantity BIGINT UNSIGNED NOT NULL,
    slot BIGINT UNSIGNED NOT NULL,
    block_height BIGINT UNSIGNED,
    PRIMARY KEY (tx_hash, tx_index, policy_id, asset_name),
    INDEX idx_twc_token (policy_id, asset_name, slot DESC),
    INDEX idx_twc_from (from_address, policy_id, asset_name),
    INDEX idx_twc_to (to_address, policy_id, asset_name),
    INDEX idx_twc_stake_pair (from_stake_address, to_stake_address, policy_id, asset_name),
    INDEX idx_twc_slot (slot DESC)
);
" 2>&1

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to create table"
    exit 1
fi
echo "Table ready."
echo ""

# Progress tracking
PROGRESS_FILE="/tmp/token_transfer_backfill_progress"
BATCH_START=$START_SLOT

# Check for resume
if [ -f "$PROGRESS_FILE" ] && [ "$1" == "" ]; then
    SAVED_SLOT=$(cat "$PROGRESS_FILE")
    if [ "$SAVED_SLOT" -gt "$START_SLOT" ]; then
        echo "Resuming from saved progress: slot $SAVED_SLOT"
        BATCH_START=$SAVED_SLOT
    fi
fi

echo "Starting backfill at $(date)"
echo ""

TOTAL_BATCHES=$(( (END_SLOT - BATCH_START + BATCH_SIZE - 1) / BATCH_SIZE ))
CURRENT_BATCH=0

while [ $BATCH_START -lt $END_SLOT ]; do
    BATCH_END=$((BATCH_START + BATCH_SIZE))
    if [ $BATCH_END -gt $END_SLOT ]; then
        BATCH_END=$END_SLOT
    fi

    CURRENT_BATCH=$((CURRENT_BATCH + 1))
    PERCENT=$(( (CURRENT_BATCH * 100) / TOTAL_BATCHES ))

    echo -n "[$(date '+%Y-%m-%d %H:%M:%S')] Batch $CURRENT_BATCH/$TOTAL_BATCHES ($PERCENT%) - Slots $BATCH_START to $BATCH_END... "

    # Set memory quota and run the backfill query
    # This query is EXPENSIVE due to the 5-table join
    RESULT=$($MYSQL_CMD -e "
    SET tidb_mem_quota_query = 2 << 30;  -- 2GB memory limit (reduced from 4GB)
    SET max_execution_time = 600000;      -- 10 minute timeout per batch

    INSERT INTO token_wallet_connections
        (tx_hash, tx_index, policy_id, asset_name, from_address, to_address,
         from_stake_address, to_stake_address, quantity, slot, block_height)
    SELECT
        output_ma.tx_hash,
        output_ma.tx_index,
        output_ma.policy,
        output_ma.name,
        input_addr.address AS from_address,
        output_addr.address AS to_address,
        '' AS from_stake_address,
        '' AS to_stake_address,
        output_ma.quantity,
        b.slot_no AS slot,
        b.block_no AS block_height
    FROM ma_tx_outs output_ma
    INNER JOIN tx_outs output_addr
        ON output_ma.tx_hash = output_addr.tx_hash
        AND output_ma.tx_index = output_addr.\`index\`
    INNER JOIN txes t ON t.hash = output_ma.tx_hash
    INNER JOIN blocks b ON b.hash = t.block_hash
    INNER JOIN tx_ins ti ON ti.tx_in_hash = output_ma.tx_hash
    INNER JOIN tx_outs input_addr
        ON ti.tx_out_hash = input_addr.tx_hash
        AND ti.tx_out_index = input_addr.\`index\`
    INNER JOIN ma_tx_outs input_ma
        ON ti.tx_out_hash = input_ma.tx_hash
        AND ti.tx_out_index = input_ma.tx_index
        AND input_ma.policy = output_ma.policy
        AND input_ma.name = output_ma.name
    WHERE b.slot_no >= $BATCH_START AND b.slot_no < $BATCH_END
        AND input_addr.address != output_addr.address
    ON DUPLICATE KEY UPDATE
        quantity = VALUES(quantity);
    " 2>&1)

    if [ $? -eq 0 ]; then
        # Get current count
        CURRENT_COUNT=$($MYSQL_CMD -N -e "SELECT COUNT(*) FROM token_wallet_connections" 2>/dev/null)
        echo "OK ($CURRENT_COUNT total transfers)"

        # Save progress
        echo $BATCH_END > "$PROGRESS_FILE"
    else
        echo "WARN: $RESULT"
        # Still save progress to avoid re-processing forever
        echo $BATCH_END > "$PROGRESS_FILE"
    fi

    BATCH_START=$BATCH_END

    # Pause between batches to let TiKV recover
    sleep $SLEEP_BETWEEN_BATCHES
done

echo ""
echo "=============================================="
echo "Backfill complete at $(date)"
FINAL_COUNT=$($MYSQL_CMD -N -e "SELECT COUNT(*) FROM token_wallet_connections" 2>/dev/null)
echo "Total token transfers indexed: $FINAL_COUNT"
echo ""

# Show some stats
echo "Top 10 tokens by transfer count:"
$MYSQL_CMD -e "
SELECT
    HEX(policy_id) as policy,
    HEX(asset_name) as asset,
    COUNT(*) as transfers
FROM token_wallet_connections
GROUP BY policy_id, asset_name
ORDER BY transfers DESC
LIMIT 10;
" 2>/dev/null

# Cleanup progress file
rm -f "$PROGRESS_FILE"
