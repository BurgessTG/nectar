#!/bin/bash
# Backfill wallet_connections in batches of 500k slots

BATCH_SIZE=500000
MAX_SLOT=18200000
START_SLOT=0

echo "Starting wallet connections backfill..."
echo "Batch size: $BATCH_SIZE slots"
echo "Max slot: $MAX_SLOT"
echo ""

while [ $START_SLOT -lt $MAX_SLOT ]; do
    END_SLOT=$((START_SLOT + BATCH_SIZE))

    echo "[$(date)] Processing slots $START_SLOT to $END_SLOT..."

    mysql -h 127.0.0.1 -P 4000 -u root nectar -e "
    SET tidb_mem_quota_query = 2 << 30;

    INSERT INTO wallet_connections
        (sender_address, receiver_address, total_tx_count, total_ada_sent, first_tx_slot, last_tx_slot, last_tx_hash)
    SELECT
        spent_out.address as sender_address,
        recv_out.address as receiver_address,
        COUNT(DISTINCT ti.tx_in_hash) as total_tx_count,
        COALESCE(SUM(recv_out.value), 0) as total_ada_sent,
        MIN(b.slot_no) as first_tx_slot,
        MAX(b.slot_no) as last_tx_slot,
        MAX(ti.tx_in_hash) as last_tx_hash
    FROM tx_ins ti
    INNER JOIN tx_outs spent_out ON spent_out.tx_hash = ti.tx_out_hash AND spent_out.\`index\` = ti.tx_out_index
    INNER JOIN tx_outs recv_out ON recv_out.tx_hash = ti.tx_in_hash
    INNER JOIN txes t ON t.hash = ti.tx_in_hash
    INNER JOIN blocks b ON b.hash = t.block_hash
    WHERE b.slot_no >= $START_SLOT AND b.slot_no < $END_SLOT
        AND spent_out.address != recv_out.address
    GROUP BY spent_out.address, recv_out.address
    ON DUPLICATE KEY UPDATE
        total_tx_count = wallet_connections.total_tx_count + VALUES(total_tx_count),
        total_ada_sent = wallet_connections.total_ada_sent + VALUES(total_ada_sent),
        last_tx_slot = GREATEST(wallet_connections.last_tx_slot, VALUES(last_tx_slot)),
        last_tx_hash = VALUES(last_tx_hash);
    " 2>&1

    if [ $? -eq 0 ]; then
        CURRENT_COUNT=$(mysql -h 127.0.0.1 -P 4000 -u root nectar -N -e "SELECT COUNT(*) FROM wallet_connections" 2>/dev/null)
        echo "[$(date)] Batch complete. Total connections: $CURRENT_COUNT"
    else
        echo "[$(date)] WARNING: Batch failed, continuing to next..."
    fi

    START_SLOT=$END_SLOT

    # Small delay between batches to let TiDB breathe
    sleep 2
done

echo ""
echo "Backfill complete!"
FINAL_COUNT=$(mysql -h 127.0.0.1 -P 4000 -u root nectar -N -e "SELECT COUNT(*) FROM wallet_connections" 2>/dev/null)
echo "Final connection count: $FINAL_COUNT"
