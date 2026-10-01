# Token Transfer Indexing

This document describes token transfer tracking in Nectar.

Current status (May 26, 2026): the live processor, `BlockProcessor` wiring, and
manual startup DDL for `token_wallet_connections` exist. Remaining gaps are
duplicate historical/backfill DDL cleanup and real-time token-transfer event-bus
wiring.

## Overview

Token transfer tracking captures when native assets (tokens) move between wallets.
This complements the existing `wallet_connections` table (ADA transfers) by providing:

- **Token-specific transfer history**: Which addresses sent token X to which other addresses
- **Transfer graph data**: Used by Honeycomb to visualize holder relationships
- **Ownership grouping**: Identify wallets that have transacted with each other

## Current Files

```
indexer/
├── models/
│   └── token_wallet_connections.go    # Database model
├── processors/
│   └── token_transfer_processor.go    # Live indexing processor
└── backfill_token_transfers.sh        # Historical data backfill script

backend/
└── migrations/
    └── 001_token_wallet_connections.sql  # Historical/unwired backend DDL duplicate
```

## Database Schema

```sql
CREATE TABLE token_wallet_connections (
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
```

## Integration Steps

Steps 1-5 below are already represented in active code. Keep them as reference
for what the current wiring looks like.

### Step 1: Add processor to BlockProcessor struct

Current status: done.

In `processors/block_processor.go`, add the processor to the struct:

```go
type BlockProcessor struct {
    // ... existing fields ...
    tokenHolderProcessor      *TokenHolderProcessor
    tokenTransferProcessor    *TokenTransferProcessor  // ADD THIS
    // ...
}
```

### Step 2: Initialize in NewBlockProcessor

Current status: done.

```go
func NewBlockProcessor(db *gorm.DB, cfg *config.IndexingConfig) *BlockProcessor {
    // ... existing code ...
    bp := &BlockProcessor{
        // ... existing fields ...
        tokenHolderProcessor:      NewTokenHolderProcessor(db, nil),
        tokenTransferProcessor:    NewTokenTransferProcessor(db, nil),  // ADD THIS
        // ...
    }
    return bp
}
```

### Step 3: Initialize in NewBlockProcessorWithEvents

Current status: partially done. The processor is constructed in
`NewBlockProcessorWithEvents`, but it receives `nil` rather than a token
transfer event channel.

```go
func NewBlockProcessorWithEvents(db *gorm.DB, cfg *config.IndexingConfig, eventChan chan<- models.TokenHolderEvent) *BlockProcessor {
    // ... existing code ...
    bp := &BlockProcessor{
        // ... existing fields ...
        tokenHolderProcessor:      NewTokenHolderProcessor(db, eventChan),
        tokenTransferProcessor:    NewTokenTransferProcessor(db, nil),  // ADD THIS (separate event channel if needed)
        // ...
    }
    return bp
}
```

### Step 4: Call processor during transaction processing

Current status: done in the active transaction path.

In the `processTransaction` function (around line 460), add after tokenHolderProcessor:

```go
// Process token holders - track incremental balance changes
if bp.tokenHolderProcessor != nil {
    if err := bp.tokenHolderProcessor.ProcessTransaction(tx, txHash, slotNo, transaction); err != nil {
        unifiederrors.Get().Warning("BlockProcessor", "ProcessTokenHolders", fmt.Sprintf("Failed to process token holders: %v", err))
    }
}

// Process token transfers - track token movement between wallets
// ADD THIS BLOCK:
if bp.tokenTransferProcessor != nil {
    if err := bp.tokenTransferProcessor.ProcessTransaction(tx, txHash, slotNo, transaction); err != nil {
        unifiederrors.Get().Warning("BlockProcessor", "ProcessTokenTransfers", fmt.Sprintf("Failed to process token transfers: %v", err))
    }
}
```

### Step 5: Register table creation in startup migration

Current status: done in `database/migration_helper.go`.

`token_wallet_connections` uses a composite primary key, so the current code
creates it with manual startup DDL rather than plain `db.AutoMigrate`. Keep any
future schema changes in that canonical startup path first, then update
backfill scripts and historical SQL snippets to match.

## Backfill Historical Data

After deploying the code changes, run the backfill script:

```bash
cd /path/to/indexer
./backfill_token_transfers.sh
```

Options:
- `./backfill_token_transfers.sh` - Full backfill from slot 0
- `./backfill_token_transfers.sh 100000000` - Start from specific slot
- `./backfill_token_transfers.sh 100000000 110000000` - Process slot range

**Note**: Backfill is SLOW (currently 10k slots per batch, several days total) due to complex joins.

## Query Examples

### Get all transfers for a token
```sql
SELECT * FROM token_wallet_connections
WHERE policy_id = UNHEX('279c909f...')
  AND asset_name = UNHEX('534e454b')
ORDER BY slot DESC
LIMIT 100;
```

### Get transfers between top holders
```sql
SELECT twc.*
FROM token_wallet_connections twc
WHERE twc.policy_id = UNHEX('279c909f...')
  AND twc.asset_name = UNHEX('534e454b')
  AND twc.from_address IN (SELECT address FROM token_holders WHERE policy = UNHEX('279c909f...') ORDER BY amount DESC LIMIT 100)
  AND twc.to_address IN (SELECT address FROM token_holders WHERE policy = UNHEX('279c909f...') ORDER BY amount DESC LIMIT 100)
ORDER BY twc.slot DESC;
```

### Aggregate transfers between address pairs
```sql
SELECT
    from_address,
    to_address,
    COUNT(*) as transfer_count,
    SUM(quantity) as total_quantity
FROM token_wallet_connections
WHERE policy_id = ? AND asset_name = ?
GROUP BY from_address, to_address
ORDER BY transfer_count DESC;
```

## Performance Considerations

1. **Backfill is slow**: The 5-table join required to identify token transfers is expensive.
   Run during off-peak hours.

2. **Live indexing is fast**: During normal operation, the processor only looks at
   current transaction data, not historical joins.

3. **Index usage**: The compound indexes on `(policy_id, asset_name)` ensure fast
   token-specific queries.

4. **Memory**: The backfill script uses `tidb_mem_quota_query = 2GB` per batch.
   Adjust based on available memory.

## Event Bus Integration (Optional)

Current status: not wired. `TokenTransferProcessor` supports an optional
`chan<- models.TokenTransferEvent`, but active constructors pass `nil`, and the
current event bus is typed around `models.TokenHolderEvent`.

To emit real-time events for token transfers:

1. Create an event channel:
```go
transferEventChan := make(chan models.TokenTransferEvent, 1000)
```

2. Pass to processor:
```go
tokenTransferProcessor: NewTokenTransferProcessor(db, transferEventChan),
```

3. Consume events (in a separate goroutine):
```go
go func() {
    for event := range transferEventChan {
        // Broadcast to WebSocket clients, etc.
        fmt.Printf("Transfer: %s -> %s (%d %s)\n",
            event.FromAddress, event.ToAddress, event.Quantity, event.AssetName)
    }
}()
```
