# Datum Storage Issue Analysis

**Date**: June 27, 2025  
**Issue**: Datums are not being stored in the database despite outputs having datum hashes  
**Severity**: Historical high-severity issue; active code now includes a witness-datum path

## Current-State Update (May 26, 2026)

The original root cause below has been addressed in active code. The current
`ScriptProcessor` implements `ProcessWitnessDatums`, calls it for Alonzo+
transactions when a witness set exists, and still processes Babbage+ inline
datums from outputs.

Remaining caveats:

- This document's SQL counts are a historical database snapshot, not current
  runtime evidence.
- Runtime correctness still depends on `BlockProcessor` running with
  `cfg.Scripts == true`, the ledger transaction exposing the witness set, and a
  database schema that includes the `data` table.
- A historical backfill may still be needed for blocks indexed before this code
  existed.
- `txes.script_size` is still not populated by active code.

## Executive Summary

Historical finding: Nectar was storing datum hashes in transaction outputs but
was not storing the actual datum values in the `data` table because
`ScriptProcessor` did not process witness-set `PlutusData`. Current active code
does include that method and call path; the rest of this document is retained as
history of the bug.

## Current State

### Database Evidence
```sql
-- Outputs with datum hashes: 780
-- Unique datum hashes: 99
-- Entries in data table: 0
```

### What's Working
- ✅ Scripts are being processed and stored (4 scripts found)
- ✅ Redeemers are being processed and stored (3 redeemers found)
- ✅ Datum hashes are being extracted from outputs and stored in `tx_outs.data_hash`
- ✅ Collateral inputs are being tracked

### What's Missing
- Historical: actual witness datum values were not being stored in the `data` table
- Historical: witness set `PlutusData` was not processed
- Current: `script_size` field is still not being populated

## Root Cause Analysis

### 1. Code Flow Analysis

The historical processing flow in `ScriptProcessor.ProcessTransaction()` was:

```go
// Line 679-707 in script_processor.go
if witnessSet != nil {
    // Process scripts ✅
    if err := sp.ProcessTransactionScripts(context.Background(), tx, txHash, witnessSet); err != nil {
        return fmt.Errorf("failed to process scripts: %w", err)
    }

    // Process redeemers ✅
    if err := sp.ProcessRedeemers(context.Background(), tx, txHash, witnessSet); err != nil {
        return fmt.Errorf("failed to process redeemers: %w", err)
    }
    
    // Historical gap: ProcessWitnessDatums was missing here.
}

// Process inline datums from outputs (Babbage+) ✅
if blockType >= 6 { // BlockTypeBabbage
    if err := sp.ProcessInlineDatums(context.Background(), tx, txHash, transaction); err != nil {
        return fmt.Errorf("failed to process inline datums: %w", err)
    }
}
```

Current active code now calls `sp.ProcessWitnessDatums(...)` after
`ProcessRedeemers(...)` for Alonzo+ transactions.

### 2. Available Data Structure

The Alonzo witness set structure (from gouroboros-nectar):

```go
// ledger/alonzo/alonzo.go
type AlonzoTransactionWitnessSet struct {
    // ... other fields ...
    WsPlutusData []cbor.Value `cbor:"4,keyasint,omitempty"`
    // ... other fields ...
}

func (w AlonzoTransactionWitnessSet) PlutusData() []cbor.Value {
    return w.WsPlutusData
}
```

### 3. Historical Missing Implementation

At the time of this report, the ScriptProcessor needed a method to process witness datums:

```go
// Historical proposal; active code now has ProcessWitnessDatums.
func (sp *ScriptProcessor) ProcessWitnessDatums(ctx context.Context, tx *gorm.DB, txHash []byte, witnessSet interface{}) error {
    switch ws := witnessSet.(type) {
    case interface{ PlutusData() []cbor.Value }:
        datums := ws.PlutusData()
        for _, datum := range datums {
            // Calculate datum hash
            datumBytes := datum.Cbor()
            h, _ := blake2b.New256(nil)
            h.Write(datumBytes)
            datumHash := h.Sum(nil)
            
            // Store using datum cache
            if err := sp.datumCache.EnsureDatum(tx, txHash, datumHash, datumBytes); err != nil {
                // Handle error
            }
        }
    }
    return nil
}
```

## Data Flow Diagram

```
Transaction with Smart Contract
├── Transaction Body
│   └── Outputs
│       └── datum_hash (stored in tx_outs.data_hash) ✅
│
└── Witness Set
    ├── Scripts (processed) ✅
    ├── Redeemers (processed) ✅
    └── PlutusData (NOT processed) ❌ <-- THE ISSUE
        └── Actual datum values that should be in data table
```

## Impact Analysis

### What This Means
1. **Smart contract interactions cannot be fully analyzed** - The datum values contain the actual data locked in script outputs
2. **DApp functionality is incomplete** - Cannot decode the state of smart contracts
3. **Historical data is missing** - All Alonzo era datums from already processed blocks are not stored

### Affected Transactions
- Any transaction that locks funds in a smart contract with a datum
- Examples from our data:
  - `3680C1EE...` has output with datum hash `46556B33...`
  - `A8FD0A2F...` has output with datum hash `BEF8663D...`
  - 780 outputs total with datum hashes

## Verification Steps

To confirm this analysis:

1. Check for PlutusData processing:
```bash
grep -rn "PlutusData" processors/
# Result: No matches found
```

2. Check datum storage calls:
```bash
grep -rn "EnsureDatum" processors/
# Result: Only found in ProcessInlineDatums (Babbage+)
```

3. Database verification:
```sql
-- Outputs with datums but no datum records
SELECT COUNT(*) FROM tx_outs WHERE data_hash IS NOT NULL; -- 780
SELECT COUNT(*) FROM data; -- 0
```

## Historical Recommended Solution

### Historical Implementation Steps

1. **Add ProcessWitnessDatums method** to ScriptProcessor (done in active code)
2. **Call it from ProcessTransaction** after processing redeemers (done in active code)
3. **Test with known transactions** that have datums
4. **Consider historical data** - may need a migration script

### Code Changes Required

In `script_processor.go`:

```go
// Add after line 693 (after ProcessRedeemers)
// Process witness datums (Alonzo+)
if blockType >= 5 { // BlockTypeAlonzo
    if err := sp.ProcessWitnessDatums(context.Background(), tx, txHash, witnessSet); err != nil {
        return fmt.Errorf("failed to process witness datums: %w", err)
    }
}
```

### Testing Approach

1. Find a transaction with known datum
2. Process it with the fix
3. Verify datum appears in data table
4. Verify datum hash matches output reference

## Historical Data Consideration

Since Nectar has already processed blocks with datums:
- Option 1: Re-sync from Alonzo start (epoch 290)
- Option 2: Create a migration script to reprocess witness sets
- Option 3: Accept the gap and only process new blocks correctly

## Conclusion

This was a critical missing feature in the ScriptProcessor. Active code now has
the witness-datum processing path, so the old conclusion should not be used as a
current bug report without re-running database verification.

The remaining operational question is whether historical gaps need a backfill
and whether live runs prove the expected `data` rows for known Alonzo+
transactions.
