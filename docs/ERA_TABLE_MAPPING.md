# Cardano Era Table Mapping for Nectar

## Overview
This document maps which database tables should contain data in each Cardano era.
It now distinguishes three different states that older versions of this file
blurred together:

- A table/model exists in the codebase.
- A processor package can write the table.
- The active `BlockProcessor` transaction path actually calls that processor.

As of May 26, 2026, the active path calls assets, metadata, scripts,
collateral/reference-input handling, wallet connections, token holders, and
token transfers. Certificate, withdrawal, and governance processors exist, but
they are not currently fields/calls in the main transaction path.

## Era Timeline
- **Byron**: Epochs 0-207 (Slots 0-4,492,799)
- **Shelley**: Epochs 208-235 (Slots 4,492,800-16,588,799)
- **Allegra**: Epochs 236-250 (Slots 16,588,800-23,068,799)
- **Mary**: Epochs 251-289 (Slots 23,068,800-39,916,799)
- **Alonzo**: Epochs 290-364 (Slots 39,916,800-72,316,799)
- **Babbage**: Epochs 365-490 (Slots 72,316,800-134,534,399)
- **Conway**: Epochs 491+ (Slots 134,534,400+)

## Tables by Era

### Active or Intended Tables by Era

#### All Eras (Byron+)
- `blocks` - Block data
- `txes` - Transactions
- `tx_ins` - Transaction inputs
- `tx_outs` - Transaction outputs
- `slot_leaders` - Block producers

#### Shelley+ (Epochs 208+)
- `epoches` - Epoch boundaries are handled by `BlockProcessor`.
- `stake_addresses`, `stake_registrations`, `stake_deregistrations`,
  `delegations`, `withdrawals`, `pool_hashes`, `pool_updates`, `pool_retires`,
  `pool_metadata_refs`, `pool_owners`, and `pool_relays` have models and
  processor-package support, but the active `BlockProcessor` transaction path
  does not currently wire certificate or withdrawal processors. Do not assume
  these tables are populated in a fresh active run without runtime verification.

#### Allegra+ (Epochs 236+)
- Transaction validity intervals (`invalid_before`, `invalid_hereafter` in `txes`)

#### Mary+ (Epochs 251+)
- `multi_assets` - Native token definitions
- `ma_tx_mints` - Token minting/burning
- `ma_tx_outs` - Multi-asset outputs

#### Alonzo+ (Epochs 290+)
- `scripts` - Smart contract scripts
- `redeemers` - Script redeemers
- `data` - Datum values
- `collateral_tx_ins` - Collateral inputs
- `collateral_tx_outs` - Collateral outputs (Babbage+)
- `reference_tx_ins` - Reference inputs (Babbage+)
- `required_signers` - Required signer hashes when script indexing is enabled

Reference script caveat: model fields exist for reference script hashes, but
the current `getOutputReferenceScript` path still returns nil because the
needed script-ref accessor is not exposed through the active ledger interface.

#### Conway+ (Epochs 491+)
- Governance models and package processors exist for DReps, votes, proposals,
  committees, constitutions, treasury withdrawals, and related data. They are
  not currently wired into the active `BlockProcessor` transaction path, so this
  is implemented package code rather than proven live population.

### Partial or Not Yet Proven State Query Tables

These tables require local state query integration or currently have partial
runtime behavior:

#### Epoch-level data
- `ada_pots` - ADA distribution calculator exists, but is not wired as an
  active `BlockProcessor` field.
- `epoch_params` - `EpochParamsProvider` is called at epoch boundaries.
- `epoch_stake_progresses` - Stake distribution snapshots

#### Reward data
- `rewards` - Local reward calculation is conditionally invoked through
  `StateQueryService`; direct live-state reward querying still contains
  placeholder/incomplete paths.
- `instant_rewards` - MIR certificates

#### Pool performance
- `pool_stats` - Pool performance metrics

## Historical Snapshot: Allegra (Epoch 242)

The counts below are a prior database snapshot, not a current verification of a
fresh run.

### ✅ Working Correctly
- 192,951 blocks
- 526,633 transactions
- 1,296,030 outputs
- 102,369 delegations
- 68,507 withdrawals
- **Validity intervals**: 99.9% of Allegra transactions use this feature

### Historical Missing/Expected
- **Metadata**: Not common until later epochs
- **Scripts**: Not available until Alonzo
- **Multi-assets**: Not available until Mary
- **Ada pots**: still not proven as actively wired
- **Epoch params**: now have an epoch-boundary provider path; verify against a live DB before claiming full db-sync parity

## Verification Commands

```sql
-- Check what's populated in current era
SELECT 
    'blocks' as table_name, COUNT(*) as count 
FROM blocks 
WHERE epoch_no >= 236
UNION ALL
SELECT 'txes', COUNT(*) 
FROM txes t 
JOIN blocks b ON t.block_hash = b.hash 
WHERE b.epoch_no >= 236;

-- Check Allegra-specific features
SELECT 
    COUNT(*) as total_txs,
    SUM(CASE WHEN invalid_before IS NOT NULL OR invalid_hereafter IS NOT NULL THEN 1 ELSE 0 END) as with_validity
FROM txes t 
JOIN blocks b ON t.block_hash = b.hash 
WHERE b.epoch_no BETWEEN 236 AND 250;
```

## Notes
- Do not treat table/model existence as proof that live indexing populates the
  table.
- State-query features are mixed: `epoch_params` has an active provider path,
  rewards are conditional/partial, and `ada_pots` is not actively wired.
- The current implementation scope still needs runtime verification before any
  broad "working as designed" claim.
