# Nectar Processor Audit Report

**Date**: June 27, 2025  
**Auditor**: Professional Code Audit  
**Overall Rating**: 7.5/10  
**Status**: Historical snapshot, not a current production-readiness verdict

## Current-State Caveat (May 26, 2026)

This report is useful as an older audit record, but several claims below no
longer match the active Go code:

- `BlockProcessor` is constructed as `NewBlockProcessor(db, cfg)` or
  `NewBlockProcessorWithEvents(db, cfg, eventChan)`, not as a config-free
  constructor.
- The active transaction path wires assets, metadata, scripts, collateral,
  reference inputs, wallet connections, token holders, and token transfers.
  Certificate, withdrawal, and governance processors exist in the package, but
  they are not fields or calls in the main `BlockProcessor` transaction path.
- `txSemaphore` is currently 16, and the default database/worker counts are 8.
- Unit tests exist under `indexer/tests/unit`; `go test ./...` passes in the
  current audit, but coverage was not measured.
- `ScriptProcessor` now processes redeemers, witness datums, and Babbage inline
  datums. Reference script extraction and `txes.script_size` population are
  still not active.
- Token transfer indexing is wired into `BlockProcessor`, and
  `token_wallet_connections` startup creation is now owned by manual DDL in
  `database/migration_helper.go`. Historical backend SQL/backfill DDL still
  duplicate that schema, and token transfer event-bus publication is not wired.

## Executive Summary

This document presents a historical audit of the Nectar blockchain indexer's processor components. It should not be used as the current source of truth without checking the active code and `docs/CURRENT_STATE_AUDIT.md`.

## Table of Contents

1. [Audit Methodology](#audit-methodology)
2. [Component Analysis](#component-analysis)
3. [Critical Findings](#critical-findings)
4. [Security Assessment](#security-assessment)
5. [Performance Analysis](#performance-analysis)
6. [Recommendations](#recommendations)
7. [Implementation Roadmap](#implementation-roadmap)

## Audit Methodology

The audit evaluated each processor component against the following criteria:
- **Code Quality**: Readability, maintainability, and adherence to Go best practices
- **Architecture**: Design patterns, separation of concerns, and scalability
- **Error Handling**: Robustness, recovery mechanisms, and logging
- **Performance**: Efficiency, caching strategies, and resource usage
- **Security**: Input validation, DoS protection, and data integrity
- **Completeness**: Feature coverage and production readiness

## Component Analysis

### 1. BlockProcessor (Rating: 8/10)

**File**: `processors/block_processor.go` (1,651 lines)

#### Strengths
- **Era-Aware Processing**: Excellent handling of all Cardano eras from Byron to Conway
- **Robust Error Handling**: Implements retry logic with exponential backoff
- **Transaction Isolation**: Proper database transaction management with isolation levels
- **Edge Case Handling**: Special logic for Byron-Shelley boundary (slot 4492800)
- **Address Management**: Clever handling of oversized Byron addresses using hash-based storage
- **Performance Optimization**: Dynamic batch sizing based on era characteristics

#### Weaknesses
- **File Size**: At 1,651 lines, violates single responsibility principle
- **Configuration**: Some processor knobs remain hardcoded; active `txSemaphore` is 16
- **Testing**: Unit tests now exist under `indexer/tests/unit`; `go test ./...` passes, but coverage was not measured
- **Documentation**: Missing godoc comments for many public methods

#### Code Example - Good Practice
```go
// Dynamic era configuration
func (bp *BlockProcessor) updateEraConfig(epochNo uint32) {
    oldEra := GetEraName(bp.currentEpoch)
    newEra := GetEraName(uint64(epochNo))
    
    bp.currentEpoch = uint64(epochNo)
    bp.currentEraConfig = GetEraConfig(uint64(epochNo))
    
    if oldEra != newEra {
        log.Printf("[ERA TRANSITION] Entering %s era at epoch %d", newEra, epochNo)
    }
}
```

### 2. ScriptProcessor (Rating: 7/10)

**File**: `processors/script_processor.go`

#### Strengths
- **Comprehensive Script Support**: Handles Native, PlutusV1, PlutusV2, and PlutusV3
- **Efficient Caching**: LRU caches with 100k entry limits for scripts and datums
- **Error Recovery**: Non-fatal error handling allows continued processing
- **Proper Hashing**: Uses Blake2b-256 for script hashes

#### Weaknesses
- **Redeemer Follow-Through**: Redeemers are now read and stored, but script-data hash, script linking, and fee fields remain incomplete
- **Missing Features**: Reference script extraction still returns nil due gouroboros interface limits, and script size tracking is not populated
- **Validation**: No script syntax or size validation
- **The Bug**: Critical logic error (now fixed) that prevented script indexing

#### Critical Bug Fix
```go
// BEFORE (incorrect - scripts were never processed)
if witnessSet == nil {
    // Process scripts when we DON'T have a witness set
    if err := sp.ProcessTransactionScripts(...); err != nil {
        return err
    }
}

// AFTER (correct - scripts processed when witness set exists)
if witnessSet != nil {
    // Process scripts when we HAVE a witness set
    if err := sp.ProcessTransactionScripts(...); err != nil {
        return err
    }
}
```

### 3. CertificateProcessor (Rating: 8.5/10)

**File**: `processors/certificate_processor.go`

Current wiring caveat: this processor exists and contains certificate handling
logic, but the active `BlockProcessor` transaction path does not currently hold
or call a `CertificateProcessor`.

#### Strengths
- **Complete Coverage**: Supports all 18 certificate types including Conway governance
- **Efficient Caching**: Pre-loads ~3,000 pool hashes on startup
- **Metadata Integration**: Queues off-chain metadata fetching
- **Clean Architecture**: Well-organized switch statement for certificate types

#### Weaknesses
- **Cache Persistence**: Pool hash cache is memory-only
- **Batch Processing**: Processes certificates individually instead of batching
- **Validation**: Missing certificate constraint validation

### 4. AssetProcessor (Rating: 7.5/10)

**File**: `processors/asset_processor.go`

#### Strengths
- **Thread Safety**: Proper mutex protection for concurrent access
- **Asset Fingerprints**: Correctly generates CIP-14 fingerprints
- **Mint/Burn Support**: Handles both positive and negative amounts
- **Duplicate Handling**: Uses ON CONFLICT clauses for race conditions

#### Weaknesses
- **Unbounded Cache**: No eviction policy leads to memory growth
- **Metadata**: Doesn't process token metadata (CIP-25/CIP-68)
- **Validation**: No policy script validation
- **Logging**: Limited context in error messages

### 5. MetadataProcessor (Rating: 6.5/10)

**File**: `processors/metadata_processor.go`

#### Strengths
- **CBOR Support**: Handles arbitrary metadata structures
- **Off-chain Integration**: Queues metadata for external fetching

#### Weaknesses
- **Basic Implementation**: Minimal functionality
- **No Validation**: Missing schema validation for standards
- **Security**: No size limits or DoS protection
- **Standards**: Doesn't parse CIP-20/CIP-25/CIP-68 metadata

### 6. WithdrawalProcessor (Rating: 7/10)

**File**: `processors/withdrawal_processor.go`

#### Strengths
- **Focused Design**: Single responsibility implementation
- **Address Validation**: Proper stake address verification

#### Weaknesses
- **Performance**: No batch processing for multiple withdrawals
- **Validation**: Missing amount sanity checks
- **Integration**: No connection to reward calculation system

### 7. GovernanceProcessor (Rating: 6/10)

Current wiring caveat: package code for voting procedures, proposal
procedures, and governance certificates exists, but the active
`BlockProcessor` transaction path does not currently call it.

**File**: `processors/governance_processor.go`

#### Strengths
- **Framework**: Structure in place for Conway features
- **Type Detection**: Identifies governance certificates

#### Weaknesses
- **Implementation**: Mostly stubbed with TODO comments
- **Features**: No vote processing or DRep tracking
- **Testing**: No test coverage for future features

## Critical Findings

### 1. Script Processing Bug (FIXED)
- **Severity**: Critical
- **Impact**: All Alonzo+ smart contract data was not indexed
- **Resolution**: Logic error fixed in commit (witnessSet nil check inverted)

### 2. Memory Management
- **Severity**: Medium
- **Impact**: Unbounded caches could cause OOM in long-running instances
- **Resolution**: Implement LRU eviction for all caches

### 3. Missing Validation
- **Severity**: Medium
- **Impact**: Invalid data could corrupt database
- **Resolution**: Add input validation layer

## Security Assessment

### Vulnerabilities Identified

1. **Input Validation**
   - No size limits on metadata
   - Missing script size validation
   - No certificate constraint checking

2. **DoS Vectors**
   - Unbounded cache growth
   - No rate limiting on metadata fetching
   - Large transaction processing could block

3. **Data Integrity**
   - No checksum validation on scripts
   - Missing referential integrity checks

### Recommendations

```go
// Example: Add input validation
func validateScriptSize(script []byte) error {
    const maxScriptSize = 16384 // 16KB limit
    if len(script) > maxScriptSize {
        return fmt.Errorf("script size %d exceeds limit %d", len(script), maxScriptSize)
    }
    return nil
}

// Example: Add rate limiting
type RateLimiter struct {
    requests chan struct{}
}

func NewRateLimiter(rps int) *RateLimiter {
    rl := &RateLimiter{
        requests: make(chan struct{}, rps),
    }
    go rl.refill(rps)
    return rl
}
```

## Performance Analysis

### Current Performance Characteristics

1. **Block Processing**: 50-100 blocks/second
2. **Memory Usage**: ~1GB after optimizations
3. **Database Connections**: current defaults are 8 max/open connections unless overridden
4. **Batch Sizes**: Era-aware batching exists, but live throughput should be measured from the current binary and database

### Bottlenecks Identified

1. **Sequential Processing**: Transactions processed one at a time
2. **Database Round Trips**: Individual inserts for some components
3. **Cache Lookups**: O(n) for some operations

### Optimization Opportunities

```go
// Example: Parallel processing with worker pool
func (bp *BlockProcessor) processTransactionsParallel(txs []Transaction) error {
    var wg sync.WaitGroup
    errors := make(chan error, len(txs))
    
    for i := 0; i < runtime.NumCPU(); i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for tx := range txChan {
                if err := bp.processTransaction(tx); err != nil {
                    errors <- err
                }
            }
        }()
    }
    
    wg.Wait()
    close(errors)
    
    // Collect errors
    var errs []error
    for err := range errors {
        errs = append(errs, err)
    }
    
    return multierror.Append(nil, errs...)
}
```

## Recommendations

### High Priority (Implement within 1 month)

1. **Add Comprehensive Testing**
   ```go
   // Example test structure
   func TestBlockProcessor_ProcessBlock(t *testing.T) {
       tests := []struct {
           name    string
           block   ledger.Block
           wantErr bool
       }{
           {
               name:    "Byron block",
               block:   mockByronBlock(),
               wantErr: false,
           },
           {
               name:    "Alonzo block with scripts",
               block:   mockAlonzoBlock(),
               wantErr: false,
           },
       }
       
       for _, tt := range tests {
           t.Run(tt.name, func(t *testing.T) {
               cfg := &config.IndexingConfig{
                   Transactions: true,
                   Blocks: true,
                   Inputs: true,
                   Outputs: true,
                   Scripts: true,
                   Collateral: true,
               }
               bp := NewBlockProcessor(mockDB(), cfg)
               err := bp.ProcessBlock(context.Background(), tt.block, 0)
               if (err != nil) != tt.wantErr {
                   t.Errorf("ProcessBlock() error = %v, wantErr %v", err, tt.wantErr)
               }
           })
       }
   }
   ```

2. **Implement Monitoring**
   ```go
   // Prometheus metrics
   var (
       blocksProcessed = prometheus.NewCounterVec(
           prometheus.CounterOpts{
               Name: "nectar_blocks_processed_total",
               Help: "Total number of blocks processed",
           },
           []string{"era"},
       )
       
       processingDuration = prometheus.NewHistogramVec(
           prometheus.HistogramOpts{
               Name: "nectar_block_processing_duration_seconds",
               Help: "Block processing duration in seconds",
           },
           []string{"era"},
       )
   )
   ```

3. **Audit Configuration Coverage**
   ```yaml
   # config.yaml
   processors:
     block:
       tx_semaphore_size: 32
       batch_sizes:
         byron: 5000
         shelley: 2000
         alonzo: 1000
     script:
       cache_size: 100000
       max_script_size: 16384
     asset:
       cache_size: 50000
       batch_size: 1000
   ```

### Medium Priority (Implement within 3 months)

1. **Refactor Large Files**
   - Split BlockProcessor into smaller components
   - Extract transaction processing logic
   - Create separate era handlers

2. **Implement or Wire Missing Features**
   - Wire and verify governance, certificate, and withdrawal processors in the active transaction path
   - Harden redeemer processing by linking script data hash, script hash, execution fee, and validation coverage
   - Implement reference script handling once the ledger interface exposes script refs

3. **Enhance Validation**
   - Add schema validation for metadata
   - Implement certificate constraint checking
   - Validate script sizes and types

### Low Priority (Implement within 6 months)

1. **Performance Optimizations**
   - Implement parallel processing where safe
   - Add connection pooling per worker
   - Optimize cache structures

2. **Advanced Features**
   - Add GraphQL API for queries
   - Implement real-time notifications
   - Add data export functionality

## Implementation Roadmap

### Phase 1: Foundation (Weeks 1-4)
- [ ] Add unit tests for all processors (40% coverage minimum)
- [ ] Implement Prometheus metrics
- [ ] Audit remaining hardcoded processor settings against the current TOML config
- [ ] Fix unbounded cache issues

### Phase 2: Security (Weeks 5-8)
- [ ] Add input validation layer
- [ ] Implement rate limiting
- [ ] Add circuit breakers
- [ ] Security audit by external firm

### Phase 3: Performance (Weeks 9-12)
- [ ] Refactor BlockProcessor
- [ ] Implement parallel processing
- [ ] Optimize database queries
- [ ] Load testing and benchmarking

### Phase 4: Features (Weeks 13-24)
- [ ] Wire and verify governance processor in the active block path
- [ ] Add metadata standards support
- [ ] Implement missing script features
- [ ] Add advanced querying APIs

## Conclusion

This historical audit captured useful processor concerns, but its original production-readiness conclusion is stale. The current source of truth is the active Go code plus the current-state audit; several processor packages exist without being wired into the live transaction path, and derived-table migration ownership still needs cleanup.

The most critical issue (script processing bug) has been resolved, and with the proposed improvements, Nectar can become a best-in-class blockchain indexer for the Cardano ecosystem.

## Appendix: Code Metrics

```
Total Lines of Code: ~5,000
Test Coverage: unit tests exist, but current coverage was not established by this report
Cyclomatic Complexity: Medium (some functions > 10)
Technical Debt Ratio: 15% (acceptable)
Duplication: Low (<5%)
```

---

*This audit was conducted according to industry best practices and standards for blockchain infrastructure code review.*
