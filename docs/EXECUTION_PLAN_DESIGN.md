# Nectar Execution Plan Design Document
**Version**: 1.0  
**Date**: June 27, 2025  
**Status**: Design proposal only; not implemented in the active binary

## Current-State Caveat (May 26, 2026)

No execution-plan runtime is active today. The current CLI supports `init`,
`migrate-env`, `version`, `help`, `--config`/`-c`, and `--clear-shelley`.
There is no active `nectar plan ...` subcommand, `--plan` flag,
execution-plan parser, modular plan processor, or generated GraphQL schema in
the indexer. The active configuration has selective indexing booleans, which
are narrower than the plan language proposed here.

## Executive Summary

This document outlines a design and implementation strategy for adding execution
plans to Nectar. It is retained as planning material, not as a description of
current runtime behavior. With the proposed execution plans, users could:

- Start indexing from any point in the chain
- Select only the data they need (e.g., specific assets, addresses, or time ranges)
- Create custom indexing strategies for specific use cases
- Dramatically reduce storage requirements and sync times
- Build specialized APIs for dApps without unnecessary overhead

## Motivation

Current Cardano indexers like cardano-db-sync and even Carp require indexing the entire blockchain, which:
- Takes 4-5 days to sync from genesis
- Requires ~40GB+ of storage (and growing)
- Indexes data that many applications never use
- Makes it expensive to run multiple specialized instances

Your BubbleMaps-style application is a perfect example: you only need:
- Transactions involving specific assets
- Wallet connections (transfers between addresses)
- Stake address relationships
- No historical data before the asset was created

## Core Concepts

### 1. Execution Plan
An execution plan is a declarative configuration that defines:
- **What** to index (data selectors)
- **When** to start/stop (temporal boundaries)
- **How** to process (processing rules)
- **Where** to store (storage strategies)

### 2. Data Selectors
Modular filters that determine which data to process:
- Asset filters (policy IDs, asset names)
- Address filters (specific addresses, stake pools)
- Transaction filters (metadata keys, script types)
- Block filters (time ranges, epochs)

### 3. Processing Modules
Independent processors that can be enabled/disabled:
- Core (blocks, transactions)
- Assets (minting, transfers)
- Staking (delegations, rewards)
- Governance (votes, proposals)
- Scripts (Plutus, native scripts)
- Metadata (transaction metadata)

## Execution Plan Schema

```toml
# nectar-execution-plan.toml
version = "1.0"

[plan]
name = "bubble-maps-indexer"
description = "Index only specific asset transfers and relationships"

# Temporal boundaries
[plan.boundaries]
start_point = "slot:45000000"  # Can be: "genesis", "slot:N", "block:hash", "tip-N"
end_point = "follow-tip"       # Can be: "slot:N", "block:hash", "follow-tip"
stop_on_error = false

# Data selectors
[plan.selectors]
# Filter by assets
[[plan.selectors.assets]]
policy_id = "f0ff48bbb7bbe9d59a40f1ce90e9e9d0ff5002ec48f232b49ca0fb9a"
asset_name = "*"  # Wildcard for all assets under this policy

# Filter by addresses (optional)
[[plan.selectors.addresses]]
type = "payment"
addresses = [
  "addr1...",
  "addr2..."
]

# Filter by stake addresses
[[plan.selectors.stake_addresses]]
type = "any"  # Track all stake addresses involved with our assets

# Processing modules
[plan.modules]
core = { enabled = true, options = { minimal = true } }
assets = { enabled = true, options = { track_history = true } }
staking = { enabled = true, options = { only_delegations = true } }
governance = { enabled = false }
scripts = { enabled = false }
metadata = { enabled = true, options = { keys = [721, 20] } }

# Storage strategies
[plan.storage]
mode = "optimized"  # "full", "optimized", "minimal"
indexes = [
  "asset_transfers",
  "address_connections",
  "stake_relationships"
]

# Performance hints
[plan.performance]
parallel_assets = true
batch_size = 1000
memory_limit = "4GB"
```

## Implementation Architecture

### 1. Plan Parser & Validator
```go
type ExecutionPlan struct {
    Version     string
    Plan        PlanConfig
    Selectors   []Selector
    Modules     map[string]ModuleConfig
    Storage     StorageConfig
    Performance PerformanceHints
}

type Selector interface {
    ShouldProcess(block Block, tx Transaction) bool
    GetDependencies() []string
}

type Module interface {
    Name() string
    Initialize(plan *ExecutionPlan) error
    Process(ctx context.Context, data BlockData) error
    Cleanup() error
}
```

### 2. Modular Processor Architecture
```go
// Current monolithic approach
func (bp *BlockProcessor) ProcessBlock(block Block) error {
    // Process everything...
}

// New modular approach
type ModularProcessor struct {
    plan     *ExecutionPlan
    modules  []Module
    selector Selector
}

func (mp *ModularProcessor) ProcessBlock(block Block) error {
    // Check if block matches plan boundaries
    if !mp.plan.InBoundaries(block) {
        return nil
    }
    
    // Pre-filter transactions
    relevantTxs := []Transaction{}
    for _, tx := range block.Transactions() {
        if mp.selector.ShouldProcess(block, tx) {
            relevantTxs = append(relevantTxs, tx)
        }
    }
    
    // Skip block if no relevant transactions
    if len(relevantTxs) == 0 {
        return mp.recordEmptyBlock(block)
    }
    
    // Process with enabled modules only
    blockData := BlockData{Block: block, Transactions: relevantTxs}
    for _, module := range mp.modules {
        if err := module.Process(ctx, blockData); err != nil {
            return err
        }
    }
    
    return nil
}
```

### 3. Dynamic Module Loading
```go
// Module registry
var moduleRegistry = map[string]ModuleFactory{
    "core":       NewCoreModule,
    "assets":     NewAssetsModule,
    "staking":    NewStakingModule,
    "governance": NewGovernanceModule,
    "scripts":    NewScriptsModule,
    "metadata":   NewMetadataModule,
}

func LoadModules(plan *ExecutionPlan) ([]Module, error) {
    modules := []Module{}
    
    for name, config := range plan.Modules {
        if !config.Enabled {
            continue
        }
        
        factory, exists := moduleRegistry[name]
        if !exists {
            return nil, fmt.Errorf("unknown module: %s", name)
        }
        
        module := factory(config.Options)
        if err := module.Initialize(plan); err != nil {
            return nil, fmt.Errorf("failed to initialize %s: %w", name, err)
        }
        
        modules = append(modules, module)
    }
    
    return modules, nil
}
```

### 4. Optimized Storage Strategies

#### Minimal Storage Mode
```sql
-- Only store what's needed for the use case
CREATE TABLE asset_transfers (
    tx_hash BYTEA PRIMARY KEY,
    slot_no BIGINT NOT NULL,
    from_address BYTEA,
    to_address BYTEA,
    asset_id BYTEA NOT NULL,
    amount NUMERIC NOT NULL,
    INDEX idx_asset_transfers_asset (asset_id),
    INDEX idx_asset_transfers_addresses (from_address, to_address)
);

CREATE TABLE address_connections (
    address_a BYTEA,
    address_b BYTEA,
    first_interaction BIGINT,
    last_interaction BIGINT,
    interaction_count INT,
    total_value NUMERIC,
    PRIMARY KEY (address_a, address_b)
);
```

#### Optimized Storage Mode
```sql
-- Balance between functionality and storage
-- Includes core tables but with filtered data
CREATE TABLE blocks (
    hash BYTEA PRIMARY KEY,
    slot_no BIGINT,
    -- Only blocks with relevant transactions
);

CREATE TABLE tx (
    hash BYTEA PRIMARY KEY,
    block_hash BYTEA REFERENCES blocks(hash),
    -- Only transactions matching selectors
);
```

### 5. Selector Implementation Examples

#### Asset Selector
```go
type AssetSelector struct {
    policies map[string]bool
    assets   map[string]bool
}

func (as *AssetSelector) ShouldProcess(block Block, tx Transaction) bool {
    // Check outputs for matching assets
    for _, output := range tx.Outputs() {
        for _, asset := range output.Assets() {
            if as.policies[asset.PolicyID] {
                return true
            }
            if as.assets[asset.FullName()] {
                return true
            }
        }
    }
    
    // Check minting
    if mint := tx.Mint(); mint != nil {
        for policyID := range mint {
            if as.policies[policyID] {
                return true
            }
        }
    }
    
    return false
}
```

#### Time Range Selector
```go
type TimeRangeSelector struct {
    startSlot uint64
    endSlot   uint64
}

func (trs *TimeRangeSelector) ShouldProcess(block Block, tx Transaction) bool {
    slot := block.Slot()
    return slot >= trs.startSlot && slot <= trs.endSlot
}
```

## Integration with Existing System

### 1. Configuration Extension
```toml
# nectar.toml
[execution_plan]
enabled = true
plan_file = "execution-plan.toml"  # Optional, can be inline
mode = "strict"  # "strict" or "adaptive"

# Inline plan (alternative to external file)
[execution_plan.inline]
start_point = "tip-1000"  # Start 1000 blocks from tip
modules = { core = true, assets = true }
```

### 2. CLI Integration
Status: design only. The active CLI currently implements `init`, `migrate-env`, `version`, `help`, `--config/-c`, and `--clear-shelley`; the `nectar plan ...` commands below are not implemented.

```bash
# Initialize with execution plan
nectar init --with-plan bubble-maps

# Generate execution plan template
nectar plan init --template asset-tracker

# Validate execution plan
nectar plan validate my-plan.toml

# Estimate resource requirements
nectar plan estimate my-plan.toml --network mainnet

# Start with execution plan
nectar --plan my-plan.toml
```

### 3. Migration Path
```go
// Backward compatibility wrapper
func CreateProcessor(cfg *Config) (Processor, error) {
    if cfg.ExecutionPlan.Enabled {
        plan, err := LoadExecutionPlan(cfg.ExecutionPlan)
        if err != nil {
            return nil, err
        }
        return NewModularProcessor(plan)
    }
    
    // Fall back to current full-chain processor
    return NewBlockProcessor(cfg)
}
```

## Use Case Examples

### 1. NFT Marketplace Indexer
```toml
[plan]
name = "nft-marketplace"

[plan.selectors.metadata]
keys = [721]  # CIP-25 NFT metadata

[plan.modules]
core = { enabled = true, options = { minimal = true } }
assets = { enabled = true, options = { only_nfts = true } }
metadata = { enabled = true, options = { parse_cip25 = true } }
```

### 2. DeFi Protocol Indexer
```toml
[plan]
name = "defi-protocol"

[plan.selectors.scripts]
hashes = [
  "script1...",  # Protocol validator
  "script2..."   # Liquidity pool validator
]

[plan.modules]
core = { enabled = true }
scripts = { enabled = true, options = { track_datum = true } }
assets = { enabled = true, options = { track_liquidity = true } }
```

### 3. Stake Pool Analytics
```toml
[plan]
name = "stake-analytics"

[plan.selectors.stake_pools]
pool_ids = ["pool1...", "pool2..."]

[plan.modules]
core = { enabled = true, options = { minimal = true } }
staking = { enabled = true, options = { 
    track_delegations = true,
    track_rewards = true,
    track_pool_updates = true
}}
```

## Performance Optimizations

### 1. Parallel Processing
- Process multiple assets in parallel
- Independent module execution
- Concurrent selector evaluation

### 2. Skip Strategies
- Skip entire epochs without relevant data
- Fast-forward through empty blocks
- Checkpoint-based resumption

### 3. Memory Management
- Bounded queues per module
- Lazy loading of block data
- Streaming processing for large blocks

## GraphQL Schema Generation

### Dynamic Schema Based on Plan
```graphql
# Generated for bubble-maps plan
type Query {
  # Only includes enabled modules
  assetTransfers(
    assetId: String!
    first: Int
    after: String
  ): AssetTransferConnection!
  
  addressConnections(
    address: String!
    minInteractions: Int
  ): [AddressConnection!]!
  
  # Disabled modules not included
  # governance: Not available
  # scripts: Not available
}

type AssetTransfer {
  txHash: String!
  slot: BigInt!
  fromAddress: String
  toAddress: String
  amount: String!
}
```

## Monitoring & Observability

### 1. Plan Metrics
```go
type PlanMetrics struct {
    BlocksProcessed   int64
    BlocksSkipped     int64
    TxProcessed       int64
    TxFiltered        int64
    StorageSaved      int64  // Bytes saved vs full sync
    ProcessingTime    time.Duration
}
```

### 2. Dashboard Integration
- Show active execution plan
- Display filtering efficiency
- Estimate time to completion
- Resource usage vs full sync

## Future Enhancements

### 1. Plan Composition
```toml
# Combine multiple plans
[plan]
name = "combined"
include = ["nft-plan.toml", "defi-plan.toml"]
```

### 2. Dynamic Plan Updates
- Add new selectors without restart
- Enable/disable modules on the fly
- Adjust performance parameters

### 3. Plan Marketplace
- Community-contributed plans
- Verified plan templates
- One-click deployment

## Implementation Timeline

### Phase 1: Foundation (2 weeks)
- [ ] Design selector interface
- [ ] Implement module system
- [ ] Create plan parser/validator
- [ ] Add CLI commands

### Phase 2: Core Modules (3 weeks)
- [ ] Refactor existing processors into modules
- [ ] Implement basic selectors
- [ ] Add storage strategies
- [ ] Create test framework

### Phase 3: Integration (2 weeks)
- [ ] Integrate with initialization
- [ ] Update configuration system
- [ ] Modify sync logic
- [ ] Update dashboard

### Phase 4: Optimization (2 weeks)
- [ ] Add parallel processing
- [ ] Implement skip strategies
- [ ] Optimize memory usage
- [ ] Performance testing

### Phase 5: Polish (1 week)
- [ ] Documentation
- [ ] Example plans
- [ ] Migration guide
- [ ] Release preparation

## Conclusion

Execution plans will transform Nectar into the most flexible Cardano indexer available. By allowing users to index only what they need, we can:

1. **Reduce sync time** from days to hours or minutes
2. **Lower storage requirements** by 90%+ for specialized use cases
3. **Enable new use cases** that were impractical with full-chain indexing
4. **Improve performance** by processing only relevant data
5. **Reduce operational costs** for running specialized indexers

This design maintains backward compatibility while opening up entirely new possibilities for Cardano developers. The modular architecture ensures that new features can be added without disrupting existing functionality, making Nectar future-proof and adaptable to evolving needs.
