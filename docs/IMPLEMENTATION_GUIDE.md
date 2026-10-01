# Nectar Implementation Guide

This guide is a historical roadmap paired with the older processor audit report.
It is not a current implementation manual. Many examples below predate the
current `config.Config`/`config.IndexingConfig` types, the
`NewBlockProcessor(db, cfg)` constructor, the existing unit-test tree, and the
active `nectar.toml` loader. Treat snippets as design notes unless they have
been rechecked against the current Go code.

Current code reality:

- Run commands from `indexer/`, not `/root/workspace/cardano-stack/Nectar`.
- Configuration file support already exists through `nectar.toml`,
  `config.Load`, `init`, `migrate-env`, `--config`, and `-c`.
- Unit tests already exist under `indexer/tests/unit`; `go test ./...` passes
  in the current audit, although coverage is still not measured.
- `BlockProcessor` currently wires assets, metadata, scripts, collateral,
  reference inputs, wallet connections, token holders, and token transfers.
  Certificate, withdrawal, and governance processors exist but are not in the
  active transaction path.
- Prometheus-style monitoring is not an indexer-wide current contract from this
  document; verify actual runtime metrics before documenting it as deployed.

## Quick Start Checklist

- [ ] Run `go mod tidy` to fix module dependencies
- [ ] Fix and run existing unit tests (target 40% coverage in Phase 1)
- [ ] Audit current configuration coverage for remaining hardcoded settings
- [ ] Add Prometheus metrics
- [ ] Fix unbounded caches
- [ ] Add input validation
- [ ] Refactor large files
- [ ] Wire package processors that are implemented but not active

## Phase 1: Foundation (Weeks 1-4)

### 1.1 Fix Module Dependencies

```bash
cd indexer
go mod tidy
```

### 1.2 Add Unit Tests

Create test files for each processor:

```go
// processors/block_processor_test.go
package processors

import (
    "context"
    "testing"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "gorm.io/driver/sqlite"
    "gorm.io/gorm"
)

func setupTestDB(t *testing.T) *gorm.DB {
    db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
    require.NoError(t, err)
    
    // Run migrations
    err = db.AutoMigrate(&models.Block{}, &models.Tx{}, &models.TxOut{}, &models.TxIn{})
    require.NoError(t, err)
    
    return db
}

func TestBlockProcessor_ProcessBlock(t *testing.T) {
    tests := []struct {
        name      string
        blockType uint
        wantErr   bool
    }{
        {
            name:      "Byron block",
            blockType: BlockTypeByronMain,
            wantErr:   false,
        },
        {
            name:      "Shelley block",
            blockType: BlockTypeShelley,
            wantErr:   false,
        },
        {
            name:      "Alonzo block with scripts",
            blockType: BlockTypeAlonzo,
            wantErr:   false,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            db := setupTestDB(t)
            bp := NewBlockProcessor(db, &config.IndexingConfig{
                Transactions: true,
                Blocks: true,
                Metadata: true,
                Assets: true,
                Minting: true,
                UTXOs: true,
                Inputs: true,
                Outputs: true,
                Scripts: true,
                Collateral: true,
            })
            
            // Create mock block
            block := createMockBlock(tt.blockType)
            
            err := bp.ProcessBlock(context.Background(), block, tt.blockType)
            if tt.wantErr {
                assert.Error(t, err)
            } else {
                assert.NoError(t, err)
            }
        })
    }
}

// processors/script_processor_test.go
func TestScriptProcessor_ProcessTransactionScripts(t *testing.T) {
    db := setupTestDB(t)
    sp := NewScriptProcessor(db)
    
    t.Run("PlutusV1 script", func(t *testing.T) {
        txHash := []byte("test-tx-hash")
        witnessSet := &mockWitnessSet{
            plutusV1Scripts: [][]byte{
                []byte("test-script-bytes"),
            },
        }
        
        err := sp.ProcessTransactionScripts(context.Background(), db, txHash, witnessSet)
        assert.NoError(t, err)
        
        // Verify script was stored
        var script models.Script
        err = db.Where("tx_hash = ?", txHash).First(&script).Error
        assert.NoError(t, err)
        assert.Equal(t, "plutus_v1", script.Type)
    })
}
```

### 1.3 Audit Configuration Support

Current status: TOML configuration support exists. The active task is to check
which processor knobs are still hardcoded or inconsistently applied, not to add
configuration support from scratch.

Historical proposal:

```go
// config/config.go
package config

import (
    "github.com/spf13/viper"
)

type Config struct {
    Processors ProcessorsConfig `mapstructure:"processors"`
    Database   DatabaseConfig   `mapstructure:"database"`
    Monitoring MonitoringConfig `mapstructure:"monitoring"`
}

type ProcessorsConfig struct {
    Block  BlockProcessorConfig  `mapstructure:"block"`
    Script ScriptProcessorConfig `mapstructure:"script"`
    Asset  AssetProcessorConfig  `mapstructure:"asset"`
}

type BlockProcessorConfig struct {
    TxSemaphoreSize int                    `mapstructure:"tx_semaphore_size"`
    BatchSizes      map[string]int         `mapstructure:"batch_sizes"`
    CacheSize       int                    `mapstructure:"cache_size"`
}

type ScriptProcessorConfig struct {
    CacheSize      int `mapstructure:"cache_size"`
    MaxScriptSize  int `mapstructure:"max_script_size"`
}

type AssetProcessorConfig struct {
    CacheSize int `mapstructure:"cache_size"`
    BatchSize int `mapstructure:"batch_size"`
}

func LoadConfig(path string) (*Config, error) {
    viper.SetConfigFile(path)
    viper.SetConfigType("yaml")
    
    // Set defaults
    viper.SetDefault("processors.block.tx_semaphore_size", 32)
    viper.SetDefault("processors.block.cache_size", 100000)
    viper.SetDefault("processors.script.cache_size", 100000)
    viper.SetDefault("processors.script.max_script_size", 16384)
    viper.SetDefault("processors.asset.cache_size", 50000)
    
    if err := viper.ReadInConfig(); err != nil {
        return nil, err
    }
    
    var config Config
    if err := viper.Unmarshal(&config); err != nil {
        return nil, err
    }
    
    return &config, nil
}
```

Historical proposal; current `BlockProcessor` already receives
`*config.IndexingConfig`, but does not match the constructor below:

```go
// processors/block_processor.go
func NewBlockProcessor(db *gorm.DB, config *config.BlockProcessorConfig) *BlockProcessor {
    bp := &BlockProcessor{
        db:                   db,
        stakeAddressCache:    NewStakeAddressCache(db),
        errorCollector:       GetGlobalErrorCollector(),
        certificateProcessor: NewCertificateProcessor(db, stakeAddressCache), // not active current field
        withdrawalProcessor:  NewWithdrawalProcessor(db),                     // not active current field
        assetProcessor:       NewAssetProcessor(db),
        metadataProcessor:    NewMetadataProcessor(db),
        governanceProcessor:  NewGovernanceProcessor(db),                     // not active current field
        scriptProcessor:      NewScriptProcessor(db, config.Script),
        adaPotsCalculator:    NewAdaPotsCalculator(db),
        epochParamsProvider:  NewEpochParamsProvider(db),
        txSemaphore:          make(chan struct{}, config.TxSemaphoreSize),
        slotLeaderCache:      NewLRUCache(config.CacheSize),
        currentEraConfig:     GetEraConfig(0),
        currentEpoch:         0,
    }
    return bp
}
```

### 1.4 Add Prometheus Metrics

```go
// metrics/metrics.go
package metrics

import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

var (
    BlocksProcessed = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "nectar_blocks_processed_total",
            Help: "Total number of blocks processed",
        },
        []string{"era"},
    )
    
    TransactionsProcessed = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "nectar_transactions_processed_total",
            Help: "Total number of transactions processed",
        },
        []string{"era"},
    )
    
    ProcessingDuration = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "nectar_processing_duration_seconds",
            Help: "Processing duration in seconds",
            Buckets: prometheus.DefBuckets,
        },
        []string{"component", "operation"},
    )
    
    CacheHitRate = promauto.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "nectar_cache_hit_rate",
            Help: "Cache hit rate percentage",
        },
        []string{"cache_name"},
    )
    
    ErrorCount = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "nectar_errors_total",
            Help: "Total number of errors",
        },
        []string{"component", "error_type"},
    )
)
```

Update processors to record metrics:

```go
// processors/block_processor.go
func (bp *BlockProcessor) ProcessBlock(ctx context.Context, block ledger.Block, blockType uint) error {
    timer := prometheus.NewTimer(metrics.ProcessingDuration.WithLabelValues("block", "process"))
    defer timer.ObserveDuration()
    
    // ... existing code ...
    
    // Record metrics
    metrics.BlocksProcessed.WithLabelValues(bp.getEraName(blockType)).Inc()
    metrics.TransactionsProcessed.WithLabelValues(bp.getEraName(blockType)).Add(float64(len(block.Transactions())))
    
    return nil
}
```

### 1.5 Fix Unbounded Caches

Update the AssetProcessor cache to use LRU:

```go
// processors/asset_processor.go
type AssetProcessor struct {
    db              *gorm.DB
    multiAssetCache *GenericLRUCache // Changed from custom cache
    errorCollector  *ErrorCollector
}

func NewAssetProcessor(db *gorm.DB, config *config.AssetProcessorConfig) *AssetProcessor {
    return &AssetProcessor{
        db:              db,
        multiAssetCache: NewGenericLRUCache(config.CacheSize),
        errorCollector:  GetGlobalErrorCollector(),
    }
}

func (ap *AssetProcessor) ensureMultiAsset(tx *gorm.DB, policyID []byte, assetName []byte) error {
    cacheKey := fmt.Sprintf("%x:%x", policyID, assetName)
    
    // Check cache first
    if _, exists := ap.multiAssetCache.Get(cacheKey); exists {
        return nil
    }
    
    // Check database
    var multiAsset models.MultiAsset
    result := tx.Where("policy = ? AND name = ?", policyID, assetName).First(&multiAsset)
    if result.Error == nil {
        ap.multiAssetCache.Put(cacheKey, true)
        return nil
    }
    
    if result.Error != gorm.ErrRecordNotFound {
        return fmt.Errorf("database error: %w", result.Error)
    }
    
    // Create new multi-asset
    multiAsset = models.MultiAsset{
        Policy:      policyID,
        Name:        assetName,
        Fingerprint: common.NewAssetFingerprint(policyID, assetName).String(),
    }
    
    if err := tx.Clauses(clause.OnConflict{
        Columns:   []clause.Column{{Name: "policy"}, {Name: "name"}},
        DoNothing: true,
    }).Create(&multiAsset).Error; err != nil {
        if !strings.Contains(err.Error(), "Duplicate entry") {
            return fmt.Errorf("failed to create multi-asset: %w", err)
        }
    }
    
    ap.multiAssetCache.Put(cacheKey, true)
    return nil
}
```

## Phase 2: Security (Weeks 5-8)

### 2.1 Add Input Validation Layer

```go
// validation/validation.go
package validation

import (
    "fmt"
    "nectar/models"
)

const (
    MaxScriptSize    = 16384  // 16KB
    MaxMetadataSize  = 16384  // 16KB
    MaxAddressLength = 2048
    MaxAssetNameSize = 32
    MaxPolicyIDSize  = 28
)

type Validator struct {
    config *Config
}

type Config struct {
    MaxScriptSize    int
    MaxMetadataSize  int
    MaxAddressLength int
}

func NewValidator(config *Config) *Validator {
    return &Validator{config: config}
}

func (v *Validator) ValidateScript(script []byte) error {
    if len(script) > v.config.MaxScriptSize {
        return fmt.Errorf("script size %d exceeds limit %d", len(script), v.config.MaxScriptSize)
    }
    return nil
}

func (v *Validator) ValidateAddress(address string) error {
    if len(address) > v.config.MaxAddressLength {
        return fmt.Errorf("address length %d exceeds limit %d", len(address), v.config.MaxAddressLength)
    }
    // Add bech32 validation
    return nil
}

func (v *Validator) ValidateMetadata(metadata []byte) error {
    if len(metadata) > v.config.MaxMetadataSize {
        return fmt.Errorf("metadata size %d exceeds limit %d", len(metadata), v.config.MaxMetadataSize)
    }
    return nil
}

func (v *Validator) ValidateAsset(policyID []byte, assetName []byte) error {
    if len(policyID) != MaxPolicyIDSize {
        return fmt.Errorf("invalid policy ID size: %d", len(policyID))
    }
    if len(assetName) > MaxAssetNameSize {
        return fmt.Errorf("asset name size %d exceeds limit %d", len(assetName), MaxAssetNameSize)
    }
    return nil
}
```

### 2.2 Implement Rate Limiting

```go
// ratelimit/ratelimit.go
package ratelimit

import (
    "context"
    "time"
    "golang.org/x/time/rate"
)

type RateLimiter struct {
    metadataLimiter *rate.Limiter
    scriptLimiter   *rate.Limiter
}

func NewRateLimiter(metadataRPS, scriptRPS int) *RateLimiter {
    return &RateLimiter{
        metadataLimiter: rate.NewLimiter(rate.Limit(metadataRPS), metadataRPS),
        scriptLimiter:   rate.NewLimiter(rate.Limit(scriptRPS), scriptRPS),
    }
}

func (rl *RateLimiter) WaitMetadata(ctx context.Context) error {
    return rl.metadataLimiter.Wait(ctx)
}

func (rl *RateLimiter) WaitScript(ctx context.Context) error {
    return rl.scriptLimiter.Wait(ctx)
}
```

### 2.3 Add Circuit Breakers

```go
// circuitbreaker/circuitbreaker.go
package circuitbreaker

import (
    "github.com/sony/gobreaker"
    "time"
)

type CircuitBreakers struct {
    Database *gobreaker.CircuitBreaker
    Metadata *gobreaker.CircuitBreaker
}

func NewCircuitBreakers() *CircuitBreakers {
    dbSettings := gobreaker.Settings{
        Name:        "database",
        MaxRequests: 3,
        Interval:    10 * time.Second,
        Timeout:     30 * time.Second,
        ReadyToTrip: func(counts gobreaker.Counts) bool {
            failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
            return counts.Requests >= 3 && failureRatio >= 0.6
        },
    }
    
    metadataSettings := gobreaker.Settings{
        Name:        "metadata",
        MaxRequests: 3,
        Interval:    10 * time.Second,
        Timeout:     60 * time.Second,
        ReadyToTrip: func(counts gobreaker.Counts) bool {
            return counts.ConsecutiveFailures >= 3
        },
    }
    
    return &CircuitBreakers{
        Database: gobreaker.NewCircuitBreaker(dbSettings),
        Metadata: gobreaker.NewCircuitBreaker(metadataSettings),
    }
}
```

## Phase 3: Performance (Weeks 9-12)

### 3.1 Refactor BlockProcessor

Split into smaller components:

```go
// processors/block/header_processor.go
package block

type HeaderProcessor struct {
    db              *gorm.DB
    slotLeaderCache *LRUCache
}

func (hp *HeaderProcessor) ProcessHeader(ctx context.Context, block ledger.Block, blockType uint) (*models.Block, error) {
    // Extract header processing logic
}

// processors/block/transaction_processor.go
package block

type TransactionProcessor struct {
    db          *gorm.DB
    semaphore   chan struct{}
    processors  *ComponentProcessors
}

type ComponentProcessors struct {
    Script      *ScriptProcessor
    Certificate *CertificateProcessor
    Asset       *AssetProcessor
    Metadata    *MetadataProcessor
    Withdrawal  *WithdrawalProcessor
    Governance  *GovernanceProcessor
}

func (tp *TransactionProcessor) ProcessTransactions(ctx context.Context, block *models.Block, txs []ledger.Transaction) error {
    // Extract transaction processing logic
}

// processors/block/era_handler.go
package block

type EraHandler interface {
    ProcessBlock(ctx context.Context, block ledger.Block) error
    GetEpochForSlot(slot uint64) uint32
    GetBlockTime(slot uint64) time.Time
}

type ByronEraHandler struct {
    baseHandler
}

type ShelleyEraHandler struct {
    baseHandler
}

type AlonzoEraHandler struct {
    baseHandler
}
```

### 3.2 Implement Parallel Processing

```go
// processors/parallel/worker_pool.go
package parallel

import (
    "context"
    "sync"
    "runtime"
)

type WorkerPool struct {
    workers   int
    taskQueue chan Task
    wg        sync.WaitGroup
}

type Task interface {
    Execute(ctx context.Context) error
}

func NewWorkerPool(workers int) *WorkerPool {
    if workers <= 0 {
        workers = runtime.NumCPU()
    }
    
    return &WorkerPool{
        workers:   workers,
        taskQueue: make(chan Task, workers*2),
    }
}

func (wp *WorkerPool) Start(ctx context.Context) {
    for i := 0; i < wp.workers; i++ {
        wp.wg.Add(1)
        go wp.worker(ctx)
    }
}

func (wp *WorkerPool) Submit(task Task) {
    wp.taskQueue <- task
}

func (wp *WorkerPool) Stop() {
    close(wp.taskQueue)
    wp.wg.Wait()
}

func (wp *WorkerPool) worker(ctx context.Context) {
    defer wp.wg.Done()
    
    for {
        select {
        case task, ok := <-wp.taskQueue:
            if !ok {
                return
            }
            if err := task.Execute(ctx); err != nil {
                // Log error
            }
        case <-ctx.Done():
            return
        }
    }
}
```

## Phase 4: Features (Weeks 13-24)

### 4.1 Wire and Verify Governance Processor

Current status: governance package code exists, including voting/proposal and
certificate-related methods. The active gap is live `BlockProcessor` wiring,
schema/runtime verification, and tests; the examples below are historical
design snippets.

```go
// processors/governance_processor.go
func (gp *GovernanceProcessor) processVotingProcedures(tx *gorm.DB, txHash []byte, procedures interface{}) error {
    // Implement voting procedure processing
    votes := make([]models.Vote, 0)
    
    // Extract votes from procedures
    // ...
    
    // Batch insert votes
    if len(votes) > 0 {
        return tx.CreateInBatches(votes, 100).Error
    }
    
    return nil
}

func (gp *GovernanceProcessor) processProposalProcedures(tx *gorm.DB, txHash []byte, proposals []interface{}) error {
    // Implement proposal processing
    dbProposals := make([]models.GovActionProposal, 0, len(proposals))
    
    for i, proposal := range proposals {
        // Extract proposal data
        // ...
        
        dbProposal := models.GovActionProposal{
            TxHash:      txHash,
            Index:       uint32(i),
            // ... other fields
        }
        
        dbProposals = append(dbProposals, dbProposal)
    }
    
    return tx.CreateInBatches(dbProposals, 100).Error
}
```

### 4.2 Add Metadata Standards Support

```go
// metadata/standards.go
package metadata

import (
    "encoding/json"
)

// CIP-20 Transaction Message Metadata
type CIP20Metadata struct {
    Msg []string `json:"msg"`
}

// CIP-25 NFT Metadata
type CIP25Metadata struct {
    Name        string                 `json:"name"`
    Image       string                 `json:"image"`
    MediaType   string                 `json:"mediaType,omitempty"`
    Description string                 `json:"description,omitempty"`
    Files       []CIP25File           `json:"files,omitempty"`
    Attributes  map[string]interface{} `json:"attributes,omitempty"`
}

type CIP25File struct {
    Name      string `json:"name"`
    MediaType string `json:"mediaType"`
    Src       string `json:"src"`
}

// CIP-68 Reference NFT Metadata
type CIP68Metadata struct {
    Name        string                 `json:"name"`
    Image       string                 `json:"image"`
    Description string                 `json:"description,omitempty"`
    Attributes  map[string]interface{} `json:"attributes,omitempty"`
}

func ParseMetadataStandard(label int, data []byte) (interface{}, error) {
    switch label {
    case 674: // CIP-20 message
        var cip20 CIP20Metadata
        if err := json.Unmarshal(data, &cip20); err != nil {
            return nil, err
        }
        return cip20, nil
        
    case 721: // CIP-25 NFT
        var cip25 map[string]map[string]CIP25Metadata
        if err := json.Unmarshal(data, &cip25); err != nil {
            return nil, err
        }
        return cip25, nil
        
    case 100: // CIP-68 reference token
        var cip68 CIP68Metadata
        if err := json.Unmarshal(data, &cip68); err != nil {
            return nil, err
        }
        return cip68, nil
        
    default:
        return data, nil
    }
}
```

## Testing Strategy

### Unit Test Coverage Goals

- Phase 1: 40% coverage
- Phase 2: 60% coverage
- Phase 3: 80% coverage
- Phase 4: 90% coverage

### Integration Tests

```go
// tests/integration/full_sync_test.go
package integration

import (
    "testing"
    "context"
)

func TestFullBlockSync(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping integration test")
    }
    
    // Setup test database
    db := setupTestDB(t)
    
    // Create processors
    config := loadTestConfig()
    bp := processors.NewBlockProcessor(db, config)
    
    // Load test blocks
    blocks := loadTestBlocks(t, "testdata/blocks")
    
    // Process blocks
    for _, block := range blocks {
        err := bp.ProcessBlock(context.Background(), block.Data, block.Type)
        require.NoError(t, err)
    }
    
    // Verify results
    var blockCount int64
    db.Model(&models.Block{}).Count(&blockCount)
    assert.Equal(t, int64(len(blocks)), blockCount)
}
```

## Monitoring Dashboard

Create a Grafana dashboard with these panels:

1. **Processing Rate**
   - Blocks per second by era
   - Transactions per second
   - Average block processing time

2. **Cache Performance**
   - Hit rate by cache type
   - Cache size over time
   - Eviction rate

3. **Error Tracking**
   - Errors by component
   - Error rate over time
   - Top error types

4. **Database Performance**
   - Query duration
   - Connection pool usage
   - Transaction rollback rate

5. **System Resources**
   - CPU usage by component
   - Memory usage
   - Goroutine count

## Deployment Checklist

- [ ] All tests passing
- [ ] Configuration files created
- [ ] Prometheus metrics exposed
- [ ] Circuit breakers configured
- [ ] Rate limiters set appropriately
- [ ] Logging configured
- [ ] Monitoring dashboards created
- [ ] Documentation updated
- [ ] Security scan completed
- [ ] Performance benchmarks met

## Maintenance Schedule

### Daily
- Monitor error rates
- Check cache hit rates
- Verify sync progress

### Weekly
- Review performance metrics
- Update dependencies
- Run security scans

### Monthly
- Performance profiling
- Code coverage report
- Architecture review

### Quarterly
- Dependency audit
- Security audit
- Performance optimization

---

This implementation guide provides concrete steps to address all findings from the audit report. Follow the phases sequentially for best results.
