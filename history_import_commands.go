package main

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	connect "connectrpc.com/connect"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	utxosync "github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync/syncconnect"
	"golang.org/x/net/http2"
	"gorm.io/gorm"

	"nectar/bootstrap"
	unifiederrors "nectar/errors"
	"nectar/processors"
	"nectar/snapshot"
)

const (
	defaultHistoryBatchSize        = 100
	defaultHistoryCommitBlocks     = 100
	defaultHistoryCheckpointSource = "dolos-dumphistory"
)

type historyImportOptions struct {
	ConfigPath          string
	DolosDir            string
	Endpoint            string
	Protocol            string
	BatchSize           int
	HistoryCommitBlocks int
	Limit               int64
	Apply               bool
	AllowPartialApply   bool
	Resume              bool
	ResetCheckpoint     bool
	CheckpointSource    string
	StartTokenHashHex   string
	StartTokenSlot      uint64
	SkipMigrations      bool
	RebuildProducts     bool
	Status              bool
	DerivedProducts     bool
	ProductChunkSlots   uint64
	Timeout             time.Duration
	ProgressEveryPages  int64
}

type historyImportStats struct {
	Pages           int64
	BlocksFetched   int64
	BlocksDecoded   int64
	BlocksProcessed int64
	Skipped         int64
	LastSlot        uint64
	LastBlockNo     uint64
	LastBlockHash   []byte
	NextTokenHash   []byte
	NextTokenIndex  uint64
	Completed       bool
	Applied         bool
	StartedAt       time.Time
	FinishedAt      time.Time
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func handleHistoryImportCommand(args []string) {
	fs := flag.NewFlagSet("history-import", flag.ExitOnError)
	configPath := fs.String("config", "nectar.toml", "Path to Nectar config")
	fs.StringVar(configPath, "c", "nectar.toml", "Path to Nectar config")
	dolosDir := fs.String("dolos-dir", firstNonEmpty(os.Getenv("DOLOS_DIR"), defaultDolosDir), "Dolos node directory used to infer the UTxO RPC endpoint")
	endpoint := fs.String("endpoint", "", "UTxO RPC base endpoint; defaults to Dolos serve.grpc")
	protocol := fs.String("protocol", snapshot.ProtocolGRPC, "UTxO RPC protocol: grpc or connect")
	batchSize := fs.Int("batch-size", defaultHistoryBatchSize, "Blocks to request per DumpHistory page; Dolos max is 100")
	historyCommitBlocks := fs.Int("history-commit-blocks", defaultHistoryCommitBlocks, "Historical blocks to commit per SQL transaction during apply; use 1 for per-block commits")
	limit := fs.Int64("limit", 0, "Maximum historical blocks to read; 0 means until Dolos archive end")
	apply := fs.Bool("apply", false, "Write decoded blocks to SQL through Nectar BlockProcessor")
	allowPartialApply := fs.Bool("allow-partial-apply", false, "Allow --apply with --limit or --start-token-hash; only for tests, not full chart data")
	resume := fs.Bool("resume", true, "Resume from the saved history import checkpoint when applying")
	resetCheckpoint := fs.Bool("reset-checkpoint", false, "Clear the saved history import checkpoint before applying")
	checkpointSource := fs.String("checkpoint-source", defaultHistoryCheckpointSource, "Checkpoint row id for this import source")
	startTokenHash := fs.String("start-token-hash", "", "Optional hex block hash to start DumpHistory from; ignored when --resume finds a checkpoint")
	startTokenSlot := fs.Uint64("start-token-slot", 0, "Optional slot to start DumpHistory from; encoded as the v1alpha BlockRef index field")
	skipMigrations := fs.Bool("skip-migrations", false, "Skip database migrations before applying")
	rebuildProducts := fs.Bool("rebuild-products", false, "Run rebuild-products for holders, wallet graph, and token graph after a completed apply")
	status := fs.Bool("status", false, "Print history import checkpoint and table counts without connecting to Dolos")
	derivedProducts := fs.Bool("derived-products", false, "Populate derived product tables during history import; slower than rebuilding products after completion")
	productChunkSlots := fs.Uint64("product-chunk-slots", defaultProductChunkSlots, "Slot range per product graph rebuild chunk")
	timeout := fs.Duration("timeout", 60*time.Second, "Per-request DumpHistory timeout")
	progressEveryPages := fs.Int64("progress-every-pages", 10, "Log progress every N pages; 0 disables periodic progress logs")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("failed to parse history-import flags: %v", err)
	}

	options := historyImportOptions{
		ConfigPath:          *configPath,
		DolosDir:            *dolosDir,
		Endpoint:            *endpoint,
		Protocol:            *protocol,
		BatchSize:           *batchSize,
		HistoryCommitBlocks: *historyCommitBlocks,
		Limit:               *limit,
		Apply:               *apply,
		AllowPartialApply:   *allowPartialApply,
		Resume:              *resume,
		ResetCheckpoint:     *resetCheckpoint,
		CheckpointSource:    strings.TrimSpace(*checkpointSource),
		StartTokenHashHex:   strings.TrimSpace(*startTokenHash),
		StartTokenSlot:      *startTokenSlot,
		SkipMigrations:      *skipMigrations,
		RebuildProducts:     *rebuildProducts,
		Status:              *status,
		DerivedProducts:     *derivedProducts,
		ProductChunkSlots:   *productChunkSlots,
		Timeout:             *timeout,
		ProgressEveryPages:  *progressEveryPages,
	}
	if err := runHistoryImportCommand(context.Background(), options); err != nil {
		log.Fatal(err)
	}
}

func runHistoryImportCommand(ctx context.Context, options historyImportOptions) error {
	if options.BatchSize <= 0 {
		return fmt.Errorf("--batch-size must be positive")
	}
	if options.BatchSize > defaultHistoryBatchSize {
		return fmt.Errorf("--batch-size cannot exceed %d because Dolos DumpHistory enforces that maximum", defaultHistoryBatchSize)
	}
	if options.HistoryCommitBlocks <= 0 {
		return fmt.Errorf("--history-commit-blocks must be positive")
	}
	if options.Timeout <= 0 {
		options.Timeout = 60 * time.Second
	}
	if options.CheckpointSource == "" {
		options.CheckpointSource = defaultHistoryCheckpointSource
	}
	if options.Apply && !options.AllowPartialApply && (options.Limit > 0 || options.StartTokenHashHex != "" || options.StartTokenSlot > 0) {
		return fmt.Errorf("--apply refuses partial historical writes by default; remove --limit/--start-token-hash/--start-token-slot for full chart data or pass --allow-partial-apply for integration tests")
	}
	if options.RebuildProducts && !options.Apply {
		return fmt.Errorf("--rebuild-products requires --apply")
	}
	if options.Status && (options.Apply || options.ResetCheckpoint || options.RebuildProducts) {
		return fmt.Errorf("--status cannot be combined with --apply, --reset-checkpoint, or --rebuild-products")
	}
	if options.ProductChunkSlots == 0 {
		return fmt.Errorf("--product-chunk-slots must be positive")
	}

	if options.Status {
		_, db := openProductCommandDB(options.ConfigPath, false)
		defer closeDB(db)
		return printHistoryImportStatus(db, options.CheckpointSource)
	}

	if strings.TrimSpace(options.Endpoint) == "" {
		report, err := bootstrap.InspectDolos(options.DolosDir, false)
		if err != nil {
			return fmt.Errorf("inspect Dolos for endpoint: %w", err)
		}
		options.Endpoint = report.GRPCEndpoint
	}
	if strings.TrimSpace(options.Endpoint) == "" {
		return fmt.Errorf("UTxO RPC endpoint is required")
	}

	var db *gorm.DB
	var processor *processors.BlockProcessor
	if options.Apply {
		unifiederrors.Initialize(nil)
		cfg, openedDB := openProductCommandDB(options.ConfigPath, !options.SkipMigrations)
		db = openedDB
		defer closeDB(db)
		if err := ensureHistoryImportCheckpointTable(db); err != nil {
			return err
		}
		if options.ResetCheckpoint {
			if err := deleteHistoryImportCheckpoint(db, options.CheckpointSource); err != nil {
				return err
			}
		}
		processor = processors.NewBlockProcessorForHistoryImport(db, &cfg.Indexing, options.DerivedProducts)
		log.Printf("[HISTORY] Applying Dolos history with driver=%s profile=%s endpoint=%s derived_products=%t history_commit_blocks=%d", cfg.Database.Driver, cfg.Indexing.Profile, options.Endpoint, options.DerivedProducts, options.HistoryCommitBlocks)
		if options.RebuildProducts {
			cp, found, err := readHistoryImportCheckpoint(db, options.CheckpointSource)
			if err != nil {
				return err
			}
			if found && cp.Completed {
				log.Printf("[HISTORY] Checkpoint %q is already complete at slot=%d block=%d; rebuilding product tables", options.CheckpointSource, cp.LastSlot, cp.LastBlockNo)
				return rebuildHistoryImportProducts(db, options.ProductChunkSlots)
			}
		}
	} else {
		log.Printf("[HISTORY] Dry-run Dolos history decode from endpoint=%s", options.Endpoint)
	}

	client, err := newHistorySyncClient(options.Endpoint, options.Protocol)
	if err != nil {
		return err
	}

	startState, err := buildInitialHistoryStart(db, options)
	if err != nil {
		return err
	}

	importer := &dolosHistoryImporter{
		client:    client,
		db:        db,
		processor: processor,
		options:   options,
	}
	stats, err := importer.Import(ctx, startState.Token, startState.Stats)
	if err != nil {
		return err
	}
	printHistoryImportStats(stats)

	if options.Apply && options.RebuildProducts && stats.Completed {
		return rebuildHistoryImportProducts(db, options.ProductChunkSlots)
	} else if options.Apply && options.RebuildProducts && !stats.Completed {
		log.Printf("[HISTORY] Skipping product rebuild because historical import did not complete")
	}

	if !options.Apply {
		fmt.Println("Dry run only. Pass --apply to write SQL rows through BlockProcessor.")
	}
	return nil
}

func rebuildHistoryImportProducts(db *gorm.DB, chunkSlots uint64) error {
	if err := rebuildTokenHolders(db); err != nil {
		return fmt.Errorf("rebuild token_holders: %w", err)
	}
	maxSlot, err := maxIndexedSlot(db)
	if err != nil {
		return fmt.Errorf("read max indexed slot: %w", err)
	}
	minSlot, err := minIndexedSlot(db)
	if err != nil {
		return fmt.Errorf("read min indexed slot: %w", err)
	}
	if err := rebuildWalletConnections(db, minSlot, maxSlot, chunkSlots); err != nil {
		return fmt.Errorf("rebuild wallet connections: %w", err)
	}
	if err := rebuildTokenWalletConnections(db, minSlot, maxSlot, chunkSlots); err != nil {
		return fmt.Errorf("rebuild token wallet connections: %w", err)
	}
	if err := printProductCounts(db); err != nil {
		return fmt.Errorf("print product counts: %w", err)
	}
	return nil
}

type dolosHistorySyncClient interface {
	DumpHistory(context.Context, *connect.Request[utxosync.DumpHistoryRequest]) (*connect.Response[utxosync.DumpHistoryResponse], error)
	ReadTip(context.Context, *connect.Request[utxosync.ReadTipRequest]) (*connect.Response[utxosync.ReadTipResponse], error)
}

type dolosHistoryImporter struct {
	client    dolosHistorySyncClient
	db        *gorm.DB
	processor *processors.BlockProcessor
	options   historyImportOptions
}

func (i *dolosHistoryImporter) Import(ctx context.Context, startToken *utxosync.BlockRef, initialStats historyImportStats) (historyImportStats, error) {
	if i.options.Apply && (i.db == nil || i.processor == nil) {
		return historyImportStats{}, fmt.Errorf("database and processor are required when apply is enabled")
	}

	stats := initialStats
	stats.Applied = i.options.Apply
	stats.StartedAt = time.Now()
	stats.FinishedAt = time.Time{}
	token := cloneHistoryBlockRef(startToken)

	for {
		reqCtx, cancel := context.WithTimeout(ctx, i.options.Timeout)
		resp, err := i.client.DumpHistory(reqCtx, connect.NewRequest(&utxosync.DumpHistoryRequest{
			StartToken: token,
			MaxItems:   uint32(i.options.BatchSize),
		}))
		cancel()
		if err != nil {
			return stats, fmt.Errorf("dump history: %w", err)
		}

		blocks := resp.Msg.GetBlock()
		if i.options.Limit > 0 {
			remaining := i.options.Limit - stats.BlocksFetched
			if remaining <= 0 {
				break
			}
			if int64(len(blocks)) > remaining {
				blocks = blocks[:remaining]
			}
		}

		if len(blocks) == 0 {
			stats.NextTokenHash = blockRefHash(resp.Msg.GetNextToken())
			stats.NextTokenIndex = blockRefIndex(resp.Msg.GetNextToken())
			stats.Completed = resp.Msg.GetNextToken() == nil
			break
		}

		pendingBlocks := make([]ledger.Block, 0, minInt(i.options.HistoryCommitBlocks, len(blocks)))
		pendingBlockTypes := make([]uint, 0, minInt(i.options.HistoryCommitBlocks, len(blocks)))
		flushPendingBlocks := func() error {
			if len(pendingBlocks) == 0 {
				return nil
			}
			if err := i.processor.ProcessHistoryBlocks(ctx, pendingBlocks, pendingBlockTypes); err != nil {
				return err
			}
			stats.BlocksProcessed += int64(len(pendingBlocks))
			pendingBlocks = pendingBlocks[:0]
			pendingBlockTypes = pendingBlockTypes[:0]
			return nil
		}

		for _, raw := range blocks {
			stats.BlocksFetched++
			block, blockType, err := decodeHistoryBlock(raw)
			if err != nil {
				stats.Skipped++
				return stats, err
			}
			stats.BlocksDecoded++

			if i.options.Apply {
				pendingBlocks = append(pendingBlocks, block)
				pendingBlockTypes = append(pendingBlockTypes, blockType)
				if len(pendingBlocks) >= i.options.HistoryCommitBlocks {
					if err := flushPendingBlocks(); err != nil {
						return stats, fmt.Errorf("process history block batch through slot=%d block=%d: %w", block.SlotNumber(), block.BlockNumber(), err)
					}
				}
			}

			hash := block.Header().Hash()
			stats.LastSlot = block.SlotNumber()
			stats.LastBlockNo = block.BlockNumber()
			stats.LastBlockHash = append(stats.LastBlockHash[:0], hash[:]...)
		}

		if i.options.Apply {
			if err := flushPendingBlocks(); err != nil {
				return stats, fmt.Errorf("process trailing history block batch: %w", err)
			}
		}

		stats.Pages++
		limitReached := i.options.Limit > 0 && stats.BlocksFetched >= i.options.Limit
		if limitReached {
			stats.NextTokenHash = append([]byte{}, stats.LastBlockHash...)
			stats.NextTokenIndex = stats.LastSlot
			stats.Completed = false
		} else {
			nextToken := resp.Msg.GetNextToken()
			stats.NextTokenHash = blockRefHash(nextToken)
			stats.NextTokenIndex = blockRefIndex(nextToken)
			stats.Completed = nextToken == nil
			token = cloneHistoryBlockRef(nextToken)
		}

		if i.options.Apply {
			if err := upsertHistoryImportCheckpoint(i.db, i.options.CheckpointSource, i.options.Endpoint, stats); err != nil {
				return stats, err
			}
		}

		if i.options.ProgressEveryPages > 0 && stats.Pages%i.options.ProgressEveryPages == 0 {
			log.Printf("[HISTORY] pages=%d fetched=%d processed=%d last_slot=%d last_block=%d next_token=%s",
				stats.Pages,
				stats.BlocksFetched,
				stats.BlocksProcessed,
				stats.LastSlot,
				stats.LastBlockNo,
				hex.EncodeToString(stats.NextTokenHash),
			)
		}

		if stats.Completed || limitReached {
			break
		}
	}

	stats.FinishedAt = time.Now()
	return stats, nil
}

func decodeHistoryBlock(raw *utxosync.AnyChainBlock) (ledger.Block, uint, error) {
	if raw == nil || len(raw.GetNativeBytes()) == 0 {
		return nil, 0, fmt.Errorf("history block is missing native bytes")
	}

	blockBytes := raw.GetNativeBytes()
	var wrapped chainsync.WrappedBlock
	if _, err := cbor.Decode(blockBytes, &wrapped); err == nil && len(wrapped.BlockCbor) > 0 {
		block, err := ledger.NewBlockFromCbor(wrapped.BlockType, wrapped.BlockCbor)
		if err != nil {
			return nil, 0, fmt.Errorf("decode wrapped block cbor: %w", err)
		}
		return block, wrapped.BlockType, nil
	}

	blockType, err := ledger.DetermineBlockType(blockBytes)
	if err != nil {
		return nil, 0, fmt.Errorf("determine block type: %w", err)
	}
	block, err := ledger.NewBlockFromCbor(blockType, blockBytes)
	if err != nil {
		return nil, 0, fmt.Errorf("decode block cbor: %w", err)
	}
	return block, blockType, nil
}

type historyImportStart struct {
	Token *utxosync.BlockRef
	Stats historyImportStats
}

func buildInitialHistoryStart(db *gorm.DB, options historyImportOptions) (historyImportStart, error) {
	if options.Apply && options.Resume && db != nil {
		cp, found, err := readHistoryImportCheckpoint(db, options.CheckpointSource)
		if err != nil {
			return historyImportStart{}, err
		}
		if found {
			if cp.Completed {
				return historyImportStart{}, fmt.Errorf("history import checkpoint %q is already completed at slot=%d block=%d; use --reset-checkpoint to run again", options.CheckpointSource, cp.LastSlot, cp.LastBlockNo)
			}
			if len(cp.NextTokenHash) > 0 || cp.NextTokenIndex > 0 {
				log.Printf("[HISTORY] Resuming checkpoint %q from token=%s index=%d", options.CheckpointSource, hex.EncodeToString(cp.NextTokenHash), cp.NextTokenIndex)
				processed := int64(cp.BlocksProcessed)
				return historyImportStart{
					Token: &utxosync.BlockRef{Hash: append([]byte{}, cp.NextTokenHash...), Index: cp.NextTokenIndex},
					Stats: historyImportStats{
						Pages:           int64(cp.PagesProcessed),
						BlocksFetched:   processed,
						BlocksDecoded:   processed,
						BlocksProcessed: processed,
						LastSlot:        cp.LastSlot,
						LastBlockNo:     cp.LastBlockNo,
						LastBlockHash:   append([]byte{}, cp.LastBlockHash...),
						NextTokenHash:   append([]byte{}, cp.NextTokenHash...),
						NextTokenIndex:  cp.NextTokenIndex,
					},
				}, nil
			}
		}
	}

	if options.StartTokenHashHex == "" && options.StartTokenSlot == 0 {
		return historyImportStart{}, nil
	}
	token := &utxosync.BlockRef{Index: options.StartTokenSlot}
	if options.StartTokenHashHex == "" {
		return historyImportStart{Token: token}, nil
	}
	hash, err := hex.DecodeString(options.StartTokenHashHex)
	if err != nil {
		return historyImportStart{}, fmt.Errorf("--start-token-hash must be hex: %w", err)
	}
	if len(hash) != 32 {
		return historyImportStart{}, fmt.Errorf("--start-token-hash must decode to 32 bytes, got %d", len(hash))
	}
	token.Hash = hash
	return historyImportStart{Token: token}, nil
}

type historyCheckpointRow struct {
	Source          string
	Endpoint        string
	NextTokenHash   []byte
	NextTokenIndex  uint64
	LastBlockHash   []byte
	LastSlot        uint64
	LastBlockNo     uint64
	PagesProcessed  uint64
	BlocksProcessed uint64
	Completed       bool
	UpdatedAt       time.Time
}

func ensureHistoryImportCheckpointTable(db *gorm.DB) error {
	return db.Exec(`CREATE TABLE IF NOT EXISTS history_import_checkpoints (
		source VARCHAR(64) NOT NULL PRIMARY KEY,
		endpoint VARCHAR(255) NOT NULL,
		next_token_hash VARBINARY(32),
		next_token_index BIGINT UNSIGNED NOT NULL DEFAULT 0,
		last_block_hash VARBINARY(32),
		last_slot BIGINT UNSIGNED NOT NULL DEFAULT 0,
		last_block_no BIGINT UNSIGNED NOT NULL DEFAULT 0,
		pages_processed BIGINT UNSIGNED NOT NULL DEFAULT 0,
		blocks_processed BIGINT UNSIGNED NOT NULL DEFAULT 0,
		completed BOOLEAN NOT NULL DEFAULT false,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		INDEX idx_history_import_last_slot (last_slot),
		INDEX idx_history_import_completed (completed)
	)`).Error
}

func readHistoryImportCheckpoint(db *gorm.DB, source string) (historyCheckpointRow, bool, error) {
	var row historyCheckpointRow
	err := db.Raw(`SELECT
			source,
			endpoint,
			next_token_hash,
			next_token_index,
			last_block_hash,
			last_slot,
			last_block_no,
			pages_processed,
			blocks_processed,
			completed,
			updated_at
		FROM history_import_checkpoints
		WHERE source = ?`, source).Scan(&row).Error
	if err != nil {
		return row, false, fmt.Errorf("read history import checkpoint: %w", err)
	}
	return row, row.Source != "", nil
}

func deleteHistoryImportCheckpoint(db *gorm.DB, source string) error {
	return db.Exec("DELETE FROM history_import_checkpoints WHERE source = ?", source).Error
}

func upsertHistoryImportCheckpoint(db *gorm.DB, source string, endpoint string, stats historyImportStats) error {
	return db.Exec(`INSERT INTO history_import_checkpoints (
			source,
			endpoint,
			next_token_hash,
			next_token_index,
			last_block_hash,
			last_slot,
			last_block_no,
			pages_processed,
			blocks_processed,
			completed
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			endpoint = VALUES(endpoint),
			next_token_hash = VALUES(next_token_hash),
			next_token_index = VALUES(next_token_index),
			last_block_hash = VALUES(last_block_hash),
			last_slot = VALUES(last_slot),
			last_block_no = VALUES(last_block_no),
			pages_processed = VALUES(pages_processed),
			blocks_processed = VALUES(blocks_processed),
			completed = VALUES(completed)`,
		source,
		endpoint,
		nullableBytes(stats.NextTokenHash),
		stats.NextTokenIndex,
		nullableBytes(stats.LastBlockHash),
		stats.LastSlot,
		stats.LastBlockNo,
		stats.Pages,
		stats.BlocksProcessed,
		stats.Completed,
	).Error
}

func nullableBytes(value []byte) interface{} {
	if len(value) == 0 {
		return nil
	}
	return value
}

func blockRefHash(ref *utxosync.BlockRef) []byte {
	if ref == nil || len(ref.GetHash()) == 0 {
		return nil
	}
	return append([]byte{}, ref.GetHash()...)
}

func blockRefIndex(ref *utxosync.BlockRef) uint64 {
	if ref == nil {
		return 0
	}
	return ref.GetIndex()
}

func cloneHistoryBlockRef(ref *utxosync.BlockRef) *utxosync.BlockRef {
	if ref == nil {
		return nil
	}
	return &utxosync.BlockRef{
		Hash:  append([]byte{}, ref.GetHash()...),
		Index: ref.GetIndex(),
	}
}

func newHistorySyncClient(endpoint string, protocol string) (dolosHistorySyncClient, error) {
	endpoint = normalizeHistoryEndpoint(endpoint)
	switch strings.ToLower(strings.TrimSpace(protocol)) {
	case "", snapshot.ProtocolGRPC:
		return syncconnect.NewSyncServiceClient(historyH2CClient(), endpoint, connect.WithGRPC()), nil
	case snapshot.ProtocolConnect:
		return syncconnect.NewSyncServiceClient(http.DefaultClient, endpoint), nil
	default:
		return nil, fmt.Errorf("unsupported utxorpc protocol %q", protocol)
	}
}

func normalizeHistoryEndpoint(endpoint string) string {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return endpoint
	}
	return "http://" + endpoint
}

func historyH2CClient() *http.Client {
	return &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, network, addr)
			},
		},
	}
}

func printHistoryImportStats(stats historyImportStats) {
	duration := stats.FinishedAt.Sub(stats.StartedAt)
	if stats.FinishedAt.IsZero() {
		duration = time.Since(stats.StartedAt)
	}
	fmt.Println("Dolos history import")
	fmt.Printf("  applied: %v\n", stats.Applied)
	fmt.Printf("  pages: %d\n", stats.Pages)
	fmt.Printf("  blocks_fetched: %d\n", stats.BlocksFetched)
	fmt.Printf("  blocks_decoded: %d\n", stats.BlocksDecoded)
	fmt.Printf("  blocks_processed: %d\n", stats.BlocksProcessed)
	fmt.Printf("  skipped: %d\n", stats.Skipped)
	fmt.Printf("  last_slot: %d\n", stats.LastSlot)
	fmt.Printf("  last_block_no: %d\n", stats.LastBlockNo)
	fmt.Printf("  last_block_hash: %s\n", hex.EncodeToString(stats.LastBlockHash))
	fmt.Printf("  next_token_hash: %s\n", hex.EncodeToString(stats.NextTokenHash))
	fmt.Printf("  next_token_index: %d\n", stats.NextTokenIndex)
	fmt.Printf("  completed: %v\n", stats.Completed)
	fmt.Printf("  duration: %s\n", duration.Round(time.Millisecond))
}

func printHistoryImportStatus(db *gorm.DB, source string) error {
	fmt.Println("Dolos history import status")

	checkpointExists, err := historyImportCheckpointTableExists(db)
	if err != nil {
		return err
	}
	if checkpointExists {
		cp, found, err := readHistoryImportCheckpoint(db, source)
		if err != nil {
			return err
		}
		if found {
			fmt.Printf("  checkpoint_source: %s\n", cp.Source)
			fmt.Printf("  checkpoint_endpoint: %s\n", cp.Endpoint)
			fmt.Printf("  checkpoint_completed: %v\n", cp.Completed)
			fmt.Printf("  checkpoint_last_slot: %d\n", cp.LastSlot)
			fmt.Printf("  checkpoint_last_block_no: %d\n", cp.LastBlockNo)
			fmt.Printf("  checkpoint_last_block_hash: %s\n", hex.EncodeToString(cp.LastBlockHash))
			fmt.Printf("  checkpoint_next_token_hash: %s\n", hex.EncodeToString(cp.NextTokenHash))
			fmt.Printf("  checkpoint_next_token_index: %d\n", cp.NextTokenIndex)
			fmt.Printf("  checkpoint_pages_processed: %d\n", cp.PagesProcessed)
			fmt.Printf("  checkpoint_blocks_processed: %d\n", cp.BlocksProcessed)
			fmt.Printf("  checkpoint_updated_at: %s\n", cp.UpdatedAt.Format(time.RFC3339))
		} else {
			fmt.Printf("  checkpoint_source: %s\n", source)
			fmt.Println("  checkpoint_found: false")
		}
	} else {
		fmt.Println("  checkpoint_table_exists: false")
	}

	for _, table := range []string{
		"blocks",
		"txes",
		"tx_outs",
		"tx_ins",
		"multi_assets",
		"ma_tx_outs",
		"ma_tx_mints",
		"token_holders",
		"wallet_connections",
		"wallet_connection_txs",
		"token_wallet_connections",
		"token_metadata",
	} {
		count, err := tableCount(db, table)
		if err != nil {
			return err
		}
		fmt.Printf("  table_%s: %d\n", table, count)
	}

	minSlot, err := minIndexedSlot(db)
	if err != nil {
		return err
	}
	maxSlot, err := maxIndexedSlot(db)
	if err != nil {
		return err
	}
	fmt.Printf("  indexed_min_slot: %d\n", minSlot)
	fmt.Printf("  indexed_max_slot: %d\n", maxSlot)
	return nil
}

func historyImportCheckpointTableExists(db *gorm.DB) (bool, error) {
	var count int64
	err := db.Raw(`
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = DATABASE()
		  AND table_name = 'history_import_checkpoints'
	`).Scan(&count).Error
	return count > 0, err
}

func tableCount(db *gorm.DB, table string) (int64, error) {
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM " + table).Scan(&count).Error; err != nil {
		return 0, fmt.Errorf("count %s: %w", table, err)
	}
	return count, nil
}
