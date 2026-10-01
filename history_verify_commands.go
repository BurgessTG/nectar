package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	connect "connectrpc.com/connect"
	utxosync "github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
	"gorm.io/gorm"

	"nectar/bootstrap"
	"nectar/database"
	unifiederrors "nectar/errors"
	"nectar/snapshot"
)

type historyVerifyOptions struct {
	ConfigPath        string
	DolosDir          string
	Endpoint          string
	Protocol          string
	CheckpointSource  string
	RequireComplete   bool
	RequireTip        bool
	MaxTipLagSlots    uint64
	Deep              bool
	SkipMigrations    bool
	TipRequestTimeout time.Duration
}

type requiredIndexSpec struct {
	Table   string
	Name    string
	Columns []string
}

type historyIntegrityCheck struct {
	Name  string
	Query string
}

func handleVerifyHistoryCommand(args []string) {
	fs := flag.NewFlagSet("verify-history", flag.ExitOnError)
	configPath := fs.String("config", "nectar.toml", "Path to configuration file")
	fs.StringVar(configPath, "c", "nectar.toml", "Path to configuration file")
	dolosDir := fs.String("dolos-dir", firstNonEmpty(os.Getenv("DOLOS_DIR"), defaultDolosDir), "Dolos node directory used to infer the UTxO RPC endpoint for tip verification")
	endpoint := fs.String("endpoint", "", "UTxO RPC base endpoint for tip verification; defaults to Dolos serve.grpc when --require-tip is set")
	protocol := fs.String("protocol", snapshot.ProtocolGRPC, "UTxO RPC protocol for tip verification: grpc or connect")
	checkpointSource := fs.String("checkpoint-source", defaultHistoryCheckpointSource, "History import checkpoint source to verify")
	requireComplete := fs.Bool("require-complete", false, "Fail unless the history import checkpoint is completed")
	requireTip := fs.Bool("require-tip", false, "Fail unless indexed max slot is close to the current Dolos UTxO RPC tip")
	maxTipLagSlots := fs.Uint64("max-tip-lag-slots", 300, "Maximum indexed slot lag behind Dolos tip when --require-tip is set")
	deep := fs.Bool("deep", false, "Run relational integrity checks across indexed base/product tables")
	skipMigrations := fs.Bool("skip-migrations", true, "Skip database migrations before verify")
	tipRequestTimeout := fs.Duration("tip-timeout", 10*time.Second, "Timeout for Dolos UTxO RPC tip verification")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("failed to parse verify-history flags: %v", err)
	}

	options := historyVerifyOptions{
		ConfigPath:        *configPath,
		DolosDir:          *dolosDir,
		Endpoint:          strings.TrimSpace(*endpoint),
		Protocol:          *protocol,
		CheckpointSource:  strings.TrimSpace(*checkpointSource),
		RequireComplete:   *requireComplete,
		RequireTip:        *requireTip,
		MaxTipLagSlots:    *maxTipLagSlots,
		Deep:              *deep,
		SkipMigrations:    *skipMigrations,
		TipRequestTimeout: *tipRequestTimeout,
	}
	if options.CheckpointSource == "" {
		options.CheckpointSource = defaultHistoryCheckpointSource
	}

	unifiederrors.Initialize(nil)
	cfg, db := openProductCommandDB(options.ConfigPath, !options.SkipMigrations)
	defer closeDB(db)

	if err := verifyHistory(context.Background(), db, cfg.Database.Driver, options); err != nil {
		log.Fatalf("history verification failed: %v", err)
	}
}

func verifyHistory(ctx context.Context, db *gorm.DB, driver string, options historyVerifyOptions) error {
	fmt.Fprintln(os.Stdout, "Nectar history verification")

	if err := verifyHistoryCheckpoint(db, options); err != nil {
		return err
	}
	if err := verifyRequiredTables(db, requiredHistoryTables()); err != nil {
		return err
	}
	if err := verifyRequiredIndexes(db, requiredHistoryIndexes(driver)); err != nil {
		return err
	}
	if err := printHistoryTableCounts(db); err != nil {
		return err
	}
	if err := printHistoryEraCounts(db); err != nil {
		return err
	}
	if options.Deep {
		if err := verifyHistoryIntegrity(db); err != nil {
			return err
		}
	}
	if options.RequireTip {
		if err := verifyHistoryTipLag(ctx, db, options); err != nil {
			return err
		}
	}

	fmt.Fprintln(os.Stdout, "history_verification=ok")
	return nil
}

func verifyHistoryTipLag(ctx context.Context, db *gorm.DB, options historyVerifyOptions) error {
	endpoint := strings.TrimSpace(options.Endpoint)
	if endpoint == "" {
		report, err := bootstrap.InspectDolos(options.DolosDir, false)
		if err != nil {
			return fmt.Errorf("inspect Dolos for tip endpoint: %w", err)
		}
		endpoint = report.GRPCEndpoint
	}
	if endpoint == "" {
		return fmt.Errorf("UTxO RPC endpoint is required for --require-tip")
	}
	timeout := options.TipRequestTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	client, err := newHistorySyncClient(endpoint, options.Protocol)
	if err != nil {
		return err
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp, err := client.ReadTip(reqCtx, connect.NewRequest(&utxosync.ReadTipRequest{}))
	if err != nil {
		return fmt.Errorf("read Dolos tip: %w", err)
	}
	tip := resp.Msg.GetTip()
	if tip == nil {
		return fmt.Errorf("Dolos ReadTip returned no tip")
	}
	tipSlot := tip.GetIndex()
	indexedMaxSlot, err := maxIndexedSlot(db)
	if err != nil {
		return err
	}

	var lag uint64
	if tipSlot > indexedMaxSlot {
		lag = tipSlot - indexedMaxSlot
	}
	fmt.Fprintf(os.Stdout, "tip_endpoint=%s\n", endpoint)
	fmt.Fprintf(os.Stdout, "tip_slot=%d\n", tipSlot)
	fmt.Fprintf(os.Stdout, "indexed_max_slot=%d\n", indexedMaxSlot)
	fmt.Fprintf(os.Stdout, "tip_lag_slots=%d\n", lag)
	fmt.Fprintf(os.Stdout, "max_tip_lag_slots=%d\n", options.MaxTipLagSlots)
	if lag > options.MaxTipLagSlots {
		return fmt.Errorf("indexed max slot %d is %d slots behind Dolos tip %d", indexedMaxSlot, lag, tipSlot)
	}
	return nil
}

func verifyHistoryCheckpoint(db *gorm.DB, options historyVerifyOptions) error {
	exists, err := historyImportCheckpointTableExists(db)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "checkpoint_table_exists=%t\n", exists)
	if !exists {
		if options.RequireComplete {
			return fmt.Errorf("history import checkpoint table is missing")
		}
		return nil
	}

	cp, found, err := readHistoryImportCheckpoint(db, options.CheckpointSource)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "checkpoint_source=%s\n", options.CheckpointSource)
	fmt.Fprintf(os.Stdout, "checkpoint_found=%t\n", found)
	if !found {
		if options.RequireComplete {
			return fmt.Errorf("history import checkpoint %q is missing", options.CheckpointSource)
		}
		return nil
	}

	fmt.Fprintf(os.Stdout, "checkpoint_completed=%t\n", cp.Completed)
	fmt.Fprintf(os.Stdout, "checkpoint_last_slot=%d\n", cp.LastSlot)
	fmt.Fprintf(os.Stdout, "checkpoint_last_block_no=%d\n", cp.LastBlockNo)
	fmt.Fprintf(os.Stdout, "checkpoint_blocks_processed=%d\n", cp.BlocksProcessed)
	if options.RequireComplete && !cp.Completed {
		return fmt.Errorf("history import checkpoint %q is not completed", options.CheckpointSource)
	}
	return nil
}

func verifyRequiredTables(db *gorm.DB, tables []string) error {
	existing, err := existingTables(db)
	if err != nil {
		return err
	}

	missing := make([]string, 0)
	for _, table := range tables {
		if !existing[table] {
			missing = append(missing, table)
		}
	}
	fmt.Fprintf(os.Stdout, "required_tables=%d\n", len(tables))
	fmt.Fprintf(os.Stdout, "missing_tables=%d\n", len(missing))
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("missing required tables: %s", strings.Join(missing, ", "))
	}
	return nil
}

func existingTables(db *gorm.DB) (map[string]bool, error) {
	rows, err := db.Raw(`
		SELECT table_name AS table_name
		FROM information_schema.tables
		WHERE table_schema = DATABASE()
	`).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	existing := make(map[string]bool)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		existing[tableName] = true
	}
	return existing, rows.Err()
}

func verifyRequiredIndexes(db *gorm.DB, specs []requiredIndexSpec) error {
	existing, err := existingIndexes(db)
	if err != nil {
		return err
	}

	missing := make([]string, 0)
	mismatched := make([]string, 0)
	for _, spec := range specs {
		key := indexMapKey(spec.Table, spec.Name)
		columns, ok := existing[key]
		if !ok {
			missing = append(missing, spec.Table+"."+spec.Name)
			continue
		}
		if len(spec.Columns) > 0 && !sameStringSlice(columns, spec.Columns) {
			mismatched = append(mismatched, fmt.Sprintf("%s.%s expected=(%s) actual=(%s)",
				spec.Table,
				spec.Name,
				strings.Join(spec.Columns, ","),
				strings.Join(columns, ","),
			))
		}
	}

	fmt.Fprintf(os.Stdout, "required_indexes=%d\n", len(specs))
	fmt.Fprintf(os.Stdout, "missing_indexes=%d\n", len(missing))
	fmt.Fprintf(os.Stdout, "mismatched_indexes=%d\n", len(mismatched))
	if len(missing) > 0 || len(mismatched) > 0 {
		sort.Strings(missing)
		sort.Strings(mismatched)
		details := append([]string{}, missing...)
		details = append(details, mismatched...)
		return fmt.Errorf("required index check failed: %s", strings.Join(details, "; "))
	}
	return nil
}

func existingIndexes(db *gorm.DB) (map[string][]string, error) {
	rows, err := db.Raw(`
		SELECT table_name AS table_name,
		       index_name AS index_name,
		       column_name AS column_name,
		       seq_in_index AS seq_in_index
		FROM information_schema.statistics
		WHERE table_schema = DATABASE()
		ORDER BY table_name, index_name, seq_in_index
	`).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	existing := make(map[string][]string)
	for rows.Next() {
		var tableName string
		var indexName string
		var columnName string
		var seqInIndex int
		if err := rows.Scan(&tableName, &indexName, &columnName, &seqInIndex); err != nil {
			return nil, err
		}
		key := indexMapKey(tableName, indexName)
		existing[key] = append(existing[key], columnName)
	}
	return existing, rows.Err()
}

func indexMapKey(table, index string) string {
	return table + "." + index
}

func sameStringSlice(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func printHistoryTableCounts(db *gorm.DB) error {
	for _, table := range historyCountTables() {
		count, err := tableCount(db, table)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "table_%s=%d\n", table, count)
	}
	return nil
}

func printHistoryEraCounts(db *gorm.DB) error {
	rows, err := db.Raw(`
		SELECT era,
		       COUNT(*) AS count,
		       COALESCE(MIN(slot_no), 0) AS min_slot,
		       COALESCE(MAX(slot_no), 0) AS max_slot
		FROM blocks
		GROUP BY era
		ORDER BY min_slot
	`).Rows()
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var era string
		var count int64
		var minSlot uint64
		var maxSlot uint64
		if err := rows.Scan(&era, &count, &minSlot, &maxSlot); err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "era_%s_blocks=%d min_slot=%d max_slot=%d\n", strings.ToLower(era), count, minSlot, maxSlot)
	}
	return rows.Err()
}

func verifyHistoryIntegrity(db *gorm.DB) error {
	failed := make([]string, 0)
	for _, check := range historyIntegrityChecks() {
		var count int64
		if err := db.Raw(check.Query).Scan(&count).Error; err != nil {
			return fmt.Errorf("%s: %w", check.Name, err)
		}
		fmt.Fprintf(os.Stdout, "%s=%d\n", check.Name, count)
		if count != 0 {
			failed = append(failed, fmt.Sprintf("%s=%d", check.Name, count))
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("history integrity checks failed: %s", strings.Join(failed, ", "))
	}
	return nil
}

func requiredHistoryTables() []string {
	return []string{
		"ada_pots",
		"blocks",
		"collateral_tx_ins",
		"collateral_tx_outs",
		"committee_deregistrations",
		"committee_hashes",
		"committee_members",
		"committee_registrations",
		"committees",
		"constitutions",
		"cost_models",
		"d_rep_distrs",
		"d_rep_hashes",
		"data",
		"delegation_votes",
		"delegations",
		"delisted_pools",
		"epoch_params",
		"epoch_stake_progresses",
		"epoch_stakes",
		"epoch_states",
		"epoches",
		"event_infos",
		"extra_key_witnesses",
		"gov_action_proposals",
		"history_import_checkpoints",
		"ma_tx_mints",
		"ma_tx_outs",
		"multi_assets",
		"off_chain_pool_data",
		"off_chain_pool_fetch_errors",
		"off_chain_vote_authors",
		"off_chain_vote_d_rep_data",
		"off_chain_vote_data",
		"off_chain_vote_external_updates",
		"off_chain_vote_fetch_errors",
		"off_chain_vote_gov_action_data",
		"off_chain_vote_references",
		"param_proposals",
		"pool_hashes",
		"pool_metadata_refs",
		"pool_owners",
		"pool_relays",
		"pool_retires",
		"pool_stats",
		"pool_updates",
		"pot_transfers",
		"redeemer_data",
		"redeemers",
		"reference_tx_ins",
		"required_signers",
		"reserved_pool_tickers",
		"reserves",
		"reward_rests",
		"rewards",
		"scripts",
		"slot_leaders",
		"stake_addresses",
		"stake_deregistrations",
		"stake_registrations",
		"token_holders",
		"token_metadata",
		"token_wallet_connections",
		"treasuries",
		"treasury_withdrawals",
		"tx_cbors",
		"tx_ins",
		"tx_metadata",
		"tx_outs",
		"txes",
		"utxo_deltas",
		"utxo_states",
		"voting_anchors",
		"voting_procedures",
		"wallet_connection_txs",
		"wallet_connections",
		"withdrawals",
	}
}

func requiredHistoryIndexes(driver string) []requiredIndexSpec {
	indexes := []requiredIndexSpec{
		{Table: "blocks", Name: "PRIMARY", Columns: []string{"hash"}},
		{Table: "blocks", Name: "idx_blocks_slot_no", Columns: []string{"slot_no"}},
		{Table: "blocks", Name: "idx_blocks_slot_no_desc", Columns: []string{"slot_no"}},
		{Table: "blocks", Name: "idx_blocks_epoch_no", Columns: []string{"epoch_no"}},
		{Table: "blocks", Name: "idx_blocks_block_no", Columns: []string{"block_no"}},
		{Table: "blocks", Name: "idx_blocks_block_no_desc", Columns: []string{"block_no"}},
		{Table: "blocks", Name: "idx_blocks_time", Columns: []string{"time"}},
		{Table: "blocks", Name: "idx_blocks_previous_hash", Columns: []string{"previous_hash"}},
		{Table: "blocks", Name: "idx_blocks_slot_leader_hash", Columns: []string{"slot_leader_hash"}},
		{Table: "blocks", Name: "idx_blocks_era", Columns: []string{"era"}},
		{Table: "blocks", Name: "idx_blocks_slot_hash", Columns: []string{"slot_no", "hash"}},
		{Table: "blocks", Name: "idx_blocks_epoch_slot", Columns: []string{"epoch_no", "slot_no"}},
		{Table: "txes", Name: "PRIMARY", Columns: []string{"hash"}},
		{Table: "txes", Name: "idx_txes_block_hash", Columns: []string{"block_hash"}},
		{Table: "txes", Name: "idx_block_composite", Columns: []string{"block_hash", "block_index"}},
		{Table: "tx_outs", Name: "PRIMARY", Columns: []string{"tx_hash", "index"}},
		{Table: "tx_outs", Name: "idx_tx_out_address_value", Columns: []string{"address", "value"}},
		{Table: "tx_outs", Name: "idx_tx_outs_lookup", Columns: []string{"tx_hash", "index"}},
		{Table: "tx_outs", Name: "idx_tx_outs_stake_address", Columns: []string{"stake_address_hash"}},
		{Table: "tx_outs", Name: "idx_tx_outs_payment_cred", Columns: []string{"payment_cred"}},
		{Table: "tx_outs", Name: "idx_tx_outs_inline_datum", Columns: []string{"inline_datum_hash"}},
		{Table: "tx_outs", Name: "idx_tx_outs_value", Columns: []string{"value"}},
		{Table: "tx_ins", Name: "PRIMARY", Columns: []string{"tx_in_hash", "tx_in_index"}},
		{Table: "tx_ins", Name: "idx_tx_ins_spent", Columns: []string{"tx_out_hash", "tx_out_index"}},
		{Table: "tx_ins", Name: "idx_tx_ins_tx_in", Columns: []string{"tx_in_hash"}},
		{Table: "tx_ins", Name: "idx_tx_ins_redeemer", Columns: []string{"redeemer_hash"}},
		{Table: "multi_assets", Name: "PRIMARY", Columns: []string{"policy", "name"}},
		{Table: "multi_assets", Name: "idx_multi_asset_fingerprint", Columns: []string{"fingerprint"}},
		{Table: "ma_tx_outs", Name: "PRIMARY", Columns: []string{"tx_hash", "tx_index", "policy", "name"}},
		{Table: "ma_tx_outs", Name: "idx_ma_tx_out_asset", Columns: []string{"policy", "name"}},
		{Table: "ma_tx_outs", Name: "idx_ma_tx_out_tx", Columns: []string{"tx_hash", "tx_index"}},
		{Table: "ma_tx_mints", Name: "PRIMARY", Columns: []string{"tx_hash", "policy", "name"}},
		{Table: "ma_tx_mints", Name: "idx_ma_tx_mint_asset", Columns: []string{"policy", "name"}},
		{Table: "ma_tx_mints", Name: "idx_ma_tx_mint_tx", Columns: []string{"tx_hash"}},
		{Table: "tx_metadata", Name: "PRIMARY", Columns: []string{"tx_hash", "key"}},
		{Table: "tx_metadata", Name: "idx_tx_metadata_tx_hash", Columns: []string{"tx_hash"}},
		{Table: "tx_metadata", Name: "idx_tx_metadata_key", Columns: []string{"key"}},
		{Table: "scripts", Name: "PRIMARY", Columns: []string{"hash"}},
		{Table: "scripts", Name: "idx_scripts_type", Columns: []string{"type"}},
		{Table: "scripts", Name: "idx_scripts_tx_hash", Columns: []string{"tx_hash"}},
		{Table: "redeemers", Name: "PRIMARY", Columns: []string{"hash"}},
		{Table: "redeemers", Name: "idx_redeemers_tx_hash", Columns: []string{"tx_hash"}},
		{Table: "redeemers", Name: "idx_redeemers_purpose", Columns: []string{"purpose"}},
		{Table: "stake_addresses", Name: "PRIMARY", Columns: []string{"hash_raw"}},
		{Table: "stake_addresses", Name: "idx_stake_addresses_view", Columns: []string{"view"}},
		{Table: "stake_addresses", Name: "idx_stake_addresses_script_hash", Columns: []string{"script_hash"}},
		{Table: "delegations", Name: "idx_delegations_addr_hash", Columns: []string{"addr_hash"}},
		{Table: "delegations", Name: "idx_delegations_pool_hash", Columns: []string{"pool_hash"}},
		{Table: "delegations", Name: "idx_delegations_active_epoch_no", Columns: []string{"active_epoch_no"}},
		{Table: "delegations", Name: "idx_delegations_addr_epoch", Columns: []string{"addr_hash", "active_epoch_no"}},
		{Table: "delegations", Name: "idx_delegations_tx_hash", Columns: []string{"tx_hash"}},
		{Table: "delegations", Name: "idx_delegations_redeemer_hash", Columns: []string{"redeemer_hash"}},
		{Table: "pool_updates", Name: "idx_pool_updates_pool_hash", Columns: []string{"pool_hash"}},
		{Table: "pool_updates", Name: "idx_pool_updates_active_epoch_no", Columns: []string{"active_epoch_no"}},
		{Table: "pool_updates", Name: "idx_pool_updates_reward_addr_hash", Columns: []string{"reward_addr_hash"}},
		{Table: "pool_updates", Name: "idx_pool_updates_tx_hash", Columns: []string{"tx_hash"}},
		{Table: "pool_updates", Name: "idx_pool_updates_pledge", Columns: []string{"pledge"}},
		{Table: "pool_updates", Name: "idx_pool_updates_margin", Columns: []string{"margin"}},
		{Table: "pool_hashes", Name: "idx_pool_hashes_view", Columns: []string{"view"}},
		{Table: "pool_owners", Name: "idx_pool_owners_update_tx", Columns: []string{"update_tx_hash"}},
		{Table: "pool_owners", Name: "idx_pool_owners_owner", Columns: []string{"owner_hash"}},
		{Table: "pool_retires", Name: "idx_pool_retires_pool_hash", Columns: []string{"pool_hash"}},
		{Table: "pool_retires", Name: "idx_pool_retires_retiring_epoch", Columns: []string{"retiring_epoch"}},
		{Table: "stake_registrations", Name: "idx_stake_registrations_addr_hash", Columns: []string{"addr_hash"}},
		{Table: "stake_registrations", Name: "idx_stake_registrations_tx_hash", Columns: []string{"tx_hash"}},
		{Table: "stake_registrations", Name: "idx_stake_registrations_redeemer_hash", Columns: []string{"redeemer_hash"}},
		{Table: "stake_deregistrations", Name: "idx_stake_deregistrations_addr_hash", Columns: []string{"addr_hash"}},
		{Table: "stake_deregistrations", Name: "idx_stake_deregistrations_tx_hash", Columns: []string{"tx_hash"}},
		{Table: "rewards", Name: "idx_rewards_addr_hash", Columns: []string{"addr_hash"}},
		{Table: "rewards", Name: "idx_rewards_pool_hash", Columns: []string{"pool_hash"}},
		{Table: "rewards", Name: "idx_rewards_earned_epoch", Columns: []string{"earned_epoch"}},
		{Table: "rewards", Name: "idx_rewards_spendable_epoch", Columns: []string{"spendable_epoch"}},
		{Table: "rewards", Name: "idx_rewards_addr_epoch", Columns: []string{"addr_hash", "earned_epoch"}},
		{Table: "rewards", Name: "idx_rewards_type", Columns: []string{"type"}},
		{Table: "pool_stats", Name: "idx_pool_stats_epoch_hash", Columns: []string{"epoch_no", "pool_hash"}},
		{Table: "withdrawals", Name: "idx_withdrawals_addr_hash", Columns: []string{"addr_hash"}},
		{Table: "withdrawals", Name: "idx_withdrawals_tx_hash", Columns: []string{"tx_hash"}},
		{Table: "history_import_checkpoints", Name: "PRIMARY", Columns: []string{"source"}},
		{Table: "history_import_checkpoints", Name: "idx_history_import_last_slot", Columns: []string{"last_slot"}},
		{Table: "history_import_checkpoints", Name: "idx_history_import_completed", Columns: []string{"completed"}},
		{Table: "token_holders", Name: "PRIMARY", Columns: []string{"policy", "name", "address"}},
		{Table: "token_holders", Name: "idx_token_holders_amount", Columns: []string{"policy", "name", "amount"}},
		{Table: "token_holders", Name: "idx_token_holders_stake_address", Columns: []string{"stake_address"}},
		{Table: "token_holders", Name: "idx_token_holders_last_updated", Columns: []string{"last_updated_slot"}},
		{Table: "wallet_connections", Name: "PRIMARY", Columns: []string{"sender_address", "receiver_address"}},
		{Table: "wallet_connections", Name: "idx_wc_sender", Columns: []string{"sender_address"}},
		{Table: "wallet_connections", Name: "idx_wc_receiver", Columns: []string{"receiver_address"}},
		{Table: "wallet_connections", Name: "idx_wc_last_slot", Columns: []string{"last_tx_slot"}},
		{Table: "wallet_connection_txs", Name: "PRIMARY", Columns: []string{"id"}},
		{Table: "wallet_connection_txs", Name: "idx_wctx_sender", Columns: []string{"sender_address"}},
		{Table: "wallet_connection_txs", Name: "idx_wctx_receiver", Columns: []string{"receiver_address"}},
		{Table: "wallet_connection_txs", Name: "idx_wctx_tx", Columns: []string{"tx_hash"}},
		{Table: "wallet_connection_txs", Name: "idx_wctx_slot", Columns: []string{"slot_no"}},
		{Table: "token_wallet_connections", Name: "PRIMARY", Columns: []string{"tx_hash", "tx_index", "policy_id", "asset_name", "from_address", "to_address"}},
		{Table: "token_wallet_connections", Name: "idx_twc_token", Columns: []string{"policy_id", "asset_name", "slot"}},
		{Table: "token_wallet_connections", Name: "idx_twc_from", Columns: []string{"from_address", "policy_id", "asset_name"}},
		{Table: "token_wallet_connections", Name: "idx_twc_to", Columns: []string{"to_address", "policy_id", "asset_name"}},
		{Table: "token_wallet_connections", Name: "idx_twc_stake_pair", Columns: []string{"from_stake_address", "to_stake_address", "policy_id", "asset_name"}},
		{Table: "token_wallet_connections", Name: "idx_twc_slot", Columns: []string{"slot"}},
		{Table: "token_metadata", Name: "PRIMARY", Columns: []string{"policy_id", "asset_name"}},
		{Table: "token_metadata", Name: "idx_token_metadata_lookup", Columns: []string{"policy_id", "asset_name"}},
	}

	if database.IsTiDBDriver(driver) {
		indexes = append(indexes,
			requiredIndexSpec{Table: "blocks", Name: "idx_blocks_slot_epoch_time", Columns: []string{"slot_no", "epoch_no", "time"}},
			requiredIndexSpec{Table: "delegations", Name: "idx_delegations_pool_epoch", Columns: []string{"pool_hash", "active_epoch_no"}},
			requiredIndexSpec{Table: "stake_addresses", Name: "idx_stake_addresses_hash_view", Columns: []string{"hash_raw", "view"}},
			requiredIndexSpec{Table: "pool_hashes", Name: "idx_pool_hashes_hash_view", Columns: []string{"hash_raw", "view"}},
		)
	}

	return indexes
}

func historyCountTables() []string {
	return []string{
		"blocks",
		"txes",
		"tx_outs",
		"tx_ins",
		"multi_assets",
		"ma_tx_outs",
		"ma_tx_mints",
		"tx_metadata",
		"scripts",
		"data",
		"redeemers",
		"stake_addresses",
		"delegations",
		"pool_updates",
		"rewards",
		"withdrawals",
		"token_holders",
		"wallet_connections",
		"wallet_connection_txs",
		"token_wallet_connections",
		"token_metadata",
	}
}

func historyIntegrityChecks() []historyIntegrityCheck {
	return []historyIntegrityCheck{
		{
			Name: "orphan_txes_without_blocks",
			Query: `SELECT COUNT(*)
				FROM txes t
				LEFT JOIN blocks b ON b.hash = t.block_hash
				WHERE b.hash IS NULL`,
		},
		{
			Name: "orphan_tx_outs_without_txes",
			Query: `SELECT COUNT(*)
				FROM tx_outs txo
				LEFT JOIN txes t ON t.hash = txo.tx_hash
				WHERE t.hash IS NULL`,
		},
		{
			Name: "orphan_tx_ins_without_spending_txes",
			Query: `SELECT COUNT(*)
				FROM tx_ins ti
				LEFT JOIN txes t ON t.hash = ti.tx_in_hash
				WHERE t.hash IS NULL`,
		},
		{
			Name: "orphan_ma_tx_outs_without_tx_outs",
			Query: `SELECT COUNT(*)
				FROM ma_tx_outs mao
				LEFT JOIN tx_outs txo
				  ON txo.tx_hash = mao.tx_hash
				 AND txo.` + "`index`" + ` = mao.tx_index
				WHERE txo.tx_hash IS NULL`,
		},
		{
			Name: "orphan_ma_tx_outs_without_assets",
			Query: `SELECT COUNT(*)
				FROM ma_tx_outs mao
				LEFT JOIN multi_assets ma
				  ON ma.policy = mao.policy
				 AND ma.name = mao.name
				WHERE ma.policy IS NULL`,
		},
		{
			Name: "orphan_ma_tx_mints_without_txes",
			Query: `SELECT COUNT(*)
				FROM ma_tx_mints mam
				LEFT JOIN txes t ON t.hash = mam.tx_hash
				WHERE t.hash IS NULL`,
		},
		{
			Name: "orphan_ma_tx_mints_without_assets",
			Query: `SELECT COUNT(*)
				FROM ma_tx_mints mam
				LEFT JOIN multi_assets ma
				  ON ma.policy = mam.policy
				 AND ma.name = mam.name
				WHERE ma.policy IS NULL`,
		},
		{
			Name: "orphan_tx_metadata_without_txes",
			Query: `SELECT COUNT(*)
				FROM tx_metadata tm
				LEFT JOIN txes t ON t.hash = tm.tx_hash
				WHERE t.hash IS NULL`,
		},
		{
			Name: "orphan_scripts_without_txes",
			Query: `SELECT COUNT(*)
				FROM scripts s
				LEFT JOIN txes t ON t.hash = s.tx_hash
				WHERE t.hash IS NULL`,
		},
		{
			Name: "orphan_data_without_txes",
			Query: `SELECT COUNT(*)
				FROM data d
				LEFT JOIN txes t ON t.hash = d.tx_hash
				WHERE t.hash IS NULL`,
		},
		{
			Name: "orphan_redeemers_without_txes",
			Query: `SELECT COUNT(*)
				FROM redeemers r
				LEFT JOIN txes t ON t.hash = r.tx_hash
				WHERE t.hash IS NULL`,
		},
		{
			Name: "orphan_token_holders_without_assets",
			Query: `SELECT COUNT(*)
				FROM token_holders th
				LEFT JOIN multi_assets ma
				  ON ma.policy = th.policy
				 AND ma.name = th.name
				WHERE ma.policy IS NULL`,
		},
		{
			Name: "orphan_wallet_connection_txs_without_txes",
			Query: `SELECT COUNT(*)
				FROM wallet_connection_txs wctx
				LEFT JOIN txes t ON t.hash = wctx.tx_hash
				WHERE t.hash IS NULL`,
		},
		{
			Name: "orphan_wallet_connections_without_last_tx",
			Query: `SELECT COUNT(*)
				FROM wallet_connections wc
				LEFT JOIN txes t ON t.hash = wc.last_tx_hash
				WHERE t.hash IS NULL`,
		},
		{
			Name: "orphan_token_wallet_connections_without_txes",
			Query: `SELECT COUNT(*)
				FROM token_wallet_connections twc
				LEFT JOIN txes t ON t.hash = twc.tx_hash
				WHERE t.hash IS NULL`,
		},
		{
			Name: "orphan_token_wallet_connections_without_assets",
			Query: `SELECT COUNT(*)
				FROM token_wallet_connections twc
				LEFT JOIN multi_assets ma
				  ON ma.policy = twc.policy_id
				 AND ma.name = twc.asset_name
				WHERE ma.policy IS NULL`,
		},
		{
			Name: "blocks_tx_count_mismatch",
			Query: `SELECT COUNT(*)
				FROM blocks b
				LEFT JOIN (
					SELECT block_hash, COUNT(*) AS tx_count
					FROM txes
					GROUP BY block_hash
				) actual ON actual.block_hash = b.hash
				WHERE b.tx_count <> COALESCE(actual.tx_count, 0)`,
		},
	}
}
