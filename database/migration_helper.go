package database

import (
	"fmt"
	"gorm.io/gorm"
	"log"
	"nectar/models"
)

// createTablesWithCompositeKeys creates tables that have composite primary keys
// GORM AutoMigrate has issues with composite keys, so we create them manually
func createTablesWithCompositeKeys(db *gorm.DB) error {
	// Create tables with composite primary keys manually
	compositePKTables := []struct {
		name string
		sql  string
	}{
		{
			name: "tx_outs",
			sql: `CREATE TABLE IF NOT EXISTS tx_outs (
				tx_hash VARBINARY(32) NOT NULL,
				` + "`index`" + ` INT UNSIGNED NOT NULL,
				address VARCHAR(2048) NOT NULL,
				address_raw VARBINARY(1024) NOT NULL,
				address_has_script BOOLEAN NOT NULL DEFAULT false,
				payment_cred VARBINARY(32),
				stake_address_hash VARBINARY(28),
				value BIGINT UNSIGNED NOT NULL,
				data_hash VARBINARY(32),
				inline_datum_hash VARBINARY(32),
				reference_script_hash VARBINARY(32),
				PRIMARY KEY (tx_hash, ` + "`index`" + `),
				INDEX idx_tx_out_address_value (address(20), value),
				INDEX idx_tx_outs_lookup (tx_hash, ` + "`index`" + `),
				INDEX idx_tx_outs_stake_address (stake_address_hash),
				INDEX idx_tx_outs_payment_cred (payment_cred),
				INDEX idx_tx_outs_inline_datum (inline_datum_hash)
			)`,
		},
		{
			name: "tx_ins",
			sql: `CREATE TABLE IF NOT EXISTS tx_ins (
				tx_in_hash VARBINARY(32) NOT NULL,
				tx_in_index INT UNSIGNED NOT NULL,
				tx_out_hash VARBINARY(32) NOT NULL,
				tx_out_index INT UNSIGNED NOT NULL,
				redeemer_hash VARBINARY(32),
				PRIMARY KEY (tx_in_hash, tx_in_index),
				INDEX idx_tx_ins_spent (tx_out_hash, tx_out_index),
				INDEX idx_tx_ins_tx_in (tx_in_hash),
				INDEX idx_tx_ins_redeemer (redeemer_hash)
			)`,
		},
		{
			name: "multi_assets",
			sql: `CREATE TABLE IF NOT EXISTS multi_assets (
				policy VARBINARY(28) NOT NULL,
				name VARBINARY(32) NOT NULL,
				fingerprint VARCHAR(44) NOT NULL,
				PRIMARY KEY (policy, name),
				UNIQUE INDEX idx_multi_asset_fingerprint (fingerprint)
			)`,
		},
		{
			name: "ma_tx_outs",
			sql: `CREATE TABLE IF NOT EXISTS ma_tx_outs (
				tx_hash VARBINARY(32) NOT NULL,
				tx_index INT UNSIGNED NOT NULL,
				policy VARBINARY(28) NOT NULL,
				name VARBINARY(32) NOT NULL,
				quantity BIGINT UNSIGNED NOT NULL,
				PRIMARY KEY (tx_hash, tx_index, policy, name),
				INDEX idx_ma_tx_out_asset (policy, name),
				INDEX idx_ma_tx_out_tx (tx_hash, tx_index)
			)`,
		},
		{
			name: "ma_tx_mints",
			sql: `CREATE TABLE IF NOT EXISTS ma_tx_mints (
				tx_hash VARBINARY(32) NOT NULL,
				policy VARBINARY(28) NOT NULL,
				name VARBINARY(32) NOT NULL,
				quantity BIGINT NOT NULL,
				PRIMARY KEY (tx_hash, policy, name),
				INDEX idx_ma_tx_mint_asset (policy, name),
				INDEX idx_ma_tx_mint_tx (tx_hash)
			)`,
		},
		{
			name: "off_chain_pool_fetch_errors",
			sql: `CREATE TABLE IF NOT EXISTS off_chain_pool_fetch_errors (
				pool_hash VARBINARY(28) NOT NULL,
				pmr_hash VARBINARY(32) NOT NULL,
				fetch_time DATETIME NOT NULL,
				fetch_error TEXT,
				retry_count INT UNSIGNED NOT NULL DEFAULT 0,
				PRIMARY KEY (pool_hash, pmr_hash, fetch_time),
				INDEX idx_pool_fetch_error_pmr (pmr_hash)
			)`,
		},
		{
			name: "off_chain_vote_gov_action_data",
			sql: `CREATE TABLE IF NOT EXISTS off_chain_vote_gov_action_data (
				off_chain_vote_data_hash VARBINARY(32) NOT NULL,
				language VARCHAR(255) NOT NULL,
				voting_anchor_hash VARBINARY(32) NOT NULL,
				title TEXT,
				abstract TEXT,
				motivation TEXT,
				rationale TEXT,
				PRIMARY KEY (off_chain_vote_data_hash, language),
				INDEX idx_vote_gov_action_anchor (voting_anchor_hash)
			)`,
		},
		{
			name: "off_chain_vote_d_rep_data",
			sql: `CREATE TABLE IF NOT EXISTS off_chain_vote_d_rep_data (
				off_chain_vote_data_hash VARBINARY(32) NOT NULL,
				d_rep_hash_raw VARBINARY(28) NOT NULL,
				language VARCHAR(255) NOT NULL,
				voting_anchor_hash VARBINARY(32) NOT NULL,
				comment TEXT,
				bio TEXT,
				email VARCHAR(255),
				payment_address VARCHAR(255),
				given_name VARCHAR(255),
				image VARCHAR(255),
				objectives TEXT,
				motivations TEXT,
				qualifications TEXT,
				do_not_list BOOLEAN NOT NULL DEFAULT false,
				PRIMARY KEY (off_chain_vote_data_hash, d_rep_hash_raw, language),
				INDEX idx_vote_drep_anchor (voting_anchor_hash)
			)`,
		},
		{
			name: "off_chain_vote_references",
			sql: `CREATE TABLE IF NOT EXISTS off_chain_vote_references (
				off_chain_vote_data_hash VARBINARY(32) NOT NULL,
				label VARCHAR(255) NOT NULL,
				uri VARCHAR(255) NOT NULL,
				reference_hash VARBINARY(32),
				PRIMARY KEY (off_chain_vote_data_hash, label),
				INDEX idx_vote_ref_data (off_chain_vote_data_hash)
			)`,
		},
		{
			name: "off_chain_vote_external_updates",
			sql: `CREATE TABLE IF NOT EXISTS off_chain_vote_external_updates (
				off_chain_vote_data_hash VARBINARY(32) NOT NULL,
				title VARCHAR(255) NOT NULL,
				uri VARCHAR(255) NOT NULL,
				PRIMARY KEY (off_chain_vote_data_hash, title),
				INDEX idx_vote_ext_data (off_chain_vote_data_hash)
			)`,
		},
		{
			name: "off_chain_vote_fetch_errors",
			sql: `CREATE TABLE IF NOT EXISTS off_chain_vote_fetch_errors (
				voting_anchor_hash VARBINARY(32) NOT NULL,
				fetch_time DATETIME NOT NULL,
				fetch_error TEXT,
				retry_count INT UNSIGNED NOT NULL DEFAULT 0,
				PRIMARY KEY (voting_anchor_hash, fetch_time),
				INDEX idx_vote_fetch_error_anchor (voting_anchor_hash)
			)`,
		},
		{
			name: "utxo_states",
			sql: `CREATE TABLE IF NOT EXISTS utxo_states (
				epoch_no INT UNSIGNED NOT NULL,
				slot_no BIGINT UNSIGNED NOT NULL,
				tx_out_count BIGINT UNSIGNED NOT NULL,
				total_value BIGINT UNSIGNED NOT NULL,
				PRIMARY KEY (epoch_no, slot_no),
				INDEX idx_utxo_state_slot (slot_no)
			)`,
		},
		{
			name: "utxo_deltas",
			sql: `CREATE TABLE IF NOT EXISTS utxo_deltas (
				epoch_no INT UNSIGNED NOT NULL,
				slot_no BIGINT UNSIGNED NOT NULL,
				created_count BIGINT NOT NULL,
				deleted_count BIGINT NOT NULL,
				created_value BIGINT NOT NULL,
				deleted_value BIGINT NOT NULL,
				PRIMARY KEY (epoch_no, slot_no),
				INDEX idx_utxo_delta_slot (slot_no)
			)`,
		},
		{
			name: "wallet_connections",
			sql: `CREATE TABLE IF NOT EXISTS wallet_connections (
				sender_address VARCHAR(256) NOT NULL,
				receiver_address VARCHAR(256) NOT NULL,
				total_tx_count INT UNSIGNED NOT NULL DEFAULT 1,
				total_ada_sent BIGINT UNSIGNED NOT NULL DEFAULT 0,
				first_tx_slot BIGINT UNSIGNED NOT NULL,
				last_tx_slot BIGINT UNSIGNED NOT NULL,
				last_tx_hash VARBINARY(32) NOT NULL,
				PRIMARY KEY (sender_address, receiver_address),
				INDEX idx_wc_sender (sender_address),
				INDEX idx_wc_receiver (receiver_address),
				INDEX idx_wc_last_slot (last_tx_slot)
			)`,
		},
		{
			name: "token_holders",
			sql: `CREATE TABLE IF NOT EXISTS token_holders (
				policy VARBINARY(28) NOT NULL,
				name VARBINARY(32) NOT NULL,
				address VARCHAR(256) NOT NULL,
				amount BIGINT UNSIGNED NOT NULL,
				tx_hash VARBINARY(32),
				output_index INT UNSIGNED,
				stake_address VARCHAR(63),
				last_updated_slot BIGINT UNSIGNED,
				PRIMARY KEY (policy, name, address),
				INDEX idx_token_holders_amount (policy, name, amount DESC),
				INDEX idx_token_holders_stake_address (stake_address),
				INDEX idx_token_holders_last_updated (last_updated_slot)
			)`,
		},
		{
			name: "token_wallet_connections",
			sql: `CREATE TABLE IF NOT EXISTS token_wallet_connections (
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
				PRIMARY KEY (tx_hash, tx_index, policy_id, asset_name, from_address, to_address),
				INDEX idx_twc_token (policy_id, asset_name, slot DESC),
				INDEX idx_twc_from (from_address, policy_id, asset_name),
				INDEX idx_twc_to (to_address, policy_id, asset_name),
				INDEX idx_twc_stake_pair (from_stake_address, to_stake_address, policy_id, asset_name),
				INDEX idx_twc_slot (slot DESC)
			)`,
		},
		{
			name: "token_metadata",
			sql: `CREATE TABLE IF NOT EXISTS token_metadata (
				policy_id VARCHAR(56) NOT NULL,
				asset_name VARCHAR(64) NOT NULL,
				name VARCHAR(255),
				ticker VARCHAR(32),
				description TEXT,
				url VARCHAR(512),
				decimals INT UNSIGNED DEFAULT 0,
				logo MEDIUMTEXT,
				raw_json JSON,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY (policy_id, asset_name),
				INDEX idx_token_metadata_lookup (policy_id, asset_name)
			)`,
		},
	}

	for _, table := range compositePKTables {
		log.Printf("Creating table with composite keys: %s", table.name)
		if err := db.Exec(table.sql).Error; err != nil {
			return fmt.Errorf("failed to create table %s: %w", table.name, err)
		}
	}

	if err := ensureTokenWalletConnectionPrimaryKey(db); err != nil {
		return err
	}

	return nil
}

func ensureTokenWalletConnectionPrimaryKey(db *gorm.DB) error {
	var keyColumnCount int64
	if err := db.Raw(`
		SELECT COUNT(*)
		FROM information_schema.key_column_usage
		WHERE table_schema = DATABASE()
		  AND table_name = 'token_wallet_connections'
		  AND constraint_name = 'PRIMARY'
		  AND column_name IN ('from_address', 'to_address')
	`).Scan(&keyColumnCount).Error; err != nil {
		log.Printf("WARNING: Could not inspect token_wallet_connections primary key: %v", err)
		return nil
	}
	if keyColumnCount == 2 {
		return nil
	}

	log.Println("Migrating token_wallet_connections primary key to include sender/receiver addresses")
	if err := db.Exec(`
		ALTER TABLE token_wallet_connections
		DROP PRIMARY KEY,
		ADD PRIMARY KEY (tx_hash, tx_index, policy_id, asset_name, from_address, to_address)
	`).Error; err != nil {
		return fmt.Errorf("failed to migrate token_wallet_connections primary key: %w", err)
	}
	return nil
}

// createAllTablesWithoutForeignKeys creates all tables without foreign key constraints
func createAllTablesWithoutForeignKeys(db *gorm.DB) error {
	// Temporarily disable foreign key creation in GORM
	db.DisableForeignKeyConstraintWhenMigrating = true
	defer func() {
		db.DisableForeignKeyConstraintWhenMigrating = false
	}()

	// First create composite key tables manually
	if err := createTablesWithCompositeKeys(db); err != nil {
		return err
	}

	// Then create simple tables with GORM
	return migrateModelsWithoutCompositeKeys(db)
}

// migrateModelsWithoutCompositeKeys migrates models that don't have composite primary keys
func migrateModelsWithoutCompositeKeys(db *gorm.DB) error {
	// Models without composite primary keys
	simpleModels := []interface{}{
		&models.SlotLeader{},
		&models.Epoch{},
		&models.Block{},
		&models.Tx{},
		&models.StakeAddress{},
		&models.PoolHash{},
		&models.Script{},
		&models.Datum{},
		&models.RedeemerData{},
		&models.Redeemer{},
		&models.CollateralTxIn{},
		&models.ReferenceTxIn{},
		&models.CollateralTxOut{},
		&models.TxMetadata{},
		&models.ExtraKeyWitness{},
		&models.TxCbor{},
		&models.PoolMetadataRef{},
		&models.PoolUpdate{},
		&models.PoolRelay{},
		&models.PoolRetire{},
		&models.PoolOwner{},
		&models.PoolStat{},
		&models.OffChainPoolData{},
		&models.StakeRegistration{},
		&models.StakeDeregistration{},
		&models.Delegation{},
		&models.Reward{},
		&models.RewardRest{},
		&models.Withdrawal{},
		&models.EpochStake{},
		&models.EpochStakeProgress{},
		&models.DelistedPool{},
		&models.ReservedPoolTicker{},
		&models.CostModel{},
		&models.EpochParam{},
		&models.VotingAnchor{},
		&models.DRepHash{},
		&models.CommitteeHash{},
		&models.ParamProposal{},
		&models.GovActionProposal{},
		&models.VotingProcedure{},
		&models.DelegationVote{},
		&models.CommitteeRegistration{},
		&models.CommitteeDeregistration{},
		&models.TreasuryWithdrawal{},
		&models.CommitteeMember{},
		&models.EpochState{},
		&models.DRepDistr{},
		&models.Committee{},
		&models.Constitution{},
		&models.Treasury{},
		&models.Reserve{},
		&models.PotTransfer{},
		&models.OffChainVoteData{},
		&models.OffChainVoteAuthor{},
		&models.AdaPots{},
		&models.EventInfo{},
		&models.RequiredSigner{},
		&models.WalletConnectionTx{},
	}

	log.Printf("Migrating %d models without composite keys...", len(simpleModels))
	if err := db.AutoMigrate(simpleModels...); err != nil {
		// Log the error but continue - TiDB has issues with some migrations on existing tables
		log.Printf("WARNING: AutoMigrate failed (ignoring): %v", err)
		// return fmt.Errorf("failed to migrate simple models: %w", err)
	}

	return nil
}

// CreateTiFlashReplicas creates TiFlash replicas for analytical queries
func CreateTiFlashReplicas(db *gorm.DB) error {
	log.Println("Creating TiFlash replicas for analytical queries...")

	// Main tables that benefit from TiFlash for analytics
	tiflashTables := []string{
		"blocks",
		"txes",
		"tx_outs",
		"tx_ins",
		"multi_assets",
		"ma_tx_outs",
		"ma_tx_mints",
		"pool_stats",
		"epoch_stakes",
		"rewards",
		"delegations",
		"stake_addresses",
		"pool_updates",
	}

	for _, table := range tiflashTables {
		sql := fmt.Sprintf("ALTER TABLE %s SET TIFLASH REPLICA 1", table)
		if err := db.Exec(sql).Error; err != nil {
			// Log but don't fail - TiFlash might not be available
			log.Printf("Warning: Failed to create TiFlash replica for %s: %v", table, err)
		} else {
			log.Printf("Created TiFlash replica for table: %s", table)
		}
	}

	// Wait a moment for replicas to start syncing
	log.Println("TiFlash replicas created. They will sync in the background.")

	return nil
}
