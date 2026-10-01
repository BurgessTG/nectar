package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"gorm.io/gorm"

	"nectar/config"
	"nectar/database"
	unifiederrors "nectar/errors"
	"nectar/processors"
)

const defaultProductChunkSlots uint64 = 500_000

func handleRebuildProductsCommand(args []string) {
	fs := flag.NewFlagSet("rebuild-products", flag.ExitOnError)
	configPath := fs.String("config", "nectar.toml", "Path to configuration file")
	fs.StringVar(configPath, "c", "nectar.toml", "Path to configuration file")
	tables := fs.String("tables", "holders,wallets,token-wallets", "Comma-separated product tables: holders,wallets,token-wallets")
	chunkSlots := fs.Uint64("chunk-slots", defaultProductChunkSlots, "Slot range per graph backfill chunk")
	skipMigrations := fs.Bool("skip-migrations", false, "Skip database migrations before rebuild")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("failed to parse rebuild-products flags: %v", err)
	}

	cfg, db := openProductCommandDB(*configPath, !*skipMigrations)
	defer closeDB(db)

	selected := parseProductTables(*tables)
	if len(selected) == 0 {
		log.Fatalf("no product tables selected")
	}
	if *chunkSlots == 0 {
		log.Fatalf("--chunk-slots must be positive")
	}

	log.Printf("[PRODUCT] Rebuilding product tables with driver=%s profile=%s", cfg.Database.Driver, cfg.Indexing.Profile)

	if selected["holders"] {
		if err := rebuildTokenHolders(db); err != nil {
			log.Fatalf("failed to rebuild token_holders: %v", err)
		}
	}
	if selected["wallets"] || selected["token-wallets"] {
		minSlot, err := minIndexedSlot(db)
		if err != nil {
			log.Fatalf("failed to read min indexed slot: %v", err)
		}
		maxSlot, err := maxIndexedSlot(db)
		if err != nil {
			log.Fatalf("failed to read max indexed slot: %v", err)
		}
		if selected["wallets"] {
			if err := rebuildWalletConnections(db, minSlot, maxSlot, *chunkSlots); err != nil {
				log.Fatalf("failed to rebuild wallet connections: %v", err)
			}
		}
		if selected["token-wallets"] {
			if err := rebuildTokenWalletConnections(db, minSlot, maxSlot, *chunkSlots); err != nil {
				log.Fatalf("failed to rebuild token wallet connections: %v", err)
			}
		}
	}

	if err := printProductCounts(db); err != nil {
		log.Fatalf("failed to print product counts: %v", err)
	}
}

func handleVerifyProductsCommand(args []string) {
	fs := flag.NewFlagSet("verify-products", flag.ExitOnError)
	configPath := fs.String("config", "nectar.toml", "Path to configuration file")
	fs.StringVar(configPath, "c", "nectar.toml", "Path to configuration file")
	policy := fs.String("policy", "", "Hex policy id to verify")
	assetName := fs.String("asset-name", "", "Hex asset name to verify")
	deep := fs.Bool("deep", false, "Run global product consistency checks against base indexed tables")
	skipMigrations := fs.Bool("skip-migrations", true, "Skip database migrations before verify")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("failed to parse verify-products flags: %v", err)
	}

	_, db := openProductCommandDB(*configPath, !*skipMigrations)
	defer closeDB(db)

	if err := printProductCounts(db); err != nil {
		log.Fatalf("failed to print product counts: %v", err)
	}

	if *deep {
		if err := verifyTokenHoldersGlobal(db); err != nil {
			log.Fatalf("failed global token holder verification: %v", err)
		}
		if err := verifyWalletConnectionsGlobal(db); err != nil {
			log.Fatalf("failed global wallet connection verification: %v", err)
		}
		if err := verifyTokenWalletConnectionsGlobal(db); err != nil {
			log.Fatalf("failed global token wallet connection verification: %v", err)
		}
	}

	if *policy == "" && *assetName == "" {
		return
	}
	if *policy == "" || *assetName == "" {
		log.Fatalf("--policy and --asset-name must be provided together")
	}
	if err := verifyTokenHolders(db, *policy, *assetName); err != nil {
		log.Fatalf("failed to verify token holders: %v", err)
	}
}

func openProductCommandDB(configPath string, migrate bool) (*config.Config, *gorm.DB) {
	unifiederrors.Initialize(nil)

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}
	applyRuntimeEnv(cfg)

	db, err := database.InitSQL(databaseConfigFromNectarConfig(cfg))
	if err != nil {
		log.Fatalf("failed to initialize database: %v", err)
	}
	if migrate {
		if err := database.AutoMigrateForDriver(db, cfg.Database.Driver); err != nil {
			closeDB(db)
			log.Fatalf("failed to run migrations: %v", err)
		}
	}
	return cfg, db
}

func closeDB(db *gorm.DB) {
	sqlDB, err := db.DB()
	if err == nil {
		_ = sqlDB.Close()
	}
}

func parseProductTables(value string) map[string]bool {
	aliases := map[string]string{
		"holder":                   "holders",
		"holders":                  "holders",
		"token_holders":            "holders",
		"wallet":                   "wallets",
		"wallets":                  "wallets",
		"wallet_connections":       "wallets",
		"token-wallet":             "token-wallets",
		"token-wallets":            "token-wallets",
		"token_wallet":             "token-wallets",
		"token_wallets":            "token-wallets",
		"token_transfers":          "token-wallets",
		"token_wallet_connections": "token-wallets",
	}

	selected := make(map[string]bool)
	for _, raw := range strings.Split(value, ",") {
		key := strings.ToLower(strings.TrimSpace(raw))
		if key == "" {
			continue
		}
		if key == "all" {
			selected["holders"] = true
			selected["wallets"] = true
			selected["token-wallets"] = true
			continue
		}
		canonical, ok := aliases[key]
		if !ok {
			log.Fatalf("unknown product table %q", key)
		}
		selected[canonical] = true
	}
	return selected
}

func rebuildTokenHolders(db *gorm.DB) error {
	log.Println("[PRODUCT] Rebuilding token_holders")
	if err := db.Exec("DELETE FROM token_holders").Error; err != nil {
		return err
	}

	return db.Exec(`
		INSERT INTO token_holders
			(policy, name, address, amount, tx_hash, output_index, stake_address, last_updated_slot)
		SELECT
			mao.policy,
			mao.name,
			txo.address,
			SUM(mao.quantity) AS amount,
			MAX(mao.tx_hash) AS tx_hash,
			MAX(mao.tx_index) AS output_index,
			COALESCE(CONCAT('stake_', HEX(txo.stake_address_hash)), '') AS stake_address,
			MAX(b.slot_no) AS last_updated_slot
		FROM ma_tx_outs mao
		JOIN tx_outs txo
			ON mao.tx_hash = txo.tx_hash
			AND mao.tx_index = txo.` + "`index`" + `
		JOIN txes t ON t.hash = mao.tx_hash
		JOIN blocks b ON b.hash = t.block_hash
		LEFT JOIN tx_ins ti
			ON ti.tx_out_hash = txo.tx_hash
			AND ti.tx_out_index = txo.` + "`index`" + `
		WHERE ti.tx_out_hash IS NULL
		GROUP BY mao.policy, mao.name, txo.address, txo.stake_address_hash
		ON DUPLICATE KEY UPDATE
			amount = VALUES(amount),
			tx_hash = VALUES(tx_hash),
			output_index = VALUES(output_index),
			stake_address = VALUES(stake_address),
			last_updated_slot = VALUES(last_updated_slot)
	`).Error
}

func rebuildWalletConnections(db *gorm.DB, minSlot, maxSlot, chunkSlots uint64) error {
	log.Println("[PRODUCT] Rebuilding wallet_connections and wallet_connection_txs")
	if err := db.Exec("DELETE FROM wallet_connection_txs").Error; err != nil {
		return err
	}
	if err := db.Exec("DELETE FROM wallet_connections").Error; err != nil {
		return err
	}

	processor := processors.NewWalletConnectionProcessor(db)
	return runSlotChunks(minSlot, maxSlot, chunkSlots, func(start, end uint64) error {
		if err := processor.BackfillFromSlotRange(start, end); err != nil {
			return err
		}
		return processor.BackfillIndividualTxs(start, end)
	})
}

func rebuildTokenWalletConnections(db *gorm.DB, minSlot, maxSlot, chunkSlots uint64) error {
	log.Println("[PRODUCT] Rebuilding token_wallet_connections")
	if err := db.Exec("DELETE FROM token_wallet_connections").Error; err != nil {
		return err
	}

	processor := processors.NewTokenTransferProcessor(db, nil)
	return runSlotChunks(minSlot, maxSlot, chunkSlots, processor.BackfillFromSlotRange)
}

func runSlotChunks(minSlot, maxSlot, chunkSlots uint64, fn func(start, end uint64) error) error {
	if maxSlot < minSlot {
		return nil
	}
	for start := minSlot; start <= maxSlot; start += chunkSlots {
		end := start + chunkSlots
		if end <= start {
			return fmt.Errorf("slot chunk overflow at %d", start)
		}
		if end > maxSlot+1 {
			end = maxSlot + 1
		}
		log.Printf("[PRODUCT] Backfilling slots [%d, %d)", start, end)
		if err := fn(start, end); err != nil {
			return err
		}
		if end == maxSlot+1 {
			break
		}
	}
	return nil
}

func minIndexedSlot(db *gorm.DB) (uint64, error) {
	var minSlot uint64
	err := db.Raw("SELECT COALESCE(MIN(slot_no), 0) FROM blocks").Scan(&minSlot).Error
	return minSlot, err
}

func maxIndexedSlot(db *gorm.DB) (uint64, error) {
	var maxSlot uint64
	err := db.Raw("SELECT COALESCE(MAX(slot_no), 0) FROM blocks").Scan(&maxSlot).Error
	return maxSlot, err
}

func printProductCounts(db *gorm.DB) error {
	counts := map[string]int64{}
	for _, table := range []string{"token_holders", "wallet_connections", "wallet_connection_txs", "token_wallet_connections", "token_metadata"} {
		var count int64
		if err := db.Raw("SELECT COUNT(*) FROM " + table).Scan(&count).Error; err != nil {
			return fmt.Errorf("count %s: %w", table, err)
		}
		counts[table] = count
	}

	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintf(os.Stdout, "%s=%d\n", name, counts[name])
	}
	return nil
}

func verifyTokenHolders(db *gorm.DB, policy, assetName string) error {
	type aggregate struct {
		HolderCount int64
		TotalAmount string
	}

	var product aggregate
	if err := db.Raw(`
		SELECT COUNT(*) AS holder_count,
		       COALESCE(CAST(SUM(amount) AS CHAR), '0') AS total_amount
		FROM token_holders
		WHERE policy = UNHEX(?) AND name = UNHEX(?)
	`, policy, assetName).Scan(&product).Error; err != nil {
		return err
	}

	var base aggregate
	if err := db.Raw(`
		SELECT COUNT(*) AS holder_count,
		       COALESCE(CAST(SUM(balance) AS CHAR), '0') AS total_amount
		FROM (
			SELECT txo.address, SUM(mao.quantity) AS balance
			FROM ma_tx_outs mao
			JOIN tx_outs txo
				ON mao.tx_hash = txo.tx_hash
				AND mao.tx_index = txo.`+"`index`"+`
			LEFT JOIN tx_ins ti
				ON ti.tx_out_hash = txo.tx_hash
				AND ti.tx_out_index = txo.`+"`index`"+`
			WHERE mao.policy = UNHEX(?)
			  AND mao.name = UNHEX(?)
			  AND ti.tx_out_hash IS NULL
			GROUP BY txo.address
		) current_holders
	`, policy, assetName).Scan(&base).Error; err != nil {
		return err
	}

	fmt.Fprintf(os.Stdout, "verify_policy=%s\n", policy)
	fmt.Fprintf(os.Stdout, "verify_asset_name=%s\n", assetName)
	fmt.Fprintf(os.Stdout, "token_holders_count=%d\n", product.HolderCount)
	fmt.Fprintf(os.Stdout, "base_utxo_holder_count=%d\n", base.HolderCount)
	fmt.Fprintf(os.Stdout, "token_holders_total=%s\n", product.TotalAmount)
	fmt.Fprintf(os.Stdout, "base_utxo_total=%s\n", base.TotalAmount)
	if product.HolderCount != base.HolderCount || product.TotalAmount != base.TotalAmount {
		return fmt.Errorf("token_holders mismatch for %s.%s", policy, assetName)
	}
	return nil
}

func verifyTokenHoldersGlobal(db *gorm.DB) error {
	type aggregate struct {
		HolderCount int64
		TotalAmount string
	}

	var product aggregate
	if err := db.Raw(`
		SELECT COUNT(*) AS holder_count,
		       COALESCE(CAST(SUM(amount) AS CHAR), '0') AS total_amount
		FROM token_holders
	`).Scan(&product).Error; err != nil {
		return err
	}

	var base aggregate
	if err := db.Raw(`
		SELECT COUNT(*) AS holder_count,
		       COALESCE(CAST(SUM(balance) AS CHAR), '0') AS total_amount
		FROM (
			SELECT mao.policy, mao.name, txo.address, SUM(mao.quantity) AS balance
			FROM ma_tx_outs mao
			JOIN tx_outs txo
				ON mao.tx_hash = txo.tx_hash
				AND mao.tx_index = txo.` + "`index`" + `
			LEFT JOIN tx_ins ti
				ON ti.tx_out_hash = txo.tx_hash
				AND ti.tx_out_index = txo.` + "`index`" + `
			WHERE ti.tx_out_hash IS NULL
			GROUP BY mao.policy, mao.name, txo.address
		) current_holders
	`).Scan(&base).Error; err != nil {
		return err
	}

	var mismatchCount int64
	if err := db.Raw(`
		WITH base_holders AS (
			SELECT mao.policy, mao.name, txo.address, SUM(mao.quantity) AS amount
			FROM ma_tx_outs mao
			JOIN tx_outs txo
				ON mao.tx_hash = txo.tx_hash
				AND mao.tx_index = txo.` + "`index`" + `
			LEFT JOIN tx_ins ti
				ON ti.tx_out_hash = txo.tx_hash
				AND ti.tx_out_index = txo.` + "`index`" + `
			WHERE ti.tx_out_hash IS NULL
			GROUP BY mao.policy, mao.name, txo.address
		)
		SELECT COUNT(*) FROM (
			SELECT b.policy, b.name, b.address
			FROM base_holders b
			LEFT JOIN token_holders th
				ON th.policy = b.policy
				AND th.name = b.name
				AND th.address = b.address
			WHERE th.policy IS NULL OR th.amount <> b.amount
			UNION ALL
			SELECT th.policy, th.name, th.address
			FROM token_holders th
			LEFT JOIN base_holders b
				ON b.policy = th.policy
				AND b.name = th.name
				AND b.address = th.address
			WHERE b.policy IS NULL
		) mismatches
	`).Scan(&mismatchCount).Error; err != nil {
		return err
	}

	fmt.Fprintf(os.Stdout, "global_token_holders_count=%d\n", product.HolderCount)
	fmt.Fprintf(os.Stdout, "global_base_utxo_holder_count=%d\n", base.HolderCount)
	fmt.Fprintf(os.Stdout, "global_token_holders_total=%s\n", product.TotalAmount)
	fmt.Fprintf(os.Stdout, "global_base_utxo_total=%s\n", base.TotalAmount)
	fmt.Fprintf(os.Stdout, "global_token_holders_mismatches=%d\n", mismatchCount)
	if product.HolderCount != base.HolderCount || product.TotalAmount != base.TotalAmount || mismatchCount != 0 {
		return fmt.Errorf("global token_holders mismatch")
	}
	return nil
}

func verifyWalletConnectionsGlobal(db *gorm.DB) error {
	type aggregate struct {
		ConnectionCount int64
		EventCount      string
		TotalAda        string
	}

	var product aggregate
	if err := db.Raw(`
		SELECT COUNT(*) AS connection_count,
		       COALESCE(CAST(SUM(total_tx_count) AS CHAR), '0') AS event_count,
		       COALESCE(CAST(SUM(total_ada_sent) AS CHAR), '0') AS total_ada
		FROM wallet_connections
	`).Scan(&product).Error; err != nil {
		return err
	}

	var base aggregate
	if err := db.Raw(walletConnectionBaseCTE() + `
		SELECT COUNT(*) AS connection_count,
		       COALESCE(CAST(SUM(event_count) AS CHAR), '0') AS event_count,
		       COALESCE(CAST(SUM(total_ada) AS CHAR), '0') AS total_ada
		FROM base_connections
	`).Scan(&base).Error; err != nil {
		return err
	}

	var productTx aggregate
	if err := db.Raw(`
		SELECT COUNT(*) AS connection_count,
		       COALESCE(CAST(COUNT(*) AS CHAR), '0') AS event_count,
		       COALESCE(CAST(SUM(ada_amount) AS CHAR), '0') AS total_ada
		FROM wallet_connection_txs
	`).Scan(&productTx).Error; err != nil {
		return err
	}

	var mismatchCount int64
	if err := db.Raw(walletConnectionBaseCTE() + `
		SELECT COUNT(*) FROM (
			SELECT b.sender_address, b.receiver_address
			FROM base_connections b
			LEFT JOIN wallet_connections wc
				ON wc.sender_address = b.sender_address
				AND wc.receiver_address = b.receiver_address
			WHERE wc.sender_address IS NULL
			   OR wc.total_tx_count <> b.event_count
			   OR wc.total_ada_sent <> b.total_ada
			   OR wc.first_tx_slot <> b.first_slot
			   OR wc.last_tx_slot <> b.last_slot
			   OR wc.last_tx_hash <> b.last_tx_hash
			UNION ALL
			SELECT wc.sender_address, wc.receiver_address
			FROM wallet_connections wc
			LEFT JOIN base_connections b
				ON b.sender_address = wc.sender_address
				AND b.receiver_address = wc.receiver_address
			WHERE b.sender_address IS NULL
		) mismatches
	`).Scan(&mismatchCount).Error; err != nil {
		return err
	}

	var txMismatchCount int64
	if err := db.Raw(walletConnectionBaseCTE() + `
		, base_tx_events AS (
			SELECT sender_address, receiver_address, tx_hash, slot_no, ada_amount, COUNT(*) AS event_count
			FROM base_events
			GROUP BY sender_address, receiver_address, tx_hash, slot_no, ada_amount
		),
		product_tx_events AS (
			SELECT sender_address, receiver_address, tx_hash, slot_no, ada_amount, COUNT(*) AS event_count
			FROM wallet_connection_txs
			GROUP BY sender_address, receiver_address, tx_hash, slot_no, ada_amount
		)
		SELECT COUNT(*) FROM (
			SELECT b.sender_address, b.receiver_address, b.tx_hash, b.slot_no, b.ada_amount
			FROM base_tx_events b
			LEFT JOIN product_tx_events p
				ON p.sender_address = b.sender_address
				AND p.receiver_address = b.receiver_address
				AND p.tx_hash = b.tx_hash
				AND p.slot_no = b.slot_no
				AND p.ada_amount = b.ada_amount
			WHERE p.sender_address IS NULL OR p.event_count <> b.event_count
			UNION ALL
			SELECT p.sender_address, p.receiver_address, p.tx_hash, p.slot_no, p.ada_amount
			FROM product_tx_events p
			LEFT JOIN base_tx_events b
				ON b.sender_address = p.sender_address
				AND b.receiver_address = p.receiver_address
				AND b.tx_hash = p.tx_hash
				AND b.slot_no = p.slot_no
				AND b.ada_amount = p.ada_amount
			WHERE b.sender_address IS NULL
		) mismatches
	`).Scan(&txMismatchCount).Error; err != nil {
		return err
	}

	fmt.Fprintf(os.Stdout, "global_wallet_connections_count=%d\n", product.ConnectionCount)
	fmt.Fprintf(os.Stdout, "global_base_wallet_connections_count=%d\n", base.ConnectionCount)
	fmt.Fprintf(os.Stdout, "global_wallet_connections_events=%s\n", product.EventCount)
	fmt.Fprintf(os.Stdout, "global_base_wallet_connection_events=%s\n", base.EventCount)
	fmt.Fprintf(os.Stdout, "global_wallet_connections_total_ada=%s\n", product.TotalAda)
	fmt.Fprintf(os.Stdout, "global_base_wallet_connections_total_ada=%s\n", base.TotalAda)
	fmt.Fprintf(os.Stdout, "global_wallet_connection_txs_events=%s\n", productTx.EventCount)
	fmt.Fprintf(os.Stdout, "global_wallet_connection_txs_total_ada=%s\n", productTx.TotalAda)
	fmt.Fprintf(os.Stdout, "global_wallet_connections_mismatches=%d\n", mismatchCount)
	fmt.Fprintf(os.Stdout, "global_wallet_connection_txs_mismatches=%d\n", txMismatchCount)
	if product.ConnectionCount != base.ConnectionCount ||
		product.EventCount != base.EventCount ||
		product.TotalAda != base.TotalAda ||
		productTx.EventCount != base.EventCount ||
		productTx.TotalAda != base.TotalAda ||
		mismatchCount != 0 ||
		txMismatchCount != 0 {
		return fmt.Errorf("global wallet connection mismatch")
	}
	return nil
}

func verifyTokenWalletConnectionsGlobal(db *gorm.DB) error {
	type aggregate struct {
		TransferCount int64
		TotalQuantity string
	}

	var product aggregate
	if err := db.Raw(`
		SELECT COUNT(*) AS transfer_count,
		       COALESCE(CAST(SUM(quantity) AS CHAR), '0') AS total_quantity
		FROM token_wallet_connections
	`).Scan(&product).Error; err != nil {
		return err
	}

	var base aggregate
	if err := db.Raw(tokenWalletConnectionBaseCTE() + `
		SELECT COUNT(*) AS transfer_count,
		       COALESCE(CAST(SUM(quantity) AS CHAR), '0') AS total_quantity
		FROM base_token_wallet_connections
	`).Scan(&base).Error; err != nil {
		return err
	}

	var mismatchCount int64
	if err := db.Raw(tokenWalletConnectionBaseCTE() + `
		SELECT COUNT(*) FROM (
			SELECT b.tx_hash, b.tx_index, b.policy_id, b.asset_name, b.from_address, b.to_address
			FROM base_token_wallet_connections b
			LEFT JOIN token_wallet_connections twc
				ON twc.tx_hash = b.tx_hash
				AND twc.tx_index = b.tx_index
				AND twc.policy_id = b.policy_id
				AND twc.asset_name = b.asset_name
				AND twc.from_address = b.from_address
				AND twc.to_address = b.to_address
			WHERE twc.tx_hash IS NULL
			   OR twc.quantity <> b.quantity
			   OR twc.slot <> b.slot
			   OR COALESCE(twc.block_height, 0) <> COALESCE(b.block_height, 0)
			UNION ALL
			SELECT twc.tx_hash, twc.tx_index, twc.policy_id, twc.asset_name, twc.from_address, twc.to_address
			FROM token_wallet_connections twc
			LEFT JOIN base_token_wallet_connections b
				ON b.tx_hash = twc.tx_hash
				AND b.tx_index = twc.tx_index
				AND b.policy_id = twc.policy_id
				AND b.asset_name = twc.asset_name
				AND b.from_address = twc.from_address
				AND b.to_address = twc.to_address
			WHERE b.tx_hash IS NULL
		) mismatches
	`).Scan(&mismatchCount).Error; err != nil {
		return err
	}

	fmt.Fprintf(os.Stdout, "global_token_wallet_connections_count=%d\n", product.TransferCount)
	fmt.Fprintf(os.Stdout, "global_base_token_wallet_connections_count=%d\n", base.TransferCount)
	fmt.Fprintf(os.Stdout, "global_token_wallet_connections_total_quantity=%s\n", product.TotalQuantity)
	fmt.Fprintf(os.Stdout, "global_base_token_wallet_connections_total_quantity=%s\n", base.TotalQuantity)
	fmt.Fprintf(os.Stdout, "global_token_wallet_connections_mismatches=%d\n", mismatchCount)
	if product.TransferCount != base.TransferCount ||
		product.TotalQuantity != base.TotalQuantity ||
		mismatchCount != 0 {
		return fmt.Errorf("global token wallet connection mismatch")
	}
	return nil
}

func walletConnectionBaseCTE() string {
	return `
		WITH sender_addresses AS (
			SELECT DISTINCT
				ti.tx_in_hash AS tx_hash,
				spent_out.address AS sender_address
			FROM tx_ins ti
			JOIN tx_outs spent_out
				ON spent_out.tx_hash = ti.tx_out_hash
				AND spent_out.` + "`index`" + ` = ti.tx_out_index
			WHERE CHAR_LENGTH(spent_out.address) <= 256
		),
		receiver_outputs AS (
			SELECT
				txo.tx_hash,
				txo.address AS receiver_address,
				txo.value AS ada_amount,
				b.slot_no
			FROM tx_outs txo
			JOIN txes t ON t.hash = txo.tx_hash
			JOIN blocks b ON b.hash = t.block_hash
			WHERE CHAR_LENGTH(txo.address) <= 256
		),
		base_events AS (
			SELECT
				s.tx_hash,
				s.sender_address,
				r.receiver_address,
				r.ada_amount,
				r.slot_no
			FROM sender_addresses s
			JOIN receiver_outputs r ON r.tx_hash = s.tx_hash
			WHERE s.sender_address != r.receiver_address
		),
		base_connections AS (
			SELECT
				sender_address,
				receiver_address,
				COUNT(*) AS event_count,
				COALESCE(SUM(ada_amount), 0) AS total_ada,
				MIN(slot_no) AS first_slot,
				MAX(slot_no) AS last_slot,
				MAX(tx_hash) AS last_tx_hash
			FROM base_events
			GROUP BY sender_address, receiver_address
		)
	`
}

func tokenWalletConnectionBaseCTE() string {
	return `
		WITH base_token_wallet_connections AS (
			SELECT DISTINCT
				output_ma.tx_hash,
				output_ma.tx_index,
				output_ma.policy AS policy_id,
				output_ma.name AS asset_name,
				input_addr.address AS from_address,
				output_addr.address AS to_address,
				COALESCE(CONCAT('stake_', HEX(input_addr.stake_address_hash)), '') AS from_stake_address,
				COALESCE(CONCAT('stake_', HEX(output_addr.stake_address_hash)), '') AS to_stake_address,
				output_ma.quantity,
				b.slot_no AS slot,
				b.block_no AS block_height
			FROM ma_tx_outs output_ma
			JOIN tx_outs output_addr
				ON output_ma.tx_hash = output_addr.tx_hash
				AND output_ma.tx_index = output_addr.` + "`index`" + `
			JOIN txes t ON t.hash = output_ma.tx_hash
			JOIN blocks b ON b.hash = t.block_hash
			JOIN tx_ins ti ON ti.tx_in_hash = output_ma.tx_hash
			JOIN tx_outs input_addr
				ON ti.tx_out_hash = input_addr.tx_hash
				AND ti.tx_out_index = input_addr.` + "`index`" + `
			JOIN ma_tx_outs input_ma
				ON ti.tx_out_hash = input_ma.tx_hash
				AND ti.tx_out_index = input_ma.tx_index
				AND input_ma.policy = output_ma.policy
				AND input_ma.name = output_ma.name
			WHERE input_addr.address != output_addr.address
				AND CHAR_LENGTH(input_addr.address) <= 256
				AND CHAR_LENGTH(output_addr.address) <= 256
		)
	`
}
