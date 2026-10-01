package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"nectar/bootstrap"
	"nectar/config"
	"nectar/database"
	"nectar/snapshot"

	"gorm.io/gorm"
)

const defaultDolosDir = "/run/media/burgess/3559061d-cad2-4a59-9603-9b402a87cf72/cardano.nodes"

func handleBootstrapDolosCommand(args []string) {
	fs := flag.NewFlagSet("bootstrap-dolos", flag.ExitOnError)
	dolosDir := fs.String("dolos-dir", firstNonEmpty(os.Getenv("DOLOS_DIR"), defaultDolosDir), "Dolos node directory containing dolos.toml")
	configPath := fs.String("config", "nectar.toml", "Path to read/write Nectar config")
	fs.StringVar(configPath, "c", "nectar.toml", "Path to read/write Nectar config")
	mysqlDSN := fs.String("mysql-dsn", firstNonEmpty(os.Getenv("NECTAR_DSN"), os.Getenv("TIDB_DSN")), "MySQL-compatible DSN to write into Nectar config")
	driver := fs.String("driver", config.DatabaseDriverMySQL, "Database driver to write: mysql or tidb")
	profile := fs.String("profile", config.IndexingProfileHoneycomb, "Indexing profile to write")
	writeConfig := fs.Bool("write", false, "Write the derived Nectar config")
	outputPath := fs.String("output", "", "Config output path when writing; defaults to --config")
	requireSocket := fs.Bool("require-socket", false, "Fail if the Dolos ouroboros socket is not present")
	writeRunbook := fs.String("write-runbook", "", "Write a generated Dolos/Nectar bootstrap runbook without executing it")
	writeScript := fs.String("write-script", "", "Write a generated Dolos/Nectar bootstrap shell script without executing it")
	bootstrapMethod := fs.String("bootstrap-method", bootstrap.BootstrapMethodSnapshot, "Bootstrap method for generated artifacts: snapshot or mithril")
	snapshotVariant := fs.String("snapshot-variant", "full", "Dolos snapshot variant for generated artifacts")
	snapshotPoint := fs.String("snapshot-point", "latest", "Dolos snapshot point for generated artifacts")
	mysqlCompose := fs.String("mysql-compose", "docker-compose.mysql.example.yml", "MySQL compose file path for generated artifacts")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("failed to parse bootstrap-dolos flags: %v", err)
	}

	report, err := bootstrap.InspectDolos(*dolosDir, *requireSocket)
	if err != nil {
		log.Fatalf("failed to inspect Dolos: %v", err)
	}
	printDolosReport(report)
	if *requireSocket && !report.SocketReady {
		log.Fatalf("Dolos socket is required but not ready: %s", report.SocketPath)
	}

	printDatabaseReadiness(*driver, *mysqlDSN)

	if *writeRunbook != "" || *writeScript != "" {
		targetConfig := *outputPath
		if targetConfig == "" {
			targetConfig = *configPath
		}
		workDir, err := os.Getwd()
		if err != nil {
			log.Fatalf("failed to resolve workdir: %v", err)
		}
		artifactOptions := bootstrap.RunbookOptions{
			WorkDir:         workDir,
			ConfigPath:      targetConfig,
			MySQLCompose:    *mysqlCompose,
			MySQLDSN:        *mysqlDSN,
			Driver:          *driver,
			Profile:         *profile,
			BootstrapMethod: *bootstrapMethod,
			SnapshotVariant: *snapshotVariant,
			SnapshotPoint:   *snapshotPoint,
		}
		if *writeRunbook != "" {
			runbook, err := bootstrap.GenerateRunbook(report, artifactOptions)
			if err != nil {
				log.Fatalf("failed to generate runbook: %v", err)
			}
			if err := writeTextFile(*writeRunbook, []byte(runbook), 0644); err != nil {
				log.Fatalf("failed to write runbook: %v", err)
			}
			fmt.Printf("Wrote bootstrap runbook: %s\n", *writeRunbook)
		}
		if *writeScript != "" {
			script, err := bootstrap.GenerateScript(report, artifactOptions)
			if err != nil {
				log.Fatalf("failed to generate script: %v", err)
			}
			if err := writeTextFile(*writeScript, []byte(script), 0755); err != nil {
				log.Fatalf("failed to write script: %v", err)
			}
			fmt.Printf("Wrote bootstrap script: %s\n", *writeScript)
		}
	}

	if !*writeConfig {
		fmt.Println()
		fmt.Println("Dry run only. Pass --write to write the derived Nectar config.")
		return
	}

	cfg, err := loadConfigForWrite(*configPath, *mysqlDSN)
	if err != nil {
		log.Fatalf("failed to load base Nectar config: %v", err)
	}
	applyDolosReportToConfig(cfg, report, *driver, *profile, *mysqlDSN)

	target := *outputPath
	if target == "" {
		target = *configPath
	}
	if err := writeNectarConfig(target, cfg); err != nil {
		log.Fatalf("failed to write Nectar config: %v", err)
	}
	fmt.Printf("Wrote Nectar config: %s\n", target)
}

func handleSnapshotImportCommand(args []string) {
	fs := flag.NewFlagSet("snapshot-import", flag.ExitOnError)
	configPath := fs.String("config", "nectar.toml", "Path to Nectar config")
	fs.StringVar(configPath, "c", "nectar.toml", "Path to Nectar config")
	source := fs.String("source", "utxorpc", "Import source: utxorpc, ndjson, or filesystem")
	dolosDir := fs.String("dolos-dir", firstNonEmpty(os.Getenv("DOLOS_DIR"), defaultDolosDir), "Dolos node directory used to infer the UTxO RPC endpoint")
	snapshotDir := fs.String("snapshot-dir", "", "Extracted Mithril snapshot directory for --source filesystem")
	utxoNDJSON := fs.String("utxo-ndjson", "", "Parsed Dingo-style current UTxO NDJSON file for --source ndjson")
	fs.StringVar(utxoNDJSON, "snapshot-file", "", "Alias for --utxo-ndjson")
	endpoint := fs.String("endpoint", "", "UTxO RPC base endpoint; defaults to Dolos serve.grpc")
	protocol := fs.String("protocol", snapshot.ProtocolGRPC, "UTxO RPC protocol: grpc or connect")
	batchSize := fs.Int("batch-size", 1000, "UTxOs to request per page")
	limit := fs.Int64("limit", 0, "Maximum UTxOs to scan; 0 means all")
	policyHex := fs.String("policy", "", "Optional hex policy id filter")
	assetNameHex := fs.String("asset-name", "", "Optional hex asset name filter")
	apply := fs.Bool("apply", false, "Write imported current UTxO rows to SQL")
	allowPartialApply := fs.Bool("allow-partial-apply", false, "Allow --apply with --limit or asset filters; only for tests, not full chart data")
	allowBroadUTxORPC := fs.Bool("allow-broad-utxorpc", false, "Allow unfiltered UTxO RPC SearchUtxos; most Dolos nodes reject this as too broad")
	replaceUTxO := fs.Bool("replace-utxo", false, "Delete token_holders, ma_tx_outs, and tx_outs before applying")
	refreshHolders := fs.Bool("refresh-holders", true, "Refresh token_holders from tx_outs/ma_tx_outs after applying")
	skipMigrations := fs.Bool("skip-migrations", false, "Skip database migrations before applying")
	manifestOnly := fs.Bool("manifest-only", false, "Build readiness/coverage manifest without connecting to UTxO RPC or SQL")
	writeManifest := fs.String("write-manifest", "", "Write snapshot import readiness/coverage manifest JSON")
	timeout := fs.Duration("timeout", 30*time.Second, "Per-request UTxO RPC timeout")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("failed to parse snapshot-import flags: %v", err)
	}
	if *manifestOnly && *apply {
		log.Fatalf("--manifest-only cannot be combined with --apply")
	}
	sourceKind := normalizeSnapshotCommandSource(*source)
	if sourceKind == "" {
		log.Fatalf("unsupported --source %q; expected utxorpc, ndjson, or filesystem", *source)
	}

	policy, err := parseHexFlag("policy", *policyHex)
	if err != nil {
		log.Fatal(err)
	}
	assetName, err := parseHexFlag("asset-name", *assetNameHex)
	if err != nil {
		log.Fatal(err)
	}
	if len(assetName) > 0 && len(policy) == 0 {
		log.Fatalf("--asset-name requires --policy")
	}
	if *batchSize <= 0 {
		log.Fatalf("--batch-size must be positive")
	}
	if *apply && !*allowPartialApply && (*limit > 0 || len(policy) > 0 || len(assetName) > 0) {
		log.Fatalf("--apply refuses partial snapshot writes by default; remove --limit/filters for chart data or pass --allow-partial-apply for tests")
	}
	if sourceKind == "ndjson" && (len(policy) > 0 || len(assetName) > 0) {
		log.Fatalf("--policy and --asset-name filters are only supported for --source utxorpc")
	}
	if sourceKind == "filesystem" && (len(policy) > 0 || len(assetName) > 0) {
		log.Fatalf("--policy and --asset-name filters are only supported for --source utxorpc")
	}
	if sourceKind == "utxorpc" && !*manifestOnly && len(policy) == 0 && len(assetName) == 0 && !*allowBroadUTxORPC {
		log.Fatalf("unfiltered UTxO RPC SearchUtxos is disabled by default because local Dolos rejects broad queries; use --source filesystem with UTxO-HD tables/tvar, --source ndjson, add --policy filters, or pass --allow-broad-utxorpc for a server that supports complete enumeration")
	}

	switch sourceKind {
	case "utxorpc":
	case "filesystem":
		root := strings.TrimSpace(*snapshotDir)
		if root == "" {
			root = *dolosDir
		}
		report, err := snapshot.InspectFilesystemSnapshot(root)
		if err != nil {
			log.Fatalf("failed to inspect filesystem snapshot: %v", err)
		}
		printFilesystemSnapshotReport(report)
		coverageSource := "filesystem"
		if report.DirectImportSupported {
			coverageSource = "tvar"
		}
		printSnapshotProductCoverage(coverageSource, false, *limit > 0)
		if *writeManifest != "" {
			manifest := snapshot.BuildImportManifest(snapshot.ImportManifestOptions{
				Source:                          coverageSource,
				DolosDir:                        *dolosDir,
				SnapshotDir:                     root,
				FilesystemReport:                report,
				Partial:                         *limit > 0,
				DirectFilesystemDecodeSupported: report.DirectImportSupported,
				Warnings:                        filesystemManifestWarnings(report),
			})
			if err := writeImportManifest(*writeManifest, manifest); err != nil {
				log.Fatalf("failed to write import manifest: %v", err)
			}
			fmt.Printf("Wrote snapshot import manifest: %s\n", *writeManifest)
		}
		if !*apply {
			if report.DirectImportSupported && *limit > 0 {
				importer := snapshot.NewTVarImporter(nil, snapshot.TVarOptions{
					Path:      report.UTxOTablePath,
					BatchSize: *batchSize,
					Limit:     *limit,
					TipSlot:   report.UTxOTableSlot,
				})
				stats, err := importer.Import(context.Background())
				if err != nil {
					log.Fatalf("filesystem tvar dry scan failed: %v", err)
				}
				printSnapshotStats(stats, report.UTxOTablePath)
			}
			if report.DirectImportSupported {
				fmt.Println("Dry run only. Filesystem source found UTxO-HD tables/tvar and is ready for direct apply.")
			} else {
				fmt.Println("Dry run only. Filesystem source currently performs Dingo-compatible snapshot discovery only.")
			}
			return
		}
		if !report.DirectImportSupported {
			log.Fatalf("filesystem apply requires UTxO-HD ledger/<slot>/tables/tvar; legacy ledger-state decode is not implemented yet")
		}
		var sqlDB *gorm.DB
		sqlDB = openSnapshotCommandDB(*configPath, !*skipMigrations)
		defer closeDB(sqlDB)

		importer := snapshot.NewTVarImporter(sqlDB, snapshot.TVarOptions{
			Path:        report.UTxOTablePath,
			BatchSize:   *batchSize,
			Limit:       *limit,
			Apply:       *apply,
			ReplaceUTxO: *replaceUTxO,
			TipSlot:     report.UTxOTableSlot,
		})
		stats, err := importer.Import(context.Background())
		if err != nil {
			log.Fatalf("snapshot import failed: %v", err)
		}
		printSnapshotStats(stats, report.UTxOTablePath)
		if *refreshHolders {
			holderRows, err := snapshot.RefreshTokenHoldersFromCurrentUTxO(sqlDB, stats.LastTipSlot)
			if err != nil {
				log.Fatalf("failed to refresh token_holders from current UTxO snapshot: %v", err)
			}
			fmt.Printf("  token_holders_refreshed: %d\n", holderRows)
		}
		printSnapshotProductCoverage("tvar", *apply, *allowPartialApply || *limit > 0)
		if *writeManifest != "" {
			manifest := snapshot.BuildImportManifest(snapshot.ImportManifestOptions{
				Source:                          "tvar",
				DolosDir:                        *dolosDir,
				SnapshotDir:                     root,
				SnapshotFile:                    report.UTxOTablePath,
				FilesystemReport:                report,
				Apply:                           *apply,
				Partial:                         *allowPartialApply || *limit > 0,
				DirectFilesystemDecodeSupported: true,
			})
			if err := writeImportManifest(*writeManifest, manifest); err != nil {
				log.Fatalf("failed to write import manifest: %v", err)
			}
			fmt.Printf("Wrote snapshot import manifest: %s\n", *writeManifest)
		}
		return
	case "ndjson":
		path := strings.TrimSpace(*utxoNDJSON)
		if path == "" {
			log.Fatalf("--utxo-ndjson is required for --source ndjson")
		}
		if *manifestOnly {
			printSnapshotProductCoverage("ndjson", false, *limit > 0)
			if *writeManifest != "" {
				manifest := snapshot.BuildImportManifest(snapshot.ImportManifestOptions{
					Source:       "ndjson",
					DolosDir:     *dolosDir,
					SnapshotFile: path,
					Partial:      *limit > 0,
					Warnings: []string{
						"manifest-only did not open the parsed UTxO NDJSON file or SQL",
						"direct filesystem ledger-state decode remains separate from parsed NDJSON apply",
					},
				})
				if err := writeImportManifest(*writeManifest, manifest); err != nil {
					log.Fatalf("failed to write import manifest: %v", err)
				}
				fmt.Printf("Wrote snapshot import manifest: %s\n", *writeManifest)
			}
			return
		}

		var sqlDB *gorm.DB
		if *apply {
			sqlDB = openSnapshotCommandDB(*configPath, !*skipMigrations)
			defer closeDB(sqlDB)
		}
		importer := snapshot.NewNDJSONImporter(sqlDB, snapshot.NDJSONOptions{
			Path:        path,
			BatchSize:   *batchSize,
			Limit:       *limit,
			Apply:       *apply,
			ReplaceUTxO: *replaceUTxO,
		})
		stats, err := importer.Import(context.Background())
		if err != nil {
			log.Fatalf("snapshot import failed: %v", err)
		}
		printSnapshotStats(stats, path)
		if *apply && *refreshHolders {
			holderRows, err := snapshot.RefreshTokenHoldersFromCurrentUTxO(sqlDB, stats.LastTipSlot)
			if err != nil {
				log.Fatalf("failed to refresh token_holders from current UTxO snapshot: %v", err)
			}
			fmt.Printf("  token_holders_refreshed: %d\n", holderRows)
		}
		printSnapshotProductCoverage("ndjson", *apply, *allowPartialApply || *limit > 0)
		if *writeManifest != "" {
			manifest := snapshot.BuildImportManifest(snapshot.ImportManifestOptions{
				Source:       "ndjson",
				DolosDir:     *dolosDir,
				SnapshotFile: path,
				Apply:        *apply,
				Partial:      *allowPartialApply || *limit > 0,
				Warnings:     []string{"direct filesystem ledger-state decode remains separate from parsed NDJSON apply"},
			})
			if err := writeImportManifest(*writeManifest, manifest); err != nil {
				log.Fatalf("failed to write import manifest: %v", err)
			}
			fmt.Printf("Wrote snapshot import manifest: %s\n", *writeManifest)
		}
		if !*apply {
			fmt.Println("Dry run only. Pass --apply to write SQL rows.")
		}
		return
	}

	var dolosReport *bootstrap.DolosReport
	if *endpoint == "" {
		report, err := bootstrap.InspectDolos(*dolosDir, false)
		if err != nil {
			log.Fatalf("failed to inspect Dolos for endpoint: %v", err)
		}
		dolosReport = report
		*endpoint = report.GRPCEndpoint
	}
	if *endpoint == "" {
		log.Fatalf("UTxO RPC endpoint is required")
	}

	if *manifestOnly {
		utxoRPCPartial := *limit > 0 || len(policy) > 0 || len(assetName) > 0
		utxoRPCUnsupportedBroad := len(policy) == 0 && len(assetName) == 0
		printSnapshotProductCoverage("utxorpc", false, utxoRPCPartial || utxoRPCUnsupportedBroad)
		if *writeManifest != "" {
			warnings := []string{"manifest-only did not connect to UTxO RPC or SQL"}
			if utxoRPCUnsupportedBroad {
				warnings = append(warnings, "local Dolos rejects unfiltered UTxO RPC SearchUtxos as too broad; use UTxO-HD tables/tvar or parsed Dingo-style NDJSON for complete current UTxO bootstrap")
			}
			manifest := snapshot.BuildImportManifest(snapshot.ImportManifestOptions{
				Source:    "utxorpc",
				DolosDir:  *dolosDir,
				Endpoint:  *endpoint,
				Partial:   utxoRPCPartial || utxoRPCUnsupportedBroad,
				LiveReady: dolosReport != nil && dolosReport.ReadyForLiveImport,
				Warnings:  warnings,
			})
			if err := writeImportManifest(*writeManifest, manifest); err != nil {
				log.Fatalf("failed to write import manifest: %v", err)
			}
			fmt.Printf("Wrote snapshot import manifest: %s\n", *writeManifest)
		}
		return
	}

	var sqlDB *gorm.DB
	if *apply {
		sqlDB = openSnapshotCommandDB(*configPath, !*skipMigrations)
		defer closeDB(sqlDB)
	}

	importer := snapshot.NewUTxORPCImporter(sqlDB, snapshot.UTxORPCOptions{
		Endpoint:    *endpoint,
		Protocol:    *protocol,
		BatchSize:   int32(*batchSize),
		Limit:       *limit,
		Policy:      policy,
		AssetName:   assetName,
		Apply:       *apply,
		ReplaceUTxO: *replaceUTxO,
		Timeout:     *timeout,
	})
	stats, err := importer.Import(context.Background())
	if err != nil {
		log.Fatalf("snapshot import failed: %v", err)
	}

	printSnapshotStats(stats, *endpoint)
	if *apply && *refreshHolders {
		holderRows, err := snapshot.RefreshTokenHoldersFromCurrentUTxO(sqlDB, stats.LastTipSlot)
		if err != nil {
			log.Fatalf("failed to refresh token_holders from current UTxO snapshot: %v", err)
		}
		fmt.Printf("  token_holders_refreshed: %d\n", holderRows)
	}
	printSnapshotProductCoverage("utxorpc", *apply, *allowPartialApply || *limit > 0 || len(policy) > 0 || len(assetName) > 0)
	if *writeManifest != "" {
		manifest := snapshot.BuildImportManifest(snapshot.ImportManifestOptions{
			Source:    "utxorpc",
			DolosDir:  *dolosDir,
			Endpoint:  *endpoint,
			Apply:     *apply,
			Partial:   *allowPartialApply || *limit > 0 || len(policy) > 0 || len(assetName) > 0,
			LiveReady: true,
		})
		if err := writeImportManifest(*writeManifest, manifest); err != nil {
			log.Fatalf("failed to write import manifest: %v", err)
		}
		fmt.Printf("Wrote snapshot import manifest: %s\n", *writeManifest)
	}
	if !*apply {
		fmt.Println("Dry run only. Pass --apply to write SQL rows.")
	}
}

func openSnapshotCommandDB(configPath string, migrate bool) *gorm.DB {
	cfg, err := config.Load(configPath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatalf("failed to load configuration: %v", err)
		}
		cfg, err = config.Load("")
		if err != nil {
			log.Fatalf("failed to build default configuration: %v", err)
		}
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
	return db
}

func loadConfigForWrite(path string, dsn string) (*config.Config, error) {
	if dsn != "" {
		restore := setTemporaryEnv("NECTAR_DSN", dsn)
		defer restore()
	}
	if _, err := os.Stat(path); err == nil {
		return config.Load(path)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if dsn == "" {
		return nil, fmt.Errorf("base config %s does not exist and --mysql-dsn was not provided", path)
	}
	return config.Load("")
}

func setTemporaryEnv(key string, value string) func() {
	oldValue, hadOldValue := os.LookupEnv(key)
	os.Setenv(key, value)
	return func() {
		if hadOldValue {
			os.Setenv(key, oldValue)
			return
		}
		os.Unsetenv(key)
	}
}

func applyDolosReportToConfig(cfg *config.Config, report *bootstrap.DolosReport, driver string, profile string, dsn string) {
	cfg.Database.Driver = config.NormalizeDatabaseDriver(driver)
	if dsn != "" {
		cfg.Database.DSN = dsn
	}
	cfg.Cardano.NodeSocket = report.SocketPath
	cfg.Cardano.NetworkMagic = report.NetworkMagic
	cfg.Indexing.Profile = strings.ToLower(strings.TrimSpace(profile))
	if cfg.Indexing.Profile == "" {
		cfg.Indexing.Profile = config.IndexingProfileHoneycomb
	}
	if cfg.Indexing.Profile == config.IndexingProfileHoneycomb {
		cfg.Indexing.Transactions = true
		cfg.Indexing.Blocks = true
		cfg.Indexing.Metadata = true
		cfg.Indexing.Assets = true
		cfg.Indexing.Minting = true
		cfg.Indexing.UTXOs = false
		cfg.Indexing.Inputs = true
		cfg.Indexing.Outputs = true
		cfg.Indexing.Certificates = false
		cfg.Indexing.Governance = false
		cfg.Indexing.Withdrawals = false
		cfg.Indexing.Scripts = false
		cfg.Indexing.Collateral = false
		cfg.Indexing.WalletConnections = true
		cfg.StateQuery.Enabled = false
	}
}

func writeNectarConfig(path string, cfg *config.Config) error {
	data, err := config.Marshal(cfg)
	if err != nil {
		return err
	}
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, data, 0644)
}

func writeTextFile(path string, data []byte, perm os.FileMode) error {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, data, perm)
}

func writeImportManifest(path string, manifest snapshot.ImportManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeTextFile(path, data, 0644)
}

func printDolosReport(report *bootstrap.DolosReport) {
	fmt.Println("Dolos preflight")
	fmt.Printf("  dir: %s\n", report.Dir)
	fmt.Printf("  config: %s %s\n", readiness(report.ConfigReady), report.ConfigPath)
	fmt.Printf("  storage: %s %s\n", readiness(report.StorageReady), report.StoragePath)
	fmt.Printf("  archive: %s %s (%d segments, %.2f GiB)\n", readiness(report.ArchiveReady), report.ArchivePath, report.ArchiveSegments, float64(report.ArchiveBytes)/(1024*1024*1024))
	fmt.Printf("  state: %s %s\n", readiness(report.StateReady), report.StatePath)
	fmt.Printf("  socket: %s %s\n", readiness(report.SocketReady), report.SocketPath)
	fmt.Printf("  grpc: %s %s -> %s\n", readiness(report.GRPCConfigured), report.GRPCListenAddress, report.GRPCEndpoint)
	fmt.Printf("  mithril: %s %s\n", readiness(report.MithrilConfigured), report.MithrilAggregator)
	fmt.Printf("  network_magic: %d testnet=%t\n", report.NetworkMagic, report.IsTestnet)
	for _, missing := range report.Missing {
		fmt.Printf("  missing: %s\n", missing)
	}
	for _, warning := range report.Warnings {
		fmt.Printf("  warning: %s\n", warning)
	}
	fmt.Printf("  ready_for_config: %t\n", report.ReadyForConfig)
	fmt.Printf("  ready_for_live_import: %t\n", report.ReadyForLiveImport)
}

func printDatabaseReadiness(driver string, dsn string) {
	fmt.Println("Database preflight")
	normalizedDriver := config.NormalizeDatabaseDriver(driver)
	fmt.Printf("  driver: %s\n", normalizedDriver)
	if strings.TrimSpace(dsn) == "" {
		fmt.Println("  dsn: pending (provide --mysql-dsn or NECTAR_DSN before writing config)")
		return
	}
	if strings.HasPrefix(strings.TrimSpace(dsn), "mysql://") {
		fmt.Println("  dsn: warning backend-style mysql:// URL; indexer config expects go-sql-driver DSN")
		return
	}
	parsed, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		fmt.Printf("  dsn: warning parse failed: %v\n", err)
		return
	}
	if parsed.DBName == "" {
		fmt.Println("  dsn: warning missing database name")
		return
	}
	addr := parsed.Addr
	if addr == "" {
		addr = "localhost"
	}
	netName := parsed.Net
	if netName == "" {
		netName = "tcp"
	}
	fmt.Printf("  dsn: ok database=%s network=%s address=%s\n", parsed.DBName, netName, addr)
}

func printFilesystemSnapshotReport(report *snapshot.FilesystemSnapshotReport) {
	fmt.Println("Filesystem snapshot preflight")
	fmt.Printf("  root: %s\n", report.Root)
	fmt.Printf("  ledger_dir: %s\n", blankOrValue(report.LedgerDir))
	fmt.Printf("  state: %s %s slot=%d format=%s\n", readiness(report.StatePath != ""), blankOrValue(report.StatePath), report.StateSlot, blankOrValue(report.StateFormat))
	fmt.Printf("  utxo_table: %s %s slot=%d format=%s\n", readiness(report.UTxOTablePath != ""), blankOrValue(report.UTxOTablePath), report.UTxOTableSlot, blankOrValue(report.UTxOTableFormat))
	fmt.Printf("  dingo_compatible: %t\n", report.DingoCompatible)
	fmt.Printf("  direct_import_supported: %t\n", report.DirectImportSupported)
	fmt.Printf("  candidates: %d\n", len(report.Candidates))
	for _, missing := range report.Missing {
		fmt.Printf("  missing: %s\n", missing)
	}
	for _, warning := range report.Warnings {
		fmt.Printf("  warning: %s\n", warning)
	}
}

func printSnapshotStats(stats snapshot.UTxORPCStats, source string) {
	fmt.Println("Snapshot/current UTxO import")
	fmt.Printf("  source: %s\n", source)
	fmt.Printf("  applied: %t\n", stats.Applied)
	fmt.Printf("  pages: %d\n", stats.Pages)
	fmt.Printf("  utxos: %d\n", stats.UTxOs)
	fmt.Printf("  tx_outs: %d\n", stats.TxOuts)
	fmt.Printf("  ma_tx_outs: %d\n", stats.MaTxOuts)
	fmt.Printf("  multi_assets_seen: %d\n", stats.MultiAssets)
	fmt.Printf("  skipped: %d\n", stats.Skipped)
	if stats.LastTipSlot > 0 || stats.LastTipHashHex != "" {
		fmt.Printf("  ledger_tip: slot=%d hash=%s\n", stats.LastTipSlot, stats.LastTipHashHex)
	}
}

func printSnapshotProductCoverage(source string, apply bool, partial bool) {
	fmt.Println("Snapshot product coverage")
	fmt.Printf("  source: %s\n", source)
	switch source {
	case "utxorpc":
		fmt.Println("  token_holders: pending; local Dolos UTxO RPC rejects unfiltered SearchUtxos as too broad")
		fmt.Println("  allTokens/walletTokens/whale/top10: require complete tables/tvar or parsed NDJSON apply")
		fmt.Println("  utxorpc: use filtered policy/asset searches for targeted checks, not full chart bootstrap")
	case "ndjson", "tvar":
		fmt.Printf("  token_holders: %s current UTxO holder distribution for full unfiltered apply\n", coverageReadiness(apply && !partial))
		fmt.Println("  allTokens/walletTokens/whale/top10: covered by token_holders after full apply")
		if source == "ndjson" {
			fmt.Println("  dingo_bridge: parsed current UTxO NDJSON writes Nectar tables; direct ledger-state file decode is still separate")
		}
		if source == "tvar" {
			fmt.Println("  dingo_bridge: UTxO-HD tables/tvar writes Nectar tables; legacy ledger-state decode is still separate")
		}
	default:
		fmt.Println("  token_holders: pending; filesystem mode discovers Dingo-style files but does not decode SQL yet")
		fmt.Println("  allTokens/walletTokens/whale/top10: pending until UTxO rows are applied")
	}
	fmt.Println("  wallet_connections: historical ChainSync required; not produced from current UTxO snapshot")
	fmt.Println("  token_wallet_connections: historical ChainSync required; not produced from current UTxO snapshot")
	fmt.Println("  token_metadata: backend Koios/cache path; not fabricated by snapshot import")
	fmt.Println("  rewards: not populated or estimated by snapshot import")
}

func coverageReadiness(ok bool) string {
	if ok {
		return "ready"
	}
	return "pending"
}

func parseHexFlag(name string, value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("--%s must be hex: %w", name, err)
	}
	return decoded, nil
}

func filesystemManifestWarnings(report *snapshot.FilesystemSnapshotReport) []string {
	if report != nil && report.DirectImportSupported {
		return []string{"manifest-only/discovery did not open SQL; UTxO-HD tables/tvar can be applied with --apply"}
	}
	return []string{"filesystem apply requires UTxO-HD tables/tvar; legacy ledger-state decode is not implemented yet"}
}

func normalizeSnapshotCommandSource(source string) string {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "", "utxorpc":
		return "utxorpc"
	case "filesystem":
		return "filesystem"
	case "ndjson", "dingo-ndjson":
		return "ndjson"
	default:
		return ""
	}
}

func readiness(ok bool) string {
	if ok {
		return "ok"
	}
	return "pending"
}

func blankOrValue(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
