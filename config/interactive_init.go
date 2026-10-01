package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// InteractiveInit runs an interactive setup wizard for Nectar configuration
func InteractiveInit(configPath string) error {
	fmt.Println("🚀 Welcome to Nectar Setup Wizard")
	fmt.Println("=================================")
	fmt.Println()
	fmt.Println("This wizard will help you create a configuration file for Nectar.")
	fmt.Println("Press Enter to use the [default] values shown.")
	fmt.Println()

	reader := bufio.NewReader(os.Stdin)

	// Check if config already exists
	if _, err := os.Stat(configPath); err == nil {
		fmt.Printf("⚠️  Configuration file already exists at: %s\n", configPath)
		fmt.Print("Do you want to overwrite it? (y/N): ")
		response, _ := reader.ReadString('\n')
		response = strings.TrimSpace(strings.ToLower(response))
		if response != "y" && response != "yes" {
			fmt.Println("Setup cancelled.")
			return nil
		}
	}

	cfg := &Config{
		Version: "1.0",
	}

	// Network Selection
	fmt.Println("\n📡 Network Configuration")
	fmt.Println("------------------------")
	fmt.Println("Select Cardano network:")
	fmt.Println("  1) Mainnet (default)")
	fmt.Println("  2) Preprod")
	fmt.Println("  3) Preview")
	fmt.Println("  4) Custom")
	fmt.Print("Choice [1]: ")

	networkChoice, _ := reader.ReadString('\n')
	networkChoice = strings.TrimSpace(networkChoice)
	if networkChoice == "" {
		networkChoice = "1"
	}

	switch networkChoice {
	case "1":
		cfg.Cardano.NetworkMagic = 764824073
		cfg.Cardano.NodeSocket = "/opt/cardano/cnode/sockets/node.socket"
		fmt.Println("✓ Selected: Mainnet")
	case "2":
		cfg.Cardano.NetworkMagic = 1
		cfg.Cardano.NodeSocket = "/opt/cardano/preprod/sockets/node.socket"
		fmt.Println("✓ Selected: Preprod")
	case "3":
		cfg.Cardano.NetworkMagic = 2
		cfg.Cardano.NodeSocket = "/opt/cardano/preview/sockets/node.socket"
		fmt.Println("✓ Selected: Preview")
	case "4":
		fmt.Print("Enter network magic number: ")
		magicStr, _ := reader.ReadString('\n')
		magic, err := strconv.ParseUint(strings.TrimSpace(magicStr), 10, 32)
		if err != nil {
			return fmt.Errorf("invalid network magic: %w", err)
		}
		cfg.Cardano.NetworkMagic = uint32(magic)
	default:
		cfg.Cardano.NetworkMagic = 764824073
		cfg.Cardano.NodeSocket = "/opt/cardano/cnode/sockets/node.socket"
	}

	// Node Socket Path
	fmt.Printf("\nCardano node socket path [%s]: ", cfg.Cardano.NodeSocket)
	socketPath, _ := reader.ReadString('\n')
	socketPath = strings.TrimSpace(socketPath)
	if socketPath != "" {
		cfg.Cardano.NodeSocket = socketPath
	}

	// Database Configuration
	fmt.Println("\n💾 Database Configuration")
	fmt.Println("------------------------")
	fmt.Println("Enter your MySQL-compatible database connection details.")

	fmt.Print("Database driver [mysql]: ")
	dbDriver, _ := reader.ReadString('\n')
	dbDriver = strings.TrimSpace(strings.ToLower(dbDriver))
	if dbDriver == "" {
		dbDriver = DatabaseDriverMySQL
	}
	if dbDriver != DatabaseDriverMySQL && dbDriver != DatabaseDriverTiDB {
		return fmt.Errorf("invalid database driver: %s", dbDriver)
	}
	cfg.Database.Driver = dbDriver

	fmt.Print("Database host [localhost]: ")
	dbHost, _ := reader.ReadString('\n')
	dbHost = strings.TrimSpace(dbHost)
	if dbHost == "" {
		dbHost = "localhost"
	}

	defaultPort := "3306"
	if cfg.Database.Driver == DatabaseDriverTiDB {
		defaultPort = "4000"
	}
	fmt.Printf("Database port [%s]: ", defaultPort)
	dbPort, _ := reader.ReadString('\n')
	dbPort = strings.TrimSpace(dbPort)
	if dbPort == "" {
		dbPort = defaultPort
	}

	fmt.Print("Database name [nectar]: ")
	dbName, _ := reader.ReadString('\n')
	dbName = strings.TrimSpace(dbName)
	if dbName == "" {
		dbName = "nectar"
	}

	fmt.Print("Database user [root]: ")
	dbUser, _ := reader.ReadString('\n')
	dbUser = strings.TrimSpace(dbUser)
	if dbUser == "" {
		dbUser = "root"
	}

	fmt.Print("Database password (will be stored in config): ")
	dbPass, _ := reader.ReadString('\n')
	dbPass = strings.TrimSpace(dbPass)

	// Build DSN
	cfg.Database.DSN = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local&timeout=300s&readTimeout=300s&writeTimeout=300s",
		dbUser, dbPass, dbHost, dbPort, dbName)

	// Performance Configuration
	fmt.Println("\n⚡ Performance Configuration")
	fmt.Println("---------------------------")

	fmt.Print("Number of worker threads [8]: ")
	workersStr, _ := reader.ReadString('\n')
	workersStr = strings.TrimSpace(workersStr)
	if workersStr == "" {
		cfg.Performance.WorkerCount = 8
	} else {
		workers, err := strconv.Atoi(workersStr)
		if err != nil || workers < 1 {
			cfg.Performance.WorkerCount = 8
		} else {
			cfg.Performance.WorkerCount = workers
		}
	}

	fmt.Print("Database connection pool size [8]: ")
	poolStr, _ := reader.ReadString('\n')
	poolStr = strings.TrimSpace(poolStr)
	if poolStr == "" {
		cfg.Database.ConnectionPool = 8
	} else {
		pool, err := strconv.Atoi(poolStr)
		if err != nil || pool < 1 {
			cfg.Database.ConnectionPool = 8
		} else {
			cfg.Database.ConnectionPool = pool
		}
	}

	// Dashboard Configuration
	fmt.Println("\n📊 Dashboard Configuration")
	fmt.Println("-------------------------")
	fmt.Println("Select dashboard type:")
	fmt.Println("  1) Terminal (default)")
	fmt.Println("  2) Web")
	fmt.Println("  3) Both")
	fmt.Println("  4) None")
	fmt.Print("Choice [1]: ")

	dashChoice, _ := reader.ReadString('\n')
	dashChoice = strings.TrimSpace(dashChoice)
	if dashChoice == "" {
		dashChoice = "1"
	}

	cfg.Dashboard.Enabled = true
	switch dashChoice {
	case "1":
		cfg.Dashboard.Type = "terminal"
	case "2":
		cfg.Dashboard.Type = "web"
		fmt.Print("Web dashboard port [8080]: ")
		portStr, _ := reader.ReadString('\n')
		portStr = strings.TrimSpace(portStr)
		if portStr == "" {
			cfg.Dashboard.WebPort = 8080
		} else {
			port, err := strconv.Atoi(portStr)
			if err != nil || port < 1 {
				cfg.Dashboard.WebPort = 8080
			} else {
				cfg.Dashboard.WebPort = port
			}
		}
	case "3":
		cfg.Dashboard.Type = "both"
		fmt.Print("Web dashboard port [8080]: ")
		portStr, _ := reader.ReadString('\n')
		portStr = strings.TrimSpace(portStr)
		if portStr == "" {
			cfg.Dashboard.WebPort = 8080
		} else {
			port, err := strconv.Atoi(portStr)
			if err != nil || port < 1 {
				cfg.Dashboard.WebPort = 8080
			} else {
				cfg.Dashboard.WebPort = port
			}
		}
	case "4":
		cfg.Dashboard.Type = "none"
		cfg.Dashboard.Enabled = false
	default:
		cfg.Dashboard.Type = "terminal"
	}

	// Set remaining defaults
	cfg.Database.MaxIdleConns = 4
	cfg.Database.MaxOpenConns = cfg.Database.ConnectionPool
	cfg.Database.ConnMaxLifetime = 3600000000000 // 1 hour in nanoseconds

	cfg.Cardano.ProtocolMode = "auto"
	cfg.Cardano.Rewards = RewardsConfig{
		TreasuryTax:       0.20,
		MonetaryExpansion: 0.003,
		OptimalPoolCount:  500,
	}

	cfg.Performance.BulkModeEnabled = false
	cfg.Performance.BulkFetchRangeSize = 2000
	cfg.Performance.StatsInterval = 3000000000 // 3s in nanoseconds
	cfg.Performance.BlockQueueSize = 10000

	cfg.Dashboard.DetailedLog = false

	cfg.Monitoring = MonitoringConfig{
		MetricsEnabled: true,
		MetricsPort:    9090,
		LogLevel:       "info",
		LogFormat:      "json",
	}

	cfg.Metadata = MetadataConfig{
		Enabled:     true,
		WorkerCount: 4,
		QueueSize:   1000,
		RateLimit:   10,
		MaxRetries:  3,
		UserAgent:   "Nectar/1.0",
	}

	cfg.StateQuery = StateQueryConfig{
		Enabled:    false,
		SocketPath: "", // Uses cardano.node_socket
	}

	cfg.Indexing = IndexingConfig{
		Profile:           IndexingProfileHoneycomb,
		Transactions:      true,
		Blocks:            true,
		Metadata:          true,
		Assets:            true,
		Minting:           true,
		UTXOs:             false,
		Inputs:            true,
		Outputs:           true,
		Certificates:      false,
		Governance:        false,
		Withdrawals:       false,
		Scripts:           false,
		Collateral:        false,
		WalletConnections: true,
	}

	// Generate config file
	fmt.Println("\n📝 Generating Configuration")
	fmt.Println("--------------------------")

	// Ensure directory exists
	dir := filepath.Dir(configPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	// Marshal to TOML
	data, err := Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to generate config: %w", err)
	}

	// Write file
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	fmt.Printf("\n✅ Configuration saved to: %s\n", configPath)

	// Show summary
	fmt.Println("\n📋 Configuration Summary")
	fmt.Println("------------------------")
	fmt.Printf("Network:        %s (magic: %d)\n", getNetworkName(cfg.Cardano.NetworkMagic), cfg.Cardano.NetworkMagic)
	fmt.Printf("Node Socket:    %s\n", cfg.Cardano.NodeSocket)
	fmt.Printf("Database:       %s@%s/%s\n", dbUser, dbHost, dbName)
	fmt.Printf("Workers:        %d\n", cfg.Performance.WorkerCount)
	fmt.Printf("Dashboard:      %s\n", cfg.Dashboard.Type)
	if cfg.Dashboard.Type == "web" || cfg.Dashboard.Type == "both" {
		fmt.Printf("Web Port:       %d\n", cfg.Dashboard.WebPort)
	}

	fmt.Println("\n🎉 Setup complete! You can now run:")
	fmt.Printf("   nectar --config %s\n", configPath)
	fmt.Println()

	return nil
}

func getNetworkName(magic uint32) string {
	switch magic {
	case 764824073:
		return "Mainnet"
	case 1:
		return "Preprod"
	case 2:
		return "Preview"
	default:
		return "Custom"
	}
}
