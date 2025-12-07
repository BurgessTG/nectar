package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/pelletier/go-toml/v2"
)

// Config represents the complete Nectar configuration
type Config struct {
	Version     string            `toml:"version"`
	Database    DatabaseConfig    `toml:"database"`
	Cardano     CardanoConfig     `toml:"cardano"`
	Performance PerformanceConfig `toml:"performance"`
	Dashboard   DashboardConfig   `toml:"dashboard"`
	Monitoring  MonitoringConfig  `toml:"monitoring"`
	Metadata    MetadataConfig    `toml:"metadata"`
	StateQuery  StateQueryConfig  `toml:"state_query"`
	Auth        AuthConfig        `toml:"auth"`
	Indexing    IndexingConfig    `toml:"indexing"`
}

// DatabaseConfig holds database connection settings
type DatabaseConfig struct {
	DSN             string        `toml:"dsn"`
	ConnectionPool  int           `toml:"connection_pool"`
	MaxIdleConns    int           `toml:"max_idle_conns"`
	MaxOpenConns    int           `toml:"max_open_conns"`
	ConnMaxLifetime time.Duration `toml:"conn_max_lifetime"`
}

// CardanoConfig holds Cardano node settings
type CardanoConfig struct {
	NodeSocket   string        `toml:"node_socket"`
	NetworkMagic uint32        `toml:"network_magic"`
	ProtocolMode string        `toml:"protocol_mode"`
	Rewards      RewardsConfig `toml:"rewards"`
}

// RewardsConfig holds reward calculation parameters
type RewardsConfig struct {
	TreasuryTax       float64 `toml:"treasury_tax"`
	MonetaryExpansion float64 `toml:"monetary_expansion"`
	OptimalPoolCount  int     `toml:"optimal_pool_count"`
}

// PerformanceConfig holds performance tuning settings
type PerformanceConfig struct {
	// System Capabilities (maximums)
	MaxWorkers      int `toml:"max_workers"`
	MaxConnections  int `toml:"max_connections"`
	MaxBatchSize    int `toml:"max_batch_size"`
	MaxQueueSize    int `toml:"max_queue_size"`
	
	// Optimization Strategy
	OptimizationMode string `toml:"optimization_mode"` // "auto", "manual", "aggressive", "conservative"
	
	// Manual Settings (only used when optimization_mode = "manual")
	ManualWorkers    int `toml:"manual_workers"`
	ManualBatchSize  int `toml:"manual_batch_size"`
	ManualQueueSize  int `toml:"manual_queue_size"`
	ManualFetchRange int `toml:"manual_fetch_range"`
	
	// Other settings
	BulkModeEnabled bool          `toml:"bulk_mode_enabled"`
	StatsInterval   time.Duration `toml:"stats_interval"`
	BlockfetchTimeout time.Duration `toml:"blockfetch_timeout"`
	
	// Legacy support (will be migrated)
	WorkerCount        int `toml:"worker_count"`
	BulkFetchRangeSize int `toml:"bulk_fetch_range_size"`
	BlockQueueSize     int `toml:"block_queue_size"`
}

// DashboardConfig holds dashboard settings
type DashboardConfig struct {
	Enabled     bool   `toml:"enabled"`
	Type        string `toml:"type"`
	WebPort     int    `toml:"web_port"`
	DetailedLog bool   `toml:"detailed_log"`
}

// MonitoringConfig holds monitoring and logging settings
type MonitoringConfig struct {
	MetricsEnabled bool   `toml:"metrics_enabled"`
	MetricsPort    int    `toml:"metrics_port"`
	LogLevel       string `toml:"log_level"`
	LogFormat      string `toml:"log_format"`
}

// MetadataConfig holds metadata fetcher settings
type MetadataConfig struct {
	Enabled     bool   `toml:"enabled"`
	WorkerCount int    `toml:"worker_count"`
	QueueSize   int    `toml:"queue_size"`
	RateLimit   int    `toml:"rate_limit"`
	MaxRetries  int    `toml:"max_retries"`
	UserAgent   string `toml:"user_agent"`
}

// StateQueryConfig holds state query service settings
type StateQueryConfig struct {
	Enabled    bool   `toml:"enabled"`
	SocketPath string `toml:"socket_path"`
}

// AuthConfig holds authentication settings
type AuthConfig struct {
	Enabled  bool   `toml:"enabled"`
	Username string `toml:"username"`
	Password string `toml:"password"`
	Secret   string `toml:"secret"`
}

// IndexingConfig controls what blockchain data to index (selective indexing)
// By default, all components are enabled for full indexing.
// Set individual options to false to skip specific data types.
type IndexingConfig struct {
	// Core components (always required)
	Transactions bool `toml:"transactions"`
	Blocks       bool `toml:"blocks"`

	// Token/Asset indexing
	Metadata bool `toml:"metadata"`
	Assets   bool `toml:"assets"`
	Minting  bool `toml:"minting"`

	// UTxO set tracking
	UTXOs   bool `toml:"utxos"`
	Inputs  bool `toml:"inputs"`
	Outputs bool `toml:"outputs"`

	// Staking and governance
	Certificates bool `toml:"certificates"`
	Governance   bool `toml:"governance"`
	Withdrawals  bool `toml:"withdrawals"`

	// Smart contracts
	Scripts    bool `toml:"scripts"`
	Collateral bool `toml:"collateral"`

	// Wallet relationship tracking
	WalletConnections bool `toml:"wallet_connections"`

	// Metadata filtering (empty = index all labels)
	MetadataLabels []uint64 `toml:"metadata_labels"`
}

// Load loads configuration from TOML file with environment variable overrides
func Load(path string) (*Config, error) {
	// Default configuration
	cfg := &Config{
		Version: "1.0",
		Database: DatabaseConfig{
			ConnectionPool:  8,
			MaxIdleConns:    4,
			MaxOpenConns:    8,
			ConnMaxLifetime: time.Hour,
		},
		Cardano: CardanoConfig{
			NodeSocket:   "/opt/cardano/cnode/sockets/node.socket",
			NetworkMagic: 764824073, // Mainnet
			ProtocolMode: "auto",
			Rewards: RewardsConfig{
				TreasuryTax:       0.20,
				MonetaryExpansion: 0.003,
				OptimalPoolCount:  500,
			},
		},
		Performance: PerformanceConfig{
			WorkerCount:        8,
			BulkModeEnabled:    false,
			BulkFetchRangeSize: 2000,
			StatsInterval:      3 * time.Second,
			BlockQueueSize:     10000,
		},
		Dashboard: DashboardConfig{
			Enabled:     true,
			Type:        "terminal",
			WebPort:     8080,
			DetailedLog: false,
		},
		Monitoring: MonitoringConfig{
			MetricsEnabled: true,
			MetricsPort:    9090,
			LogLevel:       "info",
			LogFormat:      "json",
		},
		Metadata: MetadataConfig{
			Enabled:     true,
			WorkerCount: 4,
			QueueSize:   1000,
			RateLimit:   10,
			MaxRetries:  3,
			UserAgent:   "Nectar/1.0",
		},
		StateQuery: StateQueryConfig{
			Enabled:    true,
			SocketPath: "", // Uses cardano.node_socket if empty
		},
		Auth: AuthConfig{
			Enabled:  false,
			Username: "admin",
			Password: "admin",
			Secret:   "nectar-secret-key",
		},
		Indexing: IndexingConfig{
			// Full indexing enabled by default
			// Users can disable specific components in nectar.toml
			Transactions: true,
			Blocks:       true,

			// Token/Asset indexing
			Metadata: true,
			Assets:   true,
			Minting:  true,

			// UTxO set tracking
			UTXOs:   true,
			Inputs:  true,
			Outputs: true,

			// Staking and governance
			Certificates: true,
			Governance:   true,
			Withdrawals:  true,

			// Smart contracts
			Scripts:    true,
			Collateral: true,

			// Wallet relationship tracking (optional, can be resource intensive)
			WalletConnections: false,

			// Index all metadata labels by default (empty = all)
			MetadataLabels: []uint64{},
		},
	}

	// Load from file if it exists
	if path != "" {
		if _, err := os.Stat(path); err == nil {
			data, err := os.ReadFile(path)
			if err != nil {
				return nil, fmt.Errorf("failed to read config file: %w", err)
			}

			if err := toml.Unmarshal(data, cfg); err != nil {
				return nil, fmt.Errorf("failed to parse config file: %w", err)
			}
		}
	}

	// Apply environment variable overrides
	cfg.applyEnvOverrides()

	// Validate configuration
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// applyEnvOverrides applies environment variable overrides to the configuration
func (c *Config) applyEnvOverrides() {
	// Database overrides
	if dsn := os.Getenv("TIDB_DSN"); dsn != "" {
		c.Database.DSN = dsn
	}
	if dsn := os.Getenv("NECTAR_DSN"); dsn != "" {
		c.Database.DSN = dsn
	}
	if pool := getEnvInt("DB_CONNECTION_POOL"); pool > 0 {
		c.Database.ConnectionPool = pool
	}

	// Cardano overrides
	if socket := os.Getenv("CARDANO_NODE_SOCKET"); socket != "" {
		c.Cardano.NodeSocket = socket
	}
	if magic := getEnvUint32("CARDANO_NETWORK_MAGIC"); magic > 0 {
		c.Cardano.NetworkMagic = magic
	}

	// Performance overrides
	if workers := getEnvInt("WORKER_COUNT"); workers > 0 {
		c.Performance.WorkerCount = workers
	}
	if bulk := getEnvBool("BULK_MODE_ENABLED"); bulk != nil {
		c.Performance.BulkModeEnabled = *bulk
	}
	if size := getEnvInt("BULK_FETCH_RANGE_SIZE"); size > 0 {
		c.Performance.BulkFetchRangeSize = size
	}
	if interval := getEnvDuration("STATS_INTERVAL"); interval > 0 {
		c.Performance.StatsInterval = interval
	}


	// Dashboard overrides
	if dashType := os.Getenv("DASHBOARD_TYPE"); dashType != "" {
		c.Dashboard.Type = dashType
		if dashType == "none" {
			c.Dashboard.Enabled = false
		}
	}
	if port := getEnvInt("WEB_PORT"); port > 0 {
		c.Dashboard.WebPort = port
	}

	// Monitoring overrides
	if enabled := getEnvBool("METRICS_ENABLED"); enabled != nil {
		c.Monitoring.MetricsEnabled = *enabled
	}
	if port := getEnvInt("METRICS_PORT"); port > 0 {
		c.Monitoring.MetricsPort = port
	}
	if level := os.Getenv("LOG_LEVEL"); level != "" {
		c.Monitoring.LogLevel = level
	}
}

// validate checks if the configuration is valid
func (c *Config) validate() error {
	if c.Database.DSN == "" {
		return fmt.Errorf("database DSN is required")
	}
	if c.Database.ConnectionPool <= 0 {
		return fmt.Errorf("database connection pool must be positive")
	}
	if c.Cardano.NodeSocket == "" {
		return fmt.Errorf("cardano node socket is required")
	}
	if c.Performance.WorkerCount <= 0 {
		return fmt.Errorf("worker count must be positive")
	}
	return nil
}

// Helper functions for environment variable parsing
func getEnvInt(key string) int {
	if val := os.Getenv(key); val != "" {
		i, _ := strconv.Atoi(val)
		return i
	}
	return 0
}

func getEnvUint32(key string) uint32 {
	if val := os.Getenv(key); val != "" {
		i, _ := strconv.ParseUint(val, 10, 32)
		return uint32(i)
	}
	return 0
}

func getEnvBool(key string) *bool {
	if val := os.Getenv(key); val != "" {
		b, _ := strconv.ParseBool(val)
		return &b
	}
	return nil
}

func getEnvDuration(key string) time.Duration {
	if val := os.Getenv(key); val != "" {
		d, _ := time.ParseDuration(val)
		return d
	}
	return 0
}

// Marshal converts a Config struct to TOML bytes
func Marshal(cfg *Config) ([]byte, error) {
	return toml.Marshal(cfg)
}

// ActivePerformanceConfig represents the currently active settings
type ActivePerformanceConfig struct {
	Workers      int
	Connections  int
	BatchSize    int
	QueueSize    int
	FetchRange   int
	Source       string // "auto (Byron)", "manual", etc.
}

// GetActiveConfig returns the active configuration based on mode and epoch
func (pc *PerformanceConfig) GetActiveConfig(epochNo uint64) *ActivePerformanceConfig {
	// Handle legacy migration first
	pc.migrateLegacyConfig()
	
	switch pc.OptimizationMode {
	case "manual":
		return pc.getManualConfig()
	case "aggressive":
		return pc.getAggressiveConfig()
	case "conservative":
		return pc.getConservativeConfig()
	case "auto", "":
		return pc.getEraAwareConfig(epochNo)
	default:
		// Default to auto if unknown mode
		return pc.getEraAwareConfig(epochNo)
	}
}

// migrateLegacyConfig migrates old config values to new structure
func (pc *PerformanceConfig) migrateLegacyConfig() {
	// If new values not set but legacy values exist, migrate them
	if pc.MaxWorkers == 0 && pc.WorkerCount > 0 {
		pc.MaxWorkers = pc.WorkerCount
		pc.ManualWorkers = pc.WorkerCount
	}
	if pc.MaxQueueSize == 0 && pc.BlockQueueSize > 0 {
		pc.MaxQueueSize = pc.BlockQueueSize
		pc.ManualQueueSize = pc.BlockQueueSize
	}
	if pc.MaxBatchSize == 0 && pc.BulkFetchRangeSize > 0 {
		pc.MaxBatchSize = pc.BulkFetchRangeSize * 2
		pc.ManualBatchSize = pc.BulkFetchRangeSize
		pc.ManualFetchRange = pc.BulkFetchRangeSize
	}
	
	// Set defaults if still not set
	if pc.MaxWorkers == 0 {
		pc.MaxWorkers = 16
	}
	if pc.MaxConnections == 0 {
		pc.MaxConnections = pc.MaxWorkers * 4
	}
	if pc.MaxBatchSize == 0 {
		pc.MaxBatchSize = 5000
	}
	if pc.MaxQueueSize == 0 {
		pc.MaxQueueSize = 100000
	}
	if pc.OptimizationMode == "" {
		pc.OptimizationMode = "auto"
	}
}

// getEraAwareConfig returns era-optimized settings within system limits
func (pc *PerformanceConfig) getEraAwareConfig(epochNo uint64) *ActivePerformanceConfig {
	// Import era config logic
	var workers, batchSize, queueSize, fetchRange int
	var eraName string
	
	// Determine era-specific values
	switch {
	case epochNo < 208: // Byron
		workers = 16
		batchSize = 2000
		queueSize = 100000
		fetchRange = 5000
		eraName = "Byron"
	case epochNo < 236: // Shelley
		workers = 12
		batchSize = 1000
		queueSize = 50000
		fetchRange = 2000
		eraName = "Shelley"
	case epochNo < 290: // Allegra/Mary
		workers = 10
		batchSize = 800
		queueSize = 30000
		fetchRange = 1500
		eraName = "Allegra/Mary"
	case epochNo < 365: // Alonzo
		workers = 8
		batchSize = 500
		queueSize = 20000
		fetchRange = 1000
		eraName = "Alonzo"
	default: // Babbage+
		workers = 6
		batchSize = 300
		queueSize = 10000
		fetchRange = 500
		eraName = "Babbage"
	}
	
	// Apply system limits
	workers = min(workers, pc.MaxWorkers)
	batchSize = min(batchSize, pc.MaxBatchSize)
	queueSize = min(queueSize, pc.MaxQueueSize)
	
	return &ActivePerformanceConfig{
		Workers:     workers,
		Connections: min(workers * 4, pc.MaxConnections),
		BatchSize:   batchSize,
		QueueSize:   queueSize,
		FetchRange:  fetchRange,
		Source:      fmt.Sprintf("auto (%s era)", eraName),
	}
}

// getManualConfig returns user-specified manual settings
func (pc *PerformanceConfig) getManualConfig() *ActivePerformanceConfig {
	return &ActivePerformanceConfig{
		Workers:     min(pc.ManualWorkers, pc.MaxWorkers),
		Connections: min(pc.ManualWorkers * 4, pc.MaxConnections),
		BatchSize:   min(pc.ManualBatchSize, pc.MaxBatchSize),
		QueueSize:   min(pc.ManualQueueSize, pc.MaxQueueSize),
		FetchRange:  pc.ManualFetchRange,
		Source:      "manual",
	}
}

// getAggressiveConfig returns maximum performance settings
func (pc *PerformanceConfig) getAggressiveConfig() *ActivePerformanceConfig {
	return &ActivePerformanceConfig{
		Workers:     pc.MaxWorkers,
		Connections: pc.MaxConnections,
		BatchSize:   pc.MaxBatchSize,
		QueueSize:   pc.MaxQueueSize,
		FetchRange:  pc.MaxBatchSize,
		Source:      "aggressive",
	}
}

// getConservativeConfig returns safe, stable settings
func (pc *PerformanceConfig) getConservativeConfig() *ActivePerformanceConfig {
	return &ActivePerformanceConfig{
		Workers:     max(4, pc.MaxWorkers / 4),
		Connections: max(16, pc.MaxConnections / 4),
		BatchSize:   max(100, pc.MaxBatchSize / 10),
		QueueSize:   max(1000, pc.MaxQueueSize / 10),
		FetchRange:  500,
		Source:      "conservative",
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}