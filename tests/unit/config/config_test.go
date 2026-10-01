package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"nectar/config"
)

func clearConfigEnv(t *testing.T) {
	t.Helper()

	for _, key := range []string{
		"TIDB_DSN",
		"NECTAR_DSN",
		"NECTAR_DB_DRIVER",
		"DB_DRIVER",
		"CARDANO_NODE_SOCKET",
		"CARDANO_NETWORK_MAGIC",
		"DB_CONNECTION_POOL",
		"WORKER_COUNT",
		"BULK_MODE_ENABLED",
		"BULK_FETCH_RANGE_SIZE",
		"STATS_INTERVAL",
		"DASHBOARD_TYPE",
		"WEB_PORT",
		"METRICS_ENABLED",
		"METRICS_PORT",
		"LOG_LEVEL",
		"NECTAR_INDEXING_PROFILE",
	} {
		t.Setenv(key, "")
	}
}

func TestLoadRequiresDatabaseDSN(t *testing.T) {
	clearConfigEnv(t)

	_, err := config.Load("")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "database DSN is required")
}

func TestLoadAppliesCurrentEnvironmentOverrides(t *testing.T) {
	clearConfigEnv(t)

	t.Setenv("NECTAR_DSN", "root@tcp(localhost:4000)/nectar?parseTime=true")
	t.Setenv("NECTAR_DB_DRIVER", "tidb")
	t.Setenv("CARDANO_NODE_SOCKET", "/tmp/cardano-node.socket")
	t.Setenv("CARDANO_NETWORK_MAGIC", "2")
	t.Setenv("DB_CONNECTION_POOL", "12")
	t.Setenv("WORKER_COUNT", "6")
	t.Setenv("BULK_MODE_ENABLED", "true")
	t.Setenv("BULK_FETCH_RANGE_SIZE", "250")
	t.Setenv("STATS_INTERVAL", "5s")
	t.Setenv("DASHBOARD_TYPE", "none")
	t.Setenv("WEB_PORT", "8088")
	t.Setenv("METRICS_ENABLED", "false")
	t.Setenv("METRICS_PORT", "9101")
	t.Setenv("LOG_LEVEL", "debug")

	cfg, err := config.Load("")

	require.NoError(t, err)
	assert.Equal(t, "root@tcp(localhost:4000)/nectar?parseTime=true", cfg.Database.DSN)
	assert.Equal(t, "tidb", cfg.Database.Driver)
	assert.Equal(t, "/tmp/cardano-node.socket", cfg.Cardano.NodeSocket)
	assert.Equal(t, uint32(2), cfg.Cardano.NetworkMagic)
	assert.Equal(t, 12, cfg.Database.ConnectionPool)
	assert.Equal(t, 6, cfg.Performance.WorkerCount)
	assert.True(t, cfg.Performance.BulkModeEnabled)
	assert.Equal(t, 250, cfg.Performance.BulkFetchRangeSize)
	assert.Equal(t, 5*time.Second, cfg.Performance.StatsInterval)
	assert.False(t, cfg.Dashboard.Enabled)
	assert.Equal(t, "none", cfg.Dashboard.Type)
	assert.Equal(t, 8088, cfg.Dashboard.WebPort)
	assert.False(t, cfg.Monitoring.MetricsEnabled)
	assert.Equal(t, 9101, cfg.Monitoring.MetricsPort)
	assert.Equal(t, "debug", cfg.Monitoring.LogLevel)
}

func TestLoadReadsCurrentTOMLShape(t *testing.T) {
	clearConfigEnv(t)

	path := filepath.Join(t.TempDir(), "nectar.toml")
	data := strings.Join([]string{
		`version = "test"`,
		``,
		`[database]`,
		`driver = "mysql"`,
		`dsn = "root@tcp(127.0.0.1:4000)/nectar?parseTime=true"`,
		`connection_pool = 4`,
		``,
		`[cardano]`,
		`node_socket = "/tmp/node.socket"`,
		`network_magic = 1`,
		`protocol_mode = "n2c"`,
		``,
		`[performance]`,
		`worker_count = 2`,
		`bulk_fetch_range_size = 125`,
		``,
		`[dashboard]`,
		`enabled = true`,
		`type = "web"`,
		`web_port = 8181`,
		``,
		`[indexing]`,
		`profile = "honeycomb"`,
	}, "\n")
	require.NoError(t, os.WriteFile(path, []byte(data), 0o600))

	cfg, err := config.Load(path)

	require.NoError(t, err)
	assert.Equal(t, "test", cfg.Version)
	assert.Equal(t, "mysql", cfg.Database.Driver)
	assert.Equal(t, "root@tcp(127.0.0.1:4000)/nectar?parseTime=true", cfg.Database.DSN)
	assert.Equal(t, 4, cfg.Database.ConnectionPool)
	assert.Equal(t, "/tmp/node.socket", cfg.Cardano.NodeSocket)
	assert.Equal(t, uint32(1), cfg.Cardano.NetworkMagic)
	assert.Equal(t, "n2c", cfg.Cardano.ProtocolMode)
	assert.Equal(t, 2, cfg.Performance.WorkerCount)
	assert.Equal(t, 125, cfg.Performance.BulkFetchRangeSize)
	assert.Equal(t, "web", cfg.Dashboard.Type)
	assert.Equal(t, 8181, cfg.Dashboard.WebPort)
	assert.Equal(t, "honeycomb", cfg.Indexing.Profile)
	assert.True(t, cfg.Indexing.Blocks)
	assert.True(t, cfg.Indexing.Inputs)
	assert.True(t, cfg.Indexing.Outputs)
	assert.True(t, cfg.Indexing.Assets)
	assert.True(t, cfg.Indexing.WalletConnections)
	assert.False(t, cfg.Indexing.Scripts)
	assert.False(t, cfg.Indexing.Collateral)
	assert.False(t, cfg.StateQuery.Enabled)
}

func TestValidationRejectsCurrentInvalidFields(t *testing.T) {
	clearConfigEnv(t)

	path := filepath.Join(t.TempDir(), "nectar.toml")
	data := strings.Join([]string{
		`[database]`,
		`dsn = "root@tcp(localhost:4000)/nectar?parseTime=true"`,
		`connection_pool = -1`,
		``,
		`[cardano]`,
		`node_socket = "/tmp/node.socket"`,
		``,
		`[performance]`,
		`worker_count = 1`,
	}, "\n")
	require.NoError(t, os.WriteFile(path, []byte(data), 0o600))

	_, err := config.Load(path)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "database connection pool must be positive")
}

func TestValidationRejectsInvalidDriver(t *testing.T) {
	clearConfigEnv(t)

	path := filepath.Join(t.TempDir(), "nectar.toml")
	data := strings.Join([]string{
		`[database]`,
		`driver = "postgres"`,
		`dsn = "root@tcp(localhost:4000)/nectar?parseTime=true"`,
		`connection_pool = 1`,
		``,
		`[cardano]`,
		`node_socket = "/tmp/node.socket"`,
		``,
		`[performance]`,
		`worker_count = 1`,
	}, "\n")
	require.NoError(t, os.WriteFile(path, []byte(data), 0o600))

	_, err := config.Load(path)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "database driver")
}

func TestHoneycombProfileForcesProductIndexingFootprint(t *testing.T) {
	clearConfigEnv(t)

	path := filepath.Join(t.TempDir(), "nectar.toml")
	data := strings.Join([]string{
		`[database]`,
		`dsn = "root@tcp(localhost:4000)/nectar?parseTime=true"`,
		`connection_pool = 1`,
		``,
		`[cardano]`,
		`node_socket = "/tmp/node.socket"`,
		``,
		`[performance]`,
		`worker_count = 1`,
		``,
		`[state_query]`,
		`enabled = true`,
		``,
		`[indexing]`,
		`profile = "honeycomb"`,
		`scripts = true`,
		`collateral = true`,
		`wallet_connections = false`,
	}, "\n")
	require.NoError(t, os.WriteFile(path, []byte(data), 0o600))

	cfg, err := config.Load(path)

	require.NoError(t, err)
	assert.Equal(t, "mysql", cfg.Database.Driver)
	assert.True(t, cfg.Indexing.Transactions)
	assert.True(t, cfg.Indexing.Blocks)
	assert.True(t, cfg.Indexing.Metadata)
	assert.True(t, cfg.Indexing.Assets)
	assert.True(t, cfg.Indexing.Minting)
	assert.True(t, cfg.Indexing.Inputs)
	assert.True(t, cfg.Indexing.Outputs)
	assert.True(t, cfg.Indexing.WalletConnections)
	assert.False(t, cfg.Indexing.UTXOs)
	assert.False(t, cfg.Indexing.Certificates)
	assert.False(t, cfg.Indexing.Governance)
	assert.False(t, cfg.Indexing.Withdrawals)
	assert.False(t, cfg.Indexing.Scripts)
	assert.False(t, cfg.Indexing.Collateral)
	assert.False(t, cfg.StateQuery.Enabled)
}

func TestPerformanceGetActiveConfigUsesLegacyFieldsAndMode(t *testing.T) {
	perf := config.PerformanceConfig{
		WorkerCount:        8,
		BulkFetchRangeSize: 2000,
		BlockQueueSize:     10000,
		OptimizationMode:   "manual",
	}

	active := perf.GetActiveConfig(300)

	require.NotNil(t, active)
	assert.Equal(t, "manual", active.Source)
	assert.Equal(t, 8, active.Workers)
	assert.Equal(t, 2000, active.BatchSize)
	assert.Equal(t, 2000, active.FetchRange)
	assert.Equal(t, 10000, active.QueueSize)
}

func TestMarshalEmitsCurrentTopLevelSections(t *testing.T) {
	cfg := &config.Config{
		Version: "test",
		Database: config.DatabaseConfig{
			Driver:         "mysql",
			DSN:            "root@tcp(localhost:4000)/nectar?parseTime=true",
			ConnectionPool: 1,
		},
		Cardano: config.CardanoConfig{
			NodeSocket: "/tmp/node.socket",
		},
		Performance: config.PerformanceConfig{
			WorkerCount: 1,
		},
	}

	data, err := config.Marshal(cfg)

	require.NoError(t, err)
	text := string(data)
	assert.Contains(t, text, "[database]")
	assert.Contains(t, text, "driver = ")
	assert.Contains(t, text, "dsn = ")
	assert.Contains(t, text, "[cardano]")
	assert.Contains(t, text, "[performance]")
}
