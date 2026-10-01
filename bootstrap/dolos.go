package bootstrap

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

type DolosConfig struct {
	Storage struct {
		Version string `toml:"version"`
		Path    string `toml:"path"`
	} `toml:"storage"`
	Genesis struct {
		ByronPath   string `toml:"byron_path"`
		ShelleyPath string `toml:"shelley_path"`
		AlonzoPath  string `toml:"alonzo_path"`
		ConwayPath  string `toml:"conway_path"`
	} `toml:"genesis"`
	Serve struct {
		Ouroboros struct {
			ListenPath string `toml:"listen_path"`
		} `toml:"ouroboros"`
		GRPC struct {
			ListenAddress string `toml:"listen_address"`
		} `toml:"grpc"`
	} `toml:"serve"`
	Mithril struct {
		Aggregator string `toml:"aggregator"`
		GenesisKey string `toml:"genesis_key"`
	} `toml:"mithril"`
	Chain struct {
		Type      string `toml:"type"`
		Magic     uint32 `toml:"magic"`
		IsTestnet bool   `toml:"is_testnet"`
	} `toml:"chain"`
}

type DolosReport struct {
	Dir                string
	ConfigPath         string
	StoragePath        string
	ArchivePath        string
	StatePath          string
	SocketPath         string
	GRPCListenAddress  string
	GRPCEndpoint       string
	MithrilAggregator  string
	NetworkMagic       uint32
	IsTestnet          bool
	ArchiveSegments    int
	ArchiveBytes       int64
	ConfigReady        bool
	StorageReady       bool
	ArchiveReady       bool
	StateReady         bool
	SocketReady        bool
	GRPCConfigured     bool
	MithrilConfigured  bool
	GenesisFiles       map[string]string
	Missing            []string
	Warnings           []string
	ReadyForConfig     bool
	ReadyForLiveImport bool
}

func InspectDolos(dir string, requireSocket bool) (*DolosReport, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, fmt.Errorf("dolos dir is required")
	}

	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve dolos dir: %w", err)
	}
	info, err := os.Stat(absDir)
	if err != nil {
		return nil, fmt.Errorf("inspect dolos dir: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("dolos dir is not a directory: %s", absDir)
	}

	configPath := filepath.Join(absDir, "dolos.toml")
	raw, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read dolos config: %w", err)
	}

	var cfg DolosConfig
	if err := toml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse dolos config: %w", err)
	}

	storagePath := resolvePath(absDir, cfg.Storage.Path)
	if storagePath == "" {
		storagePath = filepath.Join(absDir, "data")
	}
	socketPath := resolvePath(absDir, cfg.Serve.Ouroboros.ListenPath)
	if socketPath == "" {
		socketPath = filepath.Join(absDir, "dolos.socket")
	}

	report := &DolosReport{
		Dir:               absDir,
		ConfigPath:        configPath,
		StoragePath:       storagePath,
		ArchivePath:       filepath.Join(storagePath, "archive"),
		StatePath:         filepath.Join(storagePath, "state"),
		SocketPath:        socketPath,
		GRPCListenAddress: cfg.Serve.GRPC.ListenAddress,
		GRPCEndpoint:      LocalHTTPURL(cfg.Serve.GRPC.ListenAddress),
		MithrilAggregator: cfg.Mithril.Aggregator,
		NetworkMagic:      cfg.Chain.Magic,
		IsTestnet:         cfg.Chain.IsTestnet,
		ConfigReady:       true,
		GenesisFiles:      make(map[string]string),
	}

	report.GRPCConfigured = strings.TrimSpace(cfg.Serve.GRPC.ListenAddress) != ""
	report.MithrilConfigured = strings.TrimSpace(cfg.Mithril.Aggregator) != "" && strings.TrimSpace(cfg.Mithril.GenesisKey) != ""

	genesis := map[string]string{
		"byron":   cfg.Genesis.ByronPath,
		"shelley": cfg.Genesis.ShelleyPath,
		"alonzo":  cfg.Genesis.AlonzoPath,
		"conway":  cfg.Genesis.ConwayPath,
	}
	for name, path := range genesis {
		resolved := resolvePath(absDir, path)
		report.GenesisFiles[name] = resolved
		if resolved == "" {
			report.Missing = append(report.Missing, fmt.Sprintf("genesis.%s_path", name))
			continue
		}
		if _, err := os.Stat(resolved); err != nil {
			report.Missing = append(report.Missing, resolved)
		}
	}

	report.StorageReady = pathIsDir(report.StoragePath)
	report.ArchiveReady = pathIsDir(report.ArchivePath)
	report.StateReady = pathIsDir(report.StatePath)
	report.SocketReady = pathIsSocket(report.SocketPath)
	report.ArchiveSegments, report.ArchiveBytes = countArchiveSegments(report.ArchivePath)

	if !report.StorageReady {
		report.Missing = append(report.Missing, report.StoragePath)
	}
	if !report.ArchiveReady {
		report.Missing = append(report.Missing, report.ArchivePath)
	}
	if !report.StateReady {
		report.Missing = append(report.Missing, report.StatePath)
	}
	if !report.GRPCConfigured {
		report.Missing = append(report.Missing, "serve.grpc.listen_address")
	}
	if !report.MithrilConfigured {
		report.Warnings = append(report.Warnings, "mithril aggregator/genesis_key is not fully configured")
	}
	if report.NetworkMagic == 0 {
		report.Warnings = append(report.Warnings, "chain.magic is not set")
	}
	if requireSocket && !report.SocketReady {
		report.Missing = append(report.Missing, report.SocketPath)
	}
	if !report.SocketReady {
		report.Warnings = append(report.Warnings, "ouroboros socket is not present yet; run live Nectar indexing only after Dolos is serving")
	}

	report.ReadyForConfig = report.ConfigReady && report.StorageReady && report.ArchiveReady && report.StateReady && len(report.GenesisFiles) > 0
	report.ReadyForLiveImport = report.ReadyForConfig && report.SocketReady && report.GRPCConfigured

	return report, nil
}

func LocalHTTPURL(listenAddress string) string {
	addr := strings.TrimSpace(listenAddress)
	if addr == "" {
		return ""
	}
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return strings.TrimRight(addr, "/")
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		if strings.HasPrefix(addr, ":") {
			return "http://127.0.0.1" + addr
		}
		return "http://" + addr
	}
	host = strings.Trim(host, "[]")
	if host == "" || host == "::" || host == "0.0.0.0" || host == "[::]" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

func resolvePath(baseDir, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if filepath.IsAbs(value) {
		return filepath.Clean(value)
	}
	return filepath.Join(baseDir, value)
}

func pathIsDir(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func pathIsSocket(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeSocket != 0
}

func countArchiveSegments(path string) (int, int64) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return 0, 0
	}
	var count int
	var total int64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".segment") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		count++
		total += info.Size()
	}
	return count, total
}
