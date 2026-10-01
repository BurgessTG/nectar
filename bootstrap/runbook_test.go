package bootstrap

import (
	"strings"
	"testing"
)

func TestGenerateRunbookUsesDolosSnapshotCommand(t *testing.T) {
	report := testDolosReport()

	runbook, err := GenerateRunbook(report, RunbookOptions{
		ConfigPath:      "nectar.toml",
		MySQLDSN:        "root:nectar@tcp(127.0.0.1:3306)/nectar",
		BootstrapMethod: BootstrapMethodSnapshot,
	})
	if err != nil {
		t.Fatalf("GenerateRunbook returned error: %v", err)
	}

	for _, expected := range []string{
		"dolos bootstrap snapshot --variant 'full' --point 'latest' --continue",
		"go run . bootstrap-dolos --dolos-dir '/tmp/cardano.nodes' --require-socket",
		"--mysql-dsn 'root:nectar@tcp(127.0.0.1:3306)/nectar'",
		"UTxO RPC endpoint: `http://127.0.0.1:50051` (configured)",
	} {
		if !strings.Contains(runbook, expected) {
			t.Fatalf("runbook missing %q\n%s", expected, runbook)
		}
	}
}

func TestGenerateScriptDoesNotStartDolosDaemon(t *testing.T) {
	report := testDolosReport()

	script, err := GenerateScript(report, RunbookOptions{
		WorkDir:         "/tmp/indexer",
		ConfigPath:      "nectar.toml",
		BootstrapMethod: BootstrapMethodMithril,
	})
	if err != nil {
		t.Fatalf("GenerateScript returned error: %v", err)
	}
	if !strings.Contains(script, "dolos bootstrap mithril") {
		t.Fatalf("script missing mithril bootstrap command\n%s", script)
	}
	if !strings.Contains(script, "read -r -p 'Press Enter after Dolos socket and UTxO RPC are serving... '") {
		t.Fatalf("script should pause for manually started Dolos\n%s", script)
	}
	if strings.Contains(script, "\ndolos daemon -c ") {
		t.Fatalf("script should not directly start Dolos daemon\n%s", script)
	}
}

func testDolosReport() *DolosReport {
	return &DolosReport{
		Dir:                "/tmp/cardano.nodes",
		ConfigPath:         "/tmp/cardano.nodes/dolos.toml",
		StoragePath:        "/tmp/cardano.nodes/data",
		SocketPath:         "/tmp/cardano.nodes/dolos.socket",
		GRPCEndpoint:       "http://127.0.0.1:50051",
		GRPCConfigured:     true,
		ReadyForConfig:     true,
		ReadyForLiveImport: false,
	}
}
