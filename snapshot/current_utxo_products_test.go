package snapshot

import (
	"strings"
	"testing"
)

func TestCurrentUTxOHolderRefreshSQLUsesCurrentSnapshotTablesOnly(t *testing.T) {
	sql := strings.ToLower(CurrentUTxOHolderRefreshSQL())

	for _, required := range []string{"from ma_tx_outs", "join tx_outs", "insert into token_holders"} {
		if !strings.Contains(sql, required) {
			t.Fatalf("refresh SQL missing %q\n%s", required, sql)
		}
	}
	for _, forbidden := range []string{"join txes", "join blocks", "join tx_ins"} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("refresh SQL should not depend on historical table %q\n%s", forbidden, sql)
		}
	}
}
