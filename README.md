# Nectar Indexer

Nectar is Honeycomb's active Go Cardano indexer.

## Role

The indexer owns chain ingestion and derived blockchain tables. The backend reads the indexed data and should not connect directly to Demeter/Oura-era ingestion paths.

## Current commands

```bash
go mod download
go run .
go test ./...
go build ./...
```

Configure `nectar.toml`, database DSN, worker settings, and the Cardano/Dolos socket path before running against a real chain source.

Mithril/Dolos prep is available without starting the indexer:

```bash
go run . bootstrap-dolos --dolos-dir /path/to/cardano.nodes --config nectar.toml
go run . bootstrap-dolos --dolos-dir /path/to/cardano.nodes --config nectar.toml --mysql-dsn '<dsn>' --write
go run . bootstrap-dolos --dolos-dir /path/to/cardano.nodes --write-runbook bootstrap.md --write-script bootstrap.sh
```

Current UTxO seeding from Dolos UTxO RPC is available as a separate command:

```bash
go run . snapshot-import --config nectar.toml --limit 1000
go run . snapshot-import --config nectar.toml --manifest-only --write-manifest snapshot-manifest.json
go run . snapshot-import --config nectar.toml --apply --replace-utxo
go run . snapshot-import --source filesystem --snapshot-dir /path/to/extracted-mithril-snapshot
go run . snapshot-import --source filesystem --snapshot-dir /path/to/extracted-mithril-snapshot --config nectar.toml --apply --replace-utxo
go run . snapshot-import --source ndjson --utxo-ndjson current-utxo.ndjson --apply --replace-utxo
```

The Dolos UTxO RPC import path seeds current holder state and refreshes `token_holders` directly from the current UTxO rows. `--source filesystem` can apply a Dingo-compatible UTxO-HD `ledger/<slot>/tables/tvar` file into the same Nectar base tables (`tx_outs`, `ma_tx_outs`, `multi_assets`) and holder refresh path; the file is memory-mapped to avoid copying a mainnet-sized table into the Go heap. `--source ndjson` applies a parsed Dingo-style current UTxO NDJSON file into that same path. Full chart data requires an unfiltered apply; partial apply with `--limit`, `--policy`, or `--asset-name` is refused unless `--allow-partial-apply` is passed for tests. `--manifest-only` writes table-level coverage without connecting to live RPC. Legacy ledger-state file decoding remains separate. Historical wallet graph tables and rewards still require separate, defensible sources; they are not faked by snapshot import.

The indexer event bus listener is configured with `EVENT_BUS_ADDRESS` and defaults to `0.0.0.0:9000`. The backend subscriber uses `NECTAR_EVENT_BUS`.

## Current caveats

- The active startup migration path is GORM/manual DDL, not the historical SQL files under `indexer/migrations/`.
- DB code uses the MySQL driver but still applies TiDB-specific defaults and optimizations.
- `token_holders` and `token_wallet_connections` are created by the active startup migration path; backfill scripts and historical backend SQL still duplicate parts of that DDL.
- Some helper ops files still assume TiDB service names or port `4000`.
- The Dockerfile Go image is older than the `go.mod` toolchain declaration.

## Current cleanup stance

No source refactor is happening in the cleanup pass. Old root-level indexer planning/audit docs were archived at:

```text
archive/2026-05-cleanup/legacy-docs/indexer-root-docs/
```

Current indexer strategy notes live at `../docs/INDEXER.md` and database strategy notes live at `../docs/DATABASE.md`.

## Future organization target

After contracts and database direction are stable, split orchestration from processors, database, events, dashboard, and Mithril bootstrap code. Do not do that during cleanup-only work.
