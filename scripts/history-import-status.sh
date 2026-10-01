#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NECTAR_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$NECTAR_ROOT"

UNIT="${NECTAR_HISTORY_UNIT:-nectar-history-import.service}"
CONFIG="${NECTAR_CONFIG:-nectar.toml}"
ENDPOINT="${NECTAR_UTXORPC_ENDPOINT:-localhost:50051}"
MYSQL_CONTAINER="${NECTAR_MYSQL_CONTAINER:-nectar-mysql}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-nectar}"

echo "== Nectar History Import Service =="
if command -v systemctl >/dev/null 2>&1; then
  systemctl --user show "$UNIT" \
    -p ActiveState -p SubState -p MainPID -p ActiveEnterTimestamp -p Restart -p RestartUSec \
    --no-pager || true

  pid="$(systemctl --user show "$UNIT" -p MainPID --value --no-pager 2>/dev/null || true)"
  if [[ "$pid" =~ ^[0-9]+$ ]] && [[ "$pid" != "0" ]] && [[ -e "/proc/$pid/exe" ]]; then
    printf 'Exe=%s\n' "$(readlink "/proc/$pid/exe")"
  fi
fi

echo
echo "== Import Checkpoint =="
./run/nectar history-import --config "$CONFIG" --status

echo
echo "== Dolos Tip =="
if command -v grpcurl >/dev/null 2>&1; then
  grpcurl -plaintext -d '{}' "$ENDPOINT" utxorpc.v1alpha.sync.SyncService.ReadTip || true
else
  echo "grpcurl not found"
fi

echo
echo "== MySQL Import Tunables =="
if command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' | grep -qx "$MYSQL_CONTAINER"; then
  docker exec "$MYSQL_CONTAINER" mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -N -e "
    SELECT
      CONCAT('innodb_buffer_pool_size=', @@innodb_buffer_pool_size),
      CONCAT('innodb_redo_log_capacity=', @@innodb_redo_log_capacity),
      CONCAT('innodb_flush_log_at_trx_commit=', @@innodb_flush_log_at_trx_commit),
      CONCAT('sync_binlog=', @@sync_binlog);
    SHOW GLOBAL STATUS WHERE Variable_name IN (
      'Innodb_log_waits',
      'Innodb_row_lock_waits',
      'Innodb_row_lock_time',
      'Threads_running'
    );
  " || true

  echo
  echo "== Container Resources =="
  docker stats --no-stream "$MYSQL_CONTAINER" honeycomb-redis 2>/dev/null || docker stats --no-stream "$MYSQL_CONTAINER"
else
  echo "Docker container $MYSQL_CONTAINER is not running"
fi

echo
echo "== Recent Import Log Issues =="
log_path=""
if [[ -f run/history-import.logpath ]]; then
  log_path="$(tr -d '\n' < run/history-import.logpath)"
fi
if [[ -z "$log_path" || ! -f "$log_path" ]]; then
  log_path="$(ls -t logs/history-import*.log 2>/dev/null | head -n 1 || true)"
fi
if [[ -n "$log_path" && -f "$log_path" ]]; then
  printf 'Log=%s\n' "$log_path"
  grep -nEi 'panic|fatal|failed|invalid|deadlock|timeout|duplicate|data too long' "$log_path" | tail -n 40 || true
else
  echo "No history import log found"
fi

if [[ "${1:-}" == "--deep" ]]; then
  echo
  echo "== Deep Partial Verification =="
  ./run/nectar verify-history --config "$CONFIG" --deep --skip-migrations
fi
