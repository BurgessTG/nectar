#!/bin/bash

# Configuration
NECTAR_DIR="/home/burgess/Work/honeycomb./indexer"
LOG_FILE="$NECTAR_DIR/nectar-sync.log"
CMD="./nectar"
STALL_CHECK_INTERVAL=600  # Check for stall every 10 minutes
POLL_INTERVAL=30          # Check process health every 30 seconds

# Memory Management - CRITICAL for stability
export GOMEMLIMIT=2GiB      # Hard memory cap - Go GCs aggressively near this
export GOGC=50              # GC at 50% heap growth (default 100%)

# Performance tuning
export WORKER_COUNT=4       # Reduced for memory headroom (32GB shared with TiDB)
export DB_CONNECTION_POOL=8  # Matched to worker count

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# State tracking for stall detection
LAST_SLOT=0
LAST_SLOT_TIME=0
STALL_COUNT=0

get_max_slot() {
    mysql -h 127.0.0.1 -u root -P 4000 -N -e "USE nectar; SELECT COALESCE(MAX(slot_no), 0) FROM blocks;" 2>/dev/null | tr -d '[:space:]'
}

kill_nectar() {
    pkill -x nectar 2>/dev/null
    sleep 3
    if pgrep -x "nectar" > /dev/null; then
        killall -9 nectar 2>/dev/null
        sleep 2
    fi
}

start_nectar() {
    nohup $CMD > "$LOG_FILE" 2>&1 &
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] Nectar started (PID: $!).${NC}"
    STALL_COUNT=0
    sleep 15  # Give it time to connect and start syncing
    LAST_SLOT=$(get_max_slot)
    LAST_SLOT_TIME=$(date +%s)
}

echo -e "${GREEN}Starting Nectar Auto-Recovery Monitor...${NC}"
echo -e "${GREEN}  Stall detection: restarts if no progress for ${STALL_CHECK_INTERVAL}s${NC}"
echo -e "${GREEN}  Poll interval: ${POLL_INTERVAL}s${NC}"

cd "$NECTAR_DIR" || exit 1

# Initialize slot tracking
LAST_SLOT_TIME=$(date +%s)

while true; do
    current_time=$(date '+%Y-%m-%d %H:%M:%S')

    # 1. Check if Nectar is running
    if ! pgrep -x "nectar" > /dev/null; then
        echo -e "${RED}[$current_time] Nectar is NOT running. Starting...${NC}"
        start_nectar
        continue
    fi

    # 2. Check logs for explicit timeout errors
    if tail -n 50 "$LOG_FILE" 2>/dev/null | grep -q "protocol error: chain-sync: timeout"; then
        echo -e "${YELLOW}[$current_time] Timeout error detected in logs. Restarting Nectar...${NC}"
        kill_nectar
        start_nectar
        continue
    fi

    # 3. Stall detection - check if max slot has advanced
    NOW=$(date +%s)
    ELAPSED=$((NOW - LAST_SLOT_TIME))

    if [ "$ELAPSED" -ge "$STALL_CHECK_INTERVAL" ]; then
        CURRENT_SLOT=$(get_max_slot)

        if [ -z "$CURRENT_SLOT" ] || [ "$CURRENT_SLOT" = "0" ]; then
            # DB might be unavailable, skip this check
            echo -e "${YELLOW}[$current_time] Could not query max slot, skipping stall check${NC}"
        elif [ "$CURRENT_SLOT" = "$LAST_SLOT" ]; then
            STALL_COUNT=$((STALL_COUNT + 1))
            echo -e "${YELLOW}[$current_time] STALL DETECTED: slot stuck at $CURRENT_SLOT (stall count: $STALL_COUNT)${NC}"
            echo -e "${YELLOW}[$current_time] Restarting Nectar due to stall...${NC}"
            kill_nectar
            start_nectar
            continue
        else
            # Progress detected
            SLOTS_GAINED=$((CURRENT_SLOT - LAST_SLOT))
            RATE=$((SLOTS_GAINED / ELAPSED))
            echo -e "${GREEN}[$current_time] Sync progressing: slot $CURRENT_SLOT (+$SLOTS_GAINED, ~${RATE} slots/s)${NC}"
            LAST_SLOT=$CURRENT_SLOT
            STALL_COUNT=0
        fi

        LAST_SLOT_TIME=$NOW
    fi

    sleep $POLL_INTERVAL
done
