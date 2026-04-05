#!/usr/bin/env bash
set -uo pipefail
set -x

JAR="A9-1.0-SNAPSHOT-jar-with-dependencies.jar"
EVAL_JAR="a9_2026_eval_tests_v2.jar"
START_PORT=43100
NUM_NODES=30          # A9 dry-run minimum is 20 (submit requires 80)
NODE_FILE="nodes_local.txt"
LOG_DIR="logs"

# ── Pre-checks ──
if [ ! -f "$JAR" ]; then
    echo "JAR not found at $JAR — building..."
    mvn -q clean package -DskipTests
fi

if [ ! -f "$JAR" ]; then
    echo "ERROR: Build failed, $JAR still missing."
    exit 1
fi

# ── Clean up old instances ──
echo "Killing old instances..."
pkill -f "A9-1.0-SNAPSHOT" 2>/dev/null || true

for PORT in $(seq $START_PORT $((START_PORT + NUM_NODES - 1))); do
    fuser -k "${PORT}/udp" 2>/dev/null || true
    fuser -k "${PORT}/tcp" 2>/dev/null || true
done

sleep 3

# ── Generate node file ──
> "$NODE_FILE"
for PORT in $(seq $START_PORT $((START_PORT + NUM_NODES - 1))); do
    echo "127.0.0.1:${PORT}" >> "$NODE_FILE"
done
echo "Generated $NODE_FILE with $NUM_NODES nodes"

# ── Create log directory ──
mkdir -p "$LOG_DIR"

# ── Start seed node first ──
PIDS=()
echo "Starting SEED node on port $START_PORT..."
java -Xmx512m -jar "$JAR" "$START_PORT" "$NODE_FILE" --seed \
    > "${LOG_DIR}/node_${START_PORT}.log" 2>&1 &
PIDS+=($!)
sleep 2

# ── Start remaining nodes ──
for PORT in $(seq $((START_PORT + 1)) $((START_PORT + NUM_NODES - 1))); do
    echo "Starting node on port $PORT..."
    java -Xmx512m -jar "$JAR" "$PORT" "$NODE_FILE" \
        > "${LOG_DIR}/node_${PORT}.log" 2>&1 &
    PIDS+=($!)
    sleep 0.3
done

echo ""
echo "Waiting 20s for gossip convergence..."
sleep 40

# ── Verify ──
RUNNING=0
FAILED_PORTS=()
for i in "${!PIDS[@]}"; do
    PORT=$((START_PORT + i))
    if kill -0 "${PIDS[$i]}" 2>/dev/null; then
        ((RUNNING++))
    else
        FAILED_PORTS+=("$PORT")
    fi
done

echo "Running: $RUNNING / $NUM_NODES"
if [ ${#FAILED_PORTS[@]} -gt 0 ]; then
    echo "FAILED ports: ${FAILED_PORTS[*]}"
    echo "Check logs:"
    for P in "${FAILED_PORTS[@]}"; do
        echo "  tail ${LOG_DIR}/node_${P}.log"
    done
    echo ""
    echo "── First failed node log (last 20 lines) ──"
    tail -20 "${LOG_DIR}/node_${FAILED_PORTS[0]}.log" 2>/dev/null || echo "(empty)"
fi

echo ""
echo "── Cluster status ──"
echo "Node file:  $NODE_FILE"
echo "Logs:       $LOG_DIR/"
echo ""
echo "To test (dry-run, local):"
echo "  java -Xmx6g -jar $EVAL_JAR --servers-list $NODE_FILE --secret-code <your-secret>"
echo ""
echo "To run only performance:"
echo "  java -Xmx6g -jar $EVAL_JAR --servers-list $NODE_FILE --secret-code <your-secret> --only-performance 512"
echo ""
echo "To run only suspend/rejoin:"
echo "  java -Xmx6g -jar $EVAL_JAR --servers-list $NODE_FILE --secret-code <your-secret> --only-suspend-rejoin"
echo ""
echo "To stop:    pkill -f 'A9-1.0-SNAPSHOT' || kill ${PIDS[*]}"
