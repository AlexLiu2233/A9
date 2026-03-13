#!/bin/bash
# start_nodes.sh
# Starts 20 local nodes, each in its own terminal window.
# Usage: ./start_nodes.sh
#
# The first node is started with --seed flag.

NODES_FILE="${1:-server.list}"
BASE_PORT="${2:-13400}"
NODE_COUNT="${3:-20}"

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
JAR_NAME="A9-1.0-SNAPSHOT-jar-with-dependencies.jar"
JAR_PATH="$PROJECT_DIR/$JAR_NAME"
NODES_FILE_PATH="$PROJECT_DIR/$NODES_FILE"

if [ ! -f "$JAR_PATH" ]; then
    echo "ERROR: JAR not found: $JAR_PATH" >&2
    exit 1
fi

# Generate nodes file if it doesn't exist
if [ ! -f "$NODES_FILE_PATH" ]; then
    echo "Generating $NODES_FILE with $NODE_COUNT nodes (ports $BASE_PORT-$((BASE_PORT + NODE_COUNT - 1)))..."
    for ((i = 0; i < NODE_COUNT; i++)); do
        echo "localhost:$((BASE_PORT + i))"
    done > "$NODES_FILE_PATH"
fi

echo "Using JAR: $JAR_PATH"
echo "Nodes file: $NODES_FILE_PATH"
echo "Starting $NODE_COUNT nodes..."

for ((i = 0; i < NODE_COUNT; i++)); do
    port=$((BASE_PORT + i))
    seed_flag=""
    if [ "$i" -eq 0 ]; then
        seed_flag="--seed"
    fi

    # Start each node in the background
    java -Xmx512m -jar "$JAR_PATH" "$port" "$NODES_FILE_PATH" $seed_flag &

    if [ "$i" -eq 0 ]; then
        echo "  Started node $i on port $port [SEED]"
        sleep 2
    else
        echo "  Started node $i on port $port"
    fi
done

echo ""
echo "All $NODE_COUNT nodes launched."
echo "To stop all nodes: pkill -f '$JAR_NAME'"
