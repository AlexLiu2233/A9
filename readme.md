 ```markdown
# G20 - Distributed Systems Assignment 7

**Group ID:** G20  
**Verification Code:** 5409645920

---

## Deployment Commands

### Server EC2 Instance

```bash
# Kill any old servers
pkill -f "A7-1.0-SNAPSHOT" 2>/dev/null
sleep 2

# Create nodes file with public IP
cat > nodes_server.txt << 'EOF'
34.228.184.115:43100
34.228.184.115:43101
34.228.184.115:43102
34.228.184.115:43103
34.228.184.115:43104
34.228.184.115:43105
34.228.184.115:43106
34.228.184.115:43107
34.228.184.115:43108
34.228.184.115:43109
34.228.184.115:43110
34.228.184.115:43111
34.228.184.115:43112
34.228.184.115:43113
34.228.184.115:43114
34.228.184.115:43115
34.228.184.115:43116
34.228.184.115:43117
34.228.184.115:43118
34.228.184.115:43119
EOF

# Set up network emulation
sudo tc qdisc add dev lo root netem delay 5msec loss 2.5%
sudo tc qdisc add dev ens5 root netem delay 5msec loss 2.5%

# Launch 20 servers
for i in $(seq 0 19); do
    PORT=$((43100 + i))
    nohup java -Xmx64m -jar A7-1.0-SNAPSHOT-jar-with-dependencies.jar $PORT nodes_server.txt > server_${PORT}.log 2>&1 &
done

echo "Waiting 45s for servers to start..."
sleep 45
echo "Servers running:"
ps aux | grep A7 | grep -v grep | wc -l
```

### Client EC2 Instance

```bash
# Start standalone localhost server (required for submit mode)
echo "127.0.0.1:43100" > nodes_standalone.txt
nohup java -Xmx64m -jar A7-1.0-SNAPSHOT-jar-with-dependencies.jar 43100 nodes_standalone.txt > standalone_server.log 2>&1 &
sleep 5

# Verify standalone server is running
ps aux | grep A7 | grep -v grep

# Create servers list pointing to the remote server EC2 instance
cat > nodes_server.txt << 'EOF'
34.228.184.115:43100
34.228.184.115:43101
34.228.184.115:43102
34.228.184.115:43103
34.228.184.115:43104
34.228.184.115:43105
34.228.184.115:43106
34.228.184.115:43107
34.228.184.115:43108
34.228.184.115:43109
34.228.184.115:43110
34.228.184.115:43111
34.228.184.115:43112
34.228.184.115:43113
34.228.184.115:43114
34.228.184.115:43115
34.228.184.115:43116
34.228.184.115:43117
34.228.184.115:43118
34.228.184.115:43119
EOF

# Run eval client in submit mode
java -Xmx6g -jar a7_2026_eval_tests_v1.jar \
    --submit \
    --servers-list nodes_server.txt \
    --secret-code 5409645920
```

---

## Architecture Overview

### Consistent Hash Ring
A **consistent hash ring** was implemented to handle mapping nodes to a ring-space. The implementation uses:

| Component | Implementation | Rationale |
|-----------|---------------|-----------|
| Underlying Structure | `TreeMap` | Provides O(log n) lookup for successor nodes |
| Concurrency Control | `ReentrantReadWriteLock` | Optimized for read-heavy workloads; allows concurrent reads while ensuring write correctness |

> **Design Decision:** Since read operations are expected to be significantly more frequent than writes, the read-write lock pattern was chosen to maximize throughput while maintaining thread safety.

### Inter-Node Communication
**Gossip-style service** is used for peer-to-peer communication between nodes, enabling decentralized failure detection and state propagation.

---

## Shutdown Behavior Verification

**Proof of immediate crash/termination upon receiving shutdown request:**

| Attribute | Value |
|-----------|-------|
| **File** | `RequestHandler.java` |
| **Line** | 107 (context: lines 105-109) |
```