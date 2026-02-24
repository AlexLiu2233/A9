package com.g20.CPEN431.A7;

/**
 * Tracks the gossip state for a single node.
 * Each node maintains a heartbeat counter that it increments locally,
 * and other nodes track the last known heartbeat and local timestamp.
 */
public class NodeStatus {
    private final Node node;
    private long heartbeatCounter;
    private long localTimestamp; // local time when we last saw an update
    private boolean alive;

    public NodeStatus(Node node, long heartbeatCounter) {
        this.node = node;
        this.heartbeatCounter = heartbeatCounter;
        this.localTimestamp = System.currentTimeMillis();
        this.alive = true;
    }

    public Node getNode() {
        return node;
    }

    public long getHeartbeatCounter() {
        return heartbeatCounter;
    }

    public long getLocalTimestamp() {
        return localTimestamp;
    }

    public boolean isAlive() {
        return alive;
    }

    public void setAlive(boolean alive) {
        this.alive = alive;
    }

    /**
     * Update heartbeat if the incoming counter is higher.
     * @return true if updated, false if stale
     */
    public boolean updateHeartbeat(long incomingCounter) {
        if (incomingCounter > this.heartbeatCounter) {
            this.heartbeatCounter = incomingCounter;
            this.localTimestamp = System.currentTimeMillis();
            this.alive = true;
            return true;
        }
        return false;
    }

    /**
     * Check if this node should be considered failed based on timeout.
     */
    public boolean isExpired(long failureTimeoutMs) {
        return System.currentTimeMillis() - localTimestamp > failureTimeoutMs;
    }

    /**
     * Check if this node has been dead long enough to be cleaned up.
     */
    public boolean isCleanupReady(long cleanupTimeoutMs) {
        return !alive && (System.currentTimeMillis() - localTimestamp > cleanupTimeoutMs);
    }
}