package com.g20.CPEN431.A7;

/**
 * Tracks the gossip state for a single node.
 * Each node maintains a heartbeat counter that it increments locally,
 * and other nodes track the last known heartbeat and local timestamp.
 */
public class NodeStatus {
    private final Node node;
    private volatile long heartbeatCounter;
    private volatile long localTimestamp; // local time when we last saw an update
    private volatile boolean alive;

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

    /**
     * Atomically update heartbeat and re-add to ring if this was a rejoin.
     * Ring update is inside the lock to prevent the failure detector from
     * removing the node between the alive flip and the ring add.
     */
    public synchronized boolean updateHeartbeatAndRejoinIfDead(long incomingCounter, ConsistentHashmap hashRing) {
        if (incomingCounter > this.heartbeatCounter) {
            this.heartbeatCounter = incomingCounter;
            this.localTimestamp = System.currentTimeMillis();
            if (!this.alive) {
                this.alive = true;
                hashRing.addNode(this.node);
                return true; // rejoin detected
            }
        }
        return false;
    }

    /**
     * Atomically mark as failed and remove from ring if alive and expired.
     * Ring update is inside the lock to prevent mergeEntries from re-adding
     * the node between the alive flip and the ring remove.
     */
    public synchronized boolean markFailedIfExpired(long failureTimeoutMs, ConsistentHashmap hashRing) {
        if (this.alive && System.currentTimeMillis() - this.localTimestamp > failureTimeoutMs) {
            this.alive = false;
            hashRing.removeNode(this.node);
            return true;
        }
        return false;
    }

    /**
     * Check if this node has been dead long enough to be cleaned up.
     */
    public synchronized boolean isCleanupReady(long cleanupTimeoutMs) {
        return !alive && (System.currentTimeMillis() - localTimestamp > cleanupTimeoutMs);
    }
}