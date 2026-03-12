package com.g20.CPEN431.A9.network;

import com.g20.CPEN431.A9.storage.ConsistentHashmap;

/**
 * Tracks the gossip state for a single node.
 * Each node maintains a heartbeat counter that it increments locally,
 * and other nodes track the last known heartbeat and local timestamp.
 *
 * State machine:
 *   ALIVE   -> SUSPECT : local failure detection timeout OR gossip death report
 *   SUSPECT -> ALIVE   : fresh heartbeat received (refutes suspicion)
 *   SUSPECT -> DEAD    : suspicion timeout expires without refutation
 *   DEAD    -> ALIVE   : heartbeat received with higher generation (genuine restart)
 *   DEAD    -> removed : cleanup timeout expires
 *
 * Version ordering uses (generation, heartbeat) pairs compared lexicographically.
 * Generation is the wall-clock startup timestamp of the node. A higher generation
 * always supersedes, regardless of heartbeat counter value.
 */
public class NodeStatus {

    public enum State { ALIVE, SUSPECT, DEAD }

    private final Node node;
    private volatile long heartbeatCounter;
    private volatile long generation;      // startup timestamp — higher = newer incarnation
    private volatile long localTimestamp;   // last heartbeat update (or time of death for DEAD)
    private volatile State state;
    private volatile long suspectTimestamp; // when SUSPECT state began
    private volatile boolean onRing;       // whether node is actually on the hash ring

    public NodeStatus(Node node, long heartbeatCounter, long generation) {
        this.node = node;
        this.heartbeatCounter = heartbeatCounter;
        this.generation = generation;
        this.localTimestamp = System.currentTimeMillis();
        this.state = State.ALIVE;
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

    /** True only when strictly ALIVE (not SUSPECT). */
    public boolean isAlive() {
        return state == State.ALIVE;
    }

    /** True when the node is actually present on the hash ring. */
    public boolean isOnRing() {
        return onRing;
    }

    public void setOnRing(boolean value) {
        this.onRing = value;
    }

    public boolean isDead() {
        return state == State.DEAD;
    }

    public boolean isSuspect() {
        return state == State.SUSPECT;
    }

    /**
     * Compare incoming (generation, heartbeat) against stored values.
     * @return negative if incoming is older, 0 if equal, positive if incoming is newer
     */
    private int compareVersion(long incomingGen, long incomingHb) {
        int genCmp = Long.compare(incomingGen, this.generation);
        if (genCmp != 0) return genCmp;
        return Long.compare(incomingHb, this.heartbeatCounter);
    }

    /**
     * Atomically read heartbeat counter, alive flag, and generation for gossip message construction.
     * SUSPECT and DEAD nodes are reported as dead so that peers do not reset
     * their own failure timers from stale heartbeats propagated via gossip.
     * Each peer independently detects failure via its own T_FAIL timeout.
     * @return [heartbeatCounter, alive (1 or 0), generation]
     */
    public synchronized long[] getSnapshot() {
        return new long[]{ heartbeatCounter, state == State.ALIVE ? 1 : 0, generation };
    }

    /**
     * Update self node's heartbeat counter.
     * Only called by the gossip thread for the local node.
     */
    public synchronized void updateSelfHeartbeat(long counter) {
        this.heartbeatCounter = counter;
        this.localTimestamp = System.currentTimeMillis();
    }

    /**
     * Handle an incoming alive heartbeat report. Behavior depends on current state:
     *
     * ALIVE:   update if incoming version is newer (normal heartbeat propagation).
     *          Also ensures node is in hash ring (for bootstrap confirmation).
     * SUSPECT: if incoming version is newer, refute suspicion -> ALIVE.
     * DEAD:    accept as rejoin only if incoming generation is strictly higher
     *          (the node genuinely restarted). Same-generation gossip for a dead
     *          node is always stale.
     *
     * Version comparison uses (generation, heartbeat) lexicographically.
     *
     * @return true if this was a rejoin (DEAD -> ALIVE transition)
     */
    public synchronized boolean updateHeartbeatAndRejoinIfDead(
            long incomingGeneration, long incomingCounter, ConsistentHashmap hashRing) {
        switch (state) {
            case DEAD:
                // Only a higher generation (genuine restart) can revive a dead node.
                // Same-generation gossip is stale — the node died in this generation.
                if (incomingGeneration <= this.generation) {
                    return false;
                }
                this.generation = incomingGeneration;
                this.heartbeatCounter = incomingCounter;
                this.localTimestamp = System.currentTimeMillis();
                this.state = State.ALIVE;
                this.onRing = true;
                hashRing.addNode(this.node);
                return true; // rejoin detected

            case SUSPECT:
                // Strictly newer version refutes suspicion
                if (compareVersion(incomingGeneration, incomingCounter) > 0) {
                    this.generation = incomingGeneration;
                    this.heartbeatCounter = incomingCounter;
                    this.localTimestamp = System.currentTimeMillis();
                    this.state = State.ALIVE;
                    // Node is already on ring during SUSPECT, no need to re-add
                }
                return false;

            case ALIVE:
                if (compareVersion(incomingGeneration, incomingCounter) > 0) {
                    this.generation = incomingGeneration;
                    this.heartbeatCounter = incomingCounter;
                    this.localTimestamp = System.currentTimeMillis();
                    // Add to hash ring on first confirmation (bootstrap nodes start off-ring)
                    if (!this.onRing) {
                        hashRing.addNode(this.node);
                        this.onRing = true;
                    }
                }
                return false;

            default:
                return false;
        }
    }

    /**
     * Mark as SUSPECT when a peer reports this node as dead.
     * Only transitions ALIVE -> SUSPECT; node stays on the hash ring during suspicion.
     * Accepts the report if the incoming version is >= ours (using generation-aware
     * comparison). Updates our stored version to the reported one so that only
     * strictly newer heartbeats can refute the suspicion.
     */
    public synchronized boolean markSuspectFromGossip(long incomingGeneration, long incomingCounter) {
        if (this.state == State.ALIVE && compareVersion(incomingGeneration, incomingCounter) >= 0) {
            this.state = State.SUSPECT;
            this.suspectTimestamp = System.currentTimeMillis();
            // Update to the reported version so only strictly fresher heartbeats can refute
            this.generation = incomingGeneration;
            this.heartbeatCounter = incomingCounter;
            return true;
        }
        return false;
    }

    /**
     * Local failure detection: ALIVE -> SUSPECT if heartbeat has not updated
     * within the failure timeout. Node stays on the hash ring during suspicion.
     */
    public synchronized boolean markSuspectIfExpired(long failureTimeoutMs) {
        if (this.state == State.ALIVE
                && System.currentTimeMillis() - this.localTimestamp > failureTimeoutMs) {
            this.state = State.SUSPECT;
            this.suspectTimestamp = System.currentTimeMillis();
            return true;
        }
        return false;
    }

    /**
     * Confirm death: SUSPECT -> DEAD if the suspicion timeout has elapsed
     * without the node refuting via a fresh heartbeat.
     * Removes the node from the hash ring.
     */
    public synchronized boolean confirmDeadIfSuspectExpired(
            long suspectTimeoutMs, ConsistentHashmap hashRing) {
        if (this.state == State.SUSPECT
                && System.currentTimeMillis() - this.suspectTimestamp > suspectTimeoutMs) {
            this.state = State.DEAD;
            this.localTimestamp = System.currentTimeMillis(); // anchor cleanup timer
            this.onRing = false;
            hashRing.removeNode(this.node);
            return true;
        }
        return false;
    }

    /**
     * Check if this node has been DEAD long enough to be removed from the membership list.
     */
    public synchronized boolean isCleanupReady(long cleanupTimeoutMs) {
        return state == State.DEAD
                && (System.currentTimeMillis() - localTimestamp > cleanupTimeoutMs);
    }
}
