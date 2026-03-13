package com.g20.CPEN431.A9.network;

import com.g20.CPEN431.A9.storage.ConsistentHashmap;
import com.g20.CPEN431.A9.storage.KeyValueStore;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.g20.CPEN431.A9.Constants.*;

/**
 * Pull-based key transfer service.
 *
 * Invariant: A recovering node always pulls keys from its successor(s).
 * Keys are only served by ACTIVE nodes. A non-ACTIVE node's successor
 * is always the authoritative source for its keys.
 *
 * Node states:
 *   JOINING  - just started, waiting for ring stability / successor readiness
 *   SYNCHING - actively pulling keys from ACTIVE successor(s)
 *   ACTIVE   - fully operational, responds to pull requests from predecessors
 *
 * Pull side (this node is recovering):
 *   Sends PULL_REQUEST to each successor. Successor responds with
 *   PULL_RESPONSE (key batch), PULL_COMPLETE (no more keys), or
 *   PULL_WAIT (successor not ACTIVE yet).
 *
 * Serve side (this node is an ACTIVE successor):
 *   Receives PULL_REQUEST from a predecessor. Scans KV store for keys
 *   belonging to the requester, sends batches, then PULL_COMPLETE.
 */
public class KeyTransferService {

    private static final int MAX_PACKET_PAYLOAD = 8192;

    public enum NodeState { JOINING, SYNCHING, ACTIVE }

    private final Node selfNode;
    private final DatagramSocket socket;
    private final ConsistentHashmap hashRing;
    private final GossipService gossipService;

    // Current node state
    private volatile NodeState state = NodeState.JOINING;

    // Pull side: per-successor state
    private final ConcurrentHashMap<Integer, PerSuccessorPullState> perSuccessorState = new ConcurrentHashMap<>();
    private volatile Set<Integer> currentSuccessorIds = new HashSet<>();

    // Serve side: per-predecessor state
    private final ConcurrentHashMap<Integer, PullServerState> perPredecessorState = new ConcurrentHashMap<>();
    private final Set<Integer> activePullClients = ConcurrentHashMap.newKeySet();

    // Keys already ACK'd by a predecessor — skip in future pull responses, pending timed deletion
    private final Set<ByteString> transferredKeys = ConcurrentHashMap.newKeySet();

    // Tombstone set: keys REMOVED locally during recovery
    private final Set<ByteString> tombstones = ConcurrentHashMap.newKeySet();

    private final Thread pullThread;
    private final ScheduledExecutorService scheduler;
    private final AtomicInteger batchSeqCounter = new AtomicInteger(0);

    private volatile boolean running = true;

    // ========================
    // Inner classes
    // ========================

    /**
     * Tracks pull progress from a single successor.
     */
    private static class PerSuccessorPullState {
        final int successorId;
        volatile Node successorNode;
        volatile int nextBatchSeq;
        volatile boolean complete;
        volatile boolean waitingForResponse;
        volatile long lastRequestTime;
        volatile int retryCount;
        final List<ByteString> receivedKeys = new ArrayList<>();

        PerSuccessorPullState(int successorId, Node successorNode) {
            this.successorId = successorId;
            this.successorNode = successorNode;
            this.nextBatchSeq = 0;
            this.complete = false;
            this.waitingForResponse = false;
            this.lastRequestTime = 0;
            this.retryCount = 0;
        }
    }

    /**
     * Tracks serve-side state for a single predecessor pulling from us.
     */
    private static class PullServerState {
        final int predecessorId;
        final Node predecessorNode;
        Iterator<Map.Entry<ByteString, byte[]>> iterator;
        final Set<Integer> pendingBatchSeqs = ConcurrentHashMap.newKeySet();
        final ConcurrentHashMap<Integer, List<ByteString>> sentKeysByBatch = new ConcurrentHashMap<>();
        // Raw data for unACK'd batches so we can resend on retry
        final ConcurrentHashMap<Integer, List<byte[]>> sentRawDataByBatch = new ConcurrentHashMap<>();
        final ConcurrentHashMap<Integer, List<byte[]>> sentKeyBytesByBatch = new ConcurrentHashMap<>();
        volatile int totalKeysSent;
        volatile boolean complete;
        volatile long lastRequestTime;

        PullServerState(int predecessorId, Node predecessorNode) {
            this.predecessorId = predecessorId;
            this.predecessorNode = predecessorNode;
            this.iterator = KeyValueStore.entrySet().iterator();
            this.totalKeysSent = 0;
            this.complete = false;
            this.lastRequestTime = System.currentTimeMillis();
        }
    }

    // ========================
    // Constructor and lifecycle
    // ========================

    private final boolean isSeed;

    public KeyTransferService(Node selfNode, DatagramSocket socket,
                              ConsistentHashmap hashRing, GossipService gossipService,
                              boolean isSeed) {
        this.selfNode = selfNode;
        this.socket = socket;
        this.hashRing = hashRing;
        this.gossipService = gossipService;
        this.isSeed = isSeed;

        this.pullThread = new Thread(this::pullLoop, "KeyTransfer-PullThread");
        this.pullThread.setDaemon(true);

        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
        exec.setRemoveOnCancelPolicy(true);
        this.scheduler = exec;
    }

    public void start() {
        if (isSeed) {
            System.out.println("[KeyTransfer] Seed mode: skipping key transfer, starting ACTIVE");
            state = NodeState.ACTIVE;
        }

        pullThread.start();

        // Schedule cleanup of stale pull server states
        scheduler.scheduleWithFixedDelay(this::cleanupStalePullServerStates,
                PULL_CLIENT_TIMEOUT_MS, PULL_CLIENT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        running = false;
        pullThread.interrupt();
        scheduler.shutdownNow();
    }

    // ========================
    // State queries (for Worker)
    // ========================

    public NodeState getState() {
        return state;
    }

    public boolean isRecovering() {
        return state != NodeState.ACTIVE;
    }

    /**
     * Whether this node is actively serving pull requests to the given predecessor.
     * Used by Worker for loop-breaking.
     */
    public boolean isActivePullClient(int nodeId) {
        return activePullClients.contains(nodeId);
    }

    /**
     * Get the recovery source for a key (successor that holds it).
     * Used by Worker to forward GET/REMOVE misses during SYNCHING.
     */
    public Node getRecoverySourceForKey(ByteString key) {
        if (state == NodeState.ACTIVE) return null;

        // Find which successor holds this key
        for (PerSuccessorPullState ps : perSuccessorState.values()) {
            if (ps.successorNode != null) {
                return ps.successorNode;
            }
        }
        // Fallback: use ring to find successor
        return hashRing.getSuccessorForKey(key, selfNode.id);
    }

    /**
     * Record that a key was REMOVED during recovery.
     */
    public void markRemovedDuringRecovery(ByteString key) {
        if (state != NodeState.ACTIVE) {
            tombstones.add(key);
        }
    }

    // ========================
    // Pull loop (this node is recovering)
    // ========================

    private void pullLoop() {
        while (running) {
            try {
                switch (state) {
                    case JOINING:
                        handleJoiningState();
                        break;
                    case SYNCHING:
                        handleSynchingState();
                        break;
                    case ACTIVE:
                        // Nothing to pull; wait until woken (shouldn't happen normally)
                        synchronized (this) {
                            if (state == NodeState.ACTIVE) {
                                this.wait(5000);
                            }
                        }
                        break;
                }
            } catch (InterruptedException e) {
                if (!running) break;
            } catch (Exception e) {
                System.err.println("[KeyTransfer] Error in pull loop: " + e.getMessage());
                try { Thread.sleep(PULL_INTERVAL_MS); } catch (InterruptedException ie) {
                    if (!running) break;
                }
            }
        }
    }

    private void handleJoiningState() throws InterruptedException {
        // Wait for ring to stabilize
        if (!gossipService.isRingStable()) {
            Thread.sleep(PULL_INTERVAL_MS);
            return;
        }

        // Identify our successors
        Set<Integer> successors = hashRing.getSuccessorNodeIds(selfNode);
        if (successors.isEmpty()) {
            // Only node in the ring
            System.out.println("[KeyTransfer] Only node in ring, transitioning to ACTIVE");
            transitionToActive();
            return;
        }

        // Initialize pull state for each successor
        currentSuccessorIds = new HashSet<>(successors);
        perSuccessorState.clear();
        for (int successorId : successors) {
            Node successorNode = findNodeById(successorId);
            if (successorNode != null) {
                perSuccessorState.put(successorId, new PerSuccessorPullState(successorId, successorNode));
            }
        }

        if (perSuccessorState.isEmpty()) {
            // Couldn't find successor nodes, retry
            Thread.sleep(PULL_INTERVAL_MS);
            return;
        }

        System.out.println("[KeyTransfer] JOINING -> SYNCHING, pulling from " + successors.size() + " successor(s): " + successors);
        state = NodeState.SYNCHING;

        // Send initial PULL_REQUEST to each successor
        for (PerSuccessorPullState ps : perSuccessorState.values()) {
            sendPullRequest(ps);
        }

        Thread.sleep(PULL_INTERVAL_MS);
    }

    private void handleSynchingState() throws InterruptedException {
        // Detect successor changes
        Set<Integer> newSuccessors = hashRing.getSuccessorNodeIds(selfNode);
        if (!newSuccessors.equals(currentSuccessorIds)) {
            handleSuccessorChange(currentSuccessorIds, newSuccessors);
            currentSuccessorIds = new HashSet<>(newSuccessors);
        }

        if (currentSuccessorIds.isEmpty()) {
            // Only node in ring now
            System.out.println("[KeyTransfer] No successors remaining, transitioning to ACTIVE");
            transitionToActive();
            return;
        }

        // Check progress and send pull requests as needed
        boolean allComplete = true;
        long now = System.currentTimeMillis();

        for (PerSuccessorPullState ps : perSuccessorState.values()) {
            if (ps.complete) continue;

            allComplete = false;

            if (ps.waitingForResponse) {
                // Check for timeout
                if (now - ps.lastRequestTime > PULL_REQUEST_TIMEOUT_MS) {
                    ps.retryCount++;
                    if (ps.retryCount > MAX_PULL_RETRIES) {
                        System.err.println("[KeyTransfer] Successor " + ps.successorId
                                + " unreachable after " + MAX_PULL_RETRIES + " retries, marking complete");
                        ps.complete = true;
                        continue;
                    }
                    System.out.println("[KeyTransfer] Pull request timeout for successor " + ps.successorId
                            + " (retry " + ps.retryCount + "/" + MAX_PULL_RETRIES + ")");
                    ps.waitingForResponse = false;
                    sendPullRequest(ps);
                }
            } else {
                // Send next pull request
                sendPullRequest(ps);
            }
        }

        if (allComplete) {
            transitionToActive();
        }

        Thread.sleep(PULL_INTERVAL_MS);
    }

    private void handleSuccessorChange(Set<Integer> oldSuccessors, Set<Integer> newSuccessors) {
        // Remove pull states for successors that are no longer our successors
        Set<Integer> removed = new HashSet<>(oldSuccessors);
        removed.removeAll(newSuccessors);
        for (int removedId : removed) {
            perSuccessorState.remove(removedId);
            System.out.println("[KeyTransfer] Successor " + removedId + " removed (ring changed)");
        }

        // Add pull states for new successors
        Set<Integer> added = new HashSet<>(newSuccessors);
        added.removeAll(oldSuccessors);
        for (int addedId : added) {
            Node successorNode = findNodeById(addedId);
            if (successorNode != null) {
                PerSuccessorPullState ps = new PerSuccessorPullState(addedId, successorNode);
                perSuccessorState.put(addedId, ps);
                System.out.println("[KeyTransfer] New successor " + addedId + " discovered, will pull from it");
            }
        }
    }

    /**
     * Reset to JOINING state (e.g. after SIGSTOP/SIGCONT detection).
     * Keeps existing KV data but re-runs the pull protocol to fetch any
     * keys that were written while this node was suspended.
     */
    public void resetToJoining() {
        System.out.println("[KeyTransfer] Resetting to JOINING (suspend/resume detected)");
        state = NodeState.JOINING;
        perSuccessorState.clear();
        currentSuccessorIds = new HashSet<>();
        tombstones.clear();
        batchSeqCounter.set(0);
        // Wake pull thread if it's waiting in ACTIVE state
        synchronized (this) {
            this.notifyAll();
        }
    }

    private void transitionToActive() {
        state = NodeState.ACTIVE;
        perSuccessorState.clear();
        currentSuccessorIds = new HashSet<>();
        tombstones.clear();
        System.out.println("[KeyTransfer] Transitioned to ACTIVE");
    }

    // ========================
    // Pull-side message sending
    // ========================

    private void sendPullRequest(PerSuccessorPullState ps) {
        int seq = batchSeqCounter.getAndIncrement();
        ps.nextBatchSeq = seq;
        ps.waitingForResponse = true;
        ps.lastRequestTime = System.currentTimeMillis();

        ByteBuffer buf = ByteBuffer.allocate(PULL_REQUEST_SIZE);
        buf.put(PULL_REQUEST_MAGIC);
        buf.putInt(selfNode.id);
        buf.putInt(seq);

        try {
            byte[] data = buf.array();
            socket.send(new DatagramPacket(data, data.length,
                    ps.successorNode.ipaddress, ps.successorNode.port));
        } catch (IOException e) {
            System.err.println("[KeyTransfer] Failed to send pull request to node " + ps.successorId);
        }
    }

    private void sendPullAck(int successorId, Node successorNode, int batchSeq) {
        ByteBuffer buf = ByteBuffer.allocate(TRANSFER_ACK_SIZE);
        buf.put(TRANSFER_ACK_MAGIC);
        buf.putInt(selfNode.id);
        buf.putInt(batchSeq);

        try {
            byte[] data = buf.array();
            socket.send(new DatagramPacket(data, data.length,
                    successorNode.ipaddress, successorNode.port));
        } catch (IOException e) {
            System.err.println("[KeyTransfer] Failed to send pull ACK to node " + successorId);
        }
    }

    // ========================
    // Pull-side response handlers (this node receives data from successor)
    // ========================

    /**
     * Handle PULL_RESPONSE: successor sent a batch of keys.
     * Packet format: KEY_TRANSFER_MAGIC(4) + senderId(4) + batchSeq(4) + numEntries(2) + entries
     */
    public void handlePullResponse(byte[] data, int length) {
        if (length < KEY_TRANSFER_HEADER_SIZE) return;
        if (state != NodeState.SYNCHING) return;

        ByteBuffer buf = ByteBuffer.wrap(data, 0, length);
        buf.position(4); // skip magic
        int senderId = buf.getInt();
        int batchSeq = buf.getInt();
        int numEntries = buf.getShort() & 0xFFFF;

        PerSuccessorPullState ps = perSuccessorState.get(senderId);
        if (ps == null) return; // unknown successor or ring changed

        int stored = 0;
        int skipped = 0;
        for (int i = 0; i < numEntries; i++) {
            if (buf.remaining() < 2) break;
            int keyLen = buf.getShort() & 0xFFFF;
            if (buf.remaining() < keyLen) break;
            byte[] keyBytes = new byte[keyLen];
            buf.get(keyBytes);

            if (buf.remaining() < 4) break;
            int rawDataLen = buf.getInt();
            if (buf.remaining() < rawDataLen) break;
            byte[] rawData = new byte[rawDataLen];
            buf.get(rawData);

            ByteString key = ByteString.copyFrom(keyBytes);

            if (tombstones.contains(key)) {
                skipped++;
                continue;
            }

            if (KeyValueStore.putIfAbsentRawBytes(key, rawData)) {
                stored++;
            } else {
                skipped++;
            }
        }

        System.out.println("[KeyTransfer] Pull response from node " + senderId
                + " batch " + batchSeq + ": stored=" + stored + ", skipped=" + skipped);

        ps.waitingForResponse = false;
        ps.retryCount = 0;

        // Send ACK
        sendPullAck(senderId, ps.successorNode, batchSeq);
    }

    /**
     * Handle PULL_COMPLETE: successor has no more keys for us.
     * Packet format: TRANSFER_COMPLETE_MAGIC(4) + senderId(4) + totalKeys(4)
     */
    public void handlePullComplete(byte[] data, int length) {
        if (length < TRANSFER_COMPLETE_SIZE) return;

        ByteBuffer buf = ByteBuffer.wrap(data, 0, length);
        buf.position(4);
        int senderId = buf.getInt();
        int totalKeys = buf.getInt();

        System.out.println("[KeyTransfer] Pull complete from successor " + senderId
                + ", total=" + totalKeys + " keys");

        PerSuccessorPullState ps = perSuccessorState.get(senderId);
        if (ps != null) {
            ps.complete = true;
            ps.waitingForResponse = false;
        }

        // Check if all successors complete
        if (allSuccessorsComplete()) {
            transitionToActive();
        }
    }

    /**
     * Handle PULL_WAIT: successor is not ACTIVE yet, retry later.
     * Packet format: TRANSFER_PENDING_MAGIC(4) + senderId(4)
     */
    public void handlePullWait(byte[] data, int length) {
        if (length < TRANSFER_PENDING_SIZE) return;

        ByteBuffer buf = ByteBuffer.wrap(data, 0, length);
        buf.position(4);
        int senderId = buf.getInt();

        PerSuccessorPullState ps = perSuccessorState.get(senderId);
        if (ps != null) {
            ps.waitingForResponse = false;
            ps.retryCount = 0; // Reset retry count since successor is alive, just not ready
            System.out.println("[KeyTransfer] Successor " + senderId + " not ACTIVE, will retry");
        }
    }

    private boolean allSuccessorsComplete() {
        for (PerSuccessorPullState ps : perSuccessorState.values()) {
            if (!ps.complete) return false;
        }
        return !perSuccessorState.isEmpty() || currentSuccessorIds.isEmpty();
    }

    // ========================
    // Serve side (this node responds to predecessors' pull requests)
    // ========================

    /**
     * Handle incoming PULL_REQUEST from a predecessor.
     * Packet format: PULL_REQUEST_MAGIC(4) + senderId(4) + batchSeq(4)
     */
    public void handlePullRequest(byte[] data, int length) {
        if (length < PULL_REQUEST_SIZE) return;

        ByteBuffer buf = ByteBuffer.wrap(data, 0, length);
        buf.position(4); // skip magic
        int requesterId = buf.getInt();
        int batchSeq = buf.getInt();

        // If we're not ACTIVE, tell requester to wait
        if (state != NodeState.ACTIVE) {
            Node requesterNode = findNodeById(requesterId);
            if (requesterNode != null) {
                sendPullWait(requesterNode);
            }
            return;
        }

        // Verify the requester is actually our predecessor on the ring.
        // If not, our ring views haven't converged yet — tell them to wait.
        if (!hashRing.getPredecessorNodeIds(selfNode).contains(requesterId)) {
            Node requesterNode = findNodeById(requesterId);
            if (requesterNode != null) {
                System.out.println("[KeyTransfer] Node " + requesterId
                        + " is not our predecessor, sending PULL_WAIT");
                sendPullWait(requesterNode);
            }
            return;
        }

        // Get or create server state for this predecessor
        PullServerState serverState = perPredecessorState.computeIfAbsent(requesterId, id -> {
            Node requesterNode = findNodeById(id);
            if (requesterNode == null) return null;
            System.out.println("[KeyTransfer] New pull client: node " + id);
            return new PullServerState(id, requesterNode);
        });

        if (serverState == null) {
            System.err.println("[KeyTransfer] Cannot serve pull request: node " + requesterId + " not found");
            return;
        }

        serverState.lastRequestTime = System.currentTimeMillis();
        activePullClients.add(requesterId);

        // If there are unACK'd batches, resend the oldest one instead of
        // advancing the iterator. This prevents keys from being skipped
        // when a PULL_RESPONSE packet is lost over UDP.
        if (!serverState.pendingBatchSeqs.isEmpty()) {
            int oldestPendingSeq = serverState.pendingBatchSeqs.iterator().next();
            List<byte[]> oldKeys = serverState.sentKeyBytesByBatch.get(oldestPendingSeq);
            List<byte[]> oldRawData = serverState.sentRawDataByBatch.get(oldestPendingSeq);
            if (oldKeys != null && oldRawData != null) {
                // Resend the unACK'd batch under the new batchSeq
                serverState.pendingBatchSeqs.remove(oldestPendingSeq);
                List<ByteString> oldByteStringKeys = serverState.sentKeysByBatch.remove(oldestPendingSeq);
                serverState.sentKeyBytesByBatch.remove(oldestPendingSeq);
                serverState.sentRawDataByBatch.remove(oldestPendingSeq);

                sendPullResponse(serverState.predecessorNode, batchSeq, oldKeys, oldRawData);
                serverState.pendingBatchSeqs.add(batchSeq);
                if (oldByteStringKeys != null) {
                    serverState.sentKeysByBatch.put(batchSeq, oldByteStringKeys);
                }
                serverState.sentKeyBytesByBatch.put(batchSeq, oldKeys);
                serverState.sentRawDataByBatch.put(batchSeq, oldRawData);
                System.out.println("[KeyTransfer] Resent " + oldKeys.size() + " unACK'd keys to node "
                        + requesterId + " (old batch " + oldestPendingSeq + " -> new batch " + batchSeq + ")");
                return;
            }
        }

        // Collect a batch of keys belonging to the requester
        TreeMap<Integer, Node> snapshot = hashRing.getRingSnapshot();
        List<byte[]> batchKeys = new ArrayList<>();
        List<byte[]> batchRawData = new ArrayList<>();
        List<ByteString> batchByteStringKeys = new ArrayList<>();
        int batchSize = 0;

        while (serverState.iterator.hasNext()) {
            Map.Entry<ByteString, byte[]> entry = serverState.iterator.next();
            ByteString key = entry.getKey();
            byte[] rawData = entry.getValue();

            // Skip keys already transferred and pending deletion
            if (transferredKeys.contains(key)) continue;

            // Send all keys we don't own — covers requester and any predecessors in a recovery chain
            Node owner = ConsistentHashmap.getNodeForKeyFromSnapshot(key.toByteArray(), snapshot);
            if (owner != null && owner.id != selfNode.id) {
                byte[] keyBytes = key.toByteArray();
                int entrySize = 2 + keyBytes.length + 4 + rawData.length;

                if (batchSize + entrySize > MAX_PACKET_PAYLOAD - KEY_TRANSFER_HEADER_SIZE
                        && !batchKeys.isEmpty()) {
                    // Batch is full, send what we have
                    break;
                }

                batchKeys.add(keyBytes);
                batchRawData.add(rawData);
                batchByteStringKeys.add(key);
                batchSize += entrySize;
            }
        }

        if (!batchKeys.isEmpty()) {
            // Send batch and track which keys were in it
            sendPullResponse(serverState.predecessorNode, batchSeq, batchKeys, batchRawData);
            serverState.pendingBatchSeqs.add(batchSeq);
            serverState.sentKeysByBatch.put(batchSeq, batchByteStringKeys);
            serverState.sentKeyBytesByBatch.put(batchSeq, batchKeys);
            serverState.sentRawDataByBatch.put(batchSeq, batchRawData);
            serverState.totalKeysSent += batchKeys.size();
            System.out.println("[KeyTransfer] Sent " + batchKeys.size() + " keys to node "
                    + requesterId + " (batch " + batchSeq + ")");
        } else {
            // No more keys - send complete
            serverState.complete = true;
            sendPullComplete(serverState.predecessorNode, serverState.totalKeysSent);
            System.out.println("[KeyTransfer] Pull complete for node " + requesterId
                    + ", total " + serverState.totalKeysSent + " keys sent");

            // If no pending ACKs, finalize immediately
            if (serverState.pendingBatchSeqs.isEmpty()) {
                finalizePullServe(requesterId);
            }
        }
    }

    /**
     * Handle incoming PULL_ACK from a predecessor.
     * Packet format: TRANSFER_ACK_MAGIC(4) + senderId(4) + batchSeq(4)
     */
    public void handlePullAck(byte[] data, int length) {
        if (length < TRANSFER_ACK_SIZE) return;

        ByteBuffer buf = ByteBuffer.wrap(data, 0, length);
        buf.position(4); // skip magic
        int senderId = buf.getInt();
        int batchSeq = buf.getInt();

        PullServerState serverState = perPredecessorState.get(senderId);
        if (serverState == null) return;

        serverState.pendingBatchSeqs.remove(batchSeq);
        serverState.sentKeyBytesByBatch.remove(batchSeq);
        serverState.sentRawDataByBatch.remove(batchSeq);

        // Mark ACK'd keys as transferred — they won't be sent to other predecessors
        // and will be deleted after the hold timer
        List<ByteString> ackedKeys = serverState.sentKeysByBatch.remove(batchSeq);
        if (ackedKeys != null && !ackedKeys.isEmpty()) {
            transferredKeys.addAll(ackedKeys);
            scheduleKeyDeletion(ackedKeys);
        }

        // If complete and all ACKs received, finalize
        if (serverState.complete && serverState.pendingBatchSeqs.isEmpty()) {
            finalizePullServe(senderId);
        }
    }

    private void finalizePullServe(int predecessorId) {
        perPredecessorState.remove(predecessorId);
        activePullClients.remove(predecessorId);
    }

    // ========================
    // Serve-side message sending
    // ========================

    private void sendPullResponse(Node target, int batchSeq, List<byte[]> keys, List<byte[]> rawDataList) {
        int totalSize = KEY_TRANSFER_HEADER_SIZE;
        for (int i = 0; i < keys.size(); i++) {
            totalSize += 2 + keys.get(i).length + 4 + rawDataList.get(i).length;
        }

        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.put(KEY_TRANSFER_MAGIC);
        buf.putInt(selfNode.id);
        buf.putInt(batchSeq);
        buf.putShort((short) keys.size());

        for (int i = 0; i < keys.size(); i++) {
            buf.putShort((short) keys.get(i).length);
            buf.put(keys.get(i));
            buf.putInt(rawDataList.get(i).length);
            buf.put(rawDataList.get(i));
        }

        try {
            byte[] data = buf.array();
            socket.send(new DatagramPacket(data, data.length, target.ipaddress, target.port));
        } catch (IOException e) {
            System.err.println("[KeyTransfer] Failed to send pull response to node " + target.id);
        }
    }

    private void sendPullComplete(Node target, int totalKeys) {
        ByteBuffer buf = ByteBuffer.allocate(TRANSFER_COMPLETE_SIZE);
        buf.put(TRANSFER_COMPLETE_MAGIC);
        buf.putInt(selfNode.id);
        buf.putInt(totalKeys);

        try {
            byte[] data = buf.array();
            socket.send(new DatagramPacket(data, data.length, target.ipaddress, target.port));
        } catch (IOException e) {
            System.err.println("[KeyTransfer] Failed to send pull complete to node " + target.id);
        }
    }

    private void sendPullWait(Node target) {
        ByteBuffer buf = ByteBuffer.allocate(TRANSFER_PENDING_SIZE);
        buf.put(TRANSFER_PENDING_MAGIC);
        buf.putInt(selfNode.id);

        try {
            byte[] data = buf.array();
            socket.send(new DatagramPacket(data, data.length, target.ipaddress, target.port));
        } catch (IOException e) {
            // Best effort
        }
    }

    // ========================
    // Key cleanup
    // ========================

    /**
     * Schedule deletion of specific ACK'd keys after hold time.
     * Keys remain in transferredKeys set (so they're not re-sent) until deleted.
     */
    private void scheduleKeyDeletion(List<ByteString> keys) {
        scheduler.schedule(() -> {
            for (ByteString key : keys) {
                KeyValueStore.remove(key);
                transferredKeys.remove(key);
            }
        }, TRANSFER_HOLD_TIME_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Clean up stale pull server states (predecessor stopped pulling).
     */
    private void cleanupStalePullServerStates() {
        long now = System.currentTimeMillis();
        List<Integer> stale = new ArrayList<>();
        for (Map.Entry<Integer, PullServerState> entry : perPredecessorState.entrySet()) {
            if (now - entry.getValue().lastRequestTime > PULL_CLIENT_TIMEOUT_MS) {
                stale.add(entry.getKey());
            }
        }
        for (int id : stale) {
            perPredecessorState.remove(id);
            activePullClients.remove(id);
            System.out.println("[KeyTransfer] Cleaned up stale pull server state for node " + id);
        }
    }

    // ========================
    // Helpers
    // ========================

    private Node findNodeById(int nodeId) {
        for (Node n : hashRing.getAllNodes()) {
            if (n.id == nodeId) return n;
        }
        return null;
    }

    // ========================
    // Packet detection helpers
    // ========================

    public static boolean isPullRequestMessage(byte[] data, int length) {
        if (length < PULL_REQUEST_SIZE) return false;
        return data[0] == PULL_REQUEST_MAGIC[0]
                && data[1] == PULL_REQUEST_MAGIC[1]
                && data[2] == PULL_REQUEST_MAGIC[2]
                && data[3] == PULL_REQUEST_MAGIC[3];
    }

    public static boolean isPullResponseMessage(byte[] data, int length) {
        if (length < KEY_TRANSFER_HEADER_SIZE) return false;
        return data[0] == KEY_TRANSFER_MAGIC[0]
                && data[1] == KEY_TRANSFER_MAGIC[1]
                && data[2] == KEY_TRANSFER_MAGIC[2]
                && data[3] == KEY_TRANSFER_MAGIC[3];
    }

    public static boolean isPullCompleteMessage(byte[] data, int length) {
        if (length < TRANSFER_COMPLETE_SIZE) return false;
        return data[0] == TRANSFER_COMPLETE_MAGIC[0]
                && data[1] == TRANSFER_COMPLETE_MAGIC[1]
                && data[2] == TRANSFER_COMPLETE_MAGIC[2]
                && data[3] == TRANSFER_COMPLETE_MAGIC[3];
    }

    public static boolean isPullAckMessage(byte[] data, int length) {
        if (length < TRANSFER_ACK_SIZE) return false;
        return data[0] == TRANSFER_ACK_MAGIC[0]
                && data[1] == TRANSFER_ACK_MAGIC[1]
                && data[2] == TRANSFER_ACK_MAGIC[2]
                && data[3] == TRANSFER_ACK_MAGIC[3];
    }

    public static boolean isPullWaitMessage(byte[] data, int length) {
        if (length < TRANSFER_PENDING_SIZE) return false;
        return data[0] == TRANSFER_PENDING_MAGIC[0]
                && data[1] == TRANSFER_PENDING_MAGIC[1]
                && data[2] == TRANSFER_PENDING_MAGIC[2]
                && data[3] == TRANSFER_PENDING_MAGIC[3];
    }
}
