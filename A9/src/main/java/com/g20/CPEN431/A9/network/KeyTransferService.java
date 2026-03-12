package com.g20.CPEN431.A9.network;

import com.g20.CPEN431.A9.storage.ConsistentHashmap;
import com.g20.CPEN431.A9.storage.KeyValueStore;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.g20.CPEN431.A9.Constants.*;

/**
 * Handles key transfer when a node rejoins the cluster.
 *
 * Sender side: when gossip detects a predecessor rejoined, iterates the KV store
 * and sends keys that now map to the rejoined node. Uses ACK-based reliability:
 * each batch gets a unique ID, and the receiver ACKs each batch. Only ACK'd
 * keys are scheduled for deletion. Un-ACK'd batches are retried.
 *
 * Receiver side: accepts key transfer packets, stores entries (put-if-absent),
 * sends ACKs back, and enters recovery mode (forwarding GET/REMOVE misses
 * to the recovery source).
 *
 * Conflict resolution:
 * - put-if-absent: local PUTs during recovery always win over transfer data
 * - Tombstone set: REMOVEs seen during recovery prevent transfer from resurrecting keys
 * - Loop-breaking: successor processes locally when the recovering node forwards to it
 */
public class KeyTransferService {

    private static final int MAX_PACKET_PAYLOAD = 8192;

    private final Node selfNode;
    private final DatagramSocket socket;
    private final ConsistentHashmap hashRing;

    // Sender side
    private final ConcurrentLinkedQueue<Node> transferQueue = new ConcurrentLinkedQueue<>();
    private final Thread transferThread;
    private final ScheduledExecutorService scheduler;
    private final AtomicInteger batchIdCounter = new AtomicInteger(0);

    // Pending batches awaiting ACK, keyed by batchId
    private final ConcurrentHashMap<Integer, PendingBatch> pendingBatches = new ConcurrentHashMap<>();

    // Tracks nodes we are actively transferring to (or recently transferred to, during hold time).
    // Used by Worker for loop-breaking: if a forwarded request's senderNodeId is in this set,
    // and we have the key, process locally instead of forwarding back to the recovering node.
    private final Set<Integer> activeTransferTargets = ConcurrentHashMap.newKeySet();

    // Per-target tracking: how many batches were sent and how many have been ACK'd
    private final ConcurrentHashMap<Integer, TransferState> transferStates = new ConcurrentHashMap<>();

    // Receiver side: two-phase recovery
    private volatile boolean awaitingRecovery = true;
    private volatile boolean recovering = false;
    private volatile boolean recoveryCompleted = false;
    private volatile Node recoverySource = null;
    private volatile java.util.concurrent.ScheduledFuture<?> awaitingRecoveryTimeout;

    // Tombstone set: keys that were REMOVED locally during recovery.
    // Transfer data for tombstoned keys is skipped to prevent resurrecting deleted keys.
    private final Set<ByteString> tombstones = ConcurrentHashMap.newKeySet();

    private volatile boolean running = true;

    /**
     * Holds info about a batch that has been sent but not yet ACK'd.
     */
    private static class PendingBatch {
        final Node targetNode;
        final List<ByteString> keyStrings;
        final byte[] packetData;
        int retryCount;

        PendingBatch(Node targetNode, List<ByteString> keyStrings, byte[] packetData) {
            this.targetNode = targetNode;
            this.keyStrings = keyStrings;
            this.packetData = packetData;
            this.retryCount = 0;
        }
    }

    /**
     * Tracks the state of a transfer to a specific target node.
     */
    private static class TransferState {
        final Node target;
        final Set<Integer> pendingBatchIds = ConcurrentHashMap.newKeySet();
        final List<ByteString> ackedKeys = new ArrayList<>();
        volatile boolean finalized = false;
        volatile java.util.concurrent.ScheduledFuture<?> retryFuture;

        TransferState(Node target) {
            this.target = target;
        }
    }

    public KeyTransferService(Node selfNode, DatagramSocket socket, ConsistentHashmap hashRing) {
        this.selfNode = selfNode;
        this.socket = socket;
        this.hashRing = hashRing;

        this.transferThread = new Thread(this::transferLoop, "KeyTransfer-Thread");
        this.transferThread.setDaemon(true);

        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
        exec.setRemoveOnCancelPolicy(true);
        this.scheduler = exec;
    }

    public void start() {
        transferThread.start();

        // Schedule timeout: if no transfer arrives, assume clean bootstrap
        awaitingRecoveryTimeout = scheduler.schedule(() -> {
            if (awaitingRecovery) {
                awaitingRecovery = false;
                recoveryCompleted = true;
                System.out.println("[KeyTransfer] Awaiting-recovery timeout — assuming clean bootstrap");
            }
        }, AWAITING_RECOVERY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        running = false;
        transferThread.interrupt();
        scheduler.shutdownNow();
    }

    // ========================
    // Sender side
    // ========================

    public void onNodeRejoined(Node node) {
        if (node.id == selfNode.id) return;

        // Only the successor(s) of the rejoined node should transfer keys.
        // These are the nodes that absorbed the rejoined node's key range while it was down.
        if (!hashRing.getSuccessorNodeIds(node).contains(selfNode.id)) return;

        System.out.println("[KeyTransfer] Node " + node.id + " rejoined, queuing transfer (we are successor)");
        activeTransferTargets.add(node.id);
        transferQueue.add(node);
        synchronized (transferQueue) {
            transferQueue.notify();
        }
    }

    private void transferLoop() {
        while (running) {
            try {
                Node target = transferQueue.poll();
                if (target == null) {
                    synchronized (transferQueue) {
                        target = transferQueue.poll();
                        if (target == null) {
                            transferQueue.wait();
                            continue;
                        }
                    }
                }

                // Wait if we ourselves are recovering, notify target to keep waiting
                while ((recovering || awaitingRecovery) && running) {
                    sendTransferPending(target);
                    Thread.sleep(500);
                }

                if (running) {
                    executeTransfer(target);
                }
            } catch (InterruptedException e) {
                if (!running) break;
            } catch (Exception e) {
                System.err.println("[KeyTransfer] Error in transfer loop: " + e.getMessage());
            }
        }
    }

    private void executeTransfer(Node target) {
        TreeMap<Integer, Node> snapshot = hashRing.getRingSnapshot();

        List<byte[]> batchKeys = new ArrayList<>();
        List<byte[]> batchRawData = new ArrayList<>();
        List<ByteString> batchKeyStrings = new ArrayList<>();
        int batchSize = 0;

        // Collect all batch IDs for this target
        TransferState state = new TransferState(target);
        transferStates.put(target.id, state);

        int totalKeys = 0;

        for (Map.Entry<ByteString, byte[]> entry : KeyValueStore.entrySet()) {
            ByteString key = entry.getKey();
            byte[] rawData = entry.getValue();

            // Send all keys we don't own, not just keys mapping to the target.
            // In cascading failures (A dies, B dies, C absorbs both), C needs to
            // send A's keys to B so B can forward them to A through the successor chain.
            Node owner = ConsistentHashmap.getNodeForKeyFromSnapshot(key.toByteArray(), snapshot);
            if (owner != null && owner.id != selfNode.id) {
                byte[] keyBytes = key.toByteArray();
                int entrySize = 2 + keyBytes.length + 4 + rawData.length;

                if (batchSize + entrySize > MAX_PACKET_PAYLOAD - KEY_TRANSFER_HEADER_SIZE
                        && !batchKeys.isEmpty()) {
                    int batchId = batchIdCounter.getAndIncrement();
                    sendAndTrackBatch(target, batchId, batchKeys, batchRawData, batchKeyStrings, state);
                    totalKeys += batchKeys.size();
                    batchKeys = new ArrayList<>();
                    batchRawData = new ArrayList<>();
                    batchKeyStrings = new ArrayList<>();
                    batchSize = 0;
                }

                batchKeys.add(keyBytes);
                batchRawData.add(rawData);
                batchKeyStrings.add(key);
                batchSize += entrySize;
            }
        }

        if (!batchKeys.isEmpty()) {
            int batchId = batchIdCounter.getAndIncrement();
            sendAndTrackBatch(target, batchId, batchKeys, batchRawData, batchKeyStrings, state);
            totalKeys += batchKeys.size();
        }

        System.out.println("[KeyTransfer] Sent " + totalKeys + " keys in "
                + state.pendingBatchIds.size() + " batches to node " + target.id);

        if (state.pendingBatchIds.isEmpty()) {
            // No keys to transfer — send complete immediately
            sendTransferComplete(target, 0);
            scheduler.schedule(() -> activeTransferTargets.remove(target.id),
                    RECOVERY_GRACE_PERIOD_MS, TimeUnit.MILLISECONDS);
            transferStates.remove(target.id);
        } else {
            // Start retry scheduler for un-ACK'd batches
            scheduleRetries(target.id);
        }
    }

    private void sendAndTrackBatch(Node target, int batchId, List<byte[]> keys,
                                   List<byte[]> rawDataList, List<ByteString> keyStrings,
                                   TransferState state) {
        byte[] packetData = buildKeyTransferPacket(batchId, keys, rawDataList);

        PendingBatch batch = new PendingBatch(target, new ArrayList<>(keyStrings), packetData);

        pendingBatches.put(batchId, batch);
        state.pendingBatchIds.add(batchId);

        try {
            socket.send(new DatagramPacket(packetData, packetData.length,
                    target.ipaddress, target.port));
        } catch (IOException e) {
            System.err.println("[KeyTransfer] Failed to send batch " + batchId
                    + " to node " + target.id);
        }
    }

    private byte[] buildKeyTransferPacket(int batchId, List<byte[]> keys, List<byte[]> rawDataList) {
        int totalSize = KEY_TRANSFER_HEADER_SIZE;
        for (int i = 0; i < keys.size(); i++) {
            totalSize += 2 + keys.get(i).length + 4 + rawDataList.get(i).length;
        }

        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.put(KEY_TRANSFER_MAGIC);
        buf.putInt(selfNode.id);
        buf.putInt(batchId);
        buf.putShort((short) keys.size());

        for (int i = 0; i < keys.size(); i++) {
            buf.putShort((short) keys.get(i).length);
            buf.put(keys.get(i));
            buf.putInt(rawDataList.get(i).length);
            buf.put(rawDataList.get(i));
        }

        return buf.array();
    }

    private void scheduleRetries(int targetNodeId) {
        TransferState ts = transferStates.get(targetNodeId);
        java.util.concurrent.ScheduledFuture<?> future = scheduler.scheduleWithFixedDelay(() -> {
            TransferState state = transferStates.get(targetNodeId);
            if (state == null || state.finalized) return;

            List<Integer> toRemove = new ArrayList<>();
            for (int batchId : state.pendingBatchIds) {
                PendingBatch batch = pendingBatches.get(batchId);
                if (batch == null) {
                    // Already ACK'd
                    toRemove.add(batchId);
                    continue;
                }

                batch.retryCount++;
                if (batch.retryCount > MAX_TRANSFER_RETRIES) {
                    System.err.println("[KeyTransfer] Batch " + batchId
                            + " exceeded max retries, giving up (keys stay on sender)");
                    pendingBatches.remove(batchId);
                    toRemove.add(batchId);
                    continue;
                }

                System.out.println("[KeyTransfer] Retrying batch " + batchId
                        + " (attempt " + batch.retryCount + "/" + MAX_TRANSFER_RETRIES + ")");
                try {
                    socket.send(new DatagramPacket(batch.packetData, batch.packetData.length,
                            batch.targetNode.ipaddress, batch.targetNode.port));
                } catch (IOException e) {
                    System.err.println("[KeyTransfer] Failed to resend batch " + batchId);
                }
            }

            for (int id : toRemove) {
                state.pendingBatchIds.remove(id);
            }

            // Check if all batches are resolved (ACK'd or given up)
            if (state.pendingBatchIds.isEmpty()) {
                finalizeTransfer(targetNodeId);
            }
        }, TRANSFER_RETRY_INTERVAL_MS, TRANSFER_RETRY_INTERVAL_MS, TimeUnit.MILLISECONDS);
        if (ts != null) {
            ts.retryFuture = future;
        }
    }

    /**
     * Handle an incoming transfer ACK from the receiver.
     * Packet format: TRANSFER_ACK_MAGIC(4) + receiverId(4) + batchId(4)
     */
    public void handleTransferAck(byte[] data, int length) {
        if (length < TRANSFER_ACK_SIZE) return;

        ByteBuffer buf = ByteBuffer.wrap(data, 0, length);
        buf.position(4); // skip magic
        int receiverId = buf.getInt();
        int batchId = buf.getInt();

        PendingBatch batch = pendingBatches.remove(batchId);
        if (batch == null) {
            // Duplicate ACK or already timed out — ignore
            return;
        }

        System.out.println("[KeyTransfer] ACK received for batch " + batchId
                + " from node " + receiverId);

        TransferState state = transferStates.get(receiverId);
        if (state != null) {
            synchronized (state.ackedKeys) {
                state.ackedKeys.addAll(batch.keyStrings);
            }
            state.pendingBatchIds.remove(batchId);

            // Check if all batches for this target are done
            if (state.pendingBatchIds.isEmpty()) {
                finalizeTransfer(receiverId);
            }
        }
    }

    private synchronized void finalizeTransfer(int targetNodeId) {
        TransferState state = transferStates.get(targetNodeId);
        if (state == null || state.finalized) return;
        state.finalized = true;

        if (state.retryFuture != null) {
            state.retryFuture.cancel(false);
        }

        List<ByteString> ackedKeys;
        synchronized (state.ackedKeys) {
            ackedKeys = new ArrayList<>(state.ackedKeys);
        }

        sendTransferComplete(state.target, ackedKeys.size());

        System.out.println("[KeyTransfer] All batches resolved for node " + targetNodeId
                + ", " + ackedKeys.size() + " keys confirmed");

        if (!ackedKeys.isEmpty()) {
            scheduleKeyDeletion(ackedKeys, targetNodeId);
        } else {
            scheduler.schedule(() -> activeTransferTargets.remove(targetNodeId),
                    RECOVERY_GRACE_PERIOD_MS, TimeUnit.MILLISECONDS);
        }

        transferStates.remove(targetNodeId);
    }

    private void sendTransferComplete(Node target, int totalKeys) {
        ByteBuffer buf = ByteBuffer.allocate(TRANSFER_COMPLETE_SIZE);
        buf.put(TRANSFER_COMPLETE_MAGIC);
        buf.putInt(selfNode.id);
        buf.putInt(totalKeys);

        try {
            byte[] data = buf.array();
            socket.send(new DatagramPacket(data, data.length, target.ipaddress, target.port));
        } catch (IOException e) {
            System.err.println("[KeyTransfer] Failed to send transfer complete to node " + target.id);
        }
    }

    private void scheduleKeyDeletion(List<ByteString> keys, int targetNodeId) {
        scheduler.schedule(() -> {
            int deleted = 0;
            for (ByteString key : keys) {
                Node owner = hashRing.getNodeForKey(key);
                if (owner != null && owner.id != selfNode.id) {
                    KeyValueStore.remove(key);
                    deleted++;
                }
            }
            activeTransferTargets.remove(targetNodeId);
            System.out.println("[KeyTransfer] Cleaned up " + deleted + " transferred keys for node " + targetNodeId);
        }, TRANSFER_HOLD_TIME_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * One-time cleanup after recovery ends and outgoing transfers complete.
     * Removes keys we received during recovery that don't belong to us
     * (e.g., keys for nodes further back in the chain during cascading failures).
     * Waits for all active outgoing transfers to finish first, so we don't
     * delete keys we still need to forward.
     */
    private void schedulePostRecoveryCleanup() {
        scheduler.schedule(() -> {
            if (!activeTransferTargets.isEmpty()) {
                // Outgoing transfers still in progress — reschedule
                schedulePostRecoveryCleanup();
                return;
            }
            int deleted = 0;
            for (Map.Entry<ByteString, byte[]> entry : KeyValueStore.entrySet()) {
                Node owner = hashRing.getNodeForKey(entry.getKey());
                if (owner != null && owner.id != selfNode.id) {
                    KeyValueStore.remove(entry.getKey());
                    deleted++;
                }
            }
            if (deleted > 0) {
                System.out.println("[KeyTransfer] Post-recovery cleanup removed " + deleted + " stale keys");
            }
        }, TRANSFER_HOLD_TIME_MS, TimeUnit.MILLISECONDS);
    }

    // ========================
    // Receiver side
    // ========================

    /**
     * Handle an incoming key transfer packet.
     * Skips keys that already exist locally (put-if-absent) or are tombstoned.
     * Sends an ACK back to the sender after processing.
     */
    public void handleKeyTransferPacket(byte[] data, int length) {
        if (length < KEY_TRANSFER_HEADER_SIZE) return;

        ByteBuffer buf = ByteBuffer.wrap(data, 0, length);
        buf.position(4); // skip magic
        int senderId = buf.getInt();
        int batchId = buf.getInt();
        int numEntries = buf.getShort() & 0xFFFF;

        // Transition: awaitingRecovery -> recovering (only once)
        if (!recovering && !recoveryCompleted) {
            for (Node n : hashRing.getAllNodes()) {
                if (n.id == senderId) {
                    recoverySource = n;
                    break;
                }
            }
            awaitingRecovery = false;
            recovering = true;
            System.out.println("[KeyTransfer] Entering recovery mode, source=node " + senderId);
        }

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

            // Skip tombstoned keys (removed locally during recovery)
            if (tombstones.contains(key)) {
                skipped++;
                continue;
            }

            // put-if-absent: local writes win over transfer data
            if (KeyValueStore.putIfAbsentRawBytes(key, rawData)) {
                stored++;
            } else {
                skipped++;
            }
        }

        System.out.println("[KeyTransfer] Received batch " + batchId + ": " + (stored + skipped)
                + " keys from node " + senderId + " (stored=" + stored + ", skipped=" + skipped + ")");

        // Send ACK back to sender
        sendTransferAck(senderId, batchId);
    }

    /**
     * Send a transfer ACK back to the sender node.
     * Packet format: TRANSFER_ACK_MAGIC(4) + receiverId(4) + batchId(4)
     */
    private void sendTransferAck(int senderId, int batchId) {
        Node senderNode = null;
        for (Node n : hashRing.getAllNodes()) {
            if (n.id == senderId) {
                senderNode = n;
                break;
            }
        }
        if (senderNode == null) {
            System.err.println("[KeyTransfer] Cannot ACK batch " + batchId
                    + ": sender node " + senderId + " not found");
            return;
        }

        ByteBuffer buf = ByteBuffer.allocate(TRANSFER_ACK_SIZE);
        buf.put(TRANSFER_ACK_MAGIC);
        buf.putInt(selfNode.id);
        buf.putInt(batchId);

        try {
            byte[] data = buf.array();
            socket.send(new DatagramPacket(data, data.length,
                    senderNode.ipaddress, senderNode.port));
        } catch (IOException e) {
            System.err.println("[KeyTransfer] Failed to send ACK for batch " + batchId);
        }
    }

    /**
     * Handle an incoming transfer-pending notification.
     * The sender is our successor, still recovering itself — it will send our keys soon.
     * Resets the awaiting-recovery timeout so we don't assume a clean bootstrap.
     */
    public void handleTransferPending(byte[] data, int length) {
        if (length < TRANSFER_PENDING_SIZE) return;
        if (!awaitingRecovery) return;

        ByteBuffer buf = ByteBuffer.wrap(data, 0, length);
        buf.position(4); // skip magic
        int senderId = buf.getInt();

        // Reset the awaiting-recovery timeout
        java.util.concurrent.ScheduledFuture<?> existing = awaitingRecoveryTimeout;
        if (existing != null) {
            existing.cancel(false);
        }
        awaitingRecoveryTimeout = scheduler.schedule(() -> {
            if (awaitingRecovery) {
                awaitingRecovery = false;
                System.out.println("[KeyTransfer] Awaiting-recovery timeout — assuming clean bootstrap");
            }
        }, AWAITING_RECOVERY_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        System.out.println("[KeyTransfer] Transfer pending from node " + senderId + ", timer reset");
    }

    public void handleTransferComplete(byte[] data, int length) {
        if (length < TRANSFER_COMPLETE_SIZE) return;

        ByteBuffer buf = ByteBuffer.wrap(data, 0, length);
        buf.position(4);
        int senderId = buf.getInt();
        int totalKeys = buf.getInt();

        System.out.println("[KeyTransfer] Transfer complete from node " + senderId
                + ", total=" + totalKeys + " keys. Grace period " + RECOVERY_GRACE_PERIOD_MS + "ms");

        scheduler.schedule(() -> {
            recovering = false;
            recoveryCompleted = true;
            recoverySource = null;
            tombstones.clear();
            System.out.println("[KeyTransfer] Recovery mode ended, tombstones cleared");

            // One-time cleanup: recovery may have delivered keys for nodes further
            // back in the chain (cascading failures). After outgoing transfers finish,
            // remove any keys we don't own.
            schedulePostRecoveryCleanup();
        }, RECOVERY_GRACE_PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Record that a key was REMOVED during recovery.
     * Transfer data arriving for this key will be skipped.
     */
    public void markRemovedDuringRecovery(ByteString key) {
        tombstones.add(key);
    }

    public boolean isRecovering() {
        return recovering || awaitingRecovery;
    }

    public Node getRecoverySourceForKey(ByteString key) {
        if (recovering && recoverySource != null) {
            return recoverySource;
        }
        if (awaitingRecovery) {
            return hashRing.getSuccessorForKey(key, selfNode.id);
        }
        return null;
    }

    /**
     * Whether this node is actively transferring keys to the given node.
     * Used by Worker for loop-breaking.
     */
    public boolean isActiveTransferTarget(int nodeId) {
        return activeTransferTargets.contains(nodeId);
    }

    /**
     * Send a transfer-pending notification to a target node.
     * Tells the target that we will eventually send their keys, but we're
     * still recovering ourselves. This resets their awaiting-recovery timeout.
     */
    private void sendTransferPending(Node target) {
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
    // Packet detection helpers
    // ========================

    public static boolean isKeyTransferMessage(byte[] data, int length) {
        if (length < KEY_TRANSFER_HEADER_SIZE) return false;
        return data[0] == KEY_TRANSFER_MAGIC[0]
                && data[1] == KEY_TRANSFER_MAGIC[1]
                && data[2] == KEY_TRANSFER_MAGIC[2]
                && data[3] == KEY_TRANSFER_MAGIC[3];
    }

    public static boolean isTransferCompleteMessage(byte[] data, int length) {
        if (length < TRANSFER_COMPLETE_SIZE) return false;
        return data[0] == TRANSFER_COMPLETE_MAGIC[0]
                && data[1] == TRANSFER_COMPLETE_MAGIC[1]
                && data[2] == TRANSFER_COMPLETE_MAGIC[2]
                && data[3] == TRANSFER_COMPLETE_MAGIC[3];
    }

    public static boolean isTransferAckMessage(byte[] data, int length) {
        if (length < TRANSFER_ACK_SIZE) return false;
        return data[0] == TRANSFER_ACK_MAGIC[0]
                && data[1] == TRANSFER_ACK_MAGIC[1]
                && data[2] == TRANSFER_ACK_MAGIC[2]
                && data[3] == TRANSFER_ACK_MAGIC[3];
    }

    public static boolean isTransferPendingMessage(byte[] data, int length) {
        if (length < TRANSFER_PENDING_SIZE) return false;
        return data[0] == TRANSFER_PENDING_MAGIC[0]
                && data[1] == TRANSFER_PENDING_MAGIC[1]
                && data[2] == TRANSFER_PENDING_MAGIC[2]
                && data[3] == TRANSFER_PENDING_MAGIC[3];
    }
}
