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

import static com.g20.CPEN431.A9.Constants.*;

/**
 * Handles key transfer when a node rejoins the cluster.
 *
 * Sender side: when gossip detects a predecessor rejoined, iterates the KV store
 * and sends keys that now map to the rejoined node.
 *
 * Receiver side: accepts key transfer packets, stores entries (put-if-absent),
 * and enters recovery mode (forwarding GET/REMOVE misses to the recovery source).
 *
 * Conflict resolution:
 * - put-if-absent: local PUTs during recovery always win over transfer data
 * - Tombstone set: REMOVEs seen during recovery prevent transfer from resurrecting keys
 * - Loop-breaking: successor processes locally when the recovering node forwards to it
 */
public class KeyTransferService {

    private static final int MAX_PACKET_PAYLOAD = 8192;
    private static final long CLEANUP_INTERVAL_MS = 30_000;

    private final Node selfNode;
    private final DatagramSocket socket;
    private final ConsistentHashmap hashRing;

    // Sender side
    private final ConcurrentLinkedQueue<Node> transferQueue = new ConcurrentLinkedQueue<>();
    private final Thread transferThread;
    private final ScheduledExecutorService scheduler;

    // Tracks nodes we are actively transferring to (or recently transferred to, during hold time).
    // Used by Worker for loop-breaking: if a forwarded request's senderNodeId is in this set,
    // and we have the key, process locally instead of forwarding back to the recovering node.
    private final Set<Integer> activeTransferTargets = ConcurrentHashMap.newKeySet();

    // Receiver side: two-phase recovery
    private volatile boolean awaitingRecovery = true;
    private volatile boolean recovering = false;
    private volatile Node recoverySource = null;

    // Tombstone set: keys that were REMOVED locally during recovery.
    // Transfer data for tombstoned keys is skipped to prevent resurrecting deleted keys.
    private final Set<ByteString> tombstones = ConcurrentHashMap.newKeySet();

    private volatile boolean running = true;

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
        scheduler.schedule(() -> {
            if (awaitingRecovery) {
                awaitingRecovery = false;
                System.out.println("[KeyTransfer] Awaiting-recovery timeout — assuming clean bootstrap");
            }
        }, AWAITING_RECOVERY_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        // Schedule periodic cleanup of keys that no longer map to self
        scheduler.scheduleWithFixedDelay(this::cleanupStaleKeys,
                CLEANUP_INTERVAL_MS, CLEANUP_INTERVAL_MS, TimeUnit.MILLISECONDS);
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
        System.out.println("[KeyTransfer] Node " + node.id + " rejoined, queuing transfer");
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

                // Wait if we ourselves are recovering
                while ((recovering || awaitingRecovery) && running) {
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

        List<ByteString> transferredKeys = new ArrayList<>();
        List<byte[]> batchKeys = new ArrayList<>();
        List<byte[]> batchRawData = new ArrayList<>();
        int batchSize = 0;

        for (Map.Entry<ByteString, byte[]> entry : KeyValueStore.entrySet()) {
            ByteString key = entry.getKey();
            byte[] rawData = entry.getValue();

            Node owner = ConsistentHashmap.getNodeForKeyFromSnapshot(key.toByteArray(), snapshot);
            if (owner != null && owner.id == target.id) {
                byte[] keyBytes = key.toByteArray();
                int entrySize = 2 + keyBytes.length + 4 + rawData.length;

                if (batchSize + entrySize > MAX_PACKET_PAYLOAD - KEY_TRANSFER_HEADER_SIZE
                        && !batchKeys.isEmpty()) {
                    sendKeyTransferPacket(target, batchKeys, batchRawData);
                    batchKeys.clear();
                    batchRawData.clear();
                    batchSize = 0;
                }

                batchKeys.add(keyBytes);
                batchRawData.add(rawData);
                batchSize += entrySize;
                transferredKeys.add(key);
            }
        }

        if (!batchKeys.isEmpty()) {
            sendKeyTransferPacket(target, batchKeys, batchRawData);
        }

        sendTransferComplete(target, transferredKeys.size());

        System.out.println("[KeyTransfer] Sent " + transferredKeys.size()
                + " keys to node " + target.id);

        if (!transferredKeys.isEmpty()) {
            scheduleKeyDeletion(transferredKeys, target.id);
        } else {
            // No keys to transfer — remove from active targets after a brief delay
            scheduler.schedule(() -> activeTransferTargets.remove(target.id),
                    RECOVERY_GRACE_PERIOD_MS, TimeUnit.MILLISECONDS);
        }
    }

    private void sendKeyTransferPacket(Node target, List<byte[]> keys, List<byte[]> rawDataList) {
        int totalSize = KEY_TRANSFER_HEADER_SIZE;
        for (int i = 0; i < keys.size(); i++) {
            totalSize += 2 + keys.get(i).length + 4 + rawDataList.get(i).length;
        }

        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.put(KEY_TRANSFER_MAGIC);
        buf.putInt(selfNode.id);
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
            System.err.println("[KeyTransfer] Failed to send transfer packet to node " + target.id);
        }
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
     * Background sweep: delete keys that no longer map to this node.
     * Handles stale keys left over after ring changes.
     */
    private void cleanupStaleKeys() {
        if (recovering || awaitingRecovery) return;
        int deleted = 0;
        for (Map.Entry<ByteString, byte[]> entry : KeyValueStore.entrySet()) {
            Node owner = hashRing.getNodeForKey(entry.getKey());
            if (owner != null && owner.id != selfNode.id) {
                KeyValueStore.remove(entry.getKey());
                deleted++;
            }
        }
        if (deleted > 0) {
            System.out.println("[KeyTransfer] Background cleanup removed " + deleted + " stale keys");
        }
    }

    // ========================
    // Receiver side
    // ========================

    /**
     * Handle an incoming key transfer packet.
     * Skips keys that already exist locally (put-if-absent) or are tombstoned.
     */
    public void handleKeyTransferPacket(byte[] data, int length) {
        if (length < KEY_TRANSFER_HEADER_SIZE) return;

        ByteBuffer buf = ByteBuffer.wrap(data, 0, length);
        buf.position(4); // skip magic
        int senderId = buf.getInt();
        int numEntries = buf.getShort() & 0xFFFF;

        // Transition: awaitingRecovery -> recovering
        if (!recovering) {
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

        System.out.println("[KeyTransfer] Received " + (stored + skipped)
                + " keys from node " + senderId + " (stored=" + stored + ", skipped=" + skipped + ")");
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
            recoverySource = null;
            tombstones.clear();
            System.out.println("[KeyTransfer] Recovery mode ended, tombstones cleared");
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
}
