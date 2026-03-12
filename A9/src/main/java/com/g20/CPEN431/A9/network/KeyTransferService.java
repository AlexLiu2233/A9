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
import java.util.TreeMap;
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
 * Receiver side: accepts key transfer packets, stores entries, and enters
 * recovery mode (forwarding GET/REMOVE misses to the recovery source).
 *
 * On startup, the node enters "awaitingRecovery" mode. During this state,
 * GET/REMOVE misses are forwarded to the ring successor (who held our keys
 * while we were down). This transitions to normal "recovering" when the first
 * key transfer packet arrives, or times out for clean bootstrap.
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

    // Receiver side: two-phase recovery
    // Phase 1: awaitingRecovery (startup) — forward misses to ring successor
    // Phase 2: recovering (transfer in progress) — forward misses to known recoverySource
    private volatile boolean awaitingRecovery = true;
    private volatile boolean recovering = false;
    private volatile Node recoverySource = null;

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
    }

    public void stop() {
        running = false;
        transferThread.interrupt();
        scheduler.shutdownNow();
    }

    // ========================
    // Sender side
    // ========================

    /**
     * Called by GossipService when a node rejoin is detected.
     * Queues the node for key transfer.
     */
    public void onNodeRejoined(Node node) {
        if (node.id == selfNode.id) return;
        System.out.println("[KeyTransfer] Node " + node.id + " rejoined, queuing transfer");
        transferQueue.add(node);
        synchronized (transferQueue) {
            transferQueue.notify();
        }
    }

    /**
     * Background thread that processes the transfer queue sequentially.
     */
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

    /**
     * Execute the key transfer to the target node.
     * Snapshots the ring, iterates the KV store, batches and sends keys
     * that now map to the target.
     */
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

                // Flush batch if adding this entry would exceed packet size
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

        // Send remaining batch
        if (!batchKeys.isEmpty()) {
            sendKeyTransferPacket(target, batchKeys, batchRawData);
        }

        // Send transfer complete
        sendTransferComplete(target, transferredKeys.size());

        System.out.println("[KeyTransfer] Sent " + transferredKeys.size()
                + " keys to node " + target.id);

        // Schedule key deletion after hold time
        if (!transferredKeys.isEmpty()) {
            scheduleKeyDeletion(transferredKeys);
        }
    }

    /**
     * Build and send a batched key transfer packet.
     *
     * Format: KEY_TRANSFER_MAGIC(4) | sender_node_id(4) | num_entries(2)
     * Per entry: key_len(2) | key(var) | rawdata_len(4) | rawdata(var)
     */
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
            byte[] key = keys.get(i);
            byte[] rawData = rawDataList.get(i);
            buf.putShort((short) key.length);
            buf.put(key);
            buf.putInt(rawData.length);
            buf.put(rawData);
        }

        byte[] data = buf.array();
        try {
            DatagramPacket packet = new DatagramPacket(data, data.length, target.ipaddress, target.port);
            socket.send(packet);
        } catch (IOException e) {
            System.err.println("[KeyTransfer] Failed to send transfer packet to node " + target.id);
        }
    }

    /**
     * Send transfer complete notification.
     * Format: TRANSFER_COMPLETE_MAGIC(4) | sender_node_id(4) | total_keys(4)
     */
    private void sendTransferComplete(Node target, int totalKeys) {
        ByteBuffer buf = ByteBuffer.allocate(TRANSFER_COMPLETE_SIZE);
        buf.put(TRANSFER_COMPLETE_MAGIC);
        buf.putInt(selfNode.id);
        buf.putInt(totalKeys);

        byte[] data = buf.array();
        try {
            DatagramPacket packet = new DatagramPacket(data, data.length, target.ipaddress, target.port);
            socket.send(packet);
        } catch (IOException e) {
            System.err.println("[KeyTransfer] Failed to send transfer complete to node " + target.id);
        }
    }

    /**
     * After TRANSFER_HOLD_TIME_MS, deletes transferred keys
     * (re-checks ownership first to avoid deleting keys that should stay).
     */
    private void scheduleKeyDeletion(List<ByteString> keys) {
        scheduler.schedule(() -> {
            int deleted = 0;
            for (ByteString key : keys) {
                Node owner = hashRing.getNodeForKey(key);
                if (owner != null && owner.id != selfNode.id) {
                    KeyValueStore.remove(key);
                    deleted++;
                }
            }
            System.out.println("[KeyTransfer] Cleaned up " + deleted + " transferred keys");
        }, TRANSFER_HOLD_TIME_MS, TimeUnit.MILLISECONDS);
    }

    // ========================
    // Receiver side
    // ========================

    /**
     * Handle an incoming key transfer packet.
     * Parses entries and stores via putRawBytes.
     * Transitions from awaitingRecovery to recovering on first packet.
     */
    public void handleKeyTransferPacket(byte[] data, int length) {
        if (length < KEY_TRANSFER_HEADER_SIZE) return;

        ByteBuffer buf = ByteBuffer.wrap(data, 0, length);
        buf.position(4); // skip magic
        int senderId = buf.getInt();
        int numEntries = buf.getShort() & 0xFFFF;

        // Transition: awaitingRecovery -> recovering with known source
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
            KeyValueStore.putRawBytes(key, rawData);
            stored++;
        }

        System.out.println("[KeyTransfer] Received and stored " + stored + " keys from node " + senderId);
    }

    /**
     * Handle transfer complete notification.
     * Schedules exit from recovery mode after RECOVERY_GRACE_PERIOD_MS.
     */
    public void handleTransferComplete(byte[] data, int length) {
        if (length < TRANSFER_COMPLETE_SIZE) return;

        ByteBuffer buf = ByteBuffer.wrap(data, 0, length);
        buf.position(4); // skip magic
        int senderId = buf.getInt();
        int totalKeys = buf.getInt();

        System.out.println("[KeyTransfer] Transfer complete from node " + senderId
                + ", total=" + totalKeys + " keys. Grace period " + RECOVERY_GRACE_PERIOD_MS + "ms");

        scheduler.schedule(() -> {
            recovering = false;
            recoverySource = null;
            System.out.println("[KeyTransfer] Recovery mode ended");
        }, RECOVERY_GRACE_PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Whether this node is currently in any recovery state
     * (awaitingRecovery or actively recovering).
     */
    public boolean isRecovering() {
        return recovering || awaitingRecovery;
    }

    /**
     * Get the recovery source for a specific key.
     * - During "recovering" phase: returns the known recoverySource node.
     * - During "awaitingRecovery" phase: returns the ring successor for the key
     *   (the node that held our keys while we were down).
     * - Otherwise: returns null.
     */
    public Node getRecoverySourceForKey(ByteString key) {
        if (recovering && recoverySource != null) {
            return recoverySource;
        }
        if (awaitingRecovery) {
            return hashRing.getSuccessorForKey(key, selfNode.id);
        }
        return null;
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
