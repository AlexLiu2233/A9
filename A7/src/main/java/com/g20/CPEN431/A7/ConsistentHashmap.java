package com.g20.CPEN431.A7;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConsistentHashmap {

    // The hash ring: maps hash values to nodes
    private final TreeMap<Integer, Node> ring = new TreeMap<>();

    // Read-write lock: concurrent reads, exclusive writes
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // Number of virtual nodes per physical node (improves distribution)
    private final int virtualNodes;

    public ConsistentHashmap() {
        this(1); // Default: no virtual nodes
    }

    public ConsistentHashmap(int virtualNodes) {
        this.virtualNodes = virtualNodes;
    }

    /**
     * Add a node to the hash ring.
     * @return 0 on success, 1 if the node already exists, 2 if a hash collision occurred
     * Note: Can fail if a single collision happens with virtual nodes. Potentially overly aggressive.
     */
    public int addNode(Node node) {
        // Pre-compute hashes outside the lock
        int[] hashes = new int[virtualNodes];
        for (int i = 0; i < virtualNodes; i++) {
            hashes[i] = hash(node.id + "-" + i);
        }

        lock.writeLock().lock();
        try {
            // Check ALL virtual node hashes for duplicates/collisions before inserting
            for (int h : hashes) {
                Node existing = ring.get(h);
                if (existing != null) {
                    if (existing.id == node.id) {
                        return 1; // Duplicate node
                    }
                    return 2; // Hash collision with a different node
                }
            }

            for (int h : hashes) {
                ring.put(h, node);
            }
            return 0;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove a node from the hash ring.
     * @return 0 on success, 1 if the node does not exist
     * Note: Can fail if a single collision happens with virtual nodes. Potentially fragile. With current addNode implementation this is fine, but consider revising if that changes.
     */
    public int removeNode(Node node) {
        // Pre-compute hashes outside the lock
        int[] hashes = new int[virtualNodes];
        for (int i = 0; i < virtualNodes; i++) {
            hashes[i] = hash(node.id + "-" + i);
        }

        lock.writeLock().lock();
        try {
            // Verify ALL virtual node hashes belong to this node before removing
            for (int h : hashes) {
                Node existing = ring.get(h);
                if (existing == null || existing.id != node.id) {
                    return 1; // Node not on the ring
                }
            }

            for (int h : hashes) {
                ring.remove(h);
            }
            return 0;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get the node responsible for the given key
     */
    public Node getNodeForKey(ByteString key) {
        int hash = hash(key.toByteArray());

        lock.readLock().lock();
        try {
            if (ring.isEmpty()) {
                return null;
            }

            // Find the first node clockwise from the hash
            Map.Entry<Integer, Node> entry = ring.ceilingEntry(hash);
            if (entry == null) {
                entry = ring.firstEntry();
            }
            return entry.getValue();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get all unique physical nodes in the ring
     */
    public Collection<Node> getAllNodes() {
        lock.readLock().lock();
        try {
            return new HashSet<>(ring.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get a random node from the ring
     * @return a random node, or null if the ring is empty
     */
    public Node getRandomNode() {
        lock.readLock().lock();
        try {
            if (ring.isEmpty()) {
                return null;
            }
            List<Node> nodes = new ArrayList<>(new HashSet<>(ring.values()));
            return nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Check if ring is empty
     */
    public boolean isEmpty() {
        lock.readLock().lock();
        try {
            return ring.isEmpty();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the number of unique physical nodes in the ring
     */
    public int size() {
        lock.readLock().lock();
        try {
            return new HashSet<>(ring.values()).size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Hash function using MD5
     */
    private int hash(String key) {
        return hash(key.getBytes());
    }

    private int hash(byte[] key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(key);
            // Use first 4 bytes as integer
            return ByteBuffer.wrap(digest, 0, 4).getInt();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not available", e);
        }
    }
}
