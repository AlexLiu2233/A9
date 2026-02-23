package com.g20.CPEN431.A7;

import java.net.InetAddress;

public class Node {

    public final InetAddress ipaddress;
    public final int port;
    public final int id;

    public Node(int id, InetAddress ipaddress, int port) {
        this.ipaddress = ipaddress;
        this.port = port;
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Node)) return false;
        Node node = (Node) o;
        return id == node.id;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(id);
    }
}
