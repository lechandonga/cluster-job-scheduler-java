package com.github.lechandonga.clusterjobschedulerjava.core;

/**
 * 集群节点。
 * 账目恒等式：capacity = used + available；used 恒等于其上 RUNNING 作业需求之和。
 */
public class Node {
    private String id;
    private Resources capacity;
    private Resources used = Resources.ZERO;
    private NodeState state = NodeState.READY;
    private long registeredAtMillis;

    public Node() {
    }

    public Node(String id, Resources capacity, long registeredAtMillis) {
        this.id = id;
        this.capacity = capacity;
        this.registeredAtMillis = registeredAtMillis;
    }

    public Resources available() {
        return capacity.minus(used);
    }

    public boolean canFit(Resources requirement) {
        return state == NodeState.READY && available().contains(requirement);
    }

    public void allocate(Resources requirement) {
        if (!available().contains(requirement)) {
            throw new IllegalStateException("节点 " + id + " 资源不足，拒绝扣账: available="
                    + available() + ", requested=" + requirement);
        }
        used = used.plus(requirement);
    }

    public void release(Resources requirement) {
        used = used.minus(requirement);
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public Resources getCapacity() { return capacity; }
    public void setCapacity(Resources capacity) { this.capacity = capacity; }

    public Resources getUsed() { return used; }
    public void setUsed(Resources used) { this.used = used; }

    public NodeState getState() { return state; }
    public void setState(NodeState state) { this.state = state; }

    public long getRegisteredAtMillis() { return registeredAtMillis; }
    public void setRegisteredAtMillis(long registeredAtMillis) { this.registeredAtMillis = registeredAtMillis; }
}
