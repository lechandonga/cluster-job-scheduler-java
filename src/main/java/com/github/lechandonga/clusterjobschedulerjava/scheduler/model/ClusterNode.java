package com.github.lechandonga.clusterjobschedulerjava.scheduler.model;

import java.time.Instant;
import java.util.Objects;

/**
 * 集群节点。capacity 为注册时声明的总量；used 为已被运行作业占用的量。
 * 可用资源 = capacity - used，调度/回迁/掉线处理均只通过本类提供的方法变更账目，
 * subtract 在不足时直接抛错，保证账目不可能为负。
 */
public final class ClusterNode {

    private final String id;
    private final Resources capacity;
    private Resources used;
    private NodeState state;
    private Instant registeredAt;
    private Instant updatedAt;

    public ClusterNode(String id, Resources capacity, Instant registeredAt) {
        this.id = Objects.requireNonNull(id);
        this.capacity = Objects.requireNonNull(capacity);
        if (capacity.isZero()) {
            throw new IllegalArgumentException("节点容量必须大于 0: nodeId=" + id);
        }
        this.used = new Resources(0, 0, 0);
        this.state = NodeState.READY;
        this.registeredAt = registeredAt;
        this.updatedAt = registeredAt;
    }

    public Resources available() {
        return capacity.subtract(used);
    }

    public boolean canFit(Resources request) {
        return state == NodeState.READY && available().contains(request);
    }

    public void allocate(Resources request) {
        if (!available().contains(request)) {
            throw new IllegalStateException("节点资源不足，无法分配: nodeId=" + id
                    + ", available=" + available() + ", request=" + request);
        }
        this.used = used.add(request);
    }

    public void release(Resources request) {
        this.used = used.subtract(request);
    }

    public void transitionTo(NodeState target, Instant at) {
        this.state = target;
        this.updatedAt = at;
    }

    public void restore(NodeState target, Resources restoredUsed, Instant registeredAt, Instant updatedAt) {
        if (!capacity.contains(restoredUsed)) {
            throw new IllegalStateException("恢复的节点已用资源超过容量: nodeId=" + id);
        }
        this.state = target;
        this.used = restoredUsed;
        this.registeredAt = registeredAt;
        this.updatedAt = updatedAt;
    }

    public String id() { return id; }
    public Resources capacity() { return capacity; }
    public Resources used() { return used; }
    public NodeState state() { return state; }
    public Instant registeredAt() { return registeredAt; }
    public Instant updatedAt() { return updatedAt; }
}
