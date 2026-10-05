package com.github.lechandonga.clusterjobschedulerjava.core;

/**
 * 节点健康状态。
 * READY：可接受调度；
 * DRAINING：排空，不再接受新作业，已有作业运行完后回迁/等待，不直接判失败；
 * OFFLINE：掉线，上面运行中的作业立即回迁，超过回迁上限则失败。
 */
public enum NodeState {
    READY,
    DRAINING,
    OFFLINE
}
