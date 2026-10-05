package com.github.lechandonga.clusterjobschedulerjava.core;

/**
 * 作业状态机：
 * <pre>
 *   QUEUED ──调度成功──▶ RUNNING ──正常结束──▶ COMPLETED
 *     │                    │
 *     │                    ├──用户取消──▶ CANCELLED
 *     ├──用户取消──▶ CANCELLED
 *     │
 *     └──需求超过集群单节点容量 / 回迁次数耗尽──▶ FAILED
 *   RUNNING ──节点故障回迁──▶ QUEUED（requeueCount+1，可解释、有上限）
 * </pre>
 * COMPLETED / FAILED / CANCELLED 为终态，任何重复操作都不得再迁移。
 */
public enum JobState {
    QUEUED,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == CANCELLED;
    }
}
