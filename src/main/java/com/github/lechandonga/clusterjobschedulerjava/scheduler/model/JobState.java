package com.github.lechandonga.clusterjobschedulerjava.scheduler.model;

import java.util.EnumSet;
import java.util.Set;

/**
 * 作业状态机：
 * <pre>
 *   QUEUED -> RUNNING -> COMPLETED
 *     |         |          ^
 *     |         +----------+ (FAILED 亦可由 RUNNING 进入)
 *     v         v
 *   CANCELED  FAILED
 *     ^
 *     +-- QUEUED
 * </pre>
 * 规则：
 * COMPLETED / FAILED / CANCELED 为终态，任何重复操作都不得改写终态；
 * RUNNING 与 QUEUED 之间允许因节点排空/重启恢复而发生确定性回迁（保持回迁原因可追溯）。
 */
public enum JobState {
    QUEUED,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELED;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == CANCELED;
    }

    private static final Set<JobState> QUEUABLE_FROM = EnumSet.of(RUNNING);

    /** 是否允许从当前状态迁移到 target。 */
    public boolean canTransitionTo(JobState target) {
        if (this == target) {
            return false;
        }
        return switch (this) {
            case QUEUED -> target == RUNNING || target == CANCELED;
            case RUNNING ->
                // 正常完成 / 节点掉线失败 / 用户取消
                    target == COMPLETED || target == FAILED || target == CANCELED
                        // 节点排空或重启恢复时的确定性回迁
                        || target == QUEUED;
            case COMPLETED, FAILED, CANCELED -> false;
        };
    }

    public boolean canRequeue() {
        return QUEUABLE_FROM.contains(this);
    }
}
