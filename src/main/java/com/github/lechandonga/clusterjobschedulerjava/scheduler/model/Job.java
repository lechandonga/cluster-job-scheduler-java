package com.github.lechandonga.clusterjobschedulerjava.scheduler.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 作业聚合根。状态与资源账目的每次变更都会留下 reason / nodeId 轨迹，保证结果可解释。
 * 该类非线程安全：所有变更均由 SchedulerService 在同一把锁内串行执行。
 */
public final class Job {

    private final String id;
    private final String name;
    private final Resources request;
    private final int priority;
    private final long seq;
    private final long runDurationMillis;
    private final Instant submittedAt;

    private JobState state = JobState.QUEUED;
    private String nodeId;
    private String lastReason = "SUBMITTED";
    private int requeueCount;
    private Instant updatedAt;
    private final List<Transition> history = new ArrayList<>();

    public Job(String id, String name, Resources request, int priority, long seq,
               long runDurationMillis, Instant submittedAt) {
        this.id = Objects.requireNonNull(id);
        this.name = name;
        this.request = Objects.requireNonNull(request);
        this.priority = priority;
        this.seq = seq;
        this.runDurationMillis = runDurationMillis;
        this.submittedAt = Objects.requireNonNull(submittedAt);
        this.updatedAt = submittedAt;
        this.history.add(new Transition(JobState.QUEUED, null, "SUBMITTED", submittedAt));
    }

    public void transitionTo(JobState target, String targetNodeId, String reason, Instant at) {
        if (!state.canTransitionTo(target)) {
            throw new IllegalStateException("非法作业状态迁移: jobId=" + id + ", " + state + " -> " + target
                    + " (reason=" + reason + ")");
        }
        this.state = target;
        this.nodeId = target == JobState.RUNNING ? targetNodeId
                : (target == JobState.QUEUED ? null : this.nodeId);
        if (target == JobState.QUEUED) {
            this.requeueCount++;
        }
        this.lastReason = reason;
        this.updatedAt = at;
        this.history.add(new Transition(target, targetNodeId, reason, at));
    }

    /** 恢复快照时重建轨迹，不做状态机校验（重放事件保证顺序正确）。 */
    public void restoreState(JobState target, String targetNodeId, String reason,
                             int requeueCount, Instant updatedAt, List<Transition> transitions) {
        this.state = target;
        this.nodeId = targetNodeId;
        this.lastReason = reason;
        this.requeueCount = requeueCount;
        this.updatedAt = updatedAt;
        this.history.clear();
        this.history.addAll(transitions);
    }

    public String id() { return id; }
    public String name() { return name; }
    public Resources request() { return request; }
    public int priority() { return priority; }
    public long seq() { return seq; }
    public long runDurationMillis() { return runDurationMillis; }
    public Instant submittedAt() { return submittedAt; }
    public JobState state() { return state; }
    public String nodeId() { return nodeId; }
    public String lastReason() { return lastReason; }
    public int requeueCount() { return requeueCount; }
    public Instant updatedAt() { return updatedAt; }
    public List<Transition> history() { return List.copyOf(history); }

    /** 一次状态迁移记录，用于审计与可解释性。 */
    public record Transition(JobState state, String nodeId, String reason, Instant at) {
    }
}
