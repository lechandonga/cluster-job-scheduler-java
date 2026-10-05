package com.github.lechandonga.clusterjobschedulerjava.core;

/**
 * 作业实体。作业不可拆分：要么在一个节点上拿到全部资源运行，要么整体排队。
 *
 * @param durationMillis 模拟运行时长；本地用定时器模拟作业执行过程
 * @param requeueCount   因节点故障回迁的次数（确定性、可解释）
 * @param failReason     进入 FAILED 终态的原因码与说明
 * @param nodeId         当前运行节点；排队/终态为 null
 */
public class Job {
    private String id;
    private String name;
    private Resources requirement;
    private int priority;
    private long durationMillis;

    private JobState state = JobState.QUEUED;
    private long submitSequence;
    private long submittedAtMillis;
    private long startedAtMillis;
    private long finishedAtMillis;
    private String nodeId;
    private int requeueCount;
    private String failReason;

    public Job() {
    }

    public Job(String id, String name, Resources requirement, int priority,
               long durationMillis, long submitSequence, long submittedAtMillis) {
        this.id = id;
        this.name = name;
        this.requirement = requirement;
        this.priority = priority;
        this.durationMillis = durationMillis;
        this.submitSequence = submitSequence;
        this.submittedAtMillis = submittedAtMillis;
    }

    /** 拷贝构造：对外返回快照，避免调用方持有可变内部实体。 */
    public Job(Job other) {
        this.id = other.id;
        this.name = other.name;
        this.requirement = other.requirement;
        this.priority = other.priority;
        this.durationMillis = other.durationMillis;
        this.state = other.state;
        this.submitSequence = other.submitSequence;
        this.submittedAtMillis = other.submittedAtMillis;
        this.startedAtMillis = other.startedAtMillis;
        this.finishedAtMillis = other.finishedAtMillis;
        this.nodeId = other.nodeId;
        this.requeueCount = other.requeueCount;
        this.failReason = other.failReason;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Resources getRequirement() { return requirement; }
    public void setRequirement(Resources requirement) { this.requirement = requirement; }

    public int getPriority() { return priority; }
    public void setPriority(int priority) { this.priority = priority; }

    public long getDurationMillis() { return durationMillis; }
    public void setDurationMillis(long durationMillis) { this.durationMillis = durationMillis; }

    public JobState getState() { return state; }
    public void setState(JobState state) { this.state = state; }

    public long getSubmitSequence() { return submitSequence; }
    public void setSubmitSequence(long submitSequence) { this.submitSequence = submitSequence; }

    public long getSubmittedAtMillis() { return submittedAtMillis; }
    public void setSubmittedAtMillis(long submittedAtMillis) { this.submittedAtMillis = submittedAtMillis; }

    public long getStartedAtMillis() { return startedAtMillis; }
    public void setStartedAtMillis(long startedAtMillis) { this.startedAtMillis = startedAtMillis; }

    public long getFinishedAtMillis() { return finishedAtMillis; }
    public void setFinishedAtMillis(long finishedAtMillis) { this.finishedAtMillis = finishedAtMillis; }

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }

    public int getRequeueCount() { return requeueCount; }
    public void setRequeueCount(int requeueCount) { this.requeueCount = requeueCount; }

    public String getFailReason() { return failReason; }
    public void setFailReason(String failReason) { this.failReason = failReason; }
}
