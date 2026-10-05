package com.github.lechandonga.clusterjobschedulerjava.core;

import java.util.ArrayList;
import java.util.List;

/**
 * 调度器全量状态快照，即持久化的全部范围：
 * 节点（含资源账目）、作业（含状态机位置）、全局提交序号。
 * 运行中的模拟定时器不持久化，恢复时按作业状态重新模拟。
 */
public class SchedulerSnapshot {
    private int version = 1;
    private List<Node> nodes = new ArrayList<>();
    private List<Job> jobs = new ArrayList<>();
    private long nextSubmitSequence = 1;

    public int getVersion() { return version; }
    public void setVersion(int version) { this.version = version; }

    public List<Node> getNodes() { return nodes; }
    public void setNodes(List<Node> nodes) { this.nodes = nodes; }

    public List<Job> getJobs() { return jobs; }
    public void setJobs(List<Job> jobs) { this.jobs = jobs; }

    public long getNextSubmitSequence() { return nextSubmitSequence; }
    public void setNextSubmitSequence(long nextSubmitSequence) { this.nextSubmitSequence = nextSubmitSequence; }
}
