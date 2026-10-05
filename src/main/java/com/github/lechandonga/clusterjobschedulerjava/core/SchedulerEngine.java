package com.github.lechandonga.clusterjobschedulerjava.core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 本地集群作业调度引擎（纯内存 + 本地文件快照，无任何外部调度器/服务）。
 *
 * 一致性模型：所有状态变更都在 {@link #lock} 下串行完成，并在释放锁之前持久化快照；
 * 并发提交、取消、节点故障的结果等价于按确定顺序依次发生，账目不会重复扣减或为负。
 * 每个决策都会写入可解释的决策日志。
 */
public class SchedulerEngine {

    private static final Logger log = LoggerFactory.getLogger(SchedulerEngine.class);
    private static final int DECISION_LOG_LIMIT = 500;

    private final StateStore store;
    private final int maxRequeue;
    private final ReentrantLock lock = new ReentrantLock(true);
    private final ScheduledExecutorService simulator;

    private final Map<String, Node> nodes = new LinkedHashMap<>();
    private final Map<String, Job> jobs = new LinkedHashMap<>();
    private final Map<String, ScheduledFuture<?>> runningTimers = new LinkedHashMap<>();
    private final List<String> decisionLog = new ArrayList<>();
    private long nextSubmitSequence = 1;

    public SchedulerEngine(StateStore store, int maxRequeue) {
        this.store = store;
        this.maxRequeue = maxRequeue;
        this.simulator = Executors.newScheduledThreadPool(
                Math.max(2, Runtime.getRuntime().availableProcessors()),
                runnable -> {
                    Thread thread = new Thread(runnable, "job-simulator");
                    thread.setDaemon(true);
                    return thread;
                });
    }

    private <T> T withLock(Supplier<T> action) {
        lock.lock();
        try {
            return action.get();
        } finally {
            lock.unlock();
        }
    }

    /* ================= 生命周期与恢复 ================= */

    /** 启动：从本地快照恢复队列、账目与作业状态，并为 RUNNING 作业重新挂载模拟时钟。 */
    public void start() {
        withLock(() -> {
            SchedulerSnapshot snapshot = store.load();
            if (snapshot == null) {
                record("BOOT", "-", "无本地快照，以空集群启动");
                persist();
                return null;
            }
            snapshot.getNodes().forEach(n -> nodes.put(n.getId(), n));
            snapshot.getJobs().forEach(j -> jobs.put(j.getId(), j));
            nextSubmitSequence = snapshot.getNextSubmitSequence();
            verifyLedger("启动恢复");
            long now = System.currentTimeMillis();
            for (Job job : jobs.values()) {
                if (job.getState() == JobState.RUNNING) {
                    long remaining = Math.max(1, job.getDurationMillis() - (now - job.getStartedAtMillis()));
                    armTimer(job, remaining);
                    record("RECOVER", job.getId(), "作业重启前处于 RUNNING，节点=" + job.getNodeId()
                            + "，剩余模拟时长=" + remaining + "ms，资源账目保持不变");
                }
            }
            record("BOOT", "-", "从本地快照恢复: 节点 " + nodes.size() + " 个, 作业 " + jobs.size() + " 个");
            return null;
        });
    }

    public void stop() {
        simulator.shutdownNow();
    }

    /* ================= 节点注册与健康管理 ================= */

    public Node registerNode(String id, long cpu, long memoryMb, long gpu) {
        return withLock(() -> {
            if (isBlank(id)) {
                throw error(SchedulerException.Code.INVALID_PARAMETER, "节点 id 不能为空");
            }
            if (nodes.containsKey(id)) {
                throw error(SchedulerException.Code.DUPLICATE_NODE_ID, "节点已存在: " + id);
            }
            Resources capacity;
            try {
                capacity = new Resources(cpu, memoryMb, gpu);
            } catch (IllegalArgumentException e) {
                throw error(SchedulerException.Code.INVALID_PARAMETER, e.getMessage());
            }
            if (capacity.equals(Resources.ZERO)) {
                throw error(SchedulerException.Code.INVALID_PARAMETER, "节点容量必须至少包含一维非零资源: " + id);
            }
            Node node = new Node(id, capacity, System.currentTimeMillis());
            nodes.put(id, node);
            record("NODE_REGISTER", id, "节点注册，容量=" + capacity);
            persist();
            dispatch();
            return node;
        });
    }

    /** READY → DRAINING：优雅排空，不再接收新作业，已有作业运行到结束自然释放，不判失败。 */
    public Node drainNode(String id) {
        return withLock(() -> {
            Node node = requireNode(id);
            if (node.getState() != NodeState.READY) {
                record("NODE_DRAIN_SKIP", id, "节点当前状态=" + node.getState() + "，排空操作幂等返回");
                return node;
            }
            node.setState(NodeState.DRAINING);
            long running = countRunningOn(id);
            record("NODE_DRAIN", id, "节点进入排空：不再调度新作业，其上 " + running
                    + " 个运行中作业按优雅策略运行至结束后释放账目，不判失败");
            persist();
            return node;
        });
    }

    /** → OFFLINE：运行中作业立即整体回迁；回迁次数超过上限则失败，账目同步回收。 */
    public Node offlineNode(String id) {
        return withLock(() -> {
            Node node = requireNode(id);
            NodeState old = node.getState();
            List<Job> victims = jobs.values().stream()
                    .filter(j -> j.getState() == JobState.RUNNING && id.equals(j.getNodeId()))
                    .toList();
            if (old == NodeState.OFFLINE) {
                record("NODE_OFFLINE_SKIP", id, "节点已处于 OFFLINE，操作幂等返回");
                return node;
            }
            node.setState(NodeState.OFFLINE);
            for (Job job : victims) {
                cancelTimer(job.getId());
                node.release(job.getRequirement());
                job.setNodeId(null);
                int requeued = job.getRequeueCount() + 1;
                if (requeued > maxRequeue) {
                    job.setState(JobState.FAILED);
                    job.setFinishedAtMillis(System.currentTimeMillis());
                    job.setFailReason("REQUEUE_LIMIT_EXCEEDED: 节点 " + id + " 掉线，回迁次数 "
                            + (requeued - 1) + " 已达上限 " + maxRequeue);
                    record("JOB_FAIL", job.getId(), job.getFailReason());
                } else {
                    job.setRequeueCount(requeued);
                    job.setState(JobState.QUEUED);
                    record("JOB_REQUEUE", job.getId(), "节点 " + id + " 掉线，作业整体回迁排队（第 "
                            + requeued + " 次回迁，上限 " + maxRequeue + "），资源已回收");
                }
            }
            record("NODE_OFFLINE", id, "节点掉线，受影响运行中作业 " + victims.size() + " 个，已按回迁/失败策略处理");
            persist();
            dispatch();
            return node;
        });
    }

    /** 节点恢复为 READY（容量重新可调度）。 */
    public Node readyNode(String id) {
        return withLock(() -> {
            Node node = requireNode(id);
            node.setState(NodeState.READY);
            record("NODE_READY", id, "节点恢复就绪，可用资源=" + node.available());
            persist();
            dispatch();
            return node;
        });
    }

    /* ================= 作业提交（含数组式批量提交） ================= */

    public Job submit(SubmitRequest request) {
        return submitBatch(List.of(request)).get(0);
    }

    /**
     * 数组式批量提交：先对整批做参数、重复 id、容量上限校验，任一不合法则整批拒绝，
     * 不会部分入队；全部合法后统一入队并触发一次调度。
     */
    public List<Job> submitBatch(List<SubmitRequest> requests) {
        return withLock(() -> {
            if (requests == null || requests.isEmpty()) {
                throw error(SchedulerException.Code.INVALID_PARAMETER, "提交作业数必须大于 0");
            }
            List<Job> created = new ArrayList<>();
            List<String> batchIds = new ArrayList<>();
            long now = System.currentTimeMillis();
            for (SubmitRequest req : requests) {
                validateRequest(req, batchIds);
                batchIds.add(req.id());
            }
            for (SubmitRequest req : requests) {
                Job job = new Job(req.id(), req.name(), req.requirement(), req.priority(),
                        req.durationMillis(), nextSubmitSequence++, now);
                jobs.put(job.getId(), job);
                created.add(job);
                record("JOB_SUBMIT", job.getId(), "作业入队，需求=" + job.getRequirement()
                        + "，优先级=" + job.getPriority() + "，提交序号=" + job.getSubmitSequence());
            }
            persist();
            dispatch();
            return created;
        });
    }

    private void validateRequest(SubmitRequest req, List<String> earlierBatchIds) {
        if (req == null) {
            throw error(SchedulerException.Code.INVALID_PARAMETER, "提交项不能为空");
        }
        if (isBlank(req.id())) {
            throw error(SchedulerException.Code.INVALID_PARAMETER, "作业 id 不能为空");
        }
        if (isBlank(req.name())) {
            throw error(SchedulerException.Code.INVALID_PARAMETER, "作业 " + req.id() + " 的 name 不能为空");
        }
        if (req.cpu() <= 0) {
            throw error(SchedulerException.Code.INVALID_PARAMETER, "作业 " + req.id() + " 的 cpu 必须为正数，实际=" + req.cpu());
        }
        if (req.memoryMb() <= 0) {
            throw error(SchedulerException.Code.INVALID_PARAMETER, "作业 " + req.id() + " 的 memoryMb 必须为正数，实际=" + req.memoryMb());
        }
        if (req.gpu() < 0) {
            throw error(SchedulerException.Code.INVALID_PARAMETER, "作业 " + req.id() + " 的 gpu 不能为负数，实际=" + req.gpu());
        }
        if (req.durationMillis() <= 0) {
            throw error(SchedulerException.Code.INVALID_PARAMETER, "作业 " + req.id() + " 的 durationMillis 必须为正数，实际=" + req.durationMillis());
        }
        if (jobs.containsKey(req.id()) || earlierBatchIds.contains(req.id())) {
            throw error(SchedulerException.Code.DUPLICATE_JOB_ID, "作业 id 重复: " + req.id());
        }
        Resources requirement = req.requirement();
        boolean fitsAnyNode = nodes.values().stream()
                .anyMatch(n -> n.getCapacity().contains(requirement));
        if (!fitsAnyNode) {
            throw error(SchedulerException.Code.EXCEEDS_CLUSTER_CAPACITY,
                    "作业 " + req.id() + " 需求 " + requirement + " 超过集群任一已注册节点的容量上限，拒绝排队");
        }
    }

    /* ================= 作业生命周期与幂等操作 ================= */

    /**
     * 取消作业，幂等且可解释：
     * QUEUED/RUNNING → CANCELLED（RUNNING 同时回收资源并停掉模拟时钟）；
     * 已 CANCELLED → JOB_ALREADY_CANCELLED；其他终态 → JOB_ALREADY_TERMINAL；不存在 → JOB_NOT_FOUND。
     */
    public Job cancel(String id) {
        return withLock(() -> {
            Job job = jobs.get(id);
            if (job == null) {
                throw error(SchedulerException.Code.JOB_NOT_FOUND, "作业不存在: " + id);
            }
            if (job.getState() == JobState.CANCELLED) {
                throw error(SchedulerException.Code.JOB_ALREADY_CANCELLED,
                        "作业 " + id + " 已是 CANCELLED 终态，重复取消不会改变状态");
            }
            if (job.getState().isTerminal()) {
                throw error(SchedulerException.Code.JOB_ALREADY_TERMINAL,
                        "作业 " + id + " 已处于终态 " + job.getState() + "，不能取消");
            }
            if (job.getState() == JobState.RUNNING) {
                Node node = requireNode(job.getNodeId());
                cancelTimer(id);
                node.release(job.getRequirement());
                job.setNodeId(null);
                record("JOB_CANCEL", id, "运行中作业被取消，节点资源已回收=" + job.getRequirement());
            } else {
                record("JOB_CANCEL", id, "排队中作业被取消，未占用任何资源");
            }
            job.setState(JobState.CANCELLED);
            job.setFinishedAtMillis(System.currentTimeMillis());
            persist();
            dispatch();
            return job;
        });
    }

    private void completeJob(String id) {
        withLock(() -> {
            Job job = jobs.get(id);
            if (job == null || job.getState() != JobState.RUNNING) {
                return null;
            }
            runningTimers.remove(id);
            Node node = requireNode(job.getNodeId());
            node.release(job.getRequirement());
            String nodeId = job.getNodeId();
            job.setNodeId(null);
            job.setState(JobState.COMPLETED);
            job.setFinishedAtMillis(System.currentTimeMillis());
            record("JOB_COMPLETE", id, "作业在节点 " + nodeId + " 正常完成，资源已释放=" + job.getRequirement());
            persist();
            dispatch();
            return null;
        });
    }

    /* ================= 调度决策（确定性、可解释） ================= */

    /**
     * 调度规则（确定性）：
     * 1. 只考虑 QUEUED 作业，按优先级降序、同优先级按提交序号升序；
     * 2. 作业整体调度：必须在单个 READY 节点上一次性满足全部 CPU/内存/GPU；
     * 3. 候选节点选择 best-fit（分配后剩余资源标量最小，平手按节点 id 字典序）；
     * 4. 队首放不下不阻塞后面放得下的作业，每次跳过都记录原因。
     */
    void dispatch() {
        List<Job> queue = jobs.values().stream()
                .filter(j -> j.getState() == JobState.QUEUED)
                .sorted(Comparator.comparingInt(Job::getPriority).reversed()
                        .thenComparingLong(Job::getSubmitSequence))
                .toList();
        for (Job job : queue) {
            Node target = null;
            long bestRemaining = Long.MAX_VALUE;
            for (Node node : nodes.values()) {
                if (!node.canFit(job.getRequirement())) {
                    continue;
                }
                long remaining = remainingScalar(node.available().minus(job.getRequirement()));
                if (remaining < bestRemaining
                        || (remaining == bestRemaining && (target == null || node.getId().compareTo(target.getId()) < 0))) {
                    bestRemaining = remaining;
                    target = node;
                }
            }
            if (target == null) {
                record("SCHED_WAIT", job.getId(), "当前无 READY 且资源足够的节点，作业整体等待（不占用任何部分资源）");
                continue;
            }
            target.allocate(job.getRequirement());
            job.setState(JobState.RUNNING);
            job.setNodeId(target.getId());
            job.setStartedAtMillis(System.currentTimeMillis());
            armTimer(job, job.getDurationMillis());
            record("SCHED_PLACE", job.getId(), "作业整体调度到节点 " + target.getId()
                    + "，需求=" + job.getRequirement() + "，节点剩余=" + target.available());
            persist();
        }
    }

    /** best-fit 排序键：CPU、内存、GPU 剩余量等权求和，仅用于确定性比较。 */
    private long remainingScalar(Resources r) {
        return r.cpu() + r.memoryMb() + r.gpu();
    }

    private void armTimer(Job job, long delayMillis) {
        ScheduledFuture<?> timer = simulator.schedule(
                () -> {
                    try {
                        completeJob(job.getId());
                    } catch (RuntimeException e) {
                        log.error("作业 {} 模拟完成回调失败", job.getId(), e);
                    }
                },
                Math.max(1, delayMillis), TimeUnit.MILLISECONDS);
        runningTimers.put(job.getId(), timer);
    }

    private void cancelTimer(String jobId) {
        ScheduledFuture<?> timer = runningTimers.remove(jobId);
        if (timer != null) {
            timer.cancel(false);
        }
    }

    /* ================= 查询与账目自校验 ================= */

    public Job getJob(String id) {
        return withLock(() -> {
            Job job = jobs.get(id);
            if (job == null) {
                throw error(SchedulerException.Code.JOB_NOT_FOUND, "作业不存在: " + id);
            }
            return job;
        });
    }

    public List<Job> listJobs() {
        return withLock(() -> jobs.values().stream().map(Job::new).toList());
    }

    public List<Node> listNodes() {
        return withLock(() -> nodes.values().stream()
                .map(n -> {
                    Node copy = new Node(n.getId(), n.getCapacity(), n.getRegisteredAtMillis());
                    copy.setUsed(n.getUsed());
                    copy.setState(n.getState());
                    return copy;
                })
                .toList());
    }

    public List<String> decisionLog() {
        return withLock(() -> new ArrayList<>(decisionLog));
    }

    /**
     * 账目不变量自校验：每个节点 used 必须恰好等于其上 RUNNING 作业需求之和，
     * 且 used 各维不得超过 capacity（不可能为负，扣减时也有防御）。
     * 在启动恢复后和测试中调用，任何泄漏/重复扣减都会立即暴露。
     */
    public void verifyLedger(String phase) {
        withLock(() -> {
            for (Node node : nodes.values()) {
                Resources expected = jobs.values().stream()
                        .filter(j -> j.getState() == JobState.RUNNING && node.getId().equals(j.getNodeId()))
                        .map(Job::getRequirement)
                        .reduce(Resources.ZERO, Resources::plus);
                if (!expected.equals(node.getUsed())) {
                    throw new IllegalStateException("[" + phase + "] 节点 " + node.getId()
                            + " 账目泄漏: 期望 used=" + expected + " 实际 used=" + node.getUsed());
                }
                if (!node.getCapacity().contains(node.getUsed())) {
                    throw new IllegalStateException("[" + phase + "] 节点 " + node.getId()
                            + " 账目超扣: capacity=" + node.getCapacity() + " used=" + node.getUsed());
                }
            }
            return null;
        });
    }

    /* ================= 内部工具 ================= */

    private long countRunningOn(String nodeId) {
        return jobs.values().stream()
                .filter(j -> j.getState() == JobState.RUNNING && nodeId.equals(j.getNodeId()))
                .count();
    }

    private Node requireNode(String id) {
        Node node = nodes.get(id);
        if (node == null) {
            throw error(SchedulerException.Code.NODE_NOT_FOUND, "节点不存在: " + id);
        }
        return node;
    }

    private void record(String action, String target, String reason) {
        String entry = String.format("[%d] %-18s target=%-10s %s", System.currentTimeMillis(), action, target, reason);
        decisionLog.add(entry);
        while (decisionLog.size() > DECISION_LOG_LIMIT) {
            decisionLog.remove(0);
        }
        log.info("调度决策: {}", entry);
    }

    private void persist() {
        SchedulerSnapshot snapshot = new SchedulerSnapshot();
        snapshot.setNodes(new ArrayList<>(nodes.values()));
        snapshot.setJobs(new ArrayList<>(jobs.values()));
        snapshot.setNextSubmitSequence(nextSubmitSequence);
        store.save(snapshot);
    }

    private static SchedulerException error(SchedulerException.Code code, String message) {
        return new SchedulerException(code, message);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
