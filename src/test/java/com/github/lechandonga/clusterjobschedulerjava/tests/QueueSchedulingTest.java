package com.github.lechandonga.clusterjobschedulerjava.tests;

import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lechandonga.clusterjobschedulerjava.core.JobState;
import com.github.lechandonga.clusterjobschedulerjava.core.SchedulerEngine;
import com.github.lechandonga.clusterjobschedulerjava.core.SubmitRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * 业务：资源不足排队与确定性调度。
 * 覆盖：整体等待不占部分资源、优先级 + 提交序号排序、释放资源后按序拉起、节点排空不回迁。
 */
@DisplayName("业务2-资源不足排队与优先级调度")
class QueueSchedulingTest extends SchedulerTestSupport {

    private static final Logger log = LoggerFactory.getLogger(QueueSchedulingTest.class);

    private SchedulerEngine engine;

    @BeforeEach
    void setUp() {
        engine = newEngine();
    }

    @AfterEach
    void tearDown() {
        engine.stop();
    }

    @Test
    @DisplayName("用例2.1: 资源不足时作业整体排队；运行作业完成后按 优先级→提交序号 确定地拉起")
    void queueThenDispatchByPriorityAndSequence() throws Exception {
        logCase(log, "排队调度", "4 作业竞争 4 CPU", "高优先级先排，同级按提交顺序，全部完成且账目归零");
        engine.registerNode("n1", 4, 16_384, 0);

        engine.submit(new SubmitRequest("low-a", "低优先级A", 4, 1024, 0, 1, 300));
        engine.submit(new SubmitRequest("high", "高优先级", 4, 1024, 0, 9, 300));
        engine.submit(new SubmitRequest("low-b", "低优先级B", 2, 1024, 0, 1, 300));
        engine.submit(new SubmitRequest("low-c", "低优先级C", 2, 1024, 0, 1, 300));

        assertEquals(JobState.RUNNING, engine.getJob("low-a").getState(), "最先提交的作业先占满节点");
        assertEquals(JobState.QUEUED, engine.getJob("high").getState(), "高优先级作业也必须整体等待，不抢占");
        engine.verifyLedger("首个作业占满节点后");

        awaitState(engine, "high", JobState.RUNNING, Duration.ofSeconds(5));
        logResult(log, "优先级抢占", "low-a 完成后 high 最先被拉起（优先级 9 > 1）");

        awaitState(engine, "low-b", JobState.RUNNING, Duration.ofSeconds(5));
        awaitState(engine, "low-c", JobState.RUNNING, Duration.ofSeconds(2));
        awaitState(engine, "low-c", JobState.COMPLETED, Duration.ofSeconds(5));

        awaitFor(() -> engine.listJobs().stream().allMatch(j -> j.getState() == JobState.COMPLETED),
                Duration.ofSeconds(5), "全部作业完成");
        engine.verifyLedger("全部作业完成后");
        assertEquals(0L, engine.listNodes().get(0).getUsed().cpu(), "完成后账目必须全部释放，无泄漏");
        logResult(log, "顺序与账目", "low-b/low-c 同优先级按提交序号同节点拼箱运行，最终 used.cpu=0");
    }

    @Test
    @DisplayName("用例2.2: 节点 DRAINING 后不接收新作业，节点恢复 READY 前排队作业持续等待")
    void drainingNodeRejectsNewPlacement() throws Exception {
        logCase(log, "排队调度", "节点排空", "排空节点不再落作业，READY 恢复后继续调度");
        engine.registerNode("n1", 2, 1024, 0);
        engine.submit(new SubmitRequest("runner", "占用", 2, 256, 0, 0, 60_000));
        assertEquals(JobState.RUNNING, engine.getJob("runner").getState());

        engine.drainNode("n1");
        engine.submit(new SubmitRequest("waiting", "等待", 2, 256, 0, 0, 200));
        assertEquals(JobState.QUEUED, engine.getJob("waiting").getState(), "DRAINING 节点不能接收新作业");
        assertEquals(JobState.RUNNING, engine.getJob("runner").getState(), "排空采用优雅策略，不回迁、不杀死已有作业");

        engine.readyNode("n1");
        assertEquals(JobState.QUEUED, engine.getJob("waiting").getState(), "节点虽 READY 但资源仍被 runner 占用，继续整体等待");

        engine.cancel("runner");
        awaitState(engine, "waiting", JobState.RUNNING, Duration.ofSeconds(3));
        assertEquals("n1", engine.getJob("waiting").getNodeId());
        awaitState(engine, "waiting", JobState.COMPLETED, Duration.ofSeconds(5));
        engine.verifyLedger("排空恢复流程后");
        logResult(log, "节点排空", "DRAINING 不接新作业、不回迁；取消占用作业后 waiting 落到已恢复的 n1");
    }

    @Test
    @DisplayName("用例2.3: GPU 等多维资源必须同时满足，只差一维也必须整体等待")
    void multiDimensionAllOrNothing() throws Exception {
        logCase(log, "排队调度", "GPU 维度不足", "CPU/内存够但 GPU 不够也不启动，扩容节点后整体启动");
        engine.registerNode("cpu-only", 8, 32_768, 1);
        engine.submit(new SubmitRequest("holder", "占住 GPU", 1, 256, 1, 0, 60_000));
        engine.submit(new SubmitRequest("gpu-job", "GPU 作业", 2, 1024, 1, 0, 200));
        assertEquals(JobState.QUEUED, engine.getJob("gpu-job").getState());
        assertNotEquals(0L, engine.decisionLog().stream().filter(d -> d.contains("SCHED_WAIT")).count(),
                "等待决策必须可解释地出现在决策日志中");

        engine.registerNode("gpu-node", 8, 32_768, 2);
        awaitState(engine, "gpu-job", JobState.RUNNING, Duration.ofSeconds(3));
        assertEquals("gpu-node", engine.getJob("gpu-job").getNodeId(), "唯一有空闲 GPU 的节点，体现多维资源 all-or-nothing");
        awaitState(engine, "gpu-job", JobState.COMPLETED, Duration.ofSeconds(5));
        engine.verifyLedger("多维资源调度后");
        engine.cancel("holder");
        logResult(log, "多维资源", "唯一 GPU 被占用时整体等待并记录 SCHED_WAIT，注册 gpu-node 后立即整体调度");
    }
}
