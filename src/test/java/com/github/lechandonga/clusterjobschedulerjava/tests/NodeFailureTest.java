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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 业务：节点故障与确定性回迁。
 * 覆盖：OFFLINE 作业整体回迁到其他就绪节点、故障节点账目回收、回迁次数耗尽后 FAILED。
 */
@DisplayName("业务3-节点故障回迁与回迁上限")
class NodeFailureTest extends SchedulerTestSupport {

    private static final Logger log = LoggerFactory.getLogger(NodeFailureTest.class);

    private SchedulerEngine engine;

    @BeforeEach
    void setUp() {
        engine = newEngine(2);
    }

    @AfterEach
    void tearDown() {
        engine.stop();
    }

    @Test
    @DisplayName("用例3.1: 作业所在节点 OFFLINE，作业整体回迁到备用节点，故障节点账目归零")
    void offlineJobRequeuedToHealthyNode() throws Exception {
        logCase(log, "节点故障", "回迁到备用节点", "作业 QUEUED→RUNNING(其他节点)，原节点 used=0，requeueCount=1");
        engine.registerNode("n1", 4, 8_192, 0);
        engine.registerNode("n2", 4, 8_192, 0);
        engine.submit(new SubmitRequest("migrate", "回迁作业", 4, 1024, 0, 0, 200));

        String firstNode = engine.getJob("migrate").getNodeId();
        assertEquals("n1", firstNode, "best-fit 平手按节点 id 字典序，先落 n1");

        engine.offlineNode("n1");
        assertEquals(0L, engine.listNodes().get(0).getUsed().cpu(), "掉线节点资源必须立即回收，账目不泄漏");

        awaitState(engine, "migrate", JobState.RUNNING, Duration.ofSeconds(3));
        assertEquals("n2", engine.getJob("migrate").getNodeId(), "回迁作业在就绪备用节点上整体重启");
        assertEquals(1, engine.getJob("migrate").getRequeueCount(), "回迁次数可解释地累计");

        awaitState(engine, "migrate", JobState.COMPLETED, Duration.ofSeconds(5));
        engine.verifyLedger("回迁完成后");
        logResult(log, "回迁备用节点", "n1 账目归零，作业在 n2 完成，requeueCount=" + engine.getJob("migrate").getRequeueCount());
    }

    @Test
    @DisplayName("用例3.2: 无备用节点时回迁作业保持 QUEUED 等待，节点恢复后继续运行")
    void requeueWaitsWhenNoSpareCapacity() throws Exception {
        logCase(log, "节点故障", "无备用容量回迁", "作业保持 QUEUED 不失败，READY 恢复后重新调度");
        engine.registerNode("only", 4, 8_192, 0);
        engine.submit(new SubmitRequest("lonely", "唯一节点作业", 4, 1024, 0, 0, 200));
        assertEquals(JobState.RUNNING, engine.getJob("lonely").getState());

        engine.offlineNode("only");
        assertEquals(JobState.QUEUED, engine.getJob("lonely").getState());
        assertNull(engine.getJob("lonely").getNodeId());

        engine.readyNode("only");
        awaitState(engine, "lonely", JobState.RUNNING, Duration.ofSeconds(3));
        assertEquals("only", engine.getJob("lonely").getNodeId());
        awaitState(engine, "lonely", JobState.COMPLETED, Duration.ofSeconds(5));
        engine.verifyLedger("无备用回迁流程后");
        logResult(log, "无备用容量", "回迁作业等待节点恢复后成功完成，全程无资源泄漏");
    }

    @Test
    @DisplayName("用例3.3: 回迁次数超过上限(2)后作业 FAILED，failReason 可解释")
    void failAfterRequeueLimitExhausted() throws Exception {
        logCase(log, "节点故障", "回迁上限耗尽", "第 3 次掉线时作业 FAILED，原因 REQUEUE_LIMIT_EXCEEDED");
        engine.registerNode("a", 4, 8_192, 0);
        engine.registerNode("b", 4, 8_192, 0);
        engine.submit(new SubmitRequest("flaky", "反复故障作业", 4, 1024, 0, 0, 60_000));

        engine.offlineNode("a");
        awaitState(engine, "flaky", JobState.RUNNING, Duration.ofSeconds(3));
        assertEquals("b", engine.getJob("flaky").getNodeId());

        engine.offlineNode("b");
        engine.readyNode("a");
        awaitState(engine, "flaky", JobState.RUNNING, Duration.ofSeconds(3));
        assertEquals("a", engine.getJob("flaky").getNodeId());
        assertEquals(2, engine.getJob("flaky").getRequeueCount());

        engine.offlineNode("a");
        assertEquals(JobState.FAILED, engine.getJob("flaky").getState(), "第 3 次回迁超过上限 2，必须失败");
        assertNotNull(engine.getJob("flaky").getFailReason());
        assertEquals(true, engine.getJob("flaky").getFailReason().contains("REQUEUE_LIMIT_EXCEEDED"));
        engine.verifyLedger("回迁上限失败后");
        logResult(log, "回迁上限", "作业 FAILED，原因=" + engine.getJob("flaky").getFailReason());
    }
}
