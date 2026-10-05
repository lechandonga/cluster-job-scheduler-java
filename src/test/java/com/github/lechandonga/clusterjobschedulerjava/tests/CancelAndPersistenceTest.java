package com.github.lechandonga.clusterjobschedulerjava.tests;

import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lechandonga.clusterjobschedulerjava.core.Job;
import com.github.lechandonga.clusterjobschedulerjava.core.JobState;
import com.github.lechandonga.clusterjobschedulerjava.core.SchedulerEngine;
import com.github.lechandonga.clusterjobschedulerjava.core.SchedulerException;
import com.github.lechandonga.clusterjobschedulerjava.core.SubmitRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 业务：幂等取消与重启恢复。
 * 覆盖：排队/运行中取消、重复取消与终态操作的确定拒绝、重启后队列账目状态一致。
 */
@DisplayName("业务4-幂等取消与本地持久化恢复")
class CancelAndPersistenceTest extends SchedulerTestSupport {

    private static final Logger log = LoggerFactory.getLogger(CancelAndPersistenceTest.class);

    private SchedulerEngine engine;

    @BeforeEach
    void setUp() {
        engine = newEngine();
        engine.registerNode("n1", 4, 4_096, 0);
    }

    @AfterEach
    void tearDown() {
        engine.stop();
    }

    @Test
    @DisplayName("用例4.1: 取消排队作业与运行中作业；重复取消/终态取消得到可区分拒绝且状态不回退")
    void idempotentCancelWithDistinctErrors() throws Exception {
        logCase(log, "幂等取消", "重复取消与终态操作", "首次取消成功，重复取消 JOB_ALREADY_CANCELLED，已完成作业 JOB_ALREADY_TERMINAL");
        engine.submit(new SubmitRequest("running", "运行作业", 4, 1024, 0, 0, 60_000));
        engine.submit(new SubmitRequest("queued", "排队作业", 4, 1024, 0, 0, 60_000));

        assertEquals(JobState.RUNNING, engine.getJob("running").getState());
        assertEquals(JobState.QUEUED, engine.getJob("queued").getState());

        assertEquals(JobState.CANCELLED, engine.cancel("queued").getState());
        assertEquals(JobState.CANCELLED, engine.cancel("running").getState());
        engine.verifyLedger("取消运行中作业后");
        assertEquals(0L, engine.listNodes().get(0).getUsed().cpu(), "取消运行中作业必须回收资源");

        SchedulerException repeat = org.junit.jupiter.api.Assertions.assertThrows(SchedulerException.class,
                () -> engine.cancel("running"));
        assertEquals(SchedulerException.Code.JOB_ALREADY_CANCELLED, repeat.getCode());
        assertEquals(JobState.CANCELLED, engine.getJob("running").getState(), "重复取消不能把状态改回去");

        engine.submit(new SubmitRequest("short", "短命作业", 2, 1024, 0, 0, 150));
        awaitState(engine, "short", JobState.COMPLETED, Duration.ofSeconds(5));
        SchedulerException cancelDone = org.junit.jupiter.api.Assertions.assertThrows(SchedulerException.class,
                () -> engine.cancel("short"));
        assertEquals(SchedulerException.Code.JOB_ALREADY_TERMINAL, cancelDone.getCode());

        SchedulerException missing = org.junit.jupiter.api.Assertions.assertThrows(SchedulerException.class,
                () -> engine.cancel("ghost"));
        assertEquals(SchedulerException.Code.JOB_NOT_FOUND, missing.getCode());

        logResult(log, "幂等取消",
                "排队/运行取消成功并回收资源；重复取消=" + repeat.getCode()
                        + "，终态取消=" + cancelDone.getCode() + "，缺失作业=" + missing.getCode());
    }

    @Test
    @DisplayName("用例4.2: 服务重启后从本地文件恢复队列、作业状态与资源账目，且排队作业可继续被调度")
    void restartRestoresQueueAndLedger() throws Exception {
        logCase(log, "持久化恢复", "含排队作业的重启", "重启前 QUEUED/RUNNING/账目在重启后一致，剩余作业最终完成");
        engine.submit(new SubmitRequest("keep-running", "长作业", 4, 1024, 0, 0, 60_000));
        engine.submit(new SubmitRequest("restart-wait", "重启后等待", 4, 1024, 0, 0, 200));
        assertEquals(JobState.RUNNING, engine.getJob("keep-running").getState());
        assertEquals(JobState.QUEUED, engine.getJob("restart-wait").getState());

        engine = restartEngine(engine);
        Job running = engine.getJob("keep-running");
        Job waiting = engine.getJob("restart-wait");
        assertEquals(JobState.RUNNING, running.getState(), "RUNNING 状态从快照恢复");
        assertEquals("n1", running.getNodeId());
        assertEquals(JobState.QUEUED, waiting.getState(), "QUEUED 状态从快照恢复");
        assertEquals(4L, engine.listNodes().get(0).getUsed().cpu(), "节点账目与重启前一致，无重复扣减");
        engine.verifyLedger("重启恢复");
        logResult(log, "重启恢复", "队列、RUNNING 状态、节点 used=2 均与重启前一致");

        engine.cancel("keep-running");
        awaitState(engine, "restart-wait", JobState.RUNNING, Duration.ofSeconds(3));
        awaitState(engine, "restart-wait", JobState.COMPLETED, Duration.ofSeconds(5));
        engine.verifyLedger("恢复后调度完成");

        engine = restartEngine(engine);
        assertEquals(JobState.COMPLETED, engine.getJob("restart-wait").getState(), "终态同样持久化");
        assertEquals(JobState.CANCELLED, engine.getJob("keep-running").getState());
        assertEquals(0L, engine.listNodes().get(0).getUsed().cpu());
        logResult(log, "二次重启", "完成/取消终态与零账目在再次重启后仍然一致");
    }
}
