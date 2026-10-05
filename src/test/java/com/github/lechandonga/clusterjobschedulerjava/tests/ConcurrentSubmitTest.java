package com.github.lechandonga.clusterjobschedulerjava.tests;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lechandonga.clusterjobschedulerjava.core.JobState;
import com.github.lechandonga.clusterjobschedulerjava.core.SchedulerEngine;
import com.github.lechandonga.clusterjobschedulerjava.core.SchedulerException;
import com.github.lechandonga.clusterjobschedulerjava.core.SubmitRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：并发提交。
 * 覆盖：高并发下只有唯一作业被受理（重复 id 拒绝）、资源账目一致、非法参数与超容量拒绝。
 */
@DisplayName("业务1-并发提交与提交拒绝")
class ConcurrentSubmitTest extends SchedulerTestSupport {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentSubmitTest.class);

    private SchedulerEngine engine;

    @BeforeEach
    void setUp() {
        engine = newEngine();
        engine.registerNode("n1", 8, 16_384, 2);
    }

    @AfterEach
    void tearDown() {
        engine.stop();
    }

    @Test
    @DisplayName("用例1.1: 50 个线程并发提交同一作业 id，只有 1 个成功，其余得到 DUPLICATE_JOB_ID")
    void concurrentDuplicateIdOnlyOneAccepted() throws Exception {
        logCase(log, "并发提交", "同 id 并发提交", "仅 1 个成功，49 个以 DUPLICATE_JOB_ID 被拒绝");
        int threads = 50;
        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicated = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    engine.submit(new SubmitRequest("same-job", "并发作业", 1, 1024, 0, 0, 60_000));
                    accepted.incrementAndGet();
                } catch (SchedulerException e) {
                    if (e.getCode() == SchedulerException.Code.DUPLICATE_JOB_ID) {
                        duplicated.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        assertEquals(1, accepted.get(), "同一 id 只能被受理一次");
        assertEquals(threads - 1, duplicated.get(), "其余并发提交必须以可区分原因 DUPLICATE_JOB_ID 拒绝");
        engine.verifyLedger("并发重复提交后");
        assertEquals(1, engine.listJobs().size());
        logResult(log, "同 id 并发提交", "accepted=" + accepted.get() + ", duplicated=" + duplicated.get() + "，账目校验通过");
    }

    @Test
    @DisplayName("用例1.2: 32 个不同小作业并发提交，全部被调度且节点账目不超扣、不为负")
    void concurrentDistinctJobsLedgerConsistent() throws Exception {
        logCase(log, "并发提交", "32 个不同作业抢占 8 CPU 节点", "部分 RUNNING、其余 QUEUED，账目恒等式成立");
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int idx = i;
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    engine.submit(new SubmitRequest("j" + idx, "作业-" + idx, 1, 512, 0, 0, 60_000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        long running = engine.listJobs().stream().filter(j -> j.getState() == JobState.RUNNING).count();
        long queued = engine.listJobs().stream().filter(j -> j.getState() == JobState.QUEUED).count();
        assertEquals(threads, running + queued, "没有作业被吞掉");
        assertEquals(8, running, "节点只有 8 CPU，恰好运行 8 个 1-CPU 作业，不能超扣");
        assertEquals(24, queued, "资源不足的作业整体排队，不占部分资源");
        engine.verifyLedger("并发不同作业提交后");
        logResult(log, "32 作业抢占", "RUNNING=" + running + ", QUEUED=" + queued + "，used 不超过 capacity");
    }

    @Test
    @DisplayName("用例1.3: 非法参数 / 超单节点容量 / 批量重复 id 均整批拒绝，原因可区分")
    void invalidSubmissionsRejectedWithDistinctReasons() {
        logCase(log, "并发提交", "三类非法提交", "分别返回 INVALID_PARAMETER / EXCEEDS_CLUSTER_CAPACITY / DUPLICATE_JOB_ID");

        SchedulerException invalid = assertThrows(SchedulerException.class,
                () -> engine.submit(new SubmitRequest("bad", "x", 0, -1, 0, 0, 100)));
        assertEquals(SchedulerException.Code.INVALID_PARAMETER, invalid.getCode());

        SchedulerException tooBig = assertThrows(SchedulerException.class,
                () -> engine.submit(new SubmitRequest("big", "x", 100, 1024, 0, 0, 100)));
        assertEquals(SchedulerException.Code.EXCEEDS_CLUSTER_CAPACITY, tooBig.getCode());

        engine.submit(new SubmitRequest("dup", "x", 1, 1024, 0, 0, 60_000));
        SchedulerException duplicate = assertThrows(SchedulerException.class,
                () -> engine.submit(new SubmitRequest("dup", "x", 1, 1024, 0, 0, 60_000)));
        assertEquals(SchedulerException.Code.DUPLICATE_JOB_ID, duplicate.getCode());

        List<SubmitRequest> batch = List.of(
                new SubmitRequest(UUID.randomUUID().toString(), "a", 1, 1024, 0, 0, 100),
                new SubmitRequest("batch-dup", "b", 1, 1024, 0, 0, 100),
                new SubmitRequest("batch-dup", "c", 1, 1024, 0, 0, 100));
        SchedulerException batchRejected = assertThrows(SchedulerException.class, () -> engine.submitBatch(batch));
        assertEquals(SchedulerException.Code.DUPLICATE_JOB_ID, batchRejected.getCode());
        assertTrue(engine.listJobs().stream().noneMatch(j -> j.getName().equals("a")), "批量原子性：整批拒绝，不得部分入队");

        engine.verifyLedger("非法提交后");
        assertNotNull(engine.getJob("dup"));
        logResult(log, "三类非法提交",
                "错误码依次为 " + invalid.getCode() + " / " + tooBig.getCode() + " / " + duplicate.getCode()
                        + "，批量提交保持原子性");
    }
}
