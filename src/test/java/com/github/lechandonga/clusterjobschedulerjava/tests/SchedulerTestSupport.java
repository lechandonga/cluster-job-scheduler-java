package com.github.lechandonga.clusterjobschedulerjava.tests;

import java.nio.file.Path;
import java.time.Duration;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;

import com.github.lechandonga.clusterjobschedulerjava.core.Job;
import com.github.lechandonga.clusterjobschedulerjava.core.JobState;
import com.github.lechandonga.clusterjobschedulerjava.core.SchedulerEngine;
import com.github.lechandonga.clusterjobschedulerjava.core.StateStore;

/**
 * 测试公共工具：构造独立数据文件的引擎、业务日志、轮询等待。
 */
public abstract class SchedulerTestSupport {

    @TempDir
    Path tempDir;

    protected SchedulerEngine newEngine() {
        return newEngine(2);
    }

    protected SchedulerEngine newEngine(int maxRequeue) {
        SchedulerEngine engine = new SchedulerEngine(
                new StateStore(tempDir.resolve("scheduler-state.json")), maxRequeue);
        engine.start();
        return engine;
    }

    protected SchedulerEngine restartEngine(SchedulerEngine current) {
        current.stop();
        SchedulerEngine restarted = newEngine();
        return restarted;
    }

    protected static void logCase(Logger log, String business, String scenario, String expected) {
        log.info("========== 业务覆盖【{}】用例: {} | 预期: {} ==========", business, scenario, expected);
    }

    protected static void logResult(Logger log, String scenario, String actual) {
        log.info(">>> 用例【{}】测试结果: {}", scenario, actual);
    }

    protected static void awaitState(SchedulerEngine engine, String jobId, JobState state, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Job job = engine.getJob(jobId);
            if (job.getState() == state) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("作业 " + jobId + " 在 " + timeout + " 内未到达状态 " + state
                + "，实际=" + engine.getJob(jobId).getState());
    }

    protected static void awaitFor(BooleanSupplier condition, Duration timeout, String description)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("等待超时: " + description);
    }
}
