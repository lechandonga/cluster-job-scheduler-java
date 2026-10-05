package com.github.lechandonga.clusterjobschedulerjava.core;

/**
 * 作业提交请求。数组式批量提交即提交多个该请求，整体校验、原子受理。
 */
public record SubmitRequest(String id, String name, long cpu, long memoryMb, long gpu,
                            int priority, long durationMillis) {

    public Resources requirement() {
        return new Resources(cpu, memoryMb, gpu);
    }
}
