package com.github.lechandonga.clusterjobschedulerjava.core;

/**
 * 带可区分错误码的业务异常。拒绝提交、非法操作必须抛出明确原因，
 * 不允许静默排队或吞掉错误。
 */
public class SchedulerException extends RuntimeException {

    public enum Code {
        INVALID_PARAMETER,
        DUPLICATE_JOB_ID,
        EXCEEDS_CLUSTER_CAPACITY,
        REQUEUE_LIMIT_EXCEEDED,
        JOB_NOT_FOUND,
        JOB_ALREADY_TERMINAL,
        JOB_ALREADY_CANCELLED,
        NODE_NOT_FOUND,
        DUPLICATE_NODE_ID
    }

    private final Code code;

    public SchedulerException(Code code, String message) {
        super(message);
        this.code = code;
    }

    public Code getCode() {
        return code;
    }
}
