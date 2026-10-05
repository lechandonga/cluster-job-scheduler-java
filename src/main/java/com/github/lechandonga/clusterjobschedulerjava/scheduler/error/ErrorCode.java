package com.github.lechandonga.clusterjobschedulerjava.scheduler.error;

import org.springframework.http.HttpStatus;

/**
 * 可区分的业务拒绝/失败原因。每个原因对应稳定的字符串 code 与 HTTP 状态，
 * 调用方可依据 code 做分支处理，而不是解析错误消息文本。
 */
public enum ErrorCode {
    INVALID_PARAMETER("参数非法（资源为负、名称为空、数组范围非法等）", HttpStatus.BAD_REQUEST),
    DUPLICATE_JOB("作业 ID 重复", HttpStatus.CONFLICT),
    JOB_NOT_FOUND("作业不存在", HttpStatus.NOT_FOUND),
    NODE_NOT_FOUND("节点不存在", HttpStatus.NOT_FOUND),
    DUPLICATE_NODE("节点 ID 已注册", HttpStatus.CONFLICT),
    EXCEEDS_NODE_CAPACITY("作业需求超过任何单节点容量上限", HttpStatus.UNPROCESSABLE_ENTITY),
    INSUFFICIENT_RESOURCES("当前无足够资源，作业已排队等待", HttpStatus.CONFLICT),
    ILLEGAL_STATE_TRANSITION("目标对象当前状态不允许该操作", HttpStatus.CONFLICT);

    private final String description;
    private final HttpStatus httpStatus;

    ErrorCode(String description, HttpStatus httpStatus) {
        this.description = description;
        this.httpStatus = httpStatus;
    }

    public String code() {
        return name();
    }

    public String description() {
        return description;
    }

    public HttpStatus httpStatus() {
        return httpStatus;
    }
}
