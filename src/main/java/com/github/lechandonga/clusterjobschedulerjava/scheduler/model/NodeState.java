package com.github.lechandonga.clusterjobschedulerjava.scheduler.model;

/**
 * 节点健康状态：
 * READY    就绪，可接受调度；
 * DRAINING 排空，不再接受新作业，已运行作业被确定性回迁到其他就绪节点，回迁失败（无节点）则失败；
 * OFFLINE  离线，不再参与调度，其上运行作业按确定策略失败（NODE_OFFLINE）。
 */
public enum NodeState {
    READY,
    DRAINING,
    OFFLINE
}
