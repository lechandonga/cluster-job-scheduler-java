package com.github.lechandonga.clusterjobschedulerjava.scheduler.model;

/**
 * 不可变的资源三元组（CPU 核数、内存 MB、GPU 卡数）。
 * 所有资源账目均以该类型做整体加减，调度要求作业的需求必须被节点的可用资源整体满足。
 */
public record Resources(long cpus, long memoryMb, long gpus) {

    public Resources {
        if (cpus < 0 || memoryMb < 0 || gpus < 0) {
            throw new IllegalArgumentException("资源数量不能为负数: cpus=" + cpus + ", memoryMb=" + memoryMb + ", gpus=" + gpus);
        }
    }

    public static Resources of(long cpus, long memoryMb, long gpus) {
        return new Resources(cpus, memoryMb, gpus);
    }

    /** 是否能整体容纳 other（三种资源同时满足）。 */
    public boolean contains(Resources other) {
        return this.cpus >= other.cpus
                && this.memoryMb >= other.memoryMb
                && this.gpus >= other.gpus;
    }

    public Resources subtract(Resources other) {
        if (!contains(other)) {
            throw new IllegalStateException("资源账目被扣成负数: available=" + this + ", request=" + other);
        }
        return new Resources(cpus - other.cpus, memoryMb - other.memoryMb, gpus - other.gpus);
    }

    public Resources add(Resources other) {
        return new Resources(cpus + other.cpus, memoryMb + other.memoryMb, gpus + other.gpus);
    }

    public boolean isZero() {
        return cpus == 0 && memoryMb == 0 && gpus == 0;
    }
}
