package com.github.lechandonga.clusterjobschedulerjava.core;

/**
 * 一组计算资源（CPU 核数、内存 MB、GPU 卡数）。
 * 不可变对象，账目计算全部基于 long，杜绝精度问题与负值。
 */
public record Resources(long cpu, long memoryMb, long gpu) {

    public static Resources ZERO = new Resources(0, 0, 0);

    public Resources {
        if (cpu < 0 || memoryMb < 0 || gpu < 0) {
            throw new IllegalArgumentException("资源数量不能为负数: cpu=" + cpu + ", memoryMb=" + memoryMb + ", gpu=" + gpu);
        }
    }

    /** this 中是否每一维资源都不少于 other（作业需求能否被满足）。 */
    public boolean contains(Resources other) {
        return this.cpu >= other.cpu
                && this.memoryMb >= other.memoryMb
                && this.gpu >= other.gpu;
    }

    public Resources plus(Resources other) {
        return new Resources(
                this.cpu + other.cpu,
                this.memoryMb + other.memoryMb,
                this.gpu + other.gpu);
    }

    public Resources minus(Resources other) {
        if (!this.contains(other)) {
            throw new IllegalStateException("资源扣减后会出现负数: available=" + this + ", requested=" + other);
        }
        return new Resources(
                this.cpu - other.cpu,
                this.memoryMb - other.memoryMb,
                this.gpu - other.gpu);
    }

    @Override
    public String toString() {
        return "{cpu=" + cpu + ", memoryMb=" + memoryMb + ", gpu=" + gpu + "}";
    }
}
