package com.github.lechandonga.clusterjobschedulerjava.web;

import java.util.List;

import com.github.lechandonga.clusterjobschedulerjava.core.SubmitRequest;

public final class ApiDtos {

    private ApiDtos() {
    }

    public record NodeRequest(String id, Long cpu, Long memoryMb, Long gpu) {
    }

    public record JobSubmitRequest(String id, String name, Long cpu, Long memoryMb, Long gpu,
                                   Integer priority, Long durationMillis) {

        private static final int DEFAULT_PRIORITY = 0;
        private static final long DEFAULT_DURATION_MILLIS = 5_000;

        public SubmitRequest toCore() {
            return new SubmitRequest(
                    id,
                    name,
                    cpu == null ? 0 : cpu,
                    memoryMb == null ? 0 : memoryMb,
                    gpu == null ? 0 : gpu,
                    priority == null ? DEFAULT_PRIORITY : priority,
                    durationMillis == null ? DEFAULT_DURATION_MILLIS : durationMillis);
        }
    }

    public record JobBatchRequest(List<JobSubmitRequest> jobs) {
    }

    public record ErrorResponse(String code, String message) {
    }
}
