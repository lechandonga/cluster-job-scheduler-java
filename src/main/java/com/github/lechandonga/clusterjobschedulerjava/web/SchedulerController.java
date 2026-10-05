package com.github.lechandonga.clusterjobschedulerjava.web;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.github.lechandonga.clusterjobschedulerjava.core.Job;
import com.github.lechandonga.clusterjobschedulerjava.core.Node;
import com.github.lechandonga.clusterjobschedulerjava.core.SchedulerEngine;
import com.github.lechandonga.clusterjobschedulerjava.core.SchedulerException;
import com.github.lechandonga.clusterjobschedulerjava.core.SubmitRequest;
import com.github.lechandonga.clusterjobschedulerjava.web.ApiDtos.ErrorResponse;
import com.github.lechandonga.clusterjobschedulerjava.web.ApiDtos.JobBatchRequest;
import com.github.lechandonga.clusterjobschedulerjava.web.ApiDtos.JobSubmitRequest;
import com.github.lechandonga.clusterjobschedulerjava.web.ApiDtos.NodeRequest;

@RestController
@RequestMapping("/api")
public class SchedulerController {

    private final SchedulerEngine engine;

    public SchedulerController(SchedulerEngine engine) {
        this.engine = engine;
    }

    @PostMapping("/nodes")
    public Node registerNode(@RequestBody NodeRequest request) {
        return engine.registerNode(request.id(),
                request.cpu() == null ? 0 : request.cpu(),
                request.memoryMb() == null ? 0 : request.memoryMb(),
                request.gpu() == null ? 0 : request.gpu());
    }

    @PostMapping("/nodes/{id}/drain")
    public Node drain(@PathVariable String id) {
        return engine.drainNode(id);
    }

    @PostMapping("/nodes/{id}/offline")
    public Node offline(@PathVariable String id) {
        return engine.offlineNode(id);
    }

    @PostMapping("/nodes/{id}/ready")
    public Node ready(@PathVariable String id) {
        return engine.readyNode(id);
    }

    @GetMapping("/nodes")
    public List<Node> nodes() {
        return engine.listNodes();
    }

    @PostMapping("/jobs")
    public Job submit(@RequestBody JobSubmitRequest request) {
        return engine.submit(request.toCore());
    }

    @PostMapping("/jobs/batch")
    public List<Job> submitBatch(@RequestBody JobBatchRequest request) {
        List<SubmitRequest> requests = request.jobs().stream().map(JobSubmitRequest::toCore).toList();
        return engine.submitBatch(requests);
    }

    @PostMapping("/jobs/{id}/cancel")
    public Job cancel(@PathVariable String id) {
        return engine.cancel(id);
    }

    @GetMapping("/jobs/{id}")
    public Job job(@PathVariable String id) {
        return engine.getJob(id);
    }

    @GetMapping("/jobs")
    public List<Job> jobs() {
        return engine.listJobs();
    }

    @GetMapping("/decisions")
    public Map<String, Object> decisions() {
        return Map.of("decisions", engine.decisionLog());
    }

    @ExceptionHandler(SchedulerException.class)
    public ResponseEntity<ErrorResponse> handleScheduler(SchedulerException ex) {
        HttpStatus status = switch (ex.getCode()) {
            case JOB_NOT_FOUND, NODE_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case INVALID_PARAMETER, DUPLICATE_JOB_ID, DUPLICATE_NODE_ID,
                 EXCEEDS_CLUSTER_CAPACITY, REQUEUE_LIMIT_EXCEEDED,
                 JOB_ALREADY_TERMINAL, JOB_ALREADY_CANCELLED -> HttpStatus.UNPROCESSABLE_ENTITY;
        };
        return ResponseEntity.status(status)
                .body(new ErrorResponse(ex.getCode().name(), ex.getMessage()));
    }
}
