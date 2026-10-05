package com.github.lechandonga.clusterjobschedulerjava.web;

import java.nio.file.Path;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.github.lechandonga.clusterjobschedulerjava.core.SchedulerEngine;
import com.github.lechandonga.clusterjobschedulerjava.core.StateStore;

import jakarta.annotation.PreDestroy;

@Configuration
public class SchedulerConfig {

    @Value("${scheduler.data-file:target/scheduler-data/scheduler-state.json}")
    private String dataFile;

    @Value("${scheduler.max-requeue:2}")
    private int maxRequeue;

    private SchedulerEngine engine;

    @Bean
    public SchedulerEngine schedulerEngine() {
        engine = new SchedulerEngine(new StateStore(Path.of(dataFile)), maxRequeue);
        engine.start();
        return engine;
    }

    @PreDestroy
    public void shutdown() {
        if (engine != null) {
            engine.stop();
        }
    }
}
