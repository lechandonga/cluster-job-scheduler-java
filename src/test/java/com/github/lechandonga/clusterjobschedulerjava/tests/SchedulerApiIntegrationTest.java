package com.github.lechandonga.clusterjobschedulerjava.tests;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Path;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 业务：HTTP 接口端到端验证拒绝原因可区分、决策日志可查询。
 */
@SpringBootTest
@AutoConfigureMockMvc
@DisplayName("业务5-REST 接口端到端")
class SchedulerApiIntegrationTest extends SchedulerTestSupport {

    private static final Logger log = LoggerFactory.getLogger(SchedulerApiIntegrationTest.class);

    @TempDir
    static Path tempDataDir;

    @DynamicPropertySource
    static void dataFile(DynamicPropertyRegistry registry) {
        registry.add("scheduler.data-file", () -> tempDataDir.resolve("it-state.json").toString());
    }

    @Autowired
    private MockMvc mockMvc;

    @Test
    @DisplayName("用例5.1: 非法参数、超容量、重复 id 通过 HTTP 返回不同错误码；决策日志可查")
    void rejectionCodesAreDistinctOverHttp() throws Exception {
        logCase(log, "REST 接口", "三类拒绝 + 决策日志", "422 响应体 code 分别为 INVALID_PARAMETER/EXCEEDS_CLUSTER_CAPACITY/DUPLICATE_JOB_ID");
        mockMvc.perform(post("/api/nodes")
                        .contentType("application/json")
                        .content("{\"id\":\"web-node\",\"cpu\":4,\"memoryMb\":4096,\"gpu\":0}"))
                .andExpect(status().isOk());

        mockMvc.perform(post("/api/jobs")
                        .contentType("application/json")
                        .content("{\"id\":\"bad\",\"name\":\"x\",\"cpu\":0,\"memoryMb\":1024}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.code").value("INVALID_PARAMETER"));

        mockMvc.perform(post("/api/jobs")
                        .contentType("application/json")
                        .content("{\"id\":\"huge\",\"name\":\"x\",\"cpu\":99,\"memoryMb\":1024,\"durationMillis\":100}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.code").value("EXCEEDS_CLUSTER_CAPACITY"));

        mockMvc.perform(post("/api/jobs")
                        .contentType("application/json")
                        .content("{\"id\":\"dup\",\"name\":\"x\",\"cpu\":1,\"memoryMb\":1024,\"durationMillis\":100}"))
                .andExpect(status().isOk());
        mockMvc.perform(post("/api/jobs")
                        .contentType("application/json")
                        .content("{\"id\":\"dup\",\"name\":\"x\",\"cpu\":1,\"memoryMb\":1024,\"durationMillis\":100}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.code").value("DUPLICATE_JOB_ID"));

        mockMvc.perform(get("/api/decisions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.decisions[0]").exists());
        logResult(log, "REST 拒绝码", "三类提交拒绝原因可区分，/api/decisions 可查询调度决策");
    }
}
