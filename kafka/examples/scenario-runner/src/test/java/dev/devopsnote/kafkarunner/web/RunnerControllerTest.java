package dev.devopsnote.kafkarunner.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.command.CommandRegistry;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class RunnerControllerTest {
    MockMvc mvc;

    static Command named(String name) {
        return new Command() {
            public String name() { return name; }
            public String description() { return name + " desc"; }
            public int run(List<String> args) { System.out.print("ran " + name); return 0; }
        };
    }

    @BeforeEach
    void setup() {
        // 시나리오(broker-1-down) + 샘플(sample-produce)이 모두 등록된 레지스트리
        var registry = new CommandRegistry(
            List.of(named("broker-1-down")),
            List.of(named("sample-produce")));
        mvc = MockMvcBuilders.standaloneSetup(new RunnerController(registry, new CommandInvoker())).build();
    }

    @Test
    void whitelistIncludesErrorScenariosButNoScenarioCommands() {
        // 오류 시나리오 두 명령은 브로커를 죽이지 않으므로 웹 실행 허용 목록에 포함된다
        assertThat(RunnerController.SAMPLE_COMMANDS)
            .contains("sample-produce-invalid", "sample-consume-poison")
            .doesNotContain("broker-1-down", "broker-2-down", "total-outage", "normal-roundtrip");
    }

    @Test
    void listsOnlySampleCommands() throws Exception {
        mvc.perform(get("/api/commands"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[?(@.name=='sample-produce')]").exists())
            .andExpect(jsonPath("$[?(@.name=='broker-1-down')]").doesNotExist());
    }

    @Test
    void runsWhitelistedSample() throws Exception {
        mvc.perform(post("/api/run").contentType("application/json")
                .content("{\"command\":\"sample-produce\",\"count\":6}"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.exitCode").value(0))
            .andExpect(jsonPath("$.output").value(org.hamcrest.Matchers.containsString("ran sample-produce")));
    }

    @Test
    void rejectsScenarioCommand() throws Exception {
        mvc.perform(post("/api/run").contentType("application/json")
                .content("{\"command\":\"broker-1-down\",\"count\":1}"))
            .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsUnknownCommand() throws Exception {
        mvc.perform(post("/api/run").contentType("application/json")
                .content("{\"command\":\"sample-nope\"}"))
            .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsOutOfRangeCount() throws Exception {
        mvc.perform(post("/api/run").contentType("application/json")
                .content("{\"command\":\"sample-produce\",\"count\":99999}"))
            .andExpect(status().isBadRequest());
    }
}
