package dev.devopsnote.kafkarunner;

import static org.assertj.core.api.Assertions.assertThat;

import dev.devopsnote.kafkarunner.command.CommandRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/** Spring 컨텍스트가 브로커·SR 없이 뜨고(팩토리는 연결을 지연한다) 샘플 명령이 @Order 순서로 등록되는지. */
@SpringBootTest
class RunnerContextTest {
    @Autowired CommandRegistry registry;

    @Test
    void registersScenarioAndSampleCommands() {
        assertThat(registry.names()).containsExactly(
            "normal-roundtrip", "broker-1-down", "broker-2-down", "total-outage",
            "sample-produce", "sample-consume");
    }
}
