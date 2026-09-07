package dev.devopsnote.kafkarunner;

import static org.assertj.core.api.Assertions.assertThat;

import dev.devopsnote.kafkarunner.command.CommandRegistry;
import dev.devopsnote.kafkarunner.sample.SampleAvroConsumeCommand;
import dev.devopsnote.kafkarunner.sample.SampleConsumeCommand;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

/** Spring 컨텍스트가 브로커·SR 없이 뜨고(팩토리는 연결을 지연한다) 샘플 명령이 @Order 순서로 등록되는지. */
@SpringBootTest
class RunnerContextTest {
    @Autowired CommandRegistry registry;
    @Autowired KafkaListenerEndpointRegistry kafkaRegistry;

    @Test
    void registersScenarioAndSampleCommands() {
        assertThat(registry.names()).containsExactly(
            "normal-roundtrip", "broker-1-down", "broker-2-down", "total-outage",
            "sample-produce", "sample-consume", "sample-avro-produce", "sample-avro-consume",
            "sample-schema-evolution");
    }

    @Test
    void sampleListenerIsRegisteredButNotStarted() {
        // autoStartup=false 가 유지되는지 — 장애 시나리오 실행 중 샘플 컨슈머가 그룹에 참여하면 안 된다
        var container = kafkaRegistry.getListenerContainer(SampleConsumeCommand.LISTENER_ID);
        assertThat(container).isNotNull();
        assertThat(container.isRunning()).isFalse();

        var avroContainer = kafkaRegistry.getListenerContainer(SampleAvroConsumeCommand.LISTENER_ID);
        assertThat(avroContainer).isNotNull();
        assertThat(avroContainer.isRunning()).isFalse();
    }
}
