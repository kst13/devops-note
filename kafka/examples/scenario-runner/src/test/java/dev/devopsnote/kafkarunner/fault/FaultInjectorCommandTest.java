package dev.devopsnote.kafkarunner.fault;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class FaultInjectorCommandTest {
    @Test
    void buildsDockerCommands() {
        assertThat(FaultInjector.command("stop", "kafka2")).containsExactly("docker", "stop", "kafka2");
        assertThat(FaultInjector.command("start", "kafka1")).containsExactly("docker", "start", "kafka1");
    }
}
