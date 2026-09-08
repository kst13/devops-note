package dev.devopsnote.kafkarunner.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.devopsnote.kafkarunner.RunnerProperties;
import java.util.List;
import org.junit.jupiter.api.Test;

class CommandRegistryTest {
    private static final RunnerProperties PROPS = new RunnerProperties(
        "localhost:9092", "test.scenario.events", 3, List.of("kafka1", "kafka2", "kafka3"), "reports",
        "commerce.order.created", "commerce.order.created-avro");

    record FakeCommand(String name) implements Command {
        @Override public String description() { return "fake"; }
        @Override public int run(List<String> args) { return 0; }
    }

    @Test
    void registersScenariosFirstThenSamples() {
        var registry = new CommandRegistry(ScenarioCommand.all(PROPS), List.of(new FakeCommand("sample-x")));
        assertThat(registry.names()).containsExactly(
            "normal-roundtrip", "broker-1-down", "broker-2-down", "total-outage", "sample-x");
    }

    @Test
    void unknownNameIsEmpty() {
        var registry = new CommandRegistry(ScenarioCommand.all(PROPS), List.of());
        assertThat(registry.find("nope")).isEmpty();
        assertThat(registry.find("broker-1-down")).isPresent();
    }

    @Test
    void rejectsDuplicateNames() {
        assertThatThrownBy(() -> new CommandRegistry(ScenarioCommand.all(PROPS), List.of(new FakeCommand("normal-roundtrip"))))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("normal-roundtrip");
    }

    @Test
    void usageListsScenariosAndSamplesInSeparateGroups() {
        var registry = new CommandRegistry(ScenarioCommand.all(PROPS), List.of(new FakeCommand("sample-x")));
        String usage = registry.usage();
        assertThat(usage).contains("장애 시나리오:").contains("사용 예시:");
        assertThat(usage.indexOf("장애 시나리오:")).isLessThan(usage.indexOf("사용 예시:"));
        assertThat(usage).contains("normal-roundtrip").contains("sample-x");
    }

    @Test
    void usageOmitsSampleSectionWhenEmpty() {
        var registry = new CommandRegistry(ScenarioCommand.all(PROPS), List.of());
        assertThat(registry.usage()).doesNotContain("사용 예시:");
    }
}
