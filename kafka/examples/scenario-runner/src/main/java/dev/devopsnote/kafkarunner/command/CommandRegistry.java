package dev.devopsnote.kafkarunner.command;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** 명령 이름 → Command. 장애 시나리오 4개가 먼저, 그 뒤에 Spring 빈으로 등록된 샘플 명령. */
public class CommandRegistry {
    private final Map<String, Command> commands = new LinkedHashMap<>();
    private final List<Command> scenarios;
    private final List<Command> samples;

    public CommandRegistry(List<Command> scenarios, List<Command> samples) {
        this.scenarios = List.copyOf(scenarios);
        this.samples = List.copyOf(samples);
        this.scenarios.forEach(this::add);
        this.samples.forEach(this::add);
    }

    private void add(Command command) {
        if (commands.putIfAbsent(command.name(), command) != null) {
            throw new IllegalStateException("중복 명령 이름: " + command.name());
        }
    }

    public Optional<Command> find(String name) { return Optional.ofNullable(commands.get(name)); }

    public List<String> names() { return List.copyOf(commands.keySet()); }

    public String usage() {
        var sb = new StringBuilder("사용법: java -jar scenario-runner.jar <명령> [인자]\n\n장애 시나리오:\n");
        scenarios.forEach(c -> appendLine(sb, c));
        if (!samples.isEmpty()) {
            sb.append("\n사용 예시:\n");
            samples.forEach(c -> appendLine(sb, c));
        }
        return sb.toString();
    }

    private static void appendLine(StringBuilder sb, Command c) {
        sb.append(String.format("  %-26s %s\n", c.name(), c.description()));
    }
}
