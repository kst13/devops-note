package dev.devopsnote.kafkarunner.command;

import dev.devopsnote.kafkarunner.RunnerProperties;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** 명령 이름 → Command. 장애 시나리오 4개가 먼저, 그 뒤에 Spring 빈으로 등록된 샘플 명령. */
public class CommandRegistry {
    private final Map<String, Command> commands = new LinkedHashMap<>();

    public CommandRegistry(RunnerProperties props, List<Command> sampleCommands) {
        ScenarioCommand.all(props).forEach(this::add);
        sampleCommands.forEach(this::add);
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
        commands.values().stream().filter(c -> c instanceof ScenarioCommand).forEach(c -> appendLine(sb, c));
        sb.append("\n사용 예시:\n");
        commands.values().stream().filter(c -> !(c instanceof ScenarioCommand)).forEach(c -> appendLine(sb, c));
        return sb.toString();
    }

    private static void appendLine(StringBuilder sb, Command c) {
        sb.append(String.format("  %-26s %s%n", c.name(), c.description()));
    }
}
