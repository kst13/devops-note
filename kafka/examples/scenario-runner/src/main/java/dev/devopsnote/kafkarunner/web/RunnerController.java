package dev.devopsnote.kafkarunner.web;

import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.command.CommandRegistry;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 웹 제어판 API. 안전을 위해 sample-* 화이트리스트 명령만 실행할 수 있다. */
@RestController
@RequestMapping("/api")
public class RunnerController {
    /** 웹에서 실행 허용하는 명령 — 브로커를 죽이지 않는 것만. 순서 고정. */
    public static final Set<String> SAMPLE_COMMANDS = new LinkedHashSet<>(List.of(
        "sample-produce", "sample-consume",
        "sample-avro-produce", "sample-avro-consume", "sample-schema-evolution",
        "sample-produce-invalid", "sample-consume-poison"));

    private final CommandRegistry registry;
    private final CommandInvoker invoker;

    public RunnerController(CommandRegistry registry, CommandInvoker invoker) {
        this.registry = registry;
        this.invoker = invoker;
    }

    public record CommandInfo(String name, String description) {}
    public record RunRequest(String command, Integer count) {}

    @GetMapping("/commands")
    public List<CommandInfo> commands() {
        return SAMPLE_COMMANDS.stream()
            .map(registry::find)
            .flatMap(java.util.Optional::stream)
            .map(c -> new CommandInfo(c.name(), c.description()))
            .toList();
    }

    @PostMapping("/run")
    public ResponseEntity<?> run(@RequestBody RunRequest req) {
        if (req.command() == null || !SAMPLE_COMMANDS.contains(req.command())) {
            return ResponseEntity.badRequest().body("허용되지 않은 명령: " + req.command());
        }
        if (req.count() != null && (req.count() < 1 || req.count() > 1000)) {
            return ResponseEntity.badRequest().body("count 범위 오류(1~1000): " + req.count());
        }
        Command command = registry.find(req.command()).orElse(null);
        if (command == null) {
            return ResponseEntity.badRequest().body("등록되지 않은 명령: " + req.command());
        }
        List<String> args = req.count() == null ? List.of() : List.of(String.valueOf(req.count()));
        return ResponseEntity.ok(invoker.invoke(command, args));
    }
}
