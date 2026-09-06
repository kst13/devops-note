package dev.devopsnote.kafkarunner;

import static org.assertj.core.api.Assertions.assertThat;

import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.command.CommandRegistry;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;

class RunnerApplicationTest {
    private static final RunnerProperties PROPS = new RunnerProperties(
        "localhost:9092", "t", 3, List.of("kafka1", "kafka2", "kafka3"), "reports", "s", "s.avro");

    static Command command(String name, int exitCode) {
        return new Command() {
            @Override public String name() { return name; }
            @Override public String description() { return name; }
            @Override public int run(List<String> args) {
                if (!args.isEmpty() && args.get(0).equals("bad")) throw new IllegalArgumentException("bad arg");
                return exitCode;
            }
        };
    }

    static int exitCodeFor(String... args) throws Exception {
        var app = new RunnerApplication(new CommandRegistry(PROPS, List.of(command("ok", 0), command("fail", 1))));
        app.run(new DefaultApplicationArguments(args));
        return app.getExitCode();
    }

    @Test void noArgsIsUsageError() throws Exception { assertThat(exitCodeFor()).isEqualTo(2); }
    @Test void unknownCommandIsUsageError() throws Exception { assertThat(exitCodeFor("nope")).isEqualTo(2); }
    @Test void commandExitCodeIsPropagated() throws Exception {
        assertThat(exitCodeFor("ok")).isEqualTo(0);
        assertThat(exitCodeFor("fail")).isEqualTo(1);
    }
    @Test void illegalArgumentIsUsageError() throws Exception { assertThat(exitCodeFor("ok", "bad")).isEqualTo(2); }
}
