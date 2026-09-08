package dev.devopsnote.kafkarunner.web;

import static org.assertj.core.api.Assertions.assertThat;

import dev.devopsnote.kafkarunner.command.Command;
import java.util.List;
import org.junit.jupiter.api.Test;

class CommandInvokerTest {
    static Command printing(String text, int code) {
        return new Command() {
            public String name() { return "p"; }
            public String description() { return "p"; }
            public int run(List<String> args) { System.out.print(text + args); return code; }
        };
    }

    @Test
    void capturesStdoutAndExitCode() {
        var result = new CommandInvoker().invoke(printing("hello", 0), List.of("6"));
        assertThat(result.output()).contains("hello").contains("6");
        assertThat(result.exitCode()).isEqualTo(0);
    }

    @Test
    void propagatesNonZeroExitCode() {
        var result = new CommandInvoker().invoke(printing("x", 1), List.of());
        assertThat(result.exitCode()).isEqualTo(1);
    }

    @Test
    void exceptionBecomesExitCodeOneWithRootMessage() {
        Command boom = new Command() {
            public String name() { return "b"; }
            public String description() { return "b"; }
            public int run(List<String> args) { throw new RuntimeException(new IllegalStateException("root cause")); }
        };
        var result = new CommandInvoker().invoke(boom, List.of());
        assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.output()).contains("root cause");
    }

    @Test
    void restoresSystemOutAfterRun() {
        var original = System.out;
        new CommandInvoker().invoke(printing("x", 0), List.of());
        assertThat(System.out).isSameAs(original);
    }
}
