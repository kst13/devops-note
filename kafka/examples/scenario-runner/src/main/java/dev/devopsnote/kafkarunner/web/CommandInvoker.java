package dev.devopsnote.kafkarunner.web;

import dev.devopsnote.kafkarunner.command.Command;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.springframework.stereotype.Component;

/** 명령 하나를 실행하며 표준출력을 문자열로 캡처한다. 전역 System.out 을 교체하므로 직렬 실행한다. */
@Component
public class CommandInvoker {

    public record InvocationResult(String output, int exitCode) {}

    public synchronized InvocationResult invoke(Command command, List<String> args) {
        PrintStream original = System.out;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PrintStream capture = new PrintStream(buffer, true, StandardCharsets.UTF_8);
        System.setOut(capture);
        try {
            int code = command.run(args);
            return new InvocationResult(buffer.toString(StandardCharsets.UTF_8), code);
        } catch (Exception e) {
            return new InvocationResult(buffer.toString(StandardCharsets.UTF_8)
                + "\n실행 실패: " + rootMessage(e), 1);
        } finally {
            System.setOut(original);
            capture.flush();
        }
    }

    private static String rootMessage(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage();
    }
}
