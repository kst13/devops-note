package dev.devopsnote.kafkarunner.command;

import dev.devopsnote.kafkarunner.RunnerProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CommandConfig {
    /** Command 빈이 하나도 없어도 동작해야 하므로 List 주입 대신 ObjectProvider 를 쓴다.
     *  orderedStream() 은 @Order 를 존중한다 — 샘플 명령이 사용법에 나오는 순서. */
    @Bean
    public CommandRegistry commandRegistry(RunnerProperties props, ObjectProvider<Command> commands) {
        return new CommandRegistry(props, commands.orderedStream().toList());
    }
}
