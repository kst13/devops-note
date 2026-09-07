package dev.devopsnote.kafkarunner;

import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.command.CommandRegistry;
import dev.devopsnote.kafkarunner.command.UsageException;
import dev.devopsnote.kafkarunner.sample.SampleKafkaConfig;
import java.util.List;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/** CLI 진입점. 첫 인자로 명령을 고르고 나머지 인자를 넘긴 뒤 종료 코드만 다룬다 (0 성공, 1 실패, 2 사용법 오류). */
@SpringBootApplication
@EnableConfigurationProperties(RunnerProperties.class)
public class RunnerApplication implements ApplicationRunner, ExitCodeGenerator {
    private final CommandRegistry registry;
    private int exitCode = 0;

    public RunnerApplication(CommandRegistry registry) { this.registry = registry; }

    public static void main(String[] args) {
        SampleKafkaConfig.trustGeneratedAvroClasses(); // Avro 생성 클래스 신뢰 목록 — Avro 로딩 전에 설정해야 한다
        System.exit(SpringApplication.exit(SpringApplication.run(RunnerApplication.class, args)));
    }

    @Override public void run(ApplicationArguments args) throws Exception {
        List<String> nonOption = args.getNonOptionArgs();
        if (nonOption.isEmpty()) {
            System.out.print(registry.usage());
            exitCode = 2;
            return;
        }
        String name = nonOption.get(0);
        Command command = registry.find(name).orElse(null);
        if (command == null) {
            System.out.print("알 수 없는 명령: " + name + "\n\n" + registry.usage());
            exitCode = 2;
            return;
        }
        try {
            exitCode = command.run(nonOption.subList(1, nonOption.size()));
        } catch (UsageException e) {
            System.out.print("인자 오류: " + e.getMessage() + "\n\n" + registry.usage());
            exitCode = 2;
        }
    }

    @Override public int getExitCode() { return exitCode; }
}
