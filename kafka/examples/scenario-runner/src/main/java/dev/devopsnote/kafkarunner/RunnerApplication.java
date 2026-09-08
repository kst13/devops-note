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
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/** CLI 진입점. 첫 인자로 명령을 고른다. 첫 인자가 web 이면 로컬 웹 제어판을 띄운다. */
@SpringBootApplication
@EnableConfigurationProperties(RunnerProperties.class)
public class RunnerApplication implements ApplicationRunner, ExitCodeGenerator {
    static final String WEB = "web";

    private final CommandRegistry registry;
    private int exitCode = 0;

    public RunnerApplication(CommandRegistry registry) { this.registry = registry; }

    public static void main(String[] args) {
        SampleKafkaConfig.trustGeneratedAvroClasses(); // Avro 로딩 전에 설정
        if (args.length > 0 && args[0].equals(WEB)) {
            // application.yml 은 CLI 모드를 위해 spring.main.web-application-type=none 을 강제한다.
            // 이 프로퍼티는 SpringApplicationBuilder#web(...)보다 나중에 바인딩되어 덮어써 버리므로,
            // 시스템 프로퍼티(더 높은 우선순위)로 servlet 을 강제해 웹 모드에서 실제로 서버가 뜨게 한다.
            System.setProperty("spring.main.web-application-type", "servlet");
            // 127.0.0.1 전용 서블릿 웹으로 기동하고 서버를 유지한다(종료 코드로 내려가지 않음)
            new SpringApplicationBuilder(RunnerApplication.class)
                .web(WebApplicationType.SERVLET)
                .properties("server.address=127.0.0.1", "server.port=8088")
                .run(args);
            System.out.println("웹 제어판: http://127.0.0.1:8088  (종료: Ctrl+C)");
            return;
        }
        System.exit(SpringApplication.exit(SpringApplication.run(RunnerApplication.class, args)));
    }

    @Override public void run(ApplicationArguments args) throws Exception {
        List<String> nonOption = args.getNonOptionArgs();
        if (!nonOption.isEmpty() && nonOption.get(0).equals(WEB)) {
            return; // 웹 모드 — 서버가 요청을 처리하므로 CLI 콜백은 아무것도 하지 않는다
        }
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
