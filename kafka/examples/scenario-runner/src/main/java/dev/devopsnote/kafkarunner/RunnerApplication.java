package dev.devopsnote.kafkarunner;

import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.ledger.Judge;
import dev.devopsnote.kafkarunner.ledger.Ledger;
import dev.devopsnote.kafkarunner.report.Reporter;
import dev.devopsnote.kafkarunner.scenario.Broker1Down;
import dev.devopsnote.kafkarunner.scenario.Broker2Down;
import dev.devopsnote.kafkarunner.scenario.NormalRoundtrip;
import dev.devopsnote.kafkarunner.scenario.Scenario;
import java.util.Map;
import java.util.function.Supplier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(RunnerProperties.class)
public class RunnerApplication implements ApplicationRunner, ExitCodeGenerator {
    private final RunnerProperties props;
    private int exitCode = 0;

    public RunnerApplication(RunnerProperties props) { this.props = props; }

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(RunnerApplication.class, args)));
    }

    private static final Map<String, Supplier<Scenario>> SCENARIOS = Map.of(
        "normal-roundtrip", NormalRoundtrip::new,
        "broker-1-down", Broker1Down::new,
        "broker-2-down", Broker2Down::new);

    @Override public void run(ApplicationArguments args) throws Exception {
        if (args.getNonOptionArgs().isEmpty()) {
            System.out.println("사용법: java -jar scenario-runner.jar <시나리오>\n시나리오: " + SCENARIOS.keySet());
            exitCode = 2;
            return;
        }
        String name = args.getNonOptionArgs().get(0);
        Supplier<Scenario> supplier = SCENARIOS.get(name);
        if (supplier == null) {
            System.out.println("알 수 없는 시나리오: " + name + " / 가능: " + SCENARIOS.keySet());
            exitCode = 2;
            return;
        }

        Scenario scenario = supplier.get();
        FaultInjector injector = new FaultInjector(props.bootstrapServers(), props.containers());
        Ledger ledger = new Ledger();
        try {
            injector.ensureAllRunning();
            injector.resetTopic(props.topic(), props.partitions());
            scenario.run(props, injector, ledger);
        } finally {
            injector.startAll(); // 어떤 경우에도 클러스터 원상 복구
        }
        var result = new Judge().judge(ledger, scenario.expectation());
        new Reporter().write(props.reportDir(), scenario.name(), scenario.description(), result);
        exitCode = result.pass() ? 0 : 1;
    }

    @Override public int getExitCode() { return exitCode; }
}
