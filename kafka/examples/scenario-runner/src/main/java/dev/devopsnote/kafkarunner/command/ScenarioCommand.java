package dev.devopsnote.kafkarunner.command;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.ledger.Judge;
import dev.devopsnote.kafkarunner.ledger.Ledger;
import dev.devopsnote.kafkarunner.report.Reporter;
import dev.devopsnote.kafkarunner.scenario.Broker1Down;
import dev.devopsnote.kafkarunner.scenario.Broker2Down;
import dev.devopsnote.kafkarunner.scenario.NormalRoundtrip;
import dev.devopsnote.kafkarunner.scenario.Scenario;
import dev.devopsnote.kafkarunner.scenario.TotalOutage;
import java.util.List;

/** 장애 시나리오 하나를 Command 로 감싼다: 토픽 초기화 → 시나리오 실행 → 판정 → 리포트. */
public class ScenarioCommand implements Command {
    private final Scenario scenario;
    private final RunnerProperties props;

    public ScenarioCommand(Scenario scenario, RunnerProperties props) {
        this.scenario = scenario;
        this.props = props;
    }

    /** 등록 순서가 곧 사용법 출력 순서. */
    public static List<Command> all(RunnerProperties props) {
        return List.of(
            new ScenarioCommand(new NormalRoundtrip(), props),
            new ScenarioCommand(new Broker1Down(), props),
            new ScenarioCommand(new Broker2Down(), props),
            new ScenarioCommand(new TotalOutage(), props));
    }

    @Override public String name() { return scenario.name(); }
    @Override public String description() { return scenario.description(); }

    @Override public int run(List<String> args) throws Exception {
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
        return result.pass() ? 0 : 1;
    }
}
