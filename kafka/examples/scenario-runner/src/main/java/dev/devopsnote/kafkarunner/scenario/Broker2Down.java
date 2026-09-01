package dev.devopsnote.kafkarunner.scenario;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.ledger.Expectation;
import dev.devopsnote.kafkarunner.ledger.Ledger;
import dev.devopsnote.kafkarunner.load.LoadGenerator;
import dev.devopsnote.kafkarunner.load.VerifierConsumer;
import java.time.Duration;

/** ③ 브로커 2대 정지: min.insync.replicas=2 위반으로 전송 실패가 나는 것이 정상.
 *  복구 후 실패분 재전송까지 하면 최종 유실 0. */
public class Broker2Down implements Scenario {
    @Override public String name() { return "broker-2-down"; }
    @Override public String description() { return "부하 중 2대 정지 — 쓰기 실패(기대 동작)와 복구 후 재전송 무유실 검증"; }
    @Override public Expectation expectation() { return Expectation.expectSendFailures(); }

    @Override public void run(RunnerProperties props, FaultInjector injector, Ledger ledger) throws Exception {
        try (var generator = new LoadGenerator(props.bootstrapServers(), props.topic(), ledger);
             var verifier = new VerifierConsumer(props.bootstrapServers(), props.topic(), ledger)) {
            verifier.start();
            generator.start();
            Thread.sleep(10_000);
            injector.stop(props.containers().get(1));   // kafka2
            injector.stop(props.containers().get(2));   // kafka3
            Thread.sleep(60_000);                       // 실패 축적 구간
            injector.start(props.containers().get(1));
            injector.start(props.containers().get(2));
            injector.awaitFullIsr(props.topic(), Duration.ofMinutes(3));
            generator.stopAndDrain();
            generator.resend(ledger.sentFail());        // 실패분 재전송 → 최종 무유실
            verifier.awaitQuiet(3_000, 120_000);
        }
    }
}
