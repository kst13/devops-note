package dev.devopsnote.kafkarunner.scenario;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.ledger.Expectation;
import dev.devopsnote.kafkarunner.ledger.Ledger;
import dev.devopsnote.kafkarunner.load.LoadGenerator;
import dev.devopsnote.kafkarunner.load.VerifierConsumer;
import java.time.Duration;

/** ② 브로커 1대 정지: 부하 중 1대를 내려도 유실 0, 복구 후 ISR 완전 회복.
 *  리더 선출 순간의 일시 실패는 재전송으로 회복되면 허용. */
public class Broker1Down implements Scenario {
    @Override public String name() { return "broker-1-down"; }
    @Override public String description() { return "부하 중 브로커 1대 정지 60초 — 무중단·무유실 검증"; }
    @Override public Expectation expectation() { return Expectation.allowDuplicates(); }

    @Override public void run(RunnerProperties props, FaultInjector injector, Ledger ledger) throws Exception {
        try (var generator = new LoadGenerator(props.bootstrapServers(), props.topic(), ledger);
             var verifier = new VerifierConsumer(props.bootstrapServers(), props.topic(), ledger)) {
            verifier.start();
            generator.start();
            Thread.sleep(10_000);                       // 정상 부하 10초
            injector.stop(props.containers().get(1));   // kafka2 정지
            Thread.sleep(60_000);                       // 정지 상태에서 부하 지속
            injector.start(props.containers().get(1));
            injector.awaitFullIsr(props.topic(), Duration.ofMinutes(3));
            Thread.sleep(10_000);                       // 복구 후 부하 10초
            generator.stopAndDrain();
            generator.resend(ledger.sentFail());        // 일시 실패분 회복
            verifier.awaitQuiet(3_000, 120_000);
        }
    }
}
