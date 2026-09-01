package dev.devopsnote.kafkarunner.scenario;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.ledger.Expectation;
import dev.devopsnote.kafkarunner.ledger.Ledger;
import dev.devopsnote.kafkarunner.load.LoadGenerator;
import dev.devopsnote.kafkarunner.load.VerifierConsumer;
import java.time.Duration;

/** ④ 전체 정지 → 복구: 3대 모두 정지, 실패분은 원장이 폴백 버퍼 역할.
 *  복구 후 재전송하면 최종 유실 0 — usage-guide 07 의 폴백 패턴 검증. */
public class TotalOutage implements Scenario {
    @Override public String name() { return "total-outage"; }
    @Override public String description() { return "전체 정지 → 자력 복구 → 폴백 재전송 — 최종 무유실 검증"; }
    @Override public Expectation expectation() { return Expectation.expectSendFailures(); }

    @Override public void run(RunnerProperties props, FaultInjector injector, Ledger ledger) throws Exception {
        try (var generator = new LoadGenerator(props.bootstrapServers(), props.topic(), ledger);
             var verifier = new VerifierConsumer(props.bootstrapServers(), props.topic(), ledger)) {
            verifier.start();
            generator.start();
            Thread.sleep(10_000);
            for (String container : props.containers()) injector.stop(container);  // 전체 정지
            Thread.sleep(30_000);                        // 정지 구간 — 실패가 원장(sentFail)에 쌓인다
            generator.stopAndDrain();                    // 부하 중단
            for (String container : props.containers()) injector.start(container); // 전체 재기동
            injector.awaitClusterReady(props.topic(), Duration.ofMinutes(5));      // 쿼럼·리더 회복 대기
            injector.awaitFullIsr(props.topic(), Duration.ofMinutes(3));
            generator.resend(ledger.sentFail());         // 폴백 버퍼 재전송
            verifier.awaitQuiet(3_000, 120_000);
        }
    }
}
