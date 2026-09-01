package dev.devopsnote.kafkarunner.scenario;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.ledger.Expectation;
import dev.devopsnote.kafkarunner.ledger.Ledger;
import dev.devopsnote.kafkarunner.load.LoadGenerator;
import dev.devopsnote.kafkarunner.load.VerifierConsumer;

/** ① 정상 왕복: 1만 건 전송 → 전량 수신 → 유실 0 / 중복 0. */
public class NormalRoundtrip implements Scenario {
    @Override public String name() { return "normal-roundtrip"; }
    @Override public String description() { return "정상 상태에서 1만 건 왕복 — 유실 0, 중복 0 확인"; }
    @Override public Expectation expectation() { return Expectation.strict(); }

    @Override public void run(RunnerProperties props, FaultInjector injector, Ledger ledger) throws Exception {
        try (var generator = new LoadGenerator(props.bootstrapServers(), props.topic(), ledger);
             var verifier = new VerifierConsumer(props.bootstrapServers(), props.topic(), ledger)) {
            verifier.start();
            generator.sendExactly(10_000);
            generator.stopAndDrain();
            verifier.awaitQuiet(3_000, 60_000);
        }
    }
}
