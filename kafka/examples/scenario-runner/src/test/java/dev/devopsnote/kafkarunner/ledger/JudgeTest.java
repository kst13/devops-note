package dev.devopsnote.kafkarunner.ledger;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class JudgeTest {
    @Test
    void lossIsAlwaysFailure() {
        Ledger ledger = new Ledger();
        ledger.recordSentOk(1); // 수신 기록 없음 → 유실
        JudgeResult result = new Judge().judge(ledger, Expectation.strict());
        assertThat(result.pass()).isFalse();
        assertThat(result.reasons()).anyMatch(reason -> reason.contains("유실"));
    }

    @Test
    void duplicatesFailOnlyInStrictMode() {
        Ledger ledger = new Ledger();
        ledger.recordSentOk(1); ledger.recordReceived(1); ledger.recordReceived(1);
        assertThat(new Judge().judge(ledger, Expectation.strict()).pass()).isFalse();
        assertThat(new Judge().judge(ledger, Expectation.allowDuplicates()).pass()).isTrue();
    }

    @Test
    void expectedSendFailuresPassAfterSuccessfulResend() {
        Ledger ledger = new Ledger();
        ledger.recordSentOk(1); ledger.recordReceived(1);
        ledger.recordSentFail(2);            // 장애 구간 실패
        ledger.recordSentOk(2); ledger.recordReceived(2); // 복구 후 재전송 성공
        // 실패 이력이 있고 최종 유실·잔여가 없으면 PASS
        assertThat(new Judge().judge(ledger, Expectation.expectSendFailures()).pass()).isTrue();
        // 실패가 있어야 정상인 시나리오에서 실패 0건이면 오히려 FAIL (장애 주입 실패 의심)
        Ledger noFail = new Ledger();
        noFail.recordSentOk(1); noFail.recordReceived(1);
        assertThat(new Judge().judge(noFail, Expectation.expectSendFailures()).pass()).isFalse();
    }

    @Test
    void unresolvedFailuresAlwaysFail() {
        Ledger ledger = new Ledger();
        ledger.recordSentOk(1); ledger.recordReceived(1);
        ledger.recordSentFail(2);            // 재전송 없이 미전송으로 남음
        JudgeResult result = new Judge().judge(ledger, Expectation.expectSendFailures());
        assertThat(result.pass()).isFalse();
        assertThat(result.reasons()).anyMatch(reason -> reason.contains("미전송 잔여"));
    }
}
