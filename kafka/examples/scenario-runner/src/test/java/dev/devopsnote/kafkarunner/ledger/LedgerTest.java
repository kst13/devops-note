package dev.devopsnote.kafkarunner.ledger;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class LedgerTest {
    @Test
    void tracksSentAndReceivedSets() {
        Ledger ledger = new Ledger();
        ledger.recordSentOk(1); ledger.recordSentOk(2);
        ledger.recordSentFail(3);
        ledger.recordReceived(1);
        ledger.recordReceived(1); // 중복 수신
        assertThat(ledger.sentOk()).containsExactly(1L, 2L);
        assertThat(ledger.sentFail()).containsExactly(3L);
        assertThat(ledger.lostSeqs()).containsExactly(2L);      // 성공 기록됐지만 미수신
        assertThat(ledger.duplicateCount()).isEqualTo(1);       // 총수신 - 고유수신
    }

    @Test
    void resendSuccessClearsFailureRecord() {
        Ledger ledger = new Ledger();
        ledger.recordSentFail(7);
        ledger.recordSentOk(7);   // 재전송 성공
        assertThat(ledger.sentFail()).isEmpty();
        assertThat(ledger.sentOk()).containsExactly(7L);
    }
}
