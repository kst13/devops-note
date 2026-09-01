package dev.devopsnote.kafkarunner.ledger;

import java.util.ArrayList;
import java.util.List;

/** 원장과 기대치를 대조해 PASS/FAIL 판정. 핵심 불변식: 성공 기록 seq ⊆ 수신 seq. */
public class Judge {
    public JudgeResult judge(Ledger ledger, Expectation expectation) {
        List<String> reasons = new ArrayList<>();
        long lost = ledger.lostSeqs().size();
        if (lost > 0) reasons.add("유실 " + lost + "건: 성공으로 기록된 메시지가 수신되지 않음 " + preview(ledger));

        long duplicates = ledger.duplicateCount();
        if (!expectation.duplicatesAllowed() && duplicates > 0) reasons.add("중복 " + duplicates + "건 (허용 안 됨)");

        long unresolved = ledger.sentFail().size();
        if (unresolved > 0) reasons.add("최종 미전송 잔여 " + unresolved + "건 — 재전송 후에도 전달되지 않음");

        if (expectation.sendFailuresExpected() && ledger.failedEverCount() == 0)
            reasons.add("전송 실패가 발생해야 하는 시나리오인데 실패가 0건 — 장애 주입이 동작하지 않았을 가능성");
        if (!expectation.transientFailuresTolerated() && ledger.failedEverCount() > 0)
            reasons.add("정상 시나리오에서 전송 실패 " + ledger.failedEverCount() + "건 발생");

        return new JudgeResult(reasons.isEmpty(), reasons,
            ledger.sentOk().size(), unresolved, ledger.received().size(), lost, duplicates);
    }

    private String preview(Ledger ledger) {
        var sample = ledger.lostSeqs().stream().limit(5).toList();
        return "(예: " + sample + ")";
    }
}
