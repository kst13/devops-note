package dev.devopsnote.kafkarunner.ledger;

/** 시나리오별 기대 동작.
 *  duplicatesAllowed      재시도·재전송으로 인한 중복 수신 허용 여부
 *  sendFailuresExpected   전송 실패가 발생해야 정상인 시나리오인지 (2대 정지, 전체 정지)
 *  transientFailuresTolerated 일시 실패(재전송으로 회복) 허용 여부 */
public record Expectation(boolean duplicatesAllowed, boolean sendFailuresExpected,
                          boolean transientFailuresTolerated) {
    public static Expectation strict() { return new Expectation(false, false, false); }
    public static Expectation allowDuplicates() { return new Expectation(true, false, true); }
    public static Expectation expectSendFailures() { return new Expectation(true, true, true); }
}
