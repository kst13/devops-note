package dev.devopsnote.kafkarunner.sample;

import java.time.Instant;

/** 샘플 이벤트 생성. orderId 를 3개로 돌려 "같은 key → 같은 파티션"이 출력에서 보이게 한다. */
final class SampleEvents {
    private SampleEvents() {}

    static String orderId(int i) { return "ORD-" + (1000 + i % 3); }

    static OrderCreatedEvent json(int i) {
        return new OrderCreatedEvent(orderId(i), "CUST-" + (i % 5 + 1), 10_000L * i, Instant.now().toString());
    }
}
