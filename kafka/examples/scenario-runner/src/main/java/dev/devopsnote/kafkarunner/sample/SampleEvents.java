package dev.devopsnote.kafkarunner.sample;

import dev.devopsnote.kafkarunner.sample.avro.OrderCreated;
import java.time.Instant;

/** 샘플 이벤트 생성. orderId 를 3개로 돌려 "같은 key → 같은 파티션"이 출력에서 보이게 한다.
 *  파티션은 key 의 murmur2 해시로 정해지므로 3개 key 가 3개 파티션에 고르게 퍼진다는 보장은 없다 (실제로 두 key 가 같은 파티션에 갈 수 있다). */
final class SampleEvents {
    private SampleEvents() {}

    static String orderId(int i) { return "ORD-" + (1000 + i % 3); }

    static OrderCreatedEvent json(int i) {
        return new OrderCreatedEvent(orderId(i), 10_000L + (i % 5 + 1), 10_000L * i, Instant.now().toString());
    }

    static OrderCreated avro(int i) {
        return OrderCreated.newBuilder()
            .setOrderId(orderId(i))
            .setCustomerId(10_000L + (i % 5 + 1))
            .setAmount(10_000L * i)
            .setCreatedAt(Instant.now().toString())
            .build();
    }
}
