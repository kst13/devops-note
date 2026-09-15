package com.osstem.sample.orderpayment.event;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * order.events 토픽 이벤트. 필드명은 스네이크케이스 — Iceberg Sink 가 JSON 키를 컬럼명으로 쓰고
 * Trino 가 소문자로 정규화하므로 카멜케이스면 orderid 처럼 뭉개진다.
 * occurred_at 은 String 으로 든다 — Spring Kafka JsonSerializer 기본 ObjectMapper 가 OffsetDateTime 을
 * epoch 초(1.789E9)로 쓰기 때문에, 직렬화 설정에 기대지 않고 ISO-8601 문자열을 직접 넣는다.
 */
public record OrderEvent(
        @JsonProperty("event_id") String eventId,
        @JsonProperty("event_type") String eventType,
        @JsonProperty("order_id") String orderId,
        @JsonProperty("customer_id") String customerId,
        @JsonProperty("item_name") String itemName,
        @JsonProperty("amount") long amount,
        @JsonProperty("occurred_at") String occurredAt,
        @JsonProperty("schema_version") int schemaVersion,
        @JsonProperty("source_system") String sourceSystem) {

    public static OrderEvent of(String type, String orderId, String customerId, String itemName, long amount, String source) {
        return new OrderEvent(UUID.randomUUID().toString(), type, orderId, customerId, itemName, amount,
                OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME), 1, source);
    }
}
