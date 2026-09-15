package com.osstem.sample.orderpayment.event;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

/**
 * 이벤트 JSON 이 Iceberg Sink·Trino 와 맞는 형태인지 고정한다.
 *   - 키는 스네이크케이스 (Trino 가 소문자로 정규화하므로 카멜케이스는 뭉개진다)
 *   - occurred_at 은 ISO-8601 문자열 (숫자 배열·epoch 가 아님)
 *   - 값이 null 인 선택 필드는 빠진다
 */
class EventJsonTest {

    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Test
    void paymentEventUsesSnakeCaseAndIsoTimestamp() throws Exception {
        PaymentEvent e = PaymentEvent.of("PAYMENT_COMPLETED", "order-1", "pay-1", "customer-1", 35000, "CARD", null, "test");
        JsonNode json = mapper.readTree(mapper.writeValueAsString(e));

        assertThat(json.fieldNames()).toIterable().containsExactlyInAnyOrder(
                "event_id", "event_type", "order_id", "payment_id", "customer_id", "amount", "method",
                "occurred_at", "schema_version", "source_system");
        assertThat(json.get("occurred_at").isTextual()).isTrue();
        assertThat(json.get("occurred_at").asText()).matches("\\d{4}-\\d{2}-\\d{2}T.*[+-]\\d{2}:\\d{2}|.*Z");
        assertThat(json.has("failure_reason")).as("null 인 선택 필드는 빠진다").isFalse();
    }

    @Test
    void failedPaymentCarriesReason() throws Exception {
        PaymentEvent e = PaymentEvent.of("PAYMENT_FAILED", "order-1", "pay-1", "customer-1", 2_000_000, "CARD",
                "LIMIT_EXCEEDED", "test");
        JsonNode json = mapper.readTree(mapper.writeValueAsString(e));
        assertThat(json.get("failure_reason").asText()).isEqualTo("LIMIT_EXCEEDED");
    }

    @Test
    void orderEventShape() throws Exception {
        OrderEvent e = OrderEvent.of("ORDER_CREATED", "order-1", "customer-1", "노트북", 1_200_000, "test");
        JsonNode json = mapper.readTree(mapper.writeValueAsString(e));
        assertThat(json.fieldNames()).toIterable().containsExactlyInAnyOrder(
                "event_id", "event_type", "order_id", "customer_id", "item_name", "amount",
                "occurred_at", "schema_version", "source_system");
        assertThat(json.get("amount").asLong()).isEqualTo(1_200_000);
    }
}
