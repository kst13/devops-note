package com.osstem.sample.orderpayment.event;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/** payment.events 토픽 이벤트. 키는 order_id — 같은 주문의 결제 이벤트가 파티션 안에서 순서를 지킨다. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record PaymentEvent(
        @JsonProperty("event_id") String eventId,
        @JsonProperty("event_type") String eventType,
        @JsonProperty("order_id") String orderId,
        @JsonProperty("payment_id") String paymentId,
        @JsonProperty("customer_id") String customerId,
        @JsonProperty("amount") long amount,
        @JsonProperty("method") String method,
        @JsonProperty("failure_reason") String failureReason,
        @JsonProperty("occurred_at") String occurredAt,
        @JsonProperty("schema_version") int schemaVersion,
        @JsonProperty("source_system") String sourceSystem) {

    public static PaymentEvent of(String type, String orderId, String paymentId, String customerId, long amount,
                                  String method, String failureReason, String source) {
        return new PaymentEvent(UUID.randomUUID().toString(), type, orderId, paymentId, customerId, amount, method,
                failureReason, OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME), 1, source);
    }
}
