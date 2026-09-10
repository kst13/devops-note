package com.osstem.commerce.order;

import com.osstem.commerce.order.event.OrderCreated;
import com.osstem.commerce.order.event.OrderItem;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * 검증용 진입점. POST /orders 로 주문을 넣으면 OrderCreated 이벤트를 발행한다.
 * 실패(스키마 미등록, SR 접속 불가 등)는 500 과 원인 메시지로 돌려준다.
 */
@RestController
public class OrderController {

    public record OrderRequest(String orderId, long customerId, List<ItemRequest> items, long totalAmount) {}
    public record ItemRequest(String sku, int qty) {}

    private final OrderEventProducer producer;

    public OrderController(OrderEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/orders")
    public ResponseEntity<Map<String, Object>> create(@RequestBody OrderRequest req) throws Exception {
        OrderCreated event = OrderCreated.newBuilder()
                .setOrderId(req.orderId())
                .setCustomerId(req.customerId())
                .setItems(req.items().stream()
                        .map(i -> OrderItem.newBuilder().setSku(i.sku()).setQty(i.qty()).build())
                        .toList())
                .setTotalAmount(req.totalAmount())
                .setCreatedAt(Instant.now())
                .build();

        try {
            var md = producer.publish(event).get().getRecordMetadata();
            return ResponseEntity.ok(Map.of(
                    "orderId", req.orderId(),
                    "partition", md.partition(),
                    "offset", md.offset(),
                    "serializedValueSize", md.serializedValueSize()));
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", cause.getClass().getSimpleName(),
                    "message", String.valueOf(cause.getMessage())));
        }
    }
}
