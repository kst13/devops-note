package com.osstem.commerce.order;

import com.osstem.commerce.order.event.OrderCreated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/** usage-guide 03 의 프로듀서에서 값 타입만 생성된 Avro 클래스로 바뀐다. */
@Service
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    private final KafkaTemplate<String, OrderCreated> kafkaTemplate;
    private final String topic;

    public OrderEventProducer(KafkaTemplate<String, OrderCreated> kafkaTemplate,
                              @Value("${app.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public CompletableFuture<SendResult<String, OrderCreated>> publish(OrderCreated event) {
        // key = orderId → 같은 주문의 이벤트는 같은 파티션
        return kafkaTemplate.send(topic, event.getOrderId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("{} 발행 실패: {}", topic, event.getOrderId(), ex);
                    } else {
                        var md = result.getRecordMetadata();
                        log.info("발행 완료 key={} partition={} offset={} serializedValueSize={}B",
                                event.getOrderId(), md.partition(), md.offset(), md.serializedValueSize());
                    }
                });
    }
}
