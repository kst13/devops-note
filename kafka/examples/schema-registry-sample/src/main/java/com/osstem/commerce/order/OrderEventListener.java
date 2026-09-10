package com.osstem.commerce.order;

import com.osstem.commerce.order.event.OrderCreated;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** usage-guide 04 의 컨슈머. 값 타입이 생성된 Avro 클래스라는 점만 다르다. */
@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    @KafkaListener(topics = "${app.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void on(ConsumerRecord<String, OrderCreated> record, Acknowledgment ack) {
        OrderCreated event = record.value();
        log.info("수신 partition={} offset={} key={} customerId={} items={} totalAmount={} createdAt={}",
                record.partition(), record.offset(), record.key(),
                event.getCustomerId(), event.getItems().size(), event.getTotalAmount(), event.getCreatedAt());
        // 멱등 처리 + 비즈니스 로직 자리
        ack.acknowledge();
    }
}
