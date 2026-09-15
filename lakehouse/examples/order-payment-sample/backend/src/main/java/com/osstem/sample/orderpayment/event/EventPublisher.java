package com.osstem.sample.orderpayment.event;

import com.osstem.sample.orderpayment.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * DB 저장 후 직접 발행한다. 운영에서는 DB 트랜잭션과 발행이 따로 성공하는 문제를 막기 위해
 * Transactional Outbox 로 바꾼다 (lakehouse/concepts/06 1장). PoC 는 범위를 줄이기 위해 직접 발행한다.
 */
@Component
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AppProperties props;

    public EventPublisher(KafkaTemplate<String, Object> kafkaTemplate, AppProperties props) {
        this.kafkaTemplate = kafkaTemplate;
        this.props = props;
    }

    public void publish(OrderEvent event) {
        send(props.topics().order(), event.orderId(), event, event.eventType());
    }

    public void publish(PaymentEvent event) {
        send(props.topics().payment(), event.orderId(), event, event.eventType());
    }

    private void send(String topic, String key, Object value, String type) {
        // acks=all 이라 콜백에서 실패가 오면 실제 실패다. PoC 는 로그만 남긴다.
        kafkaTemplate.send(topic, key, value).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("발행 실패 topic={} key={} type={}", topic, key, type, ex);
            } else {
                log.info("발행 topic={} partition={} offset={} type={} key={}", topic,
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset(), type, key);
            }
        });
    }
}
