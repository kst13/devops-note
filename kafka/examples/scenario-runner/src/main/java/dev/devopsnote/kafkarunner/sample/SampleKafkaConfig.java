package dev.devopsnote.kafkarunner.sample;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

/** sample-* 명령 전용 Kafka 빈. 접속·acks·idempotence·auto-offset-reset 은 application.yml 의 spring.kafka.* 에서 오고,
 *  여기서는 명령마다 다른 serializer/deserializer 만 지정한다. 러너 내부(load/)는 부하 제어를 위해 kafka-clients 를 직접 쓴다. */
@Configuration
public class SampleKafkaConfig {
    static final String JSON_LISTENER_FACTORY = "sampleJsonListenerFactory";

    private final KafkaProperties kafka;

    public SampleKafkaConfig(KafkaProperties kafka) { this.kafka = kafka; }

    /** usage-guide 05 와 같은 조합: String key + JsonSerializer. */
    @Bean
    public KafkaTemplate<String, OrderCreatedEvent> sampleJsonTemplate() {
        Map<String, Object> props = kafka.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    /** usage-guide 04·05: JsonDeserializer + trusted packages + 수동 ack. */
    @Bean(JSON_LISTENER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> sampleJsonListenerFactory() {
        Map<String, Object> props = kafka.buildConsumerProperties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "dev.devopsnote.kafkarunner.sample"); // 미설정 시 역직렬화 예외
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderCreatedEvent.class.getName());
        var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
