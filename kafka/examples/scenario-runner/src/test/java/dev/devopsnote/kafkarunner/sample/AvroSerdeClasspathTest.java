package dev.devopsnote.kafkarunner.sample;

import static org.assertj.core.api.Assertions.assertThatCode;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.junit.jupiter.api.Test;

/** Confluent serializer 와 Spring Boot 가 고정한 kafka-clients 버전이 맞는지. 8.x serializer 는 kafka-clients 4.1 의
 *  Monitorable 을 요구해 3.9.1 에서는 클래스 로딩 자체가 실패한다 — 브로커 없이 잡을 수 있는 문제라 여기서 확인한다. */
class AvroSerdeClasspathTest {
    @Test
    void confluentSerdesLoadOnTheResolvedKafkaClients() {
        assertThatCode(() -> { new KafkaAvroSerializer().close(); new KafkaAvroDeserializer().close(); })
            .doesNotThrowAnyException();
    }
}
