package dev.devopsnote.kafkarunner.sample;

import static org.assertj.core.api.Assertions.assertThat;

import dev.devopsnote.kafkarunner.sample.avro.OrderCreated;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** 브로커·SR 없이 Confluent serializer → 바이트 → deserializer 왕복을 검증한다.
 *  Avro 1.12 의 생성 클래스 신뢰 목록(trustGeneratedAvroClasses)이 없으면 이 경로가 SecurityException 으로 막힌다. */
class AvroRoundTripTest {
    private static final String TOPIC = "commerce.order.created.avro";

    @BeforeAll
    static void trustGeneratedClasses() { SampleKafkaConfig.trustGeneratedAvroClasses(); }

    @Test
    void specificRecordSurvivesSerializerRoundTrip() {
        var registry = new MockSchemaRegistryClient();
        Map<String, Object> config = Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://round-trip",
                                            KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        OrderCreated original = SampleEvents.avro(7);

        try (var serializer = new KafkaAvroSerializer(registry, config);
             var deserializer = new KafkaAvroDeserializer(registry, config)) {
            byte[] bytes = serializer.serialize(TOPIC, original);
            // wire format: magic 0x00 + schema id 4B + payload — 스키마 전체가 아니라 id 만 실린다
            assertThat(bytes[0]).isZero();
            assertThat(bytes.length).isLessThan(100);
            Object back = deserializer.deserialize(TOPIC, bytes);
            assertThat(back).isInstanceOf(OrderCreated.class).isEqualTo(original);
        }
    }
}
