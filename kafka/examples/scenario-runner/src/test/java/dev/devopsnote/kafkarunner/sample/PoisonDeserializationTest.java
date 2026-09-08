package dev.devopsnote.kafkarunner.sample;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.devopsnote.kafkarunner.sample.avro.OrderCreated;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

/** sample-consume-poison 의 핵심: Avro 가 아닌 바이트(poison pill)를 Avro 컨슈머가 만나면 역직렬화가 실패한다.
 *  MockSchemaRegistryClient 로 브로커·실제 SR 없이 "깨진 메시지는 컨슈머에서 막힌다"를 고정한다. */
class PoisonDeserializationTest {
    private static final String TOPIC = "commerce.order.created-avro-poison";
    private static final Map<String, Object> CFG = Map.of("schema.registry.url", "mock://poison-test");

    @Test
    void nonAvroBytesFailDeserialization() {
        var deserializer = new KafkaAvroDeserializer(new MockSchemaRegistryClient(), CFG);
        byte[] poison = "this-is-not-avro".getBytes(StandardCharsets.UTF_8);
        // 첫 바이트가 Avro magic byte(0)가 아니라 "Unknown magic byte" 로 거부된다
        assertThatThrownBy(() -> deserializer.deserialize(TOPIC, poison))
            .isInstanceOf(SerializationException.class);
    }

    @Test
    void validAvroBytesRoundTrip() {
        // 앱은 main() 에서 이걸 호출한다 (Avro 1.12 는 생성 클래스 로딩에 신뢰 목록을 요구)
        SampleKafkaConfig.trustGeneratedAvroClasses();
        var registry = new MockSchemaRegistryClient();
        var serializer = new KafkaAvroSerializer(registry, CFG);
        // specific.avro.reader=true → 앱의 avroConsumerProps 와 동일하게 생성 클래스로 역직렬화
        var deserializer = new KafkaAvroDeserializer(registry,
            Map.of("schema.registry.url", "mock://poison-test", "specific.avro.reader", true));
        OrderCreated event = OrderCreated.newBuilder()
            .setOrderId("ORD-1").setCustomerId(10001L).setAmount(1000L).setCreatedAt("2026-09-08T00:00:00Z").build();
        byte[] bytes = serializer.serialize(TOPIC, event);
        assertThat(deserializer.deserialize(TOPIC, bytes)).isInstanceOf(OrderCreated.class);
    }
}
