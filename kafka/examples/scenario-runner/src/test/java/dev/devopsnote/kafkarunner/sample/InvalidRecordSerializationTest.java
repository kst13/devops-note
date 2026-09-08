package dev.devopsnote.kafkarunner.sample;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.devopsnote.kafkarunner.sample.avro.OrderCreated;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

/** sample-produce-invalid 의 핵심: 스키마에 맞지 않는 데이터는 KafkaAvroSerializer 가 직렬화 단계에서 거부한다.
 *  명령이 실제로 의존하는 경로(serialize → SerializationException)를 MockSchemaRegistryClient 로 브로커 없이 고정한다. */
class InvalidRecordSerializationTest {
    private static final Schema SCHEMA = OrderCreated.getClassSchema();
    private static final String TOPIC = "commerce.order.created-avro";
    private static final Map<String, Object> CFG = Map.of("schema.registry.url", "mock://invalid-test");

    private static GenericRecord record(Object amount) {
        GenericRecord r = new GenericData.Record(SCHEMA);
        r.put("orderId", "ORD-9001");
        r.put("customerId", 10001L);
        r.put("amount", amount);
        r.put("createdAt", "2026-09-08T00:00:00Z");
        return r;
    }

    @Test
    void wrongTypedFieldFailsSerialization() {
        var serializer = new KafkaAvroSerializer(new MockSchemaRegistryClient(), CFG);
        GenericRecord bad = record("not-a-number");   // amount 는 long — 문자열은 스키마 위반
        assertThatThrownBy(() -> serializer.serialize(TOPIC, bad))
            .isInstanceOf(SerializationException.class);
    }

    @Test
    void correctlyTypedRecordSerializes() {
        var serializer = new KafkaAvroSerializer(new MockSchemaRegistryClient(), CFG);
        byte[] bytes = serializer.serialize(TOPIC, record(55_000L));
        assertThat(bytes).isNotEmpty();
    }
}
