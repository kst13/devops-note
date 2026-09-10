package com.osstem.commerce.order;

import com.osstem.commerce.order.event.OrderCreated;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 운영 절차(auto.register.schemas=false + 스키마를 REST 로 먼저 등록)가 통하는 조건을 SR 없이 검증한다.
 * 전제: JVM 에 -Dorg.apache.avro.SERIALIZABLE_PACKAGES=com.osstem (build.gradle.kts 의 tasks.test 참고).
 */
class ManualRegistrationTest {

    private static final String TOPIC = "kst.order.created";
    private static final String SUBJECT = TOPIC + "-value";
    private static final Schema RAW_AVSC = parse("src/main/avro/OrderCreated.avsc");

    @Test
    void 함정_avsc_원문을_등록하고_기본_설정으로_직렬화하면_40403() throws Exception {
        SchemaRegistryClient sr = registered(RAW_AVSC);
        try (var s = serializer(sr, Map.of())) {
            var ex = assertThrows(SerializationException.class, () -> s.serialize(TOPIC, sample()));
            assertTrue(rootMessage(ex).contains("40403"), rootMessage(ex));
        }
    }

    @Test
    void 해법A_avro_remove_java_properties() throws Exception {
        SchemaRegistryClient sr = registered(RAW_AVSC);
        try (var s = serializer(sr, Map.of("avro.remove.java.properties", true))) {
            byte[] bytes = s.serialize(TOPIC, sample());
            assertSchemaId(1, bytes);
            assertRoundTrip(sr, bytes);
        }
    }

    @Test
    void 해법B_use_latest_version() throws Exception {
        SchemaRegistryClient sr = registered(RAW_AVSC);
        try (var s = serializer(sr, Map.of("use.latest.version", true))) {
            byte[] bytes = s.serialize(TOPIC, sample());
            assertSchemaId(1, bytes);
            assertRoundTrip(sr, bytes);
        }
    }

    @Test
    void 해법C_생성_클래스의_SCHEMA를_등록하면_기본_설정으로_된다() throws Exception {
        SchemaRegistryClient sr = registered(OrderCreated.getClassSchema());   // avro.java.string 포함
        try (var s = serializer(sr, Map.of())) {
            byte[] bytes = s.serialize(TOPIC, sample());
            assertSchemaId(1, bytes);
            assertRoundTrip(sr, bytes);
        }
    }

    @Test
    void 참고_원문과_생성_스키마는_canonical_form_은_같고_equals_만_다르다() {
        var gen = OrderCreated.getClassSchema();
        assertNotEquals(RAW_AVSC, gen);
        assertEquals(org.apache.avro.SchemaNormalization.toParsingForm(RAW_AVSC),
                     org.apache.avro.SchemaNormalization.toParsingForm(gen));
    }

    // ---- helpers ----

    private static SchemaRegistryClient registered(Schema schema) throws Exception {
        var sr = new MockSchemaRegistryClient();
        assertEquals(1, sr.register(SUBJECT, new AvroSchema(schema)));
        return sr;
    }

    private static KafkaAvroSerializer serializer(SchemaRegistryClient sr, Map<String, Object> extra) {
        var s = new KafkaAvroSerializer(sr);
        var cfg = new HashMap<String, Object>(Map.of(
                "schema.registry.url", "mock://test",
                "auto.register.schemas", false,
                "use.latest.version", false));
        cfg.putAll(extra);
        s.configure(cfg, false);
        return s;
    }

    private static void assertSchemaId(int expected, byte[] bytes) {
        assertEquals(0, bytes[0], "매직바이트");
        int id = ((bytes[1] & 0xff) << 24) | ((bytes[2] & 0xff) << 16) | ((bytes[3] & 0xff) << 8) | (bytes[4] & 0xff);
        assertEquals(expected, id, "REST 로 등록한 스키마 ID 를 써야 한다");
    }

    private static void assertRoundTrip(SchemaRegistryClient sr, byte[] bytes) {
        try (var d = new KafkaAvroDeserializer(sr)) {
            d.configure(Map.of("schema.registry.url", "mock://test", "specific.avro.reader", true), false);
            Object v = d.deserialize(TOPIC, bytes);
            assertInstanceOf(OrderCreated.class, v);
            assertEquals("ORD-1", ((OrderCreated) v).getOrderId());
        }
    }

    private static OrderCreated sample() {
        return OrderCreated.newBuilder()
                .setOrderId("ORD-1").setCustomerId(1L).setItems(List.of())
                .setTotalAmount(0L).setCreatedAt(Instant.now()).build();
    }

    private static Schema parse(String path) {
        try { return new Schema.Parser().parse(new File(path)); }
        catch (java.io.IOException e) { throw new IllegalStateException(path, e); }
    }

    private static String rootMessage(Throwable t) {
        while (t.getCause() != null) t = t.getCause();
        return String.valueOf(t.getMessage());
    }
}
