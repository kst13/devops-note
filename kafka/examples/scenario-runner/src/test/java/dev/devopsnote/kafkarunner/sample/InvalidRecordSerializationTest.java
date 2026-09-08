package dev.devopsnote.kafkarunner.sample;

import static org.assertj.core.api.Assertions.assertThat;

import dev.devopsnote.kafkarunner.sample.avro.OrderCreated;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

/** sample-produce-invalid 의 핵심: 스키마에 맞지 않는 데이터는 Avro 가 거부한다.
 *  브로커·SR 없이 Avro 검증 API 로 "잘못된 데이터는 직렬화 이전에 걸러진다"를 고정한다. */
class InvalidRecordSerializationTest {
    private static final Schema SCHEMA = OrderCreated.getClassSchema();

    @Test
    void wrongTypedFieldFailsSchemaValidation() {
        GenericRecord bad = new GenericData.Record(SCHEMA);
        bad.put("orderId", "ORD-9001");
        bad.put("customerId", 10001L);
        bad.put("amount", "not-a-number");   // amount 는 long — 문자열은 스키마 위반
        bad.put("createdAt", "2026-09-08T00:00:00Z");
        assertThat(GenericData.get().validate(SCHEMA, bad)).isFalse();
    }

    @Test
    void correctlyTypedRecordPassesValidation() {
        GenericRecord good = new GenericData.Record(SCHEMA);
        good.put("orderId", "ORD-9001");
        good.put("customerId", 10001L);
        good.put("amount", 55_000L);
        good.put("createdAt", "2026-09-08T00:00:00Z");
        assertThat(GenericData.get().validate(SCHEMA, good)).isTrue();
    }
}
