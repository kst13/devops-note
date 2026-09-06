package dev.devopsnote.kafkarunner.sample;

import static org.assertj.core.api.Assertions.assertThat;

import dev.devopsnote.kafkarunner.sample.avro.OrderCreated;
import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.junit.jupiter.api.Test;

/** BACKWARD 호환 = 새 스키마(reader)로 옛 데이터(writer=v1)를 읽을 수 있다. SR 이 하는 검사와 같은 규칙을 로컬에서 확인한다. */
class SchemaEvolutionTest {
    private static final Schema V1 = OrderCreated.getClassSchema();

    static Schema load(String name) throws IOException {
        try (InputStream in = SchemaEvolutionTest.class.getResourceAsStream("/schemas/" + name)) {
            assertThat(in).as("resource /schemas/" + name).isNotNull();
            return new Schema.Parser().parse(in);
        }
    }

    @Test
    void v2IsBackwardCompatibleWithV1() throws IOException {
        var result = SchemaCompatibility.checkReaderWriterCompatibility(load("order-created-v2.avsc"), V1);
        assertThat(result.getType()).isEqualTo(SchemaCompatibilityType.COMPATIBLE);
    }

    @Test
    void incompatibleSchemaBreaksBackwardCompatibility() throws IOException {
        var result = SchemaCompatibility.checkReaderWriterCompatibility(load("order-created-incompatible.avsc"), V1);
        assertThat(result.getType()).isEqualTo(SchemaCompatibilityType.INCOMPATIBLE);
    }

    @Test
    void evolvedSchemasKeepTheRecordName() throws IOException {
        // 이름이 다르면 SR 이 "다른 타입"으로 보고 호환 검사 자체가 달라진다
        assertThat(load("order-created-v2.avsc").getFullName()).isEqualTo(V1.getFullName());
        assertThat(load("order-created-incompatible.avsc").getFullName()).isEqualTo(V1.getFullName());
    }
}
