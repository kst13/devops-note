package dev.devopsnote.kafkarunner.sample;

import static org.assertj.core.api.Assertions.assertThat;

import dev.devopsnote.kafkarunner.sample.avro.OrderCreated;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType;
import org.junit.jupiter.api.Test;

/** BACKWARD 호환 = 새 스키마(reader)로 옛 데이터(writer=v1)를 읽을 수 있다.
 *  Schema Registry 의 AvroSchema.isBackwardCompatible 이 호출하는 것과 같은 Avro API 를 같은 인자 순서로 쓴다. */
class SchemaEvolutionTest {
    private static final Schema V1 = OrderCreated.getClassSchema();

    static Schema load(String name) throws IOException {
        try (InputStream in = SchemaEvolutionTest.class.getResourceAsStream("/schemas/" + name)) {
            assertThat(in).as("resource /schemas/" + name).isNotNull();
            return new Schema.Parser().parse(in);
        }
    }

    static List<String> fieldNames(Schema schema) {
        return schema.getFields().stream().map(Schema.Field::name).toList();
    }

    @Test
    void v2IsBackwardCompatibleWithV1() throws IOException {
        var result = SchemaCompatibility.checkReaderWriterCompatibility(load("order-created-v2.avsc"), V1);
        assertThat(result.getType()).as("%s", result.getResult().getIncompatibilities())
            .isEqualTo(SchemaCompatibilityType.COMPATIBLE);
    }

    @Test
    void incompatibleSchemaFailsBecauseNewFieldHasNoDefault() throws IOException {
        var result = SchemaCompatibility.checkReaderWriterCompatibility(load("order-created-incompatible.avsc"), V1);
        // "비호환"이기만 하면 통과하는 검사는 다른 이유(오타 등)로도 통과한다 — 의도한 바로 그 이유인지 단정한다
        assertThat(result.getResult().getIncompatibilities()).singleElement().satisfies(i -> {
            assertThat(i.getType()).isEqualTo(SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE);
            assertThat(i.getMessage()).isEqualTo("channel");
        });
    }

    @Test
    void evolvedSchemasExtendV1WithoutRenaming() throws IOException {
        // 세 파일이 v1 필드를 각자 복사해 갖고 있어 v1 이 바뀌면 조용히 어긋날 수 있다 — 상위집합 관계를 고정한다.
        // 이름이 바뀌면 NAME_MISMATCH 라는 엉뚱한 이유로 비호환 테스트가 통과할 수 있으므로 이름도 고정한다.
        for (String name : List.of("order-created-v2.avsc", "order-created-incompatible.avsc")) {
            Schema evolved = load(name);
            assertThat(evolved.getFullName()).as(name).isEqualTo(V1.getFullName());
            assertThat(fieldNames(evolved)).as(name).containsAll(fieldNames(V1));
        }
    }
}
