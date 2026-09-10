package com.osstem.commerce.order;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * usage-guide 09 7장의 호환성 표를 Schema Registry 없이 검증한다.
 * SR 의 BACKWARD 검사 = "새 스키마(reader)로 옛 데이터(writer)를 읽을 수 있나" 와 같은 판정이다.
 */
class SchemaEvolutionTest {

    private static final Schema V1 = parse("src/main/avro/OrderCreated.avsc");
    private static final Schema V2 = parse("schemas/order-created-v2.avsc");
    private static final Schema INCOMPATIBLE = parse("schemas/order-created-incompatible.avsc");

    @Test
    void v2는_기본값_있는_필드_추가라_BACKWARD_호환이다() {
        // reader = v2 (새 컨슈머), writer = v1 (옛 데이터)
        assertEquals(SchemaCompatibilityType.COMPATIBLE, check(V2, V1));
    }

    @Test
    void v2는_옛_컨슈머가_새_데이터를_읽는_FORWARD_방향도_호환이다() {
        // reader = v1 (옛 컨슈머), writer = v2 (새 데이터) → couponCode 를 모르는 필드로 무시
        assertEquals(SchemaCompatibilityType.COMPATIBLE, check(V1, V2));
    }

    @Test
    void 기본값_없는_필드_추가는_BACKWARD_위반이다() {
        // reader = incompatible (새 컨슈머), writer = v1 (옛 데이터) → channel 값이 없어 읽을 수 없음
        assertEquals(SchemaCompatibilityType.INCOMPATIBLE, check(INCOMPATIBLE, V1));
    }

    private static SchemaCompatibilityType check(Schema reader, Schema writer) {
        return SchemaCompatibility.checkReaderWriterCompatibility(reader, writer).getType();
    }

    private static Schema parse(String path) {
        try {
            return new Schema.Parser().parse(new File(path));
        } catch (IOException e) {
            throw new IllegalStateException(path, e);
        }
    }
}
