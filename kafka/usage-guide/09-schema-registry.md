# Schema Registry 사용하기 (Avro)

> 핵심은 세 가지입니다. **코드는 그대로, 직렬화기만 바뀝니다.** 스키마 파일(`.avsc`) 하나가 이벤트 형식의 단일 기준이 됩니다. **필드 추가는 기본값과 함께, 삭제·이름 변경·타입 변경은 금지**입니다. 이 규칙만 지키면 프로듀서와 컨슈머를 따로 배포해도 깨지지 않습니다.

## 1. 왜 쓰나 — JSON과 무엇이 다른가

지금까지의 가이드는 `JsonSerializer`를 씁니다. 브로커는 메시지를 바이트로만 저장하고 형식을 검사하지 않습니다. 그래서 프로듀서가 필드 이름을 바꾸면 **컨슈머가 운영에서 터지고 나서야** 알게 됩니다.

| | JSON (`JsonSerializer`) | Avro + Schema Registry |
| --- | --- | --- |
| 형식의 기준 | 각 팀의 DTO 클래스. 프로듀서와 컨슈머가 따로 가짐 | `.avsc` 파일 하나. Schema Registry에 등록된 버전이 기준 |
| 잘못된 변경을 잡는 시점 | 컨슈머 운영 장애 | **프로듀서 배포 전** (등록 시 호환성 검사로 거부) |
| 잘못된 데이터를 잡는 시점 | 컨슈머 역직렬화 실패 | **프로듀서 직렬화 실패** (토픽에 들어가지 않음) |
| 메시지 크기 | 필드 이름이 매번 실림 | 필드 이름 없이 값만. 보통 절반 이하 |
| 타입 | 문자열·숫자만 구분. 날짜는 관례 | `long`/`int`/`timestamp-millis` 등 명시적 |
| 다른 팀이 형식을 알아내는 방법 | 소스 코드나 담당자에게 문의 | Schema Registry 조회 한 번 |

**언제 Avro로 갈지**는 이렇게 정합니다.

- 다른 팀이 소비하는 토픽, 또는 두 팀 이상이 프로듀서를 만드는 토픽은 **Avro를 씁니다**. 형식이 팀 사이의 계약이기 때문입니다.
- 한 팀이 프로듀서와 컨슈머를 모두 소유하는 내부 토픽은 JSON을 유지해도 됩니다. 필드 변경 사고를 한 번 겪으면 그때 옮깁니다.
- 한 토픽 안에서 JSON과 Avro를 섞지 않습니다. 옮길 때는 새 토픽을 만듭니다([01 토픽 명명 규칙](01-topic-naming.md) 4장의 `.v2` 절차).

## 2. 동작 원리 (2분)

```text
프로듀서 ──(1) 스키마 등록/조회 → ID(예: 21)──▶ Schema Registry ◀──(3) ID로 스키마 조회── 컨슈머
    └──(2) [매직바이트 1B][스키마ID 4B][Avro 페이로드] ──▶ Kafka 토픽 ──────────────────────┘
```

1. 프로듀서의 `KafkaAvroSerializer`가 첫 전송 때 스키마를 Schema Registry에 등록하거나 조회해 **ID**를 받습니다. 이후로는 캐시해서 다시 묻지 않습니다.
2. 메시지에는 스키마 전체가 아니라 **ID 4바이트**만 앞에 붙습니다. 그래서 크기가 작습니다.
3. 컨슈머의 `KafkaAvroDeserializer`가 ID로 스키마를 받아 역직렬화합니다. 역시 ID별로 캐시합니다.

Schema Registry는 **경로에 있지 않습니다.** 메시지는 브로커로만 갑니다. Schema Registry가 잠시 죽어도 이미 캐시된 스키마로 계속 동작하고, **처음 보는 스키마**를 등록·조회할 때만 실패합니다. 앱 재시작 직후가 그 시점입니다.

스키마는 토픽이 아니라 **subject** 단위로 관리됩니다. 기본 규칙은 `<토픽명>-value`입니다. `commerce.order.created` 토픽의 값 스키마는 subject `commerce.order.created-value`에 버전 1, 2, 3으로 쌓입니다.

## 3. 준비물

[08 온보딩](08-onboarding.md)의 준비물 3종에 더해 아래가 필요합니다.

| 항목 | 내용 |
| --- | --- |
| Schema Registry 주소 | 2대. 예: `http://10.0.0.11:8081,http://10.0.0.13:8081`. **반드시 둘 다** 적습니다. 한 대 점검 중이어도 앱이 떠야 합니다 |
| 방화벽 | 앱 서버 → Schema Registry 2대 `:8081`. 온보딩 신청에 함께 적으면 처리됩니다 |
| 빌드 저장소 | `https://packages.confluent.io/maven/`. Avro 직렬화기는 Maven Central에 없습니다. 사내 미러를 쓰면 이 주소를 허용 목록에 추가해야 합니다 |
| 스키마 등록 권한 | 개발·스테이징은 앱이 자동 등록합니다. **운영은 자동 등록을 끄고** 배포 전에 CI 또는 플랫폼팀 요청으로 등록합니다(7장) |

Schema Registry는 현재 인증 없는 HTTP입니다. 접근 제어는 방화벽으로만 하므로, 조회 명령을 아무 데서나 실행할 수 있다고 가정하지 마세요.

### 의존성 (Gradle, Spring Boot 3.5 기준)

```kotlin
// build.gradle.kts
buildscript {
    repositories { gradlePluginPortal() }
    dependencies {
        classpath("com.github.davidmc24.gradle.plugin:gradle-avro-plugin:1.9.1")
        classpath("org.apache.avro:avro-compiler:1.12.2")   // 런타임 avro 와 같은 버전으로 고정
    }
}
apply(plugin = "com.github.davidmc24.gradle.plugin.avro")

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

val confluentVersion = "7.9.9"   // Spring Boot 3.5 = kafka-clients 3.9.x → Confluent 7.9.x 와 짝
val avroVersion = "1.12.2"

dependencies {
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.apache.avro:avro:$avroVersion")
    implementation("io.confluent:kafka-avro-serializer:$confluentVersion")
}
```

Confluent 8.x 직렬화기는 kafka-clients 4.1 API를 요구합니다. Spring Boot 3.5가 3.9.x를 고정하므로 **7.9.x**를 씁니다. Spring Boot 4로 올릴 때 함께 검토합니다.

## 4. 스키마 파일 작성 (`.avsc`)

`src/main/avro/` 아래에 이벤트당 파일 하나를 둡니다. 빌드 시 플러그인이 Java 클래스를 생성합니다.

```json
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "com.osstem.commerce.order.event",
  "doc": "주문 생성 이벤트. 토픽 commerce.order.created 의 값.",
  "fields": [
    { "name": "orderId",     "type": "string", "doc": "주문 ID. 메시지 key 와 동일" },
    { "name": "customerId",  "type": "long" },
    { "name": "items", "type": {
        "type": "array",
        "items": {
          "type": "record", "name": "OrderItem",
          "fields": [
            { "name": "sku", "type": "string" },
            { "name": "qty", "type": "int" }
          ]
        }
      }
    },
    { "name": "totalAmount", "type": "long", "doc": "원 단위 정수" },
    { "name": "createdAt",   "type": { "type": "long", "logicalType": "timestamp-millis" } },
    { "name": "couponCode",  "type": ["null", "string"], "default": null, "doc": "v2 에서 추가" }
  ]
}
```

작성 규칙입니다.

- **`namespace`는 Java 패키지**, **`name`은 이벤트명 PascalCase**입니다. 생성 클래스가 `com.osstem.commerce.order.event.OrderCreated`가 됩니다. 한 번 정하면 바꾸지 않습니다. 바꾸면 컨슈머가 클래스를 못 찾습니다.
- **`doc`을 씁니다.** 다른 팀이 Schema Registry에서 스키마만 보고 이해해야 합니다.
- **없을 수 있는 필드는 `["null", "타입"]` + `"default": null`** 로 씁니다. `null`이 union의 **첫 번째**여야 default가 null이 됩니다.
- 금액은 `long`(원 단위)을 씁니다. Avro `decimal` 논리 타입은 생성 클래스에서 `ByteBuffer`로 다뤄져 불편합니다.
- 시각은 `timestamp-millis`를 씁니다. 생성 클래스에서 `java.time.Instant`가 됩니다. 문자열 ISO-8601도 허용되지만 팀 간에는 논리 타입이 안전합니다.
- `enum`은 신중히 씁니다. 값을 추가하려면 `"default"` 심볼을 미리 정의해 둬야 옛 컨슈머가 새 값을 읽을 수 있습니다. 확장 가능성이 있으면 `string`이 낫습니다.
- 필드 이름은 `camelCase`, 토픽 이벤트 JSON 설계 원칙([01](01-topic-naming.md) 6장)은 그대로 적용됩니다.

## 5. 설정 (`application.yml`) — 기존 설정에서 바꿀 곳

[05 접속 설정](05-connection-config.md)의 SASL_SSL 레시피를 기준으로 **추가 5곳, 변경 2곳, 삭제 1곳**입니다. 보안 블록, acks, 압축, 배치, 수동 커밋은 손대지 않습니다.

| 구분 | 위치 | 항목 | 값 |
| --- | --- | --- | --- |
| 추가 | `spring.kafka.properties` | `schema.registry.url` | SR 2대 주소, 쉼표 구분. `http://` 포함 |
| 추가 | `spring.kafka.properties` | `auto.register.schemas` | 운영 `false`, 개발·스테이징 `true` |
| 추가 | `spring.kafka.properties` | `avro.remove.java.properties` | `true` |
| 변경 | `producer.value-serializer` | `JsonSerializer` → `KafkaAvroSerializer` | |
| 변경 | `consumer.value-deserializer` | `JsonDeserializer` → `ErrorHandlingDeserializer` | |
| 추가 | `consumer.properties` | `spring.deserializer.value.delegate.class` | `KafkaAvroDeserializer` |
| 추가 | `consumer.properties` | `specific.avro.reader` | `true` |
| 삭제 | `consumer.properties` | `spring.json.trusted.packages` | JSON 전용 |
| 추가 (코드) | `main()` | `org.apache.avro.SERIALIZABLE_PACKAGES` | 6장 참고 |

같은 내용을 05 레시피 전체에 표시한 것입니다. `★`가 없는 줄은 그대로입니다.

```yaml
spring:
  kafka:
    bootstrap-servers: kafka1:9094,kafka2:9094,kafka3:9094
    properties:
      security.protocol: SASL_SSL
      sasl.mechanism: SCRAM-SHA-512
      sasl.jaas.config: >
        org.apache.kafka.common.security.scram.ScramLoginModule required
        username="${KAFKA_USER}" password="${KAFKA_PASSWORD}";
      ssl.truststore.location: /app/secrets/truststore.jks
      ssl.truststore.password: ${KAFKA_TRUSTSTORE_PASSWORD}
      ssl.endpoint.identification.algorithm: https
      schema.registry.url: http://10.0.0.11:8081,http://10.0.0.13:8081   # ★ 추가 — 2대 모두
      auto.register.schemas: false                                       # ★ 추가 — 운영 false, 개발 true
      avro.remove.java.properties: true                                  # ★ 추가 — 아래 표 참고
    producer:
      acks: all
      compression-type: lz4
      batch-size: 65536
      properties:
        enable.idempotence: true
        linger.ms: 20
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer   # ★ 변경 — 기존 JsonSerializer
    consumer:
      group-id: notification-service
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer   # ★ 변경 — 기존 JsonDeserializer
      properties:
        spring.deserializer.value.delegate.class: io.confluent.kafka.serializers.KafkaAvroDeserializer   # ★ 추가
        specific.avro.reader: true                                         # ★ 추가
        isolation.level: read_committed
        # spring.json.trusted.packages: "com.osstem.*"                     # ★ 삭제 — JSON 전용
    listener:
      ack-mode: manual
```

`sasl.jaas.config`처럼 `>`로 시작하는 블록 **안에는 `#` 주석을 넣지 마세요.** 블록 안에서는 `#`이 주석이 아니라 값의 일부라서, JAAS 파서가 그 텍스트를 읽다 기동에 실패합니다. 주석은 `>` 옆이나 윗줄에 둡니다.

| 항목 | 왜 |
| --- | --- |
| `schema.registry.url` 2대 | 클라이언트가 순서대로 시도합니다. 한 대만 적으면 그 서버 점검 중 앱 시작이 실패합니다. `http://`가 빠지면 `MalformedURLException: no protocol` |
| `auto.register.schemas=false` | 운영에서 앱이 실수로 새 스키마를 등록하는 것을 막습니다. 미등록 스키마로 전송하면 직렬화 실패로 **배포 직후 바로 드러납니다** |
| `avro.remove.java.properties=true` | 생성 클래스의 스키마에는 `avro.java.string` 속성이 붙어 `.avsc` 원문과 **다른 스키마**로 취급됩니다. 자동 등록을 끄고 `.avsc`를 REST로 등록하는 운영 절차에서는 이 옵션이 없으면 `40403 Schema Not Found`가 납니다 |
| `KafkaAvroSerializer` | 전송 시 스키마 ID를 조회하고 `[매직바이트][ID][Avro]` 형식으로 직렬화합니다 |
| `ErrorHandlingDeserializer` + delegate | Avro가 아닌 메시지나 스키마 조회 실패가 리스너를 무한 재시도에 빠뜨리지 않게 감쌉니다([06 자주 하는 실수](06-common-mistakes.md) 7장). 실제 역직렬화는 delegate인 `KafkaAvroDeserializer`가 합니다 |
| `specific.avro.reader=true` | 없으면 `GenericRecord`가 와서 `ClassCastException`이 납니다. 가장 흔한 실수 |
| `spring.json.trusted.packages` 삭제 | JSON 역직렬화 전용 설정이라 남겨 둬도 무해하지만, 지워야 "이 앱은 Avro"가 분명해집니다 |
| key는 `String` 유지 | key는 파티션 결정용 식별자입니다. Avro로 감쌀 이유가 없습니다 |

개발·스테이징과 운영의 차이는 `auto.register.schemas` 하나뿐이므로 프로파일로 나눕니다([05](05-connection-config.md) 5장). 실행 가능한 예시는 [examples/schema-registry-sample](../examples/schema-registry-sample/README.md)의 `application.yml`에 있습니다.

## 6. 코드 — 달라지는 것이 거의 없다

DTO record 대신 **생성된 Avro 클래스**를 쓰는 것이 전부입니다.

```java
// 프로듀서
OrderCreated event = OrderCreated.newBuilder()
        .setOrderId(order.id())
        .setCustomerId(order.customerId())
        .setItems(order.items().stream()
                .map(i -> OrderItem.newBuilder().setSku(i.sku()).setQty(i.qty()).build())
                .toList())
        .setTotalAmount(order.totalAmount())
        .setCreatedAt(Instant.now())
        .setCouponCode(order.couponCode().orElse(null))
        .build();

kafkaTemplate.send("commerce.order.created", event.getOrderId(), event);   // 03 과 동일
```

```java
// 컨슈머
@KafkaListener(topics = "commerce.order.created", groupId = "notification-service")
public void on(ConsumerRecord<String, OrderCreated> record, Acknowledgment ack) {
    OrderCreated event = record.value();
    // 멱등 처리 + 비즈니스 로직 (04 컨슈머와 동일)
    ack.acknowledge();
}
```

- `KafkaTemplate<String, OrderCreated>`처럼 제네릭 타입만 바꿉니다.
- 생성 클래스는 **빌드 산출물**입니다. Git에 커밋하지 않고 `.avsc`만 커밋합니다.
- 프로듀서와 컨슈머가 다른 서비스면 각자 같은 `.avsc`를 가져야 합니다. 공용 스키마 저장소(Git 리포지토리 또는 사내 Maven 아티팩트)로 배포하는 것을 권장합니다. 복사해 두면 반드시 어긋납니다.
- **Avro 1.12부터 생성 클래스 신뢰 목록이 필수입니다.** 직렬화기가 스키마 이름으로 생성 클래스를 찾을 때 검사하며, 없으면 첫 전송에서 `SecurityException: Forbidden ... is not trusted to be included in Avro schemas`가 납니다. JVM 옵션 `-Dorg.apache.avro.SERIALIZABLE_PACKAGES=com.osstem`을 주거나, `main()`에서 `SpringApplication.run` 전에 같은 시스템 프로퍼티를 설정합니다. Avro 클래스가 로딩된 뒤에는 바꿔도 반영되지 않으므로 반드시 앞에 둡니다.

## 7. 스키마 변경하기 (진화) — 가장 중요한 규칙

### 호환성 모드

Schema Registry는 새 버전을 등록할 때 이전 버전과의 **호환성**을 검사하고, 어긋나면 `HTTP 409`로 거부합니다. 검사 방향은 subject의 호환성 모드가 정합니다.

| 모드 | 검사 내용 | 배포 순서 |
| --- | --- | --- |
| `BACKWARD` (기본) | **새 스키마로 옛 데이터**를 읽을 수 있나 | 컨슈머 먼저, 프로듀서 나중 |
| `FORWARD` | **옛 스키마로 새 데이터**를 읽을 수 있나 | 프로듀서 먼저, 컨슈머 나중 |
| `FULL` | 둘 다 | 순서 무관 |
| `*_TRANSITIVE` | 직전 버전이 아니라 **모든 이전 버전**과 검사 | 위와 같음 |

클러스터 기본은 `BACKWARD`입니다. 다른 팀이 소비하는 토픽은 배포 순서를 맞추기 어려우므로 **`FULL_TRANSITIVE`를 플랫폼팀에 요청**하세요. 아래 "안전한 변경"만 하면 어느 모드에서도 통과합니다.

### 허용되는 변경과 금지되는 변경

| 변경 | BACKWARD | FULL | 비고 |
| --- | --- | --- | --- |
| 필드 추가 **(기본값 있음)** | ○ | ○ | **유일하게 권장하는 변경.** `["null","string"]` + `default: null` |
| 필드 추가 (기본값 없음) | ✕ | ✕ | 옛 데이터에 값이 없어 새 컨슈머가 못 읽음 |
| 필드 삭제 (기본값 있던 필드) | ○ | ○ | 옛 컨슈머는 default로 채움 |
| 필드 삭제 (기본값 없던 필드) | ○ | ✕ | 옛 컨슈머가 새 데이터를 못 읽음. 하지 마세요 |
| 필드 이름 변경 | ✕ | ✕ | 삭제 + 추가로 취급됨. `aliases`로 우회 가능하나 비권장 |
| 타입 변경 (`long`→`string` 등) | ✕ | ✕ | 금지 |
| 타입 확장 (`int`→`long`) | ○ | ✕ | 방향에 따라 다름. 피하세요 |
| `doc`·순서 변경 | ○ | ○ | 호환성과 무관 |
| enum 심볼 추가 | enum에 `default`가 있으면 ○ | 같음 | 없으면 옛 컨슈머가 실패 |

정리하면 **"기본값 있는 필드 추가"만 하세요.** 그 외 변경이 필요하면 새 토픽(`.v2`)입니다. 필드가 "쓰지 않게 됐다"면 삭제하지 말고 `doc`에 폐기 예정을 적은 채 남겨 둡니다.

### 변경 절차

1. `.avsc`를 수정합니다. 새 필드에 `default`가 있는지 확인합니다.
2. 로컬에서 호환성을 **미리 검사**합니다. 등록이 아니라 검사만 하는 API입니다.

```bash
SR=http://10.0.0.11:8081
SUBJECT=commerce.order.created-value

# .avsc 를 {"schema": "<문자열>"} 로 감싸서 보냄
jq -n --rawfile s src/main/avro/OrderCreated.avsc '{schema: $s}' \
  | curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
      --data @- $SR/compatibility/subjects/$SUBJECT/versions/latest
# {"is_compatible":true}  ← 통과.  false 면 이유가 "messages" 에 옵니다
```

3. 스테이징에 배포해 동작을 확인합니다. 스테이징은 `auto.register.schemas=true`라 앱이 등록합니다.
4. 운영 등록은 배포 파이프라인 또는 플랫폼팀 요청으로 합니다.

```bash
jq -n --rawfile s src/main/avro/OrderCreated.avsc '{schema: $s}' \
  | curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
      --data @- $SR/subjects/$SUBJECT/versions
# {"id":22}   ← 새 버전의 스키마 ID
```

5. 운영 앱을 배포합니다. `FULL` 모드면 순서는 무관하고, `BACKWARD`면 컨슈머를 먼저 배포합니다.

같은 스키마를 다시 등록하면 새 버전이 생기지 않고 기존 ID를 돌려줍니다. 등록은 **멱등**이라 파이프라인에서 매 배포마다 실행해도 안전합니다.

## 8. 확인·디버깅 명령

```bash
SR=http://10.0.0.11:8081

curl -s $SR/subjects                                              # 등록된 subject 목록
curl -s $SR/subjects/commerce.order.created-value/versions        # [1,2]
curl -s $SR/subjects/commerce.order.created-value/versions/latest | jq -r .schema | jq   # 최신 스키마 보기 좋게
curl -s $SR/schemas/ids/22                                        # 메시지 헤더의 ID 로 스키마 찾기
curl -s $SR/config/commerce.order.created-value                   # 이 subject 의 호환성 모드 (없으면 전역값 사용)
curl -s $SR/config                                                # 전역 호환성 모드
```

토픽에 실제로 어떤 스키마 ID로 쓰였는지 보려면 메시지 앞 5바이트를 봅니다.

```bash
kafka-console-consumer.sh --bootstrap-server kafka1:9094 --consumer.config client.properties \
  --topic commerce.order.created --max-messages 1 --from-beginning | xxd | head -1
# 00000000: 0000 0000 16 ...   ← 매직 0x00, 스키마 ID 0x00000016 = 22
```

**삭제는 하지 마세요.** subject나 버전을 지우면 그 ID로 쓰인 기존 메시지를 아무도 못 읽습니다. 잘못 등록했으면 플랫폼팀에 알리고, 새 버전으로 덮는 방향으로 해결합니다.

## 9. 자주 나는 오류

| 오류 메시지 | 원인 | 해결 |
| --- | --- | --- |
| `Schema being registered is incompatible with an earlier schema` (409) | 7장의 금지된 변경 | 기본값 추가, 또는 새 토픽 |
| `Error retrieving Avro schema` / `Subject not found` (40401) | 운영에서 `auto.register.schemas=false`인데 스키마가 미등록 | 7장 4단계로 먼저 등록 |
| `Schema Not Found` (40403) — subject는 있는데 | 등록한 `.avsc` 원문과 생성 클래스의 스키마(`avro.java.string` 포함)가 달라 정확히 같은 ID를 못 찾음 | `avro.remove.java.properties=true` (5장) |
| `SecurityException: Forbidden com.osstem... is not trusted to be included in Avro schemas` | Avro 1.12 생성 클래스 신뢰 목록 미설정 | `-Dorg.apache.avro.SERIALIZABLE_PACKAGES=com.osstem` (6장) |
| `Error serializing Avro message` + `Connection refused` | Schema Registry 주소·방화벽. 앱 시작 직후 첫 전송에서 남 | 주소 2대 확인, 8081 방화벽 |
| `ClassCastException: GenericData$Record cannot be cast to OrderCreated` | `specific.avro.reader` 누락 | 5장 설정 추가 |
| `Could not find class com.example.OrderCreated specified in writer's schema` | 프로듀서와 컨슈머의 `namespace`/`name`이 다름 | 같은 `.avsc`를 쓰도록 통일 |
| `Unknown magic byte!` | Avro 컨슈머가 JSON 메시지를 읽음. 토픽에 형식이 섞였거나 잘못된 토픽 구독 | 형식은 토픽 단위로 하나. `ErrorHandlingDeserializer`로 격리 |
| `Field 'couponCode' type:UNION ... Expected start-union. Got VALUE_STRING` | JSON으로 Avro 스키마 테스트 시 union 표기 오류 | union 값은 `{"string": "X"}` 형태. 코드에서는 무관 |
| `Could not resolve io.confluent:kafka-avro-serializer` | Confluent 저장소 미등록 또는 사내 미러 차단 | 3장의 저장소 추가 |
| `Invalid default for field couponCode` | `["string","null"]` 순서로 `default: null` | `null`을 union 첫 번째로 |

## 10. 체크리스트

- `.avsc`에 `namespace`·`name`·`doc`이 있고, nullable 필드는 `["null", T]` + `default: null`인가
- `schema.registry.url`에 2대를 모두 적었는가
- 운영 프로파일에 `auto.register.schemas=false`가 있는가
- 컨슈머에 `specific.avro.reader=true`와 `ErrorHandlingDeserializer`가 있는가
- `avro.remove.java.properties=true`와 `org.apache.avro.SERIALIZABLE_PACKAGES` 설정이 있는가
- 변경 전에 `/compatibility` API로 검사했는가
- 다른 팀이 소비하는 토픽이면 호환성 모드 `FULL_TRANSITIVE`를 요청했는가
- 프로듀서·컨슈머가 같은 `.avsc`를 참조하는가 (복사본 아님)

## 관련 문서

- Schema Registry가 푸는 문제와 Debezium: [concepts/09-concepts-qna](../concepts/09-concepts-qna.md) 10장
- 실제로 돌려보기: [examples/scenario-runner](../examples/scenario-runner/README.md)의 `sample-avro-*`, `sample-schema-evolution`
- 접속 설정 기본: [05 접속 설정](05-connection-config.md)
- poison pill 처리: [06 자주 하는 실수](06-common-mistakes.md) 7장
- 깨지는 변경의 새 토픽 절차: [01 토픽 명명 규칙](01-topic-naming.md) 4장
