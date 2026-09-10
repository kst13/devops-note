# Schema Registry 샘플 — 사용 가이드 09 검증용

[usage-guide/09-schema-registry.md](../../usage-guide/09-schema-registry.md)에 적힌 대로 하면 실제로 되는지 확인하는 최소 Spring Boot 프로젝트입니다. 가이드의 장 번호와 이 프로젝트의 파일이 1:1로 대응합니다.

| 가이드 | 이 프로젝트 |
| --- | --- |
| 3장 의존성 | `build.gradle.kts` |
| 4장 스키마 파일 | `src/main/avro/OrderCreated.avsc` (v1), `schemas/order-created-v2.avsc`, `schemas/order-created-incompatible.avsc` |
| 5장 설정 | `src/main/resources/application.yml` (`local` 기본, `strict`, `prod` 프로파일) |
| 6장 코드 | `OrderEventProducer`, `OrderEventListener`, `OrderController` |
| 7장 호환성 규칙 | `SchemaEvolutionTest` (SR 없이 검증) + 아래 4~5단계 (SR 로 검증) |
| 5·7장 운영 등록 절차가 실제로 통하는 조건 | `ManualRegistrationTest` — `.avsc` 원문 등록 후 직렬화가 되는 설정 조합 |
| 전부 (IntelliJ) | `schema-registry.http` — 아래 curl 을 순서대로 옮긴 것. 환경은 `http-client.env.json`(local) / `http-client.private.env.json`(prod, git 제외) |
| 8·9장 확인 명령·오류 | 아래 3, 6, 7단계 |

## 전제 조건

- Docker (Compose v2), JDK 21 이상
- 포트 9092(브로커), 8081(Schema Registry), 8088(앱) 미사용
- `https://packages.confluent.io/maven/` 접근 가능 (사내 Nexus를 쓴다면 프록시 등록 필요)

## 1. 기동과 빌드

```bash
docker compose up -d
# Schema Registry 가 뜰 때까지 30초 안팎. 아래가 [] 를 돌려주면 준비 완료
curl -s localhost:8081/subjects

./gradlew bootJar
java -jar build/libs/schema-registry-sample.jar
```

빌드 로그에 `generateAvroJava` 태스크가 보이고 `build/generated-main-avro-java/com/osstem/commerce/order/event/OrderCreated.java`가 생기면 4장의 코드 생성이 동작한 것입니다.

## 2. 이벤트 발행과 수신 (가이드 6장)

```bash
curl -s -X POST localhost:8088/orders -H 'Content-Type: application/json' -d '{
  "orderId": "ORD-20260910-0001",
  "customerId": 10234,
  "items": [{"sku": "A-100", "qty": 2}, {"sku": "B-250", "qty": 1}],
  "totalAmount": 51000
}'
# {"orderId":"ORD-20260910-0001","partition":1,"offset":0,"serializedValueSize":58}
```

확인할 것:

- 응답의 `serializedValueSize`가 수십 바이트입니다. 같은 내용을 JSON으로 보내면 150바이트 이상입니다. 메시지에 스키마 대신 ID 4바이트만 실리기 때문입니다.
- 앱 로그에 `발행 완료 ...`에 이어 `수신 partition=... customerId=10234 items=2 ...`가 찍힙니다. 컨슈머가 `specific.avro.reader=true`로 생성 클래스 `OrderCreated`를 받은 것입니다.

## 3. Schema Registry 에 등록된 것 보기 (가이드 8장)

```bash
curl -s localhost:8081/subjects
# ["commerce.order.created-value"]          ← subject = <토픽>-value

curl -s localhost:8081/subjects/commerce.order.created-value/versions
# [1]

curl -s localhost:8081/subjects/commerce.order.created-value/versions/latest | jq -r .schema | jq
# OrderCreated.avsc 내용. 첫 전송 때 KafkaAvroSerializer 가 등록한 것 (local 프로파일은 auto.register.schemas=true)

curl -s localhost:8081/config
# {"compatibilityLevel":"BACKWARD"}
```

토픽의 실제 바이트를 보면 매직바이트와 스키마 ID 가 앞에 있습니다.

```bash
docker exec sr-sample-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 --topic commerce.order.created \
  --from-beginning --max-messages 1 2>/dev/null | xxd | head -1
# 00000000: 0000 0000 01...   ← 0x00 매직, 0x00000001 = 스키마 ID 1
```

## 4. 호환되는 변경 — 기본값 있는 필드 추가 (가이드 7장)

`schemas/order-created-v2.avsc`는 v1에 `couponCode`를 `["null","string"]` + `default: null`로 추가한 것입니다.

```bash
SR=http://localhost:8081
SUBJECT=commerce.order.created-value

# 등록 전 검사만
jq -n --rawfile s schemas/order-created-v2.avsc '{schema: $s}' \
  | curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
      --data @- $SR/compatibility/subjects/$SUBJECT/versions/latest
# {"is_compatible":true}

# 등록
jq -n --rawfile s schemas/order-created-v2.avsc '{schema: $s}' \
  | curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
      --data @- $SR/subjects/$SUBJECT/versions
# {"id":2}

curl -s $SR/subjects/$SUBJECT/versions
# [1,2]
```

앱(v1 스키마로 컴파일됨)은 그대로 동작합니다. 2단계의 POST를 다시 보내면 여전히 ID 1로 전송하고 수신합니다. 새 버전이 등록됐다고 기존 앱이 영향을 받지 않는다는 뜻입니다.

## 5. 호환되지 않는 변경 — 기본값 없는 필드 추가

`schemas/order-created-incompatible.avsc`는 `channel: string`을 기본값 없이 추가한 것입니다.

```bash
jq -n --rawfile s schemas/order-created-incompatible.avsc '{schema: $s}' \
  | curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
      --data @- $SR/compatibility/subjects/$SUBJECT/versions/latest
# {"is_compatible":false,"messages":["Incompatibility.Type: READER_FIELD_MISSING_DEFAULT_VALUE ..."]}

jq -n --rawfile s schemas/order-created-incompatible.avsc '{schema: $s}' \
  | curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
      --data @- $SR/subjects/$SUBJECT/versions
# {"error_code":409,"message":"Schema being registered is incompatible with an earlier schema ..."}

curl -s $SR/subjects/$SUBJECT/versions
# [1,2]   ← 거부된 스키마는 버전에 남지 않는다
```

같은 판정을 SR 없이 Avro 라이브러리로 확인하는 것이 `SchemaEvolutionTest`입니다.

```bash
./gradlew test
```

## 6. 운영처럼 자동 등록을 막았을 때 (가이드 5장 `auto.register.schemas=false`)

`strict` 프로파일은 `local`과 같지만 자동 등록만 끕니다. 아직 등록되지 않은 스키마로 전송하면 어떻게 되는지 봅니다.

```bash
# 1) SR 을 비운 상태로 만들기 (로컬 검증이므로 삭제. 운영에서는 절대 하지 않는다 — 가이드 8장)
docker compose down -v && docker compose up -d
until curl -sf localhost:8081/subjects >/dev/null; do sleep 3; done

# 2) strict 로 기동
java -jar build/libs/schema-registry-sample.jar --spring.profiles.active=strict

# 3) 전송 → 실패
curl -s -X POST localhost:8088/orders -H 'Content-Type: application/json' \
  -d '{"orderId":"ORD-1","customerId":1,"items":[],"totalAmount":0}'
# {"error":"SerializationException","message":"Error retrieving Avro schema ... Subject 'commerce.order.created-value' not found.; error code: 40401"}
```

이것이 가이드 9장의 `Subject not found (40401)`입니다. 운영에서 스키마 등록을 빠뜨리면 **첫 전송에서 바로 드러납니다**.

```bash
# 4) 가이드 7장 4단계처럼 배포 파이프라인 역할로 등록
jq -n --rawfile s src/main/avro/OrderCreated.avsc '{schema: $s}' \
  | curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
      --data @- localhost:8081/subjects/commerce.order.created-value/versions
# {"id":1}

# 5) 앱 재시작 없이 다시 전송 → 성공 (직렬화기가 SR 에 다시 조회한다)
curl -s -X POST localhost:8088/orders -H 'Content-Type: application/json' \
  -d '{"orderId":"ORD-1","customerId":1,"items":[],"totalAmount":0}'
# {"orderId":"ORD-1","partition":...,"offset":0,"serializedValueSize":...}
```

## 7. poison pill — Avro 가 아닌 메시지가 들어왔을 때 (가이드 5장 `ErrorHandlingDeserializer`)

```bash
echo 'not-avro' | docker exec -i sr-sample-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:29092 --topic commerce.order.created
```

앱 로그에 `Unknown magic byte!`를 포함한 역직렬화 오류가 찍히지만 **리스너는 멈추지 않고** 다음 메시지를 계속 처리합니다. `ErrorHandlingDeserializer`가 예외를 헤더로 바꿔 넘기고 기본 에러 핸들러가 그 레코드를 건너뛰기 때문입니다. 이 줄을 `application.yml`에서 `KafkaAvroDeserializer`로 직접 바꾸고 다시 해 보면 같은 메시지에서 무한 재시도에 빠지는 것을 볼 수 있습니다.

## 8. 운영 프로파일 (`prod`) 은 값만 다르다

`application.yml`의 `prod` 블록이 가이드 5장의 SASL_SSL 설정입니다. 로컬에서는 실행하지 않고, 어떤 값이 환경변수로 빠지는지만 봅니다.

| 환경변수 | 값 예 |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka1:9094,kafka2:9094,kafka3:9094` |
| `KAFKA_USER` / `KAFKA_PASSWORD` | 발급받은 서비스 계정 |
| `KAFKA_TRUSTSTORE_LOCATION` / `KAFKA_TRUSTSTORE_PASSWORD` | `/app/secrets/truststore.jks` |
| `SCHEMA_REGISTRY_URL` | `http://10.0.0.11:8081,http://10.0.0.13:8081` (**2대 모두**) |

## 검증 중 발견한 함정 두 가지 (가이드에 반영됨)

- **Avro 1.12 신뢰 목록**: `org.apache.avro.SERIALIZABLE_PACKAGES`가 없으면 첫 직렬화에서 `SecurityException ... not trusted`. `SampleApplication.main`이 `SpringApplication.run` 전에 설정한다. 테스트 JVM은 `build.gradle.kts`의 `tasks.test`에서 준다.
- **`.avsc` 원문 등록 ≠ 생성 클래스 스키마**: `stringType=String`으로 생성된 클래스의 `SCHEMA$`에는 `avro.java.string` 속성이 붙는다. 자동 등록을 끄고 `.avsc`를 REST로 등록하면 subject는 있어도 `40403 Schema Not Found`. `avro.remove.java.properties=true`로 해결한다. `ManualRegistrationTest`가 함정과 해법 세 가지(remove.java.properties / use.latest.version / 생성 스키마 등록)를 모두 검증한다.

## 정리

```bash
docker compose down -v
```

## 관련 문서

- 가이드 본문: [usage-guide/09-schema-registry.md](../../usage-guide/09-schema-registry.md)
- 3브로커 + 장애 시나리오 + Avro 샘플이 함께 있는 러너: [scenario-runner](../scenario-runner/README.md)
