# scenario-runner 사용 예시 명령 설계

- 날짜: 2026-09-06
- 상태: 승인됨
- 범위: `kafka/examples/scenario-runner/` — 기존 장애 시나리오 CLI에 프로듀서/컨슈머 사용 예시와 Schema Registry(SR)+Avro 예시를 `sample-*` 명령으로 추가

## 목표

usage-guide 03~05가 보여주는 Spring Kafka 코드(`KafkaTemplate`, `@KafkaListener`, `JsonSerializer`, `OrderCreatedEvent`)를 실제로 실행할 수 있는 형태로 제공한다. 이어서 Schema Registry가 무엇을 하는지를 전송·수신·스키마 진화(호환 등록 성공, 비호환 등록 거부) 세 장면으로 재현한다. 저장소에는 현재 SR을 쓰는 코드가 한 줄도 없고, usage-guide 코드 조각은 실행 가능한 프로젝트가 아니다.

## 결정 사항

- 배치: 별도 프로젝트나 Maven 멀티모듈이 아니라 **scenario-runner와 같은 jar의 새 명령**. 빌드·클러스터·README 진입 절차를 재사용한다.
- 스타일: 샘플은 **Spring Kafka 방식**(`KafkaTemplate`, `@KafkaListener`). 러너 내부(`LoadGenerator`, `VerifierConsumer`)는 부하 제어를 위해 `kafka-clients`를 직접 쓰므로 한 프로젝트에 두 스타일이 공존한다. 샘플의 존재 이유가 usage-guide 코드와의 1:1 일치이므로 이 불균일은 감수하고, 패키지를 `sample/`로 분리해 경계를 둔다.
- 단계: 1단계 JSON(String key + `JsonSerializer`), 2단계 Avro + SR. 두 단계 모두 같은 도메인 `OrderCreatedEvent` / `commerce.order.created`를 쓴다.
- SR 범위: 전송·수신에 더해 스키마 진화까지 한 명령으로 재현한다.
- 대상 클러스터: 러너에 동봉된 PLAINTEXT 3브로커 compose. SASL_SSL, SR 인증, home-lab 연동은 범위 밖 (보안 설정은 usage-guide 05가 다룬다).
- 버전(2026-09-06 확인): `confluentinc/cp-schema-registry:8.3.1`, `io.confluent:kafka-avro-serializer:8.3.1`, `avro-maven-plugin:1.12.2`. 모두 Kafka 4.0 기반이라 기존 `apache/kafka:4.0.0`과 맞는다.

## 명령 구조

현재 `RunnerApplication`은 `Map<String, Supplier<Scenario>>`로 인자를 시나리오에 매핑하고 토픽 초기화 → 장애 주입 → 판정 → 리포트를 직접 수행한다. 샘플은 이 흐름을 타지 않으므로 얇은 `Command` 인터페이스를 도입하고, 기존 흐름은 `ScenarioCommand`로 감싼다. 시나리오 4개의 코드와 동작은 바뀌지 않는다.

```text
command/
  Command.java              name(), description(), run(List<String> args) -> 종료 코드
  ScenarioCommand.java      기존 시나리오 흐름(FaultInjector/Ledger/Judge/Reporter) 그대로
sample/
  SampleKafkaConfig.java    KafkaTemplate, 리스너 컨테이너 팩토리(JSON/Avro) @Bean
  SampleTopics.java         AdminClient 로 샘플 토픽 생성(이미 있으면 무시)
  OrderCreatedEvent.java    JSON 단계용 record (orderId, customerId, amount, createdAt)
  SampleProduceCommand      sample-produce [count=10]
  SampleConsumeCommand      sample-consume [count=10]
  SampleAvroProduceCommand  sample-avro-produce [count=10]
  SampleAvroConsumeCommand  sample-avro-consume [count=10]
  SchemaEvolutionCommand    sample-schema-evolution
  avro/OrderCreated         src/main/avro/OrderCreated.avsc 에서 생성 (target/generated-sources)
```

`RunnerApplication`은 명령 이름으로 `Command`를 찾아 실행하고 종료 코드만 다룬다. 사용법 출력은 시나리오 그룹과 샘플 그룹을 구분해 보여준다. 종료 코드 규약은 기존과 같다: 정상(또는 PASS) 0, 실패/예외 1, 사용법 오류 2.

### 샘플 명령의 동작

| 명령 | 동작 | 확인하게 하는 것 |
| --- | --- | --- |
| `sample-produce [N]` | `commerce.order.created` 토픽을 없으면 생성(compose가 자동 생성을 끔). `OrderCreatedEvent` N건을 `KafkaTemplate.send(topic, orderId, event)`로 전송. 콜백에서 key·파티션·오프셋 출력 | 같은 orderId → 같은 파티션. 실패는 콜백에서만 알 수 있음 |
| `sample-consume [N]` | `@KafkaListener(groupId="notification-service", autoStartup=false)`를 명령에서 켜고 N건 수신 또는 30초 무수신이면 종료. 수동 ack, `spring.json.trusted.packages` | usage-guide 04·05와 같은 설정으로 실제 수신 |
| `sample-avro-produce [N]` | 위와 같은 구조, serializer만 `KafkaAvroSerializer`, 토픽 `commerce.order.created.avro`, 값은 생성 클래스 `OrderCreated`. 전송 후 SR REST로 subject 최신 버전과 schema id 조회·출력 | 코드에 SR 호출이 없어도 serializer가 등록·조회함. 메시지에는 id만 실림 |
| `sample-avro-consume [N]` | `KafkaAvroDeserializer` + `specific.avro.reader=true` | id → 스키마 조회 → 역직렬화 |
| `sample-schema-evolution` | ① v2(필드 추가, 기본값 있음)를 `CachedSchemaRegistryClient.register()`로 등록 ② v2 `GenericRecord` 1건 전송 ③ 새 그룹(`auto.offset.reset=latest` 기준으로 전송 직전에 구독)에서 v1 생성 클래스로 그 1건을 소비해 정상 수신 출력 ④ 비호환 스키마(기본값 없는 필드 추가) 등록 시도 → SR 409 출력 | 호환성 검사는 SR이 하는 유일한 관문. reader/writer 스키마 해석으로 구버전 컨슈머가 신버전 메시지를 읽음 |

### Spring 설정 분리

- 접속 값은 `application.yml`의 `spring.kafka.*` 블록을 그대로 쓰고, `spring.kafka.properties.schema.registry.url: http://localhost:8081`을 추가한다.
- serializer/deserializer는 명령마다 다르므로 yml이 아니라 `SampleKafkaConfig`의 팩토리 빈에서 지정한다.
- 샘플 토픽명은 `runner.sample-topic`, `runner.sample-avro-topic`으로 `RunnerProperties`에 추가한다.
- 리스너는 `autoStartup=false`. 시나리오 실행 중 샘플 컨슈머가 떠서 브로커에 그룹을 만드는 것을 막는다.

## 인프라와 빌드

### compose에 SR 추가

기존 `docker-compose.yml`에 항상 켜지는 서비스로 추가한다. 프로필로 분리하지 않는 이유는 `docker compose up -d` 한 줄이라는 진입 절차를 유지하기 위해서다.

```yaml
schema-registry:
  image: confluentinc/cp-schema-registry:8.3.1
  container_name: schema-registry
  restart: "no"
  depends_on: [kafka1, kafka2, kafka3]
  ports: ["8081:8081"]
  environment:
    SCHEMA_REGISTRY_HOST_NAME: schema-registry
    SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka1:29092,kafka2:29092,kafka3:29092
    SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR: 3
    SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: backward
```

- 브로커 INTERNAL 리스너(29092)로 접속. `_schemas`는 복제 3이라 브로커 1대 정지 중에도 SR이 동작한다.
- 장애 시나리오는 `runner.containers` 목록만 stop/start하므로 SR은 시나리오에 영향을 주지 않는다. `total-outage`에서 브로커가 전부 내려가면 SR은 에러 로그를 내며 대기하다 복구 후 재접속한다. 이것이 "SR은 브로커에 의존하지만 브로커는 SR을 모른다"의 재현이다.

### pom.xml

- `<repositories>`에 Confluent 저장소 `https://packages.confluent.io/maven/` 추가. Maven Central에 없다. 사내 미러 환경은 이 주소를 허용해야 한다는 점을 README에 명시한다.
- 의존성 `io.confluent:kafka-avro-serializer:8.3.1` (schema-registry-client, avro 전이 포함).
- `avro-maven-plugin:1.12.2`를 `generate-sources`에 걸어 `src/main/avro/*.avsc` → `dev.devopsnote.kafkarunner.sample.avro` 패키지로 생성. 출력은 `target/generated-sources/avro`라 sync의 `target` skip 규칙에 걸린다.

### 스키마 파일

```text
src/main/avro/OrderCreated.avsc                              v1 — 코드 생성 대상 (orderId, customerId, amount, createdAt)
src/main/resources/schemas/order-created-v2.avsc             v1 + couponCode: ["null","string"] default null — BACKWARD 호환
src/main/resources/schemas/order-created-incompatible.avsc   v1 + channel: string (default 없음) — BACKWARD 위반
```

v2와 incompatible은 코드 생성 대상이 아니다. `sample-schema-evolution`이 파일을 읽어 등록하고, v2 전송은 `GenericRecord`로 한다.

## 문서

- scenario-runner README에 "사용 예시" 절 추가: 1단계(JSON), 2단계(Avro+SR)로 나눠 명령, 기대 출력, 각 출력 줄의 의미(파티션 고정, schema id, 409 거부). 이 README는 sync가 `examples` 문서로 사이트에 올린다.
- usage-guide 03, 04, 08 끝에 "이 코드를 실행해 보려면" 한 줄과 scenario-runner README 상대 링크 추가.
- 러너 내부가 `kafka-clients`를 직접 쓰는 이유(부하 제어)를 README에 한 줄 명시.

## 테스트와 검증

### 단위 테스트 (`mvn test`, Docker 불필요)

- `CommandRegistryTest`: 알 수 없는 이름 → 종료 코드 2. 시나리오 4개 + 샘플 5개 이름이 모두 등록됨. 기존 시나리오 이름 회귀 방지.
- `SchemaEvolutionTest`: 세 avsc를 Avro `SchemaCompatibility` API로 로컬 비교. v2는 BACKWARD 호환, incompatible은 실제 비호환.
- 기존 `JudgeTest`, `LedgerTest`, `FaultInjectorCommandTest`는 변경 없이 통과.

### 실행 검증 (로컬 Docker)

1. `docker compose up -d` 후 `curl localhost:8081/subjects` → `[]`.
2. `sample-produce` → `sample-consume`: N건 수신.
3. `sample-avro-produce` → `sample-avro-consume` → `sample-schema-evolution`: `/subjects/commerce.order.created.avro-value/versions` → `[1,2]`, 409 출력.
4. `normal-roundtrip`, `broker-1-down` 재실행 → PASS 유지 (SR 추가가 시나리오에 영향 없음을 확인).
5. `cd web && npm run lint && npm test` 통과. Web 테스트는 토픽 id 목록과 문서 수 하한만 검사하므로 수정 대상 없음. `.avsc`와 `target/`은 사이트에 새지 않는다.

## 범위 밖

- SASL_SSL 접속, SR 인증(`basic.auth.*`), home-lab 클러스터 연동
- `auto.register.schemas=false` + CI 사전 등록 운영 패턴 (README에서 언급만)
- Avro 이외 포맷(JSON Schema, Protobuf)
