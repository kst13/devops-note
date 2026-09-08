# Kafka 장애 시나리오 러너

문서로 정리한 Kafka 장애 시나리오를 로컬 3브로커 클러스터에서 실제로 재현하고, 메시지 유실 여부를 기계적으로 판정해 리포트로 남기는 CLI 도구입니다. "브로커 1대가 죽어도 유실이 없다", "2대가 죽으면 쓰기가 실패한다", "전체 정지에도 폴백으로 데이터를 지킨다"를 말이 아니라 실행 결과로 증명합니다.

원리는 단순합니다. 프로듀서가 순번(seq)이 박힌 메시지를 계속 보내고 컨슈머가 수신 순번을 기록하는 동안, 러너가 `docker stop/start`로 장애를 주입합니다. 마지막에 "성공으로 기록된 순번 집합 ⊆ 수신 순번 집합"이라는 불변식을 검사해 PASS/FAIL을 판정합니다.

장애 시나리오와 별개로, usage-guide 의 프로듀서/컨슈머 코드와 Schema Registry+Avro 흐름을 실제로 돌려보는 `sample-*` 명령도 같은 jar 에 들어 있습니다(아래 [사용 예시](#사용-예시)).

## 전제 조건

- Docker (Compose v2 포함)
- JDK 21 이상 (Gradle 은 wrapper 가 포함되어 별도 설치 불필요)
- 포트 9092, 9192, 9292(브로커), 8081(Schema Registry) 미사용 상태 — `home-lab` 예제를 띄워 두었다면 먼저 내려야 합니다
- 빌드가 `https://packages.confluent.io/maven/` 에 접근 가능해야 합니다 (Confluent Avro serializer 는 Maven Central 에 없음). 사내 미러를 쓰면 이 저장소를 허용 목록에 추가하세요

## 실행 방법

```bash
# 1) 로컬 3브로커 클러스터 + Schema Registry 기동 (브로커 healthcheck 통과 후 SR 이 뜨므로 최초 30~60초 대기, curl localhost:8081/subjects 가 [] 를 반환하면 준비 완료)
docker compose up -d

# 2) 빌드
./gradlew bootJar

# 3) 시나리오 실행 (종료 코드: 0=PASS, 1=FAIL, 2=사용법 오류)
java -jar build/libs/scenario-runner.jar normal-roundtrip
java -jar build/libs/scenario-runner.jar broker-1-down
java -jar build/libs/scenario-runner.jar broker-2-down
java -jar build/libs/scenario-runner.jar total-outage
```

시나리오가 끝나면 러너가 컨테이너를 모두 start 상태로 되돌리므로 연속 실행이 가능합니다. 각 실행은 이전 데이터를 지우기 위해 테스트 토픽(`test.scenario.events`)을 삭제 후 재생성합니다.

## 시나리오

| 시나리오 | 절차 | PASS 기준 |
| --- | --- | --- |
| normal-roundtrip | 1만 건 전송 → 전량 소비 | 유실 0, 중복 0 |
| broker-1-down | 부하 중 브로커 1대 정지 60초 → 복구 | 전송 지속(재시도 허용), 유실 0, ISR 완전 회복 |
| broker-2-down | 부하 중 2대 정지 60초 → 복구 → 실패분 재전송 | 정지 구간 쓰기 실패가 발생하는 것이 정상(min.insync.replicas=2), 최종 유실 0 |
| total-outage | 부하 중 3대 정지 → 전체 재기동 → 폴백 재전송 | 쿼럼·리더 자력 복구, 재전송 포함 최종 유실 0 |

broker-2-down과 total-outage는 "실패가 0건이면 오히려 FAIL"로 판정합니다 — 실패가 없다는 것은 장애 주입 자체가 동작하지 않았다는 뜻이기 때문입니다.

## 리포트 해석

리포트는 `reports/<시나리오>-<시각>.md`로 저장됩니다.

- **전송 성공** — acks=all 응답을 받아 성공으로 기록된 메시지 수
- **미전송 잔여** — 재전송 후에도 전달되지 않은 메시지 수 (0이 아니면 항상 FAIL)
- **유실** — 성공으로 기록됐는데 수신되지 않은 수 (0이 아니면 항상 FAIL — 가장 심각)
- **중복** — 같은 순번이 두 번 이상 수신된 수. 재시도·재전송이 있는 시나리오에서는 정상이며, 컨슈머 멱등 처리가 필요한 이유를 보여줍니다

## 사용 예시

장애 시나리오와 같은 클러스터에서 usage-guide 의 코드를 실행해 봅니다. 시나리오와 달리 판정·리포트는 없고, 종료 코드는 0 정상 / 1 실패 / 2 인자 오류입니다. 코드는 `src/main/java/dev/devopsnote/kafkarunner/sample/` 에 있습니다. 러너 내부(`load/`)는 부하 제어를 위해 `kafka-clients` 를 직접 쓰지만, 샘플은 문서와 같은 Spring Kafka(`KafkaTemplate`, `@KafkaListener`) 스타일입니다.

### 1단계 — JSON 프로듀서/컨슈머

[03 프로듀서](../../usage-guide/03-producer.md), [04 컨슈머](../../usage-guide/04-consumer.md), [05 접속 설정](../../usage-guide/05-connection-config.md)의 코드 그대로입니다.

```bash
java -jar build/libs/scenario-runner.jar sample-produce 6     # OrderCreatedEvent 6건 전송 (기본 10건)
java -jar build/libs/scenario-runner.jar sample-consume 6     # notification-service 그룹으로 6건 수신
```

출력에서 볼 것:

- `전송 OK key=ORD-1001 partition=2 offset=…` — key 가 orderId 라서 같은 주문은 항상 같은 파티션에 갑니다. `ORD-1000/1001/1002` 각각의 partition 번호가 매번 같은지 확인하세요.
- `수신 partition=… offset=…` 뒤에 `ack.acknowledge()` 가 호출되어 오프셋이 커밋됩니다. 딱 지정한 건수까지만 커밋되고 그 이후에 도착한 레코드는 커밋되지 않으므로 다음 실행에서 다시 읽힙니다. 새 메시지가 없는 상태에서 바로 다시 `sample-consume` 을 실행하면 30초를 기다린 뒤 `30초 동안 신규 메시지 없음 — 종료` 로 끝나는데, 종료 코드는 0입니다 — 커밋된 오프셋 이후 새 메시지가 없는 것은 정상이지 실패가 아닙니다. 새 메시지를 보려면 `sample-produce` 를 다시 실행하세요.
- 토픽은 명령이 직접 만듭니다. compose 가 운영처럼 자동 생성을 꺼 두었기 때문입니다.

### 2단계 — Avro + Schema Registry

[Schema Registry 개념](../../concepts/09-concepts-qna.md)에서 설명한 흐름을 실제로 확인합니다. 설정 차이는 serializer 클래스와 `schema.registry.url` 뿐이고, 앱 코드에는 SR 호출이 없습니다. Schema Registry 이미지(`cp-schema-registry`)와 serializer 의존성(`kafka-avro-serializer`)은 Confluent 7.9.9 입니다 — 8.x 의 serializer 는 kafka-clients 4.1 API 를 요구하는데 Spring Boot 3.5 는 3.9.1 을 고정하기 때문입니다.

```bash
java -jar build/libs/scenario-runner.jar sample-avro-produce 6
curl -s localhost:8081/subjects/commerce.order.created-avro-value/versions   # → [1]
java -jar build/libs/scenario-runner.jar sample-avro-consume 6
java -jar build/libs/scenario-runner.jar sample-schema-evolution
```

출력에서 볼 것:

- `sample-avro-produce` 의 `serializedValueSize` 는 수십 바이트입니다. 스키마 전체가 아니라 `magic 1B + schema id 4B + 페이로드` 만 실리기 때문입니다. 끝에 나오는 `SR 확인: subject=… version=… schemaId=…` 는 이 실행이 실제로 쓴 writer 스키마의 id·버전이지 "최신"이 아닙니다 — 이 등록은 `KafkaAvroSerializer` 가 첫 전송 때 한 것이라 처음 실행하면 `version=1 schemaId=1` 이지만, `sample-schema-evolution` 으로 v2 를 등록해 둔 뒤라면 최신 버전과 달라질 수 있습니다.
- `sample-avro-consume` 은 메시지 앞의 id 로 SR 에서 스키마를 받아 역직렬화합니다(id 별로 캐시되어 SR 이 잠시 죽어도 이미 본 스키마는 계속 처리됩니다).
- `sample-schema-evolution` 은 `v1 등록 (이미 있으면 기존 id)` 로 시작해 ①~④ 를 순서대로 출력하고, 마지막 줄 `버전 목록: [1, 2]` 로 마칩니다 — 거부된 비호환 스키마는 버전에 남지 않습니다.
- `① v2 등록 성공  schemaId=2` — `couponCode` 를 기본값 `null` 로 추가한 스키마는 BACKWARD 호환이라 등록됩니다.
- `② v2 메시지 전송  key=ORD-2001  couponCode=WELCOME10` — `GenericRecord` 로 새 스키마 메시지를 보냅니다.
- `③ v1 클래스로 수신 OK  key=ORD-2001  …  (writer=v2 schemaId=2, reader=v1 …)` — 옛 생성 클래스로 새 메시지를 읽습니다. Avro 가 writer(v2)/reader(v1) 스키마를 대조해 모르는 필드를 버립니다. 컨슈머 배포 없이 프로듀서만 먼저 바꿔도 되는 이유입니다. 바로 이어지는 `주의: 이 방향(옛 reader × 새 데이터)은 FORWARD 호환이다 … BACKWARD 설정만으로는 보장되지 않는다` 줄이 핵심입니다 — SR 이 등록 시점에 검사한 것은 BACKWARD(새 reader 가 옛 데이터를 읽는지)뿐이고, 지금 확인한 방향(옛 reader 가 새 데이터를 읽는 FORWARD)은 이번 변경이 필드에 기본값을 둬 우연히 FULL 호환이라 되는 것입니다.
- `④ SR 거부  HTTP 409` — 기본값 없는 필드 `channel` 을 추가한 스키마는 옛 데이터를 읽을 수 없으므로 SR 이 거부합니다. 브로커는 이 검사를 하지 않습니다. SR 이 유일한 관문입니다.
- 스키마 파일: `src/main/avro/OrderCreated.avsc`(v1, 코드 생성), `src/main/resources/schemas/order-created-v2.avsc`, `order-created-incompatible.avsc`. 세 파일의 호환 관계는 `SchemaEvolutionTest` 가 SR 없이 검증합니다.
- Schema Registry 가 응답하지 않으면 `sample-avro-produce`/`sample-schema-evolution` 은 각각 `직렬화 실패 — Schema Registry …`, `SR 호출 실패 — …` 를 출력하고 종료 코드 1로 끝납니다.
- Avro 1.12 는 생성 클래스 로딩에 신뢰 목록을 요구하는데, `RunnerApplication.main` 이 시작 시 이를 등록해 두어 `java -jar` 로 바로 실행됩니다. 운영 앱이라면 JVM 옵션 `-Dorg.apache.avro.SERIALIZABLE_PACKAGES=...` 로 두는 것이 일반적입니다.

운영에서는 앱이 스키마를 마음대로 등록하지 못하게 `auto.register.schemas=false` 로 잠그고 CI 에서 미리 등록하는 방식을 권장합니다. 이 예제는 흐름을 보여주기 위해 기본값(자동 등록)을 씁니다.

## 관련 문서

- [클러스터 전체 정지와 복구 절차](../../troubleshooting/cluster-total-outage.md) — 운영자 관점의 복구
- [장애에 대비하는 애플리케이션 설계](../../usage-guide/07-failure-resilience.md) — 이 러너의 폴백 재전송이 검증하는 패턴
- [프로듀서 acks와 복제 원리](../../concepts/02-producer-and-replication.md)

## 정리

```bash
docker compose down    # 클러스터와 데이터 제거
```

이 클러스터는 시나리오 검증 전용입니다. 운영 구성은 [SASL_SSL 3노드 예제](../compose-3node-kraft/README.md)를 사용합니다.
