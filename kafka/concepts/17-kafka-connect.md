# Kafka Connect: 워커·커넥터·태스크, 분산 모드와 장애 복구

Kafka Connect는 외부 시스템(DB, 파일, 검색엔진, 오브젝트 스토리지)과 Kafka 사이에서 데이터를 옮기는 **프레임워크**입니다. 프로듀서·컨슈머 코드를 직접 짜지 않고, 설정(JSON)만으로 "PostgreSQL 변경을 토픽으로", "토픽을 S3로" 같은 연동을 실행합니다. 이 문서는 Connect의 구성 요소, 분산 모드 동작, 컨버터와 SMT, 에러 처리, REST API, 장애 복구 순으로 정리합니다. 배치 위치는 [15-deployment-layout-options](15-deployment-layout-options.md), Debezium 소개는 [09-concepts-qna](09-concepts-qna.md) 10장을 참고합니다.

## 1. 왜 Connect를 쓰나

직접 프로듀서·컨슈머 앱을 만들어도 같은 일을 할 수 있습니다. 차이는 **반복되는 운영 문제를 프레임워크가 대신 푼다**는 점입니다.

| 직접 구현 | Kafka Connect |
| --- | --- |
| 오프셋 저장, 재시작 시 이어가기를 직접 구현 | 프레임워크가 오프셋을 Kafka 내부 토픽에 저장·복구 |
| 장애 시 다른 서버로 넘기는 로직 직접 구현 | 워커가 죽으면 다른 워커가 태스크 인계 |
| 병렬화(파티션 분할)를 직접 설계 | `tasks.max`로 태스크 수만 지정 |
| 형식 변환 코드 직접 작성 | 컨버터·SMT 설정으로 해결 |
| 시스템마다 새로 개발 | 검증된 커넥터 플러그인 재사용 (Debezium, JDBC, S3, Elasticsearch 등) |

반대로 **메시지를 조인·집계·상태 기반으로 가공**해야 하면 Connect 범위를 벗어납니다. 그것은 Kafka Streams나 일반 컨슈머 앱의 일입니다. Connect는 "옮기기 + 메시지 한 건 단위의 가벼운 변형"까지만 맡습니다.

## 2. 구성 요소: 워커, 커넥터, 태스크, 플러그인

| 구성 요소 | 무엇인가 | 실행 단위 |
| --- | --- | --- |
| **Worker(워커)** | Connect 런타임 JVM 프로세스. 커넥터와 태스크를 실행하고 REST API를 제공 | 서버당 보통 1개 |
| **Connector(커넥터)** | "어디서 어디로 옮길지"를 정의한 논리 단위. 설정을 받아 작업을 태스크로 나눔 | 커넥터 인스턴스 1개가 태스크 N개를 만듦 |
| **Task(태스크)** | 실제 데이터를 읽고 쓰는 스레드. 커넥터가 쪼갠 작업 한 조각을 맡음 | 워커들에 분산 배치 |
| **Plugin(플러그인)** | 커넥터·컨버터·SMT 구현 jar 묶음 | `plugin.path` 아래 디렉터리 |

커넥터는 데이터를 직접 옮기지 않습니다. 커넥터는 "테이블 10개를 태스크 2개에 5개씩 나눠라" 같은 **분할 계획**만 세우고, 데이터는 태스크가 옮깁니다. 이 구분이 REST API 상태 확인과 재시작에서 중요합니다. 커넥터는 RUNNING인데 태스크만 FAILED인 상황이 흔하기 때문입니다.

### 소스 커넥터와 싱크 커넥터

```text
[소스] 외부 시스템 ──▶ SourceTask ──▶ Converter(직렬화) ──▶ Kafka 토픽
[싱크] Kafka 토픽 ──▶ Converter(역직렬화) ──▶ SinkTask ──▶ 외부 시스템
```

- **소스(Source)**: 외부에서 읽어 Kafka로 씁니다. 내부적으로 프로듀서입니다. "어디까지 읽었나"(DB의 WAL 위치, 파일 오프셋 등)를 **소스 오프셋**으로 Connect 내부 토픽에 저장합니다.
- **싱크(Sink)**: Kafka에서 읽어 외부로 씁니다. 내부적으로 컨슈머 그룹이며 그룹 이름은 `connect-<커넥터명>`입니다. 오프셋은 일반 컨슈머처럼 `__consumer_offsets`에 저장됩니다.

`tasks.max`는 태스크 수의 **상한**입니다. 싱크는 토픽 파티션 수보다 많은 태스크를 만들어도 남는 태스크가 놀고, 소스는 커넥터 구현이 나눌 수 있는 만큼만 만듭니다. Debezium처럼 DB 로그 하나를 순서대로 읽어야 하는 소스는 태스크가 1개로 고정됩니다.

## 3. Standalone 모드와 Distributed 모드

| | Standalone | Distributed |
| --- | --- | --- |
| 프로세스 | 워커 1개 | 워커 N개가 하나의 그룹 |
| 설정 위치 | 커넥터 설정을 로컬 properties 파일로 기동 시 전달 | REST API로 등록, Kafka 내부 토픽에 저장 |
| 오프셋 저장 | 로컬 파일(`offset.storage.file.filename`) | Kafka 내부 토픽 |
| 장애 복구 | 없음. 프로세스가 죽으면 멈춤 | 다른 워커가 태스크 인계 |
| 용도 | 개발·테스트, 로그 파일 수집 에이전트 | 운영 환경 전부 |

운영은 워커가 1대여도 **분산 모드**로 띄웁니다. 설정과 오프셋이 Kafka에 남아 있어 서버를 바꿔도 이어갈 수 있고, 나중에 워커를 추가하기만 하면 고가용성이 됩니다.

### 분산 모드의 내부 토픽 3개

분산 모드 워커 그룹은 상태를 Kafka 토픽 3개에 저장합니다. 같은 `group.id`를 쓰는 워커는 **반드시 같은 토픽 이름 3개**를 써야 합니다.

| 설정 | 기본 이름 | 저장 내용 | 요구사항 |
| --- | --- | --- | --- |
| `config.storage.topic` | `connect-configs` | 커넥터·태스크 설정 | **파티션 1개**(순서 보장 필수), compact, RF 3 |
| `offset.storage.topic` | `connect-offsets` | 소스 커넥터 오프셋 | 파티션 여러 개(기본 25), compact, RF 3 |
| `status.storage.topic` | `connect-status` | 커넥터·태스크 상태(RUNNING/FAILED 등) | 파티션 여러 개(기본 5), compact, RF 3 |

- 워커가 기동 시 토픽이 없으면 자동 생성합니다. 이때 `config.storage.replication.factor` 같은 설정으로 RF를 지정합니다. 브로커 3대면 3으로 둡니다.
- `config.storage.topic`이 파티션 2개 이상이면 워커가 기동을 거부합니다. 설정 변경 순서가 뒤섞이면 안 되기 때문입니다.
- `cleanup.policy`는 `compact`여야 합니다. `delete`면 보관 기간이 지나 오프셋이 사라지고, 재시작 시 처음부터 다시 읽습니다.

## 4. 워커 설정 (`connect-distributed.properties`)

이 클러스터(SASL_SSL, SCRAM) 기준 핵심 설정입니다. 전체 항목은 공식 문서의 Connect Configs를 참고합니다.

```properties
# 클러스터 접속
bootstrap.servers=kafka1:9094,kafka2:9094,kafka3:9094

# 워커 그룹 — 같은 그룹 = 같은 Connect 클러스터
group.id=connect-cluster-prod

# 내부 토픽 (그룹 내 모든 워커가 동일해야 함)
config.storage.topic=connect-configs
offset.storage.topic=connect-offsets
status.storage.topic=connect-status
config.storage.replication.factor=3
offset.storage.replication.factor=3
status.storage.replication.factor=3

# 기본 컨버터 (커넥터별로 덮어쓸 수 있음)
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://schema-registry-1:8081,http://schema-registry-2:8081

# 플러그인 위치 (워커마다 같은 내용이어야 함)
plugin.path=/opt/connect/plugins

# REST
listeners=http://0.0.0.0:8083
rest.advertised.host.name=connect1.internal     # 다른 워커가 이 워커로 요청을 포워딩할 때 쓰는 주소

# 소스 오프셋 커밋 주기
offset.flush.interval.ms=10000

# 워커 자신의 브로커 접속 보안 (내부 토픽 읽기/쓰기용)
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="connect-worker" password="${CONNECT_WORKER_PASSWORD}";
ssl.truststore.location=/opt/connect/secret/truststore.jks
ssl.truststore.password=${TRUSTSTORE_PASSWORD}

# 커넥터가 내부적으로 쓰는 프로듀서·컨슈머의 보안 (접두사로 별도 지정)
producer.security.protocol=SASL_SSL
producer.sasl.mechanism=SCRAM-SHA-512
producer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="connect-worker" password="${CONNECT_WORKER_PASSWORD}";
producer.ssl.truststore.location=/opt/connect/secret/truststore.jks
producer.ssl.truststore.password=${TRUSTSTORE_PASSWORD}
consumer.security.protocol=SASL_SSL
consumer.sasl.mechanism=SCRAM-SHA-512
consumer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="connect-worker" password="${CONNECT_WORKER_PASSWORD}";
consumer.ssl.truststore.location=/opt/connect/secret/truststore.jks
consumer.ssl.truststore.password=${TRUSTSTORE_PASSWORD}

# 커넥터 설정에서 producer.override.* / consumer.override.* 허용 범위
connector.client.config.override.policy=All
```

설정이 세 벌(워커 자신, `producer.`, `consumer.`)인 이유가 자주 나오는 질문입니다.

- **워커 자신**의 설정은 내부 토픽 3개를 읽고 쓰는 데 씁니다.
- **`producer.` 접두사**는 소스 커넥터가 데이터를 쓸 때 쓰는 프로듀서에 적용됩니다.
- **`consumer.` 접두사**는 싱크 커넥터가 데이터를 읽을 때 쓰는 컨슈머에 적용됩니다.
- 커넥터 설정 안에서 `producer.override.acks=all`처럼 개별 커넥터만 다르게 줄 수 있습니다. 이를 허용하는 범위가 `connector.client.config.override.policy`입니다. `None`이면 전부 거부, `Principal`이면 인증 정보만, `All`이면 전부 허용입니다.

워커 계정(`connect-worker`)에는 내부 토픽 3개에 대한 Create·Read·Write, 그룹 `connect-cluster-prod`에 대한 Read, 그리고 커넥터가 실제로 다룰 토픽 권한이 필요합니다. 싱크 커넥터의 컨슈머 그룹 `connect-*`에 대한 Read도 필요합니다. 계정 발급 절차는 [account-provisioning-runbook](../commands/account-provisioning-runbook.md)을 따릅니다.

## 5. 컨버터 (Converter)

컨버터는 **Kafka의 바이트 배열과 Connect 내부 데이터 구조 사이를 변환**합니다. 커넥터는 메시지가 JSON인지 Avro인지 모릅니다. 커넥터는 Connect의 `Struct`·`Schema` 객체만 다루고, 바이트로 바꾸는 일은 컨버터가 합니다. 그래서 같은 JDBC 싱크 커넥터가 Avro 토픽도 JSON 토픽도 읽을 수 있습니다.

```text
소스: SourceTask → Struct ─[Converter]─▶ byte[] → Kafka
싱크: Kafka → byte[] ─[Converter]─▶ Struct → SinkTask
```

| 컨버터 | 특징 | 언제 쓰나 |
| --- | --- | --- |
| `JsonConverter` | `schemas.enable=true`(기본)면 `{"schema":…, "payload":…}` 봉투 구조로 직렬화. `false`면 payload만 | 스키마 레지스트리 없는 환경. 일반 앱이 보낸 JSON을 읽을 땐 **반드시 `schemas.enable=false`** |
| `AvroConverter` | Schema Registry에 스키마 등록, 메시지엔 스키마 ID만 포함. 크기 작고 호환성 검사 가능 | 운영 표준. Debezium + Schema Registry 조합 |
| `ProtobufConverter`, `JsonSchemaConverter` | Avro와 같은 방식, 형식만 다름 | 앱이 해당 형식을 쓸 때 |
| `StringConverter` | 문자열 그대로 | 키가 단순 문자열일 때 |
| `ByteArrayConverter` | 변환 없음 | 바이트를 그대로 다른 토픽·시스템에 복사할 때 |

워커 설정의 `key.converter`·`value.converter`가 기본값이고, 커넥터 설정에 같은 키를 넣으면 그 커넥터만 덮어씁니다.

### 가장 흔한 컨버터 오류

```text
JsonConverter with schemas.enable requires "schema" and "payload" fields
and may not contain additional fields.
```

Spring Boot 앱이 `JsonSerializer`로 보낸 평범한 JSON을 싱크 커넥터가 `JsonConverter` 기본값으로 읽을 때 납니다. 앱 JSON에는 `schema`·`payload` 봉투가 없기 때문입니다. 해결은 커넥터 설정에 아래를 추가하는 것입니다.

```json
"value.converter": "org.apache.kafka.connect.json.JsonConverter",
"value.converter.schemas.enable": "false"
```

단, 스키마가 없으면 JDBC 싱크처럼 **컬럼 타입이 필요한 커넥터**는 동작하지 않습니다. 그런 경우가 Schema Registry를 도입하는 계기가 됩니다.

## 6. SMT (Single Message Transform)

SMT는 메시지 **한 건**을 커넥터와 컨버터 사이에서 가볍게 바꾸는 변환기입니다. 여러 개를 체인으로 연결하며, `transforms`에 이름을 나열하고 이름별로 클래스와 옵션을 줍니다.

```json
"transforms": "unwrap,route,mask",
"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
"transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.route.regex": "cdc\\.shop\\.public\\.(.*)",
"transforms.route.replacement": "cdc.shop.$1",
"transforms.mask.type": "org.apache.kafka.connect.transforms.MaskField$Value",
"transforms.mask.fields": "card_number"
```

자주 쓰는 내장 SMT입니다.

| SMT | 하는 일 |
| --- | --- |
| `InsertField` | 토픽명·파티션·타임스탬프 같은 메타데이터를 필드로 추가 |
| `ReplaceField` | 필드 이름 변경 또는 제외 |
| `MaskField` | 필드 값을 null·빈 값으로 마스킹 |
| `ExtractField` | 구조체에서 필드 하나만 꺼내 키나 값으로 사용 |
| `RegexRouter` | 정규식으로 대상 토픽 이름 변경 |
| `TimestampConverter` | 타임스탬프 형식 변환(epoch ↔ 문자열) |
| `Filter` + `predicates` | 조건에 맞는 메시지 삭제 |
| `Cast` | 필드 타입 변환 |

한계도 시험에 나옵니다. SMT는 **메시지 한 건만** 봅니다. 두 메시지를 합치거나, 이전 메시지를 기억하거나, 외부 조회가 필요한 변환은 SMT로 할 수 없습니다. 그런 일은 Kafka Streams로 별도 토픽을 만든 뒤 Connect가 그 토픽을 옮기는 구조로 풉니다.

## 7. 에러 처리와 Dead Letter Queue

기본 동작은 **에러 한 건에 태스크가 FAILED로 멈추는 것**입니다. `errors.tolerance` 기본값이 `none`이기 때문입니다. 운영에서는 보통 아래처럼 바꿉니다.

```json
"errors.tolerance": "all",
"errors.log.enable": "true",
"errors.log.include.messages": "true",
"errors.deadletterqueue.topic.name": "dlq.shop.orders-sink",
"errors.deadletterqueue.topic.replication.factor": "3",
"errors.deadletterqueue.context.headers.enable": "true",
"errors.retry.timeout": "60000",
"errors.retry.delay.max.ms": "5000"
```

| 설정 | 의미 |
| --- | --- |
| `errors.tolerance=all` | 변환·컨버터 오류가 난 메시지를 건너뛰고 계속 진행 |
| `errors.log.enable` | 건너뛴 메시지를 워커 로그에 기록 |
| `errors.deadletterqueue.topic.name` | 건너뛴 메시지를 이 토픽으로 보냄. **싱크 커넥터에서만** 동작 |
| `errors.deadletterqueue.context.headers.enable` | 원본 토픽·파티션·오프셋·예외 클래스를 헤더에 기록. 원인 추적에 필수 |
| `errors.retry.timeout` | 일시 오류(네트워크 등)를 몇 ms까지 재시도할지. 기본 0 = 재시도 안 함 |

DLQ가 소스 커넥터에 없는 이유는, 소스 오류는 외부 시스템을 읽는 단계에서 나기 때문입니다. Kafka에 넣을 메시지 자체가 만들어지지 않아 보낼 것이 없습니다. 소스는 `errors.tolerance`로 컨버터 단계 오류를 건너뛰거나 로그만 남길 수 있습니다.

에러 처리 범위도 정확히 알아야 합니다. 이 설정은 **컨버터와 SMT 단계**의 오류에 적용됩니다. 싱크 커넥터가 외부 시스템에 쓰다 실패한 경우(DB 제약 위반 등)는 커넥터 구현이 자체적으로 처리하며, 커넥터마다 별도 설정(`behavior.on.error` 등)이 있습니다.

## 8. REST API

모든 워커가 8083에서 같은 API를 제공합니다. 어느 워커에 보내도 됩니다. 설정 변경 요청은 내부적으로 **리더 워커**에게 포워딩되며, 이때 `rest.advertised.host.name`을 씁니다. 이 값이 잘못되면 "포워딩 실패" 오류가 납니다.

| 메서드·경로 | 하는 일 |
| --- | --- |
| `GET /connectors` | 커넥터 이름 목록. `?expand=status,info`로 상태·설정 한 번에 |
| `POST /connectors` | 커넥터 생성. 본문 `{"name":…, "config":{…}}` |
| `PUT /connectors/{name}/config` | 설정 생성 또는 수정. 없으면 만들고 있으면 바꿈. **멱등**이라 자동화에 적합 |
| `GET /connectors/{name}/status` | 커넥터 상태 + 태스크별 상태·워커·에러 스택 |
| `GET /connectors/{name}/tasks` | 태스크 목록과 각 태스크 설정 |
| `PUT /connectors/{name}/pause` | 일시 정지. 태스크는 살아 있고 데이터만 멈춤 |
| `PUT /connectors/{name}/resume` | 재개 |
| `PUT /connectors/{name}/stop` | 정지. 태스크를 모두 내림(3.5+). 오프셋 변경 전에 필요 |
| `POST /connectors/{name}/restart` | 커넥터 재시작. `?includeTasks=true&onlyFailed=true`로 실패한 태스크만 함께 |
| `POST /connectors/{name}/tasks/{id}/restart` | 태스크 하나만 재시작 |
| `DELETE /connectors/{name}` | 삭제. 설정은 지워지지만 오프셋은 남음 |
| `GET /connectors/{name}/offsets` | 오프셋 조회(3.5+) |
| `PATCH` / `DELETE /connectors/{name}/offsets` | 오프셋 변경 / 초기화(3.6+). STOPPED 상태에서만 |
| `GET /connector-plugins` | 설치된 플러그인 목록. 클래스 이름 확인용 |
| `PUT /connector-plugins/{class}/config/validate` | 설정 검증. 등록 전에 오타·필수값 누락 확인 |

자주 쓰는 순서입니다.

```bash
CONNECT=http://connect1.internal:8083

# 플러그인이 실제로 로드됐는지 (class 이름 복사해서 쓰기)
curl -s $CONNECT/connector-plugins | jq '.[].class'

# 등록 전 검증
curl -s -X PUT $CONNECT/connector-plugins/io.debezium.connector.postgresql.PostgresConnector/config/validate \
  -H 'Content-Type: application/json' -d @orders-cdc-config.json | jq '.error_count'

# 등록 (PUT이라 같은 파일로 수정도 됨)
curl -s -X PUT $CONNECT/connectors/orders-cdc/config \
  -H 'Content-Type: application/json' -d @orders-cdc-config.json

# 상태
curl -s $CONNECT/connectors/orders-cdc/status | jq

# 실패한 태스크만 재시작
curl -s -X POST "$CONNECT/connectors/orders-cdc/restart?includeTasks=true&onlyFailed=true"
```

`GET .../config`로 설정을 읽으면 비밀번호가 평문으로 보입니다. 그래서 8083은 관리자 IP로만 열고, 비밀값은 **Config Provider**로 파일이나 환경변수에서 읽게 합니다.

```properties
# 워커 설정
config.providers=file
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
```

```json
"database.password": "${file:/opt/connect/secret/debezium.properties:db.password}"
```

이렇게 하면 REST로 조회해도 `${file:…}` 문자열만 보이고, 실제 값은 워커가 실행 시점에 파일에서 읽습니다.

## 9. 태스크 분배, 리밸런싱, 장애 복구

### 워커 그룹과 리더

분산 모드 워커들은 컨슈머 그룹과 같은 **그룹 프로토콜**로 서로를 발견합니다. 그중 한 워커가 리더가 되어 "어느 커넥터·태스크를 어느 워커가 맡을지" 배정합니다. 리더는 별도 설정이 아니라 그룹 조인 과정에서 자동으로 정해집니다.

### 점진적 협력 리밸런싱 (Incremental Cooperative Rebalancing)

Kafka 2.3 이전에는 워커 하나가 들어오거나 나가면 **모든 태스크가 멈추고 전부 재배정**됐습니다(eager). 2.3부터는 영향받는 태스크만 옮깁니다.

- 워커가 추가되면: 기존 태스크 일부만 새 워커로 이동. 나머지는 계속 실행.
- 워커가 사라지면: 그 워커의 태스크는 바로 재배정하지 않고 **`scheduled.rebalance.max.delay.ms`(기본 5분) 동안 기다립니다**. 워커가 재시작이나 일시적 네트워크 단절로 곧 돌아올 가능성이 크기 때문입니다. 돌아오면 원래 태스크를 그대로 받고, 5분이 지나면 다른 워커에 배정합니다.

이 5분이 운영에서 중요합니다. 워커를 **배포 목적으로 재시작**할 때는 5분 안에 돌아오므로 태스크 이동이 없습니다. 반대로 워커 서버가 실제로 죽으면 **5분 동안 그 워커의 커넥터는 데이터를 옮기지 않습니다**. lag 알람 기준을 잡을 때 이 지연을 감안해야 합니다. 더 빨리 넘기고 싶으면 이 값을 줄이되, 배포 시마다 태스크가 왕복하는 비용과 맞바꿉니다.

### 상태 모델

| 상태 | 커넥터 | 태스크 | 의미 |
| --- | --- | --- | --- |
| `RUNNING` | ○ | ○ | 정상 |
| `PAUSED` | ○ | ○ | 사용자가 일시 정지. 태스크는 할당된 채 대기 |
| `STOPPED` | ○ | - | 사용자가 정지. 태스크 할당 해제(3.5+) |
| `FAILED` | ○ | ○ | 예외로 중단. **자동 복구되지 않음** |
| `UNASSIGNED` | ○ | ○ | 어느 워커에도 배정되지 않음. 리밸런싱 중이거나 워커 부족 |
| `RESTARTING` | ○ | ○ | 재시작 요청 처리 중 |

`FAILED`는 Connect가 스스로 되살리지 않습니다. 원인을 고친 뒤 restart API를 호출해야 합니다. 그래서 모니터링에서 `connect_connector_task_status{status="failed"}`류 지표나 `GET /status` 폴링이 필요합니다. 자주 있는 원인은 대상 DB 접속 끊김, 스키마 변경으로 컨버터 실패, 외부 시스템 제약 위반입니다.

### 오프셋과 전달 보장

- **소스 커넥터**: 태스크가 외부 시스템의 위치(WAL LSN, 파일 오프셋 등)를 `offset.storage.topic`에 `offset.flush.interval.ms` 주기로 커밋합니다. 태스크가 죽고 다른 워커에서 살아나면 마지막 커밋 지점부터 다시 읽습니다. 커밋 직전에 죽으면 그 구간은 **중복 전송**됩니다. 기본은 at-least-once입니다.
- Kafka 3.3부터 소스 커넥터도 `exactly.once.source.support=enabled`(워커)와 `exactly.once.support=required`(커넥터)로 정확히 한 번을 지원합니다. 커넥터 구현이 이를 지원해야 하며, 내부적으로 트랜잭션 프로듀서를 씁니다.
- **싱크 커넥터**: 일반 컨슈머이므로 `__consumer_offsets`에 커밋합니다. 외부 시스템에 쓴 뒤 커밋하므로 역시 at-least-once입니다. 외부 시스템이 멱등 쓰기(upsert)를 지원하면 중복이 결과에 영향을 주지 않습니다. JDBC 싱크의 `insert.mode=upsert`가 그 예입니다.

### 오프셋 초기화

"처음부터 다시 옮기고 싶다"는 요구가 종종 있습니다. 방법이 소스와 싱크에서 다릅니다.

- **싱크**: 컨슈머 그룹 `connect-<커넥터명>`의 오프셋을 `kafka-consumer-groups.sh --reset-offsets`로 조정합니다. 커넥터를 먼저 STOPPED 또는 삭제해야 합니다. 그룹이 활성 상태면 리셋이 거부됩니다.
- **소스**: 3.6 이상이면 `PUT .../stop` 후 `DELETE /connectors/{name}/offsets`로 초기화합니다. 그 이전 버전은 `offset.storage.topic`에 해당 커넥터 키로 null 값(tombstone)을 직접 써야 했습니다. 커넥터 이름을 바꿔 새로 등록하는 우회도 흔합니다. 오프셋 키에 커넥터 이름이 포함되기 때문입니다.

## 10. 이 클러스터에서의 운영 체크리스트

배치 방안([15](15-deployment-layout-options.md))대로 워커 2대를 서로 다른 서버에 둘 때 확인할 항목입니다.

- `group.id`와 내부 토픽 3개 이름이 두 워커에서 **정확히 같은가**. `group.id`만 같고 토픽 이름이 다르면 두 워커가 서로 다른 설정을 보면서 같은 그룹에 있는 분열 상태가 됩니다.
- `plugin.path` 아래 플러그인 jar 버전이 두 워커에서 같은가. 다르면 태스크가 어느 워커에 배정되느냐에 따라 동작이 달라집니다.
- `rest.advertised.host.name`이 **다른 워커에서 접근 가능한 주소**인가. 컨테이너면 컨테이너 이름이 아니라 호스트 주소여야 합니다.
- 워커 계정에 내부 토픽 Create 권한이 있는가. 없으면 토픽을 미리 만들어 둡니다. 이때 `connect-configs`는 파티션 1, 셋 모두 `cleanup.policy=compact`, RF 3으로 만듭니다.
- 8083이 관리자 대역에만 열려 있는가. 비밀값은 Config Provider로 분리했는가.
- 워커 힙은 `KAFKA_HEAP_OPTS`로 지정합니다. 커넥터 수와 배치 크기에 따라 1~2GB에서 시작합니다.
- Prometheus 수집: 워커에 JMX Exporter 에이전트를 붙이면 `kafka_connect_connector_task_status`, `kafka_connect_source_task_source_record_poll_total` 같은 지표가 나옵니다. [14](14-monitoring-prometheus-grafana.md)의 JMX 경로에 `connect:7072` 타깃을 추가합니다.

## 11. 시험 포인트 요약

- 커넥터는 분할 계획, 태스크가 실제 이동. `tasks.max`는 상한.
- 분산 모드 내부 토픽 3개. `config.storage.topic`은 **파티션 1개**, 셋 모두 compact.
- 같은 `group.id` + 같은 내부 토픽 3개 = 같은 Connect 클러스터.
- 컨버터는 바이트 ↔ Connect 데이터 변환. 커넥터는 형식을 모름. `JsonConverter` 기본은 `schemas.enable=true`.
- SMT는 메시지 한 건 단위. 조인·집계 불가.
- `errors.tolerance` 기본 `none`. DLQ는 **싱크 전용**.
- REST 요청은 어느 워커든 받고 리더로 포워딩. `PUT .../config`는 멱등.
- 워커 손실 시 `scheduled.rebalance.max.delay.ms`(5분) 후 재배정.
- `FAILED`는 자동 복구 없음. restart API 필요.
- 소스 오프셋은 `offset.storage.topic`, 싱크 오프셋은 `__consumer_offsets`. 기본 at-least-once.
- `connector.client.config.override.policy`가 커넥터별 `producer.override.*` 허용 범위를 정함.

## 관련 문서

- Debezium과 Schema Registry가 푸는 문제: [09-concepts-qna](09-concepts-qna.md) 10장
- 워커 배치와 메모리: [15-deployment-layout-options](15-deployment-layout-options.md)
- 워커 계정·ACL 발급: [account-provisioning-runbook](../commands/account-provisioning-runbook.md)
- 모니터링 연결: [14-monitoring-prometheus-grafana](14-monitoring-prometheus-grafana.md)

## 참고한 공식 문서

- Apache Kafka Documentation — Kafka Connect (User Guide, Configs, REST API)
- KIP-415: Incremental Cooperative Rebalancing in Kafka Connect
- KIP-298: Error Handling in Connect (errors.tolerance, dead letter queue)
- KIP-618: Exactly-Once Support for Source Connectors
- KIP-875: First-class offsets support in Kafka Connect (stop, offsets API)
- Debezium Documentation — Deployment, Transformations
