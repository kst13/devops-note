# 구축 순서: 로컬 PoC부터 실서버까지

> 저장(MinIO) → 카탈로그 → 조회(Trino) → 첫 테이블 검증 → 적재(Kafka Connect) → 초기 적재 → 정합성 검증 → 운영 작업 → 조회 제공 순서로 올립니다. **한 단계를 검증하고 다음 단계로** 갑니다. 어느 단계에서 막혔는지 알 수 있어야 고칠 수 있습니다.

로컬에서는 [로컬 PoC](../examples/local-poc/README.md)의 `setup.sh`가 1~6단계를 자동으로 수행합니다. 이 문서는 그 스크립트가 하는 일을 실서버 기준으로 풀어 쓴 것입니다. 각 단계 끝에 "PoC에서는"으로 대응 관계를 적었습니다. 서버 배치와 스펙은 [MinIO: 06 레이크하우스 서버 배치](../../minio/concepts/06-lakehouse-deployment-layout.md)를 따릅니다.

## 0. 전체 순서 한눈에

| 단계 | 하는 일 | 완료 기준 | 서버 |
| --- | --- | --- | --- |
| 1 | 준비: 요구사항·버전·포트·계정·인증서 | 결정 항목 표가 채워짐 | — |
| 2 | MinIO 구축 | `mc ls` 로 `lakehouse` 버킷에 전용 계정으로 읽기·쓰기 | ① |
| 3 | 카탈로그 구축 | Postgres에 `iceberg` DB와 계정, 백업 잡 동작 | ③ |
| 4 | Trino 구축 | `SELECT 1`, `SHOW CATALOGS`에 `lakehouse` | ② |
| 5 | 첫 테이블 검증 | INSERT → 스냅샷 생성 → MinIO 파일 생성 → 시점 조회 | ② |
| 6 | 적재 경로(Kafka Connect + Iceberg Sink) | 토픽에 넣은 이벤트가 커밋 주기 안에 Trino에서 보임 | Kafka 생태계 서버 |
| 7 | 초기 적재(기존 DB) | 건수·금액 합계가 원본과 일치 | ② |
| 8 | 정합성·장애 검증 | 강제 종료 후 재시작에 누락·중복 0, 반영 지연 측정 | — |
| 9 | 운영 작업 등록 | 병합·스냅샷 만료·고아 파일 정리·백업·알람이 주기 실행 | ②③ |
| 10 | 조회 제공 | 권한·마스킹 뷰·BI 연결, 대표 쿼리 응답 시간 충족 | ② |
| 11 | 콜드 데이터 이관·도메인 확장 | [05](05-cold-data-migration.md)·[08](08-rollout-plan.md) 기준 | — |

```text
서버① MinIO :9000/:9001      서버② Trino :8080            서버③ 카탈로그 Postgres :5432
  lakehouse/ 버킷  ◀── 파일 ──  coordinator+worker  ── 포인터 ──▶  iceberg DB
        ▲                            ▲
        │ 파일 쓰기                    │ 테이블 커밋 (카탈로그 경유)
  Kafka 생태계 서버: Kafka Connect :8083 + Iceberg Sink  ◀── Kafka 브로커 3대
```

## 1. 준비

### 1-1. 결정 항목

[08 단계별 구축과 PoC 계획](08-rollout-plan.md) 6장의 표를 채웁니다. 최소한 다음은 구축 전에 정합니다.

| 항목 | 이 문서의 기본값 |
| --- | --- |
| 첫 테이블 | `commerce.order_events` (이벤트 이력, append만) |
| 카탈로그 | 1단계 Postgres JDBC → 필요 시 REST 승격 |
| Writer | Kafka Connect Iceberg Sink (Confluent Hub `iceberg/iceberg-kafka-connect`) |
| 이벤트 형식 | JSON(스키마 없음)으로 시작, Avro + Schema Registry는 다음 단계 |
| 커밋 주기 | 1분 (반영 지연 목표 5분 안) |
| 파티션 | `days(occurred_at)` |

### 1-2. 버전 고정

| 구성요소 | 버전 (2026-09 기준) | 근거 |
| --- | --- | --- |
| MinIO | 최신 RELEASE | [MinIO 03 구축 구성안](../../minio/concepts/03-setup-plan.md) |
| Postgres | 16 | JDBC 카탈로그는 저부하, LTS면 충분 |
| Trino | 483 (Java 25 내장 Docker 이미지) | Java 25 필수. 호스트 JDK와 분리하려면 Docker |
| Kafka Connect | Confluent 7.9.x | [scenario-runner 제약](../../kafka/examples/scenario-runner/README.md)과 같은 라인 |
| Iceberg Sink | 1.9.2 | Confluent Hub 최신 |

### 1-3. 네트워크·포트

| 출발 → 도착 | 포트 | 용도 |
| --- | --- | --- |
| 사용자·BI → Trino | 8080 (TLS 시 8443) | SQL |
| Trino → MinIO | 9000 | 파일 읽기 |
| Trino → Postgres | 5432 | 카탈로그 |
| Connect → MinIO | 9000 | 파일 쓰기 |
| Connect → Postgres 또는 REST 카탈로그 | 5432 / 8181 | 테이블 커밋 |
| Connect → Kafka | 9094 (SASL_SSL) | 이벤트 읽기, 제어 토픽 |
| 운영자 → MinIO 콘솔 | 9001 | 파일 확인 |

MinIO·Postgres 포트는 서버②와 Connect 서버 대역에만 엽니다.

### 1-4. 계정과 비밀

| 계정 | 어디에 | 권한 |
| --- | --- | --- |
| MinIO `lakehouse` | 서버① | `lakehouse` 버킷 한정 읽기·쓰기 정책 |
| Postgres `iceberg` | 서버③ | `iceberg` DB 소유 |
| Kafka `connect` (SCRAM) | 브로커 | Connect 내부 토픽 3개, 데이터 토픽 읽기, 제어 토픽 읽기·쓰기, 그룹·트랜잭션 ID |
| Trino 사용자 | 서버② | 조회 권한은 10단계에서 |

비밀은 `.env`·Secret으로 주입하고 저장소에 커밋하지 않습니다. 인증서는 Kafka 때 만든 사내 CA를 재사용합니다.

PoC에서는: `setup.sh` 1단계가 `~/lakehouse-poc/.env`에 MinIO 루트·`lakehouse`·`connect` 비밀번호를 생성합니다.

## 2. MinIO 구축 (서버①)

1. [MinIO 03 구축 구성안](../../minio/concepts/03-setup-plan.md)대로 MinIO를 올립니다. 파일 저장소와 겸용이면 이미 있는 인스턴스를 씁니다.
2. 버킷을 만듭니다. 파일용과 분리합니다.

```bash
mc alias set our-minio https://서버①:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb --ignore-existing our-minio/lakehouse
mc version enable our-minio/lakehouse        # 실수 삭제 대비. 스냅샷 만료와 별개
```

3. 전용 계정과 버킷 한정 정책을 만듭니다. 루트 계정을 Trino·Connect에 주지 않습니다.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"], "Resource": ["arn:aws:s3:::lakehouse"] },
    { "Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"], "Resource": ["arn:aws:s3:::lakehouse/*"] }
  ]
}
```

```bash
mc admin user add our-minio lakehouse "$LAKEHOUSE_SECRET_KEY"
mc admin policy create our-minio lakehouse-rw lakehouse-policy.json
mc admin policy attach our-minio lakehouse-rw --user lakehouse
```

4. 검증: 전용 계정으로 쓰고 읽고 지웁니다. 다른 버킷은 거부돼야 합니다.

```bash
mc alias set lh https://서버①:9000 lakehouse "$LAKEHOUSE_SECRET_KEY"
echo ok | mc pipe lh/lakehouse/_smoke.txt && mc cat lh/lakehouse/_smoke.txt && mc rm lh/lakehouse/_smoke.txt
mc ls lh/files    # Access Denied 가 정상
```

PoC에서는: compose의 `minio-init`이 버킷·계정·정책을 만듭니다.

## 3. 카탈로그 구축 (서버③)

카탈로그는 "테이블 이름 → 현재 metadata.json 위치"를 원자적으로 바꿔 주는 곳입니다([02 구성요소](02-components.md) 7장). 1단계는 Postgres JDBC 카탈로그입니다.

1. Postgres 16을 설치하고 DB·계정을 만듭니다.

```sql
CREATE USER iceberg WITH PASSWORD '${ICEBERG_CATALOG_PW}';
CREATE DATABASE iceberg OWNER iceberg;
```

카탈로그 테이블(`iceberg_tables`, `iceberg_namespace_properties`)은 Trino가 처음 접속할 때 자동으로 만듭니다. 미리 만들 필요가 없습니다.

2. 백업을 **지금** 등록합니다. 카탈로그를 잃으면 MinIO의 파일이 온전해도 테이블을 못 찾습니다.

```bash
# 매일 03:00, 7일 보관. MinIO 의 별도 버킷(backup)에 올린다
0 3 * * * pg_dump -U iceberg -Fc iceberg > /backup/iceberg-$(date +\%F).dump && mc cp /backup/iceberg-$(date +\%F).dump our-minio/backup/catalog/ && find /backup -name 'iceberg-*.dump' -mtime +7 -delete
```

3. 검증: 서버②에서 접속되는지 확인합니다.

```bash
psql "host=서버③ dbname=iceberg user=iceberg" -c 'select 1'
```

REST 카탈로그로 승격할 때는 REST 서버(예: Nessie, Polaris)를 서버③에 추가로 올리고 Trino·Connect 설정의 카탈로그 부분만 바꿉니다. 데이터와 MinIO는 그대로입니다.

PoC에서는: Postgres 대신 `apache/iceberg-rest-fixture`(REST + sqlite)를 씁니다. 운영 카탈로그가 아니라 검증용입니다.

## 4. Trino 구축 (서버②)

1. Docker로 올립니다. Java 25가 필요해서 호스트 JDK와 분리하는 편이 안전합니다.

```yaml
# /opt/trino/docker-compose.yml
services:
  trino:
    image: trinodb/trino:483
    container_name: trino
    restart: unless-stopped
    ports: ["8080:8080"]
    volumes:
      - ./etc/config.properties:/etc/trino/config.properties:ro
      - ./etc/jvm.config:/etc/trino/jvm.config:ro
      - ./etc/catalog:/etc/trino/catalog:ro
    env_file: .env          # ICEBERG_CATALOG_PW, LAKEHOUSE_ACCESS_KEY, LAKEHOUSE_SECRET_KEY
```

```properties
# etc/config.properties — 1단계는 coordinator 가 worker 를 겸한다
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
query.max-memory=8GB
query.max-memory-per-node=6GB
```

```text
# etc/jvm.config — 힙은 노드 RAM 의 70~85%. 16GB 노드 예시
-server
-Xmx12G
-XX:+UseG1GC
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+HeapDumpOnOutOfMemoryError
-Djdk.attach.allowAttachSelf=true
```

2. 카탈로그 파일을 놓습니다. 파일명이 SQL의 카탈로그 이름입니다.

```properties
# etc/catalog/lakehouse.properties
connector.name=iceberg
iceberg.catalog.type=jdbc
iceberg.jdbc-catalog.catalog-name=lakehouse
iceberg.jdbc-catalog.driver-class=org.postgresql.Driver
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://서버③:5432/iceberg
iceberg.jdbc-catalog.connection-user=iceberg
iceberg.jdbc-catalog.connection-password=${ENV:ICEBERG_CATALOG_PW}
iceberg.jdbc-catalog.default-warehouse-dir=s3://lakehouse/warehouse

fs.native-s3.enabled=true
s3.endpoint=https://서버①:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=${ENV:LAKEHOUSE_ACCESS_KEY}
s3.aws-secret-key=${ENV:LAKEHOUSE_SECRET_KEY}
```

REST 카탈로그면 `iceberg.catalog.type=rest`, `iceberg.rest-catalog.uri=http://서버③:8181`, `iceberg.rest-catalog.warehouse=s3://lakehouse/warehouse`로 바뀝니다. Postgres JDBC 드라이버는 Trino에 내장돼 있습니다.

3. 기동하고 검증합니다.

```bash
docker compose up -d
curl -s http://localhost:8080/v1/info      # "starting":false 가 될 때까지
docker compose exec -it trino trino
```

```sql
SELECT 1;
SHOW CATALOGS;                      -- lakehouse 가 보여야 함
SHOW SCHEMAS FROM lakehouse;        -- 비어 있어도 오류 없이 돌아오면 카탈로그·S3 접속 성공
```

PoC에서는: `setup.sh` 2단계. 카탈로그는 REST, 자격증명은 `${ENV:...}`로 주입합니다.

## 5. 첫 테이블 검증

Writer를 붙이기 전에 Trino만으로 테이블 생명주기를 확인합니다. 여기서 실패하면 원인은 카탈로그나 S3 설정입니다.

```sql
CREATE SCHEMA IF NOT EXISTS lakehouse.commerce;

CREATE TABLE lakehouse.commerce.order_events (
  event_id       varchar,
  event_type     varchar,
  order_id       varchar,
  customer_id    varchar,
  amount         bigint,
  occurred_at    timestamp(6) with time zone,
  ingested_at    timestamp(6) with time zone,
  schema_version integer
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['day(occurred_at)'],
  format_version = 2
);

INSERT INTO lakehouse.commerce.order_events VALUES
  ('evt-0001', 'ORDER_CREATED', 'order-2001', 'customer-3001', 35000, TIMESTAMP '2026-09-13 10:00:00 +09:00', current_timestamp, 1),
  ('evt-0002', 'PAYMENT_COMPLETED', 'order-2001', 'customer-3001', 35000, TIMESTAMP '2026-09-13 10:00:05 +09:00', current_timestamp, 1);

SELECT count(*) FROM lakehouse.commerce.order_events;
SELECT snapshot_id, committed_at, operation FROM lakehouse.commerce."order_events$snapshots";
SELECT file_path, record_count FROM lakehouse.commerce."order_events$files";
```

확인할 것:

- 스냅샷이 INSERT마다 하나씩 생깁니다.
- MinIO 콘솔에서 `lakehouse/warehouse/commerce/order_events-<uuid>/data/`와 `metadata/`에 파일이 보입니다.
- `SELECT ... FOR VERSION AS OF <첫 snapshot_id>`로 과거 시점이 조회됩니다.
- Postgres `iceberg_tables`에 행이 하나 생기고 `metadata_location`이 MinIO 경로를 가리킵니다.

테이블은 여기서 DDL로 만듭니다. Connect의 `auto-create`는 PoC 편의 기능이고, 운영에서는 파티션·타입을 DDL로 통제합니다. 대표 쿼리 3개(예: 고객별 월 합계, 기간별 결제 성공률, 주문·결제 불일치)도 이 시점에 SQL로 고정해 두고 10단계에서 응답 시간을 잽니다.

PoC에서는: `setup.sh` 3단계가 `commerce.poc_smoke`로 같은 검증을 합니다.

## 6. 적재 경로 구축 (Kafka Connect + Iceberg Sink)

Kafka 생태계 서버(Schema Registry가 있는 곳)에 Connect 워커를 올립니다.

### 6-1. 브로커 쪽: 계정·토픽·ACL

브로커는 `auto.create.topics.enable=false`이므로 토픽을 미리 만듭니다. 명령은 브로커 서버에서 admin 계정으로 실행합니다.

```bash
A="--bootstrap-server kafka1:9094 --command-config /etc/kafka/secrets/admin.properties"

# 계정
kafka-configs.sh $A --alter --add-config "SCRAM-SHA-512=[password=$CONNECT_PASSWORD]" --entity-type users --entity-name connect

# Connect 내부 토픽 (compact 필수), 데이터 토픽, Iceberg 제어 토픽
for t in lakehouse-connect-configs lakehouse-connect-offsets lakehouse-connect-status; do
  kafka-topics.sh $A --create --if-not-exists --topic $t --partitions 1 --replication-factor 3 --config cleanup.policy=compact
done
kafka-topics.sh $A --create --if-not-exists --topic lakehouse.order.events --partitions 6 --replication-factor 3 --config min.insync.replicas=2
kafka-topics.sh $A --create --if-not-exists --topic lakehouse-control-iceberg --partitions 1 --replication-factor 3 --config min.insync.replicas=2

# ACL
ACL="kafka-acls.sh $A --add --allow-principal User:connect"
for t in lakehouse-connect-configs lakehouse-connect-offsets lakehouse-connect-status; do
  $ACL --operation Read --operation Write --operation Describe --operation DescribeConfigs --topic $t
done
$ACL --operation Read --group lakehouse-connect                                            # 워커 그룹
$ACL --operation Read --operation Describe --topic lakehouse.order.events                   # 데이터 토픽
$ACL --operation Read --operation Write --operation Describe --topic lakehouse-control-iceberg
$ACL --operation Read --group connect-order-events-sink --resource-pattern-type prefixed   # 커넥터 컨슈머 + -coord
$ACL --operation Read --group cg-control- --resource-pattern-type prefixed                 # 워커 제어 컨슈머 (UUID 접미사)
$ACL --operation Write --operation Describe --transactional-id '*'                         # 제어 토픽 exactly-once
$ACL --operation IdempotentWrite --cluster
```

제어 토픽 컨슈머 그룹이 `cg-control-<매번 다른 UUID>`라서 접두사 ACL이 필요합니다. 이름을 고정해 주면 태스크가 `GroupAuthorizationException`으로 죽습니다(PoC 실측).

### 6-2. Connect 워커

```yaml
# /opt/kafka-ecosystem/connect/docker-compose.yml
services:
  connect:
    image: confluentinc/cp-kafka-connect:7.9.1
    restart: unless-stopped
    ports: ["8083:8083"]
    volumes:
      - /etc/kafka/secrets:/etc/kafka/secrets:ro
      - connect-plugins:/usr/share/confluent-hub-components
    command:
      - bash
      - -c
      - |
        [ -d /usr/share/confluent-hub-components/iceberg-iceberg-kafka-connect ] || confluent-hub install --no-prompt iceberg/iceberg-kafka-connect:1.9.2
        exec /etc/confluent/docker/run
    environment:
      CONNECT_BOOTSTRAP_SERVERS: kafka1:9094,kafka2:9094,kafka3:9094
      CONNECT_REST_PORT: 8083
      CONNECT_REST_ADVERTISED_HOST_NAME: connect-host
      CONNECT_GROUP_ID: lakehouse-connect
      CONNECT_CONFIG_STORAGE_TOPIC: lakehouse-connect-configs
      CONNECT_OFFSET_STORAGE_TOPIC: lakehouse-connect-offsets
      CONNECT_STATUS_STORAGE_TOPIC: lakehouse-connect-status
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 3
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 3
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 3
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
      CONNECT_PLUGIN_PATH: /usr/share/java,/usr/share/confluent-hub-components
      # Iceberg Sink 워커는 put() 때만 제어 토픽을 폴링한다. 유휴 시에도 제어 이벤트를 처리하도록 짧게
      CONNECT_OFFSET_FLUSH_INTERVAL_MS: 1000
      # 워커·producer·consumer 모두 connect 계정 SASL_SSL (세 접두사에 같은 값)
      CONNECT_SECURITY_PROTOCOL: SASL_SSL
      CONNECT_SASL_MECHANISM: SCRAM-SHA-512
      CONNECT_SASL_JAAS_CONFIG: org.apache.kafka.common.security.scram.ScramLoginModule required username="connect" password="${CONNECT_PASSWORD}";
      CONNECT_SSL_TRUSTSTORE_LOCATION: /etc/kafka/secrets/ca.crt
      CONNECT_SSL_TRUSTSTORE_TYPE: PEM
      CONNECT_PRODUCER_SECURITY_PROTOCOL: SASL_SSL
      CONNECT_PRODUCER_SASL_MECHANISM: SCRAM-SHA-512
      CONNECT_PRODUCER_SASL_JAAS_CONFIG: org.apache.kafka.common.security.scram.ScramLoginModule required username="connect" password="${CONNECT_PASSWORD}";
      CONNECT_PRODUCER_SSL_TRUSTSTORE_LOCATION: /etc/kafka/secrets/ca.crt
      CONNECT_PRODUCER_SSL_TRUSTSTORE_TYPE: PEM
      CONNECT_CONSUMER_SECURITY_PROTOCOL: SASL_SSL
      CONNECT_CONSUMER_SASL_MECHANISM: SCRAM-SHA-512
      CONNECT_CONSUMER_SASL_JAAS_CONFIG: org.apache.kafka.common.security.scram.ScramLoginModule required username="connect" password="${CONNECT_PASSWORD}";
      CONNECT_CONSUMER_SSL_TRUSTSTORE_LOCATION: /etc/kafka/secrets/ca.crt
      CONNECT_CONSUMER_SSL_TRUSTSTORE_TYPE: PEM
volumes:
  connect-plugins:
```

`offset.flush.interval.ms`를 줄이는 이유: Iceberg Sink 워커는 Connect가 `put()`을 부를 때만 제어 토픽을 폴링합니다. 데이터가 뜸하면 `put()`이 이 간격마다만 불려 제어 컨슈머가 그룹에 못 붙고, 코디네이터는 "Commit timeout reached, committed to 0 table(s)"만 반복합니다(PoC 실측).

카탈로그가 Postgres JDBC면 커넥터 배포판에 JDBC 드라이버가 없으므로 `postgresql-<ver>.jar`를 플러그인 디렉터리에 추가합니다. REST 카탈로그는 추가 드라이버가 없습니다. **이 차이 때문에 Connect를 붙일 시점에 REST 카탈로그 승격을 함께 검토합니다.**

기동 후 확인:

```bash
docker compose up -d
curl -s http://localhost:8083/connector-plugins | grep -o 'IcebergSinkConnector'
```

### 6-3. 커넥터 등록

```json
{
  "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
  "tasks.max": "2",
  "topics": "lakehouse.order.events",
  "iceberg.tables": "commerce.order_events",
  "iceberg.tables.auto-create-enabled": "false",
  "iceberg.tables.evolve-schema-enabled": "false",
  "iceberg.tables.schema-case-insensitive": "true",

  "iceberg.catalog.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
  "iceberg.catalog.uri": "jdbc:postgresql://서버③:5432/iceberg",
  "iceberg.catalog.jdbc.user": "iceberg",
  "iceberg.catalog.jdbc.password": "${ICEBERG_CATALOG_PW}",
  "iceberg.catalog.warehouse": "s3://lakehouse/warehouse",
  "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
  "iceberg.catalog.s3.endpoint": "https://서버①:9000",
  "iceberg.catalog.s3.path-style-access": "true",
  "iceberg.catalog.s3.access-key-id": "${LAKEHOUSE_ACCESS_KEY}",
  "iceberg.catalog.s3.secret-access-key": "${LAKEHOUSE_SECRET_KEY}",
  "iceberg.catalog.client.region": "us-east-1",

  "iceberg.control.topic": "lakehouse-control-iceberg",
  "iceberg.control.commit.interval-ms": "60000",

  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false"
}
```

- 이벤트 필드명이 `eventId`처럼 카멜케이스면 테이블 컬럼 `event_id`와 맞지 않습니다. 발행 규약을 스네이크케이스로 정하거나, Connect의 `ReplaceField` 변환으로 이름을 바꿉니다. `schema-case-insensitive`는 대소문자만 무시합니다.
- Sink의 카탈로그 유형은 REST·Hive·Hadoop만 `iceberg.catalog.type`으로 지정합니다. JDBC는 `iceberg.catalog.catalog-impl`에 클래스명을 줍니다. REST면 `"iceberg.catalog.type": "rest"`, `"iceberg.catalog.uri": "http://서버③:8181"`로 바뀝니다.
- 비밀은 Connect의 `config.providers`(파일·환경변수)로 주입합니다. JSON에 평문으로 두지 않습니다.

```bash
curl -s -X PUT -H 'Content-Type: application/json' --data @order-events-sink.json \
  http://localhost:8083/connectors/order-events-sink/config
curl -s http://localhost:8083/connectors/order-events-sink/status     # connector·task 모두 RUNNING
```

### 6-4. 검증

```bash
# 이벤트 1건 발행 (발행 계정으로)
echo '{"event_id":"evt-1001","event_type":"ORDER_CREATED","order_id":"order-2001","customer_id":"customer-3001","amount":35000,"occurred_at":"2026-09-13T10:00:00+09:00","schema_version":1}' \
  | kafka-console-producer.sh --bootstrap-server kafka1:9094 --producer.config /etc/kafka/secrets/app.properties --topic lakehouse.order.events
```

```sql
-- 커밋 주기(1분) 뒤
SELECT * FROM lakehouse.commerce.order_events WHERE event_id = 'evt-1001';
SELECT committed_at, summary['kafka.connect.offsets.lakehouse-control-iceberg.connect-order-events-sink']
FROM lakehouse.commerce."order_events$snapshots" ORDER BY committed_at DESC LIMIT 1;
```

스냅샷 `summary`에 커밋 시점의 Kafka 오프셋이 들어 있으면 재시작 시 여기서부터 이어 읽습니다.

PoC에서는: `setup.sh --connect` 4~6단계. 커밋 주기는 30초, 테이블은 auto-create.

## 7. 초기 적재 (기존 DB 데이터)

이미 DB에 쌓인 이력은 Kafka를 거치지 않고 Trino로 직접 옮깁니다. 서버②에 원본 DB 커넥터를 추가합니다.

```properties
# etc/catalog/orderdb.properties (MySQL 예)
connector.name=mysql
connection-url=jdbc:mysql://주문DB:3306
connection-user=${ENV:ORDERDB_RO_USER}
connection-password=${ENV:ORDERDB_RO_PW}
```

읽기 전용 계정을 쓰고, 부하를 피하려면 복제본에 붙입니다. 기간별로 나눠 넣습니다.

```sql
INSERT INTO lakehouse.commerce.order_events
SELECT event_id, event_type, order_id, customer_id, amount, occurred_at, current_timestamp, 1
FROM orderdb.commerce.order_event_log
WHERE occurred_at >= TIMESTAMP '2024-01-01 00:00:00 +09:00'
  AND occurred_at <  TIMESTAMP '2024-02-01 00:00:00 +09:00';
```

검증은 구간마다 합니다.

```sql
SELECT count(*), sum(amount), min(occurred_at), max(occurred_at)
FROM lakehouse.commerce.order_events
WHERE occurred_at >= TIMESTAMP '2024-01-01 00:00:00 +09:00' AND occurred_at < TIMESTAMP '2024-02-01 00:00:00 +09:00';
-- 원본에서 같은 조건으로 뽑은 값과 비교
```

초기 적재 구간과 Kafka 적재 시작 시점이 겹치면 같은 `event_id`가 두 번 들어갑니다. 겹치는 구간은 조회 뷰에서 `event_id` 기준 1건만 취하거나, 초기 적재 완료 오프셋을 기록해 두고 겹침 구간을 한 번 정리합니다. 절차와 삭제 정책은 [05 콜드 데이터 이관](05-cold-data-migration.md).

PoC에서는: 생략. `poc_smoke` INSERT가 "Trino로 직접 쓰기"의 최소 예입니다.

## 8. 정합성·장애 검증

[08 단계별 구축과 PoC 계획](08-rollout-plan.md) 2장의 항목을 실제로 돌립니다.

| 검증 | 방법 | 기준 |
| --- | --- | --- |
| 누락·중복 | Connect 강제 종료 → 이벤트 N건 추가 → 재시작 | 재시작 후 N건 증가, `event_id` 중복 0 |
| 반영 지연 | 발행 시각과 첫 조회 성공 시각의 차이를 10회 측정 | 목표(예: 5분) 안 |
| 되감기 | 컨슈머 그룹 오프셋을 1시간 전으로 리셋 → 재적재 | 중복이 생기고, 중복 제거 뷰가 걸러냄 |
| 카탈로그 복구 | Postgres를 백업에서 복원 → Trino 조회 | 복원 시점 이후 커밋만 손실, 이전은 조회됨 |
| 브로커 1대 장애 | 브로커 1대 정지 | Connect 재연결, 적재 계속 |

```sql
-- 중복 검사
SELECT event_id, count(*) FROM lakehouse.commerce.order_events GROUP BY event_id HAVING count(*) > 1;

-- 중복 제거 뷰 (조회 표준)
CREATE OR REPLACE VIEW lakehouse.commerce.order_events_dedup AS
SELECT * FROM (
  SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY ingested_at DESC) AS rn
  FROM lakehouse.commerce.order_events
) WHERE rn = 1;
```

PoC에서는: README "장애 재현" 절. 실측 결과 60초 안에 복구, 중복 0.

## 9. 운영 작업 등록

적재가 돌기 시작한 날부터 필요합니다. 서버②에서 Trino CLI로 실행하는 스크립트를 cron(또는 사내 스케줄러)에 겁니다.

```bash
#!/usr/bin/env bash
# /opt/trino/maintenance.sh — 매일 04:00
T="docker compose --project-directory /opt/trino exec -T trino trino --execute"
for tbl in commerce.order_events commerce.payment_events; do
  $T "ALTER TABLE lakehouse.$tbl EXECUTE optimize(file_size_threshold => '128MB')"
  $T "ALTER TABLE lakehouse.$tbl EXECUTE expire_snapshots(retention_threshold => '7d')"
done
# 주 1회 (일요일)
[ "$(date +%u)" = 7 ] && for tbl in commerce.order_events commerce.payment_events; do
  $T "ALTER TABLE lakehouse.$tbl EXECUTE remove_orphan_files(retention_threshold => '3d')"
done
```

| 작업 | 주기 | 확인 |
| --- | --- | --- |
| 병합(optimize) | 매일 | `$files` 파일 수 감소 |
| 스냅샷 만료 | 매일, 7일 유지 | `$snapshots` 행 수 |
| 고아 파일 정리 | 주 1회, 3일 이상 보관 | 쓰기 작업 시간보다 짧게 잡지 않음 |
| 카탈로그 백업 | 매일 | 복원 리허설 분기 1회 |
| MinIO 버킷 복제 | 매일 `mc mirror` (삭제 전파 없이) | [MinIO 03](../../minio/concepts/03-setup-plan.md) 5장 |

모니터링은 기존 Prometheus·Grafana에 잡을 추가합니다([Kafka 14 모니터링](../../kafka/concepts/14-monitoring-prometheus-grafana.md)).

| 지표 | 출처 | 알람 |
| --- | --- | --- |
| `kafka_consumergroup_lag{group="connect-order-events-sink"}` | kafka_exporter | 10분 연속 증가 |
| 커넥터·태스크 상태 | `GET /connectors/*/status` (스크립트 → Pushgateway) | FAILED |
| 마지막 커밋 시각 | `$snapshots` 최신 `committed_at` | 커밋 주기 × 5 초과 |
| 파일 수·평균 크기 | `$files` | 파티션당 파일 100개 초과 |
| Trino 쿼리 실패·메모리 | Trino JMX | `EXCEEDED_LOCAL_MEMORY_LIMIT` |
| 유지보수 스크립트 실패 | cron 종료 코드 | 0이 아닐 때 |

## 10. 조회 제공

1. Trino 인증·권한을 켭니다. 1단계는 파일 기반 접근 제어면 충분합니다.

```json
// etc/rules.json — 분석 그룹은 commerce 스키마 읽기만, 개인정보 컬럼은 마스킹 뷰로
{
  "catalogs": [{ "group": "analysts", "catalog": "lakehouse", "allow": "read-only" }],
  "schemas":  [{ "group": "analysts", "catalog": "lakehouse", "schema": "commerce", "owner": false }],
  "tables":   [{ "group": "analysts", "catalog": "lakehouse", "schema": "commerce", "table": "order_events_masked", "privileges": ["SELECT"] }]
}
```

2. 마스킹 뷰와 분석 테이블을 만듭니다. 원본 이력 테이블은 플랫폼팀만 봅니다.

```sql
CREATE VIEW lakehouse.commerce.order_events_masked AS
SELECT event_id, event_type, order_id, to_hex(sha256(to_utf8(customer_id))) AS customer_key, amount, occurred_at
FROM lakehouse.commerce.order_events_dedup;
```

3. BI 도구·Query API를 연결하고 대표 쿼리 3개의 p95를 잽니다. 목표를 못 맞추면 파티션·파일 크기·워커 메모리 순으로 봅니다([06 운영](06-consistency-and-operations.md) 5장).

## 11. 다음 단계

- 콜드 데이터 이관: [05 콜드 데이터 이관](05-cold-data-migration.md)의 선정 → 복사 → 검증 → DB 정리 → 조회 경로.
- 도메인 확장: 결제·환불 토픽을 같은 커넥터에 `iceberg.tables.dynamic-enabled`로 라우팅하거나 커넥터를 나눕니다. 회원·채팅은 [04 역할 분담](04-role-split-and-platform-layers.md) 5장.
- Trino 스펙 상향과 워커 추가: [MinIO 06 서버 배치](../../minio/concepts/06-lakehouse-deployment-layout.md) 3장.

## 체크리스트

- 1단계 결정 항목 표가 채워졌다
- MinIO `lakehouse` 버킷에 전용 계정으로만 접근된다
- Postgres 카탈로그 백업이 매일 돌고 복원을 한 번 해봤다
- Trino `SHOW SCHEMAS FROM lakehouse`가 성공한다
- DDL로 만든 `order_events`에 INSERT → 스냅샷 → MinIO 파일 → 시점 조회가 된다
- Connect 태스크가 RUNNING이고 발행한 이벤트가 커밋 주기 안에 조회된다
- 초기 적재 구간별 건수·금액이 원본과 일치한다
- 강제 종료 → 재시작에 누락·중복 0
- 병합·스냅샷 만료·고아 파일 정리·백업이 스케줄에 있다
- lag·태스크 상태·마지막 커밋 시각 알람이 걸려 있다
- 분석 그룹은 마스킹 뷰만 보고, 대표 쿼리 p95가 목표 안이다

## 관련 문서

- [로컬 PoC](../examples/local-poc/README.md) — 이 순서를 한 대에서 자동으로
- [06 데이터 정합성과 운영 원칙](06-consistency-and-operations.md)
- [08 단계별 구축과 PoC 계획](08-rollout-plan.md)
- [MinIO: 06 레이크하우스 서버 배치](../../minio/concepts/06-lakehouse-deployment-layout.md)
- [Kafka: 계정 발급 런북](../../kafka/commands/account-provisioning-runbook.md)

## 참고한 공식 문서

- [Trino — Iceberg connector](https://trino.io/docs/current/connector/iceberg.html), [Trino — Deploying Trino](https://trino.io/docs/current/installation/deployment.html), [Trino — File-based access control](https://trino.io/docs/current/security/file-system-access-control.html)
- [Iceberg — Kafka Connect](https://iceberg.apache.org/docs/latest/kafka-connect/), [Iceberg — Maintenance](https://iceberg.apache.org/docs/latest/maintenance/), [Iceberg — JDBC Catalog](https://iceberg.apache.org/docs/latest/jdbc/)
- [MinIO — mc admin policy](https://min.io/docs/minio/linux/reference/minio-mc-admin/mc-admin-policy.html)
