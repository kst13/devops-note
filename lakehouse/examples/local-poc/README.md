# Lakehouse 로컬 PoC (setup.sh)

Kafka → Kafka Connect Iceberg Sink → MinIO + Iceberg REST Catalog → Trino 흐름을 로컬 PC 한 대에서 재현하는 스크립트입니다. Kafka는 새로 띄우지 않고 [Kafka 홈랩](../../../kafka/examples/home-lab/README.md)의 브로커(SASL_SSL, ACL)를 그대로 씁니다. Lakehouse 부분만 별도 compose 프로젝트(`lakehouse-poc`)로 붙습니다.

```bash
./setup.sh              # 1단계: MinIO + Catalog + Trino (기본 ~/lakehouse-poc)
./setup.sh --connect    # 2단계: 홈랩 Kafka 에 Kafka Connect Iceberg Sink 연결
./setup.sh /원하는/경로  # 작업 디렉터리 지정
```

두 단계로 나눈 이유는 어디서 막혔는지 알기 위해서입니다. 1단계가 끝나면 Catalog와 S3 설정은 검증된 상태라, 2단계 문제는 Connect 쪽으로 좁혀집니다.

## 구성

| 구성요소 | 이미지 | 호스트 포트 | 역할 |
| --- | --- | --- | --- |
| MinIO | `quay.io/minio/minio` | 9000 API, 9001 콘솔 | `lakehouse` 버킷. 파일이 실제로 생기는지 콘솔에서 확인 |
| minio-init | `quay.io/minio/mc` | — | 버킷, `lakehouse` 전용 계정, 버킷 한정 정책 생성 후 종료 |
| Iceberg REST Catalog | `apache/iceberg-rest-fixture:1.10.1` | 8181 | 테이블 등록. sqlite 파일을 `catalog-data/`에 두어 재시작에도 유지 |
| Trino | `trinodb/trino:483` 1노드 | 8080 | `lakehouse` 카탈로그 (REST + 네이티브 S3 → MinIO) |
| Kafka Connect + Iceberg Sink | `cp-kafka-connect:7.9.1` + Confluent Hub `iceberg/iceberg-kafka-connect:1.9.2` | 8083 | 홈랩 Kafka에서 읽어 Iceberg 테이블에 30초마다 커밋 (2단계) |

```text
kafka-home-lab (기존)                       lakehouse-poc (이 예제)
┌─────────────────────┐   kafka-home-lab-net   ┌────────────────────────────────────────────┐
│ kafka (SASL_SSL,ACL)│◀───────────────────────│ connect (Iceberg Sink)                      │
│ lakehouse.order.    │                        │   ④ 파일 ──▶ minio (bucket lakehouse)       │
│   events 토픽        │                        │   ⑤ 커밋 ──▶ iceberg-rest (Catalog)         │
└─────────────────────┘                        │                     ▲          ▲            │
                                               │ trino ── 테이블 위치 ┘   파일 읽기 ┘            │
                                               └────────────────────────────────────────────┘
```

## 스크립트가 하는 일

1. 사전 점검 — docker(compose v2)·curl, 호스트 포트 8080/8181/9000/9001. `--connect`면 `~/kafka-home-lab`과 실행 중인 `kafka-home-lab` 컨테이너 확인
2. 작업 디렉터리 — 이 예제의 compose·설정을 `~/lakehouse-poc`로 복사하고 `.env`(MinIO 루트·`lakehouse` 계정·`connect` 비밀번호) 생성. 재실행 시 재사용
3. 1단계 기동 — MinIO → minio-init(버킷·계정·정책) → REST Catalog → Trino. Trino `/v1/info`가 `starting:false`가 될 때까지 대기
4. 1단계 검증 — Trino에서 `commerce.poc_smoke` 테이블을 만들고 두 번 INSERT. 행 수, 스냅샷 2개, MinIO의 `data/`·`metadata/` 파일 목록 출력
5. (`--connect`) 홈랩 브로커에 `connect` 계정(SCRAM), 토픽 5개(Connect 내부 3개 + `lakehouse.order.events` + 제어 토픽), ACL 생성
6. (`--connect`) Kafka Connect 기동(최초 1회 Confluent Hub에서 커넥터 설치) → 커넥터 등록 → 태스크 RUNNING 대기
7. (`--connect`) 샘플 주문 이벤트 5건 발행 → 커밋 대기 → Trino에서 `commerce.order_events` 5건 조회

## 1단계에서 직접 확인할 것

```bash
cd ~/lakehouse-poc && docker compose exec -it trino trino
```

```sql
SELECT * FROM lakehouse.commerce.poc_smoke;
SELECT snapshot_id, committed_at, operation FROM lakehouse.commerce."poc_smoke$snapshots";
SELECT file_path, record_count, file_size_in_bytes FROM lakehouse.commerce."poc_smoke$files";
-- 첫 번째 스냅샷 시점으로 되돌아가 보기 (snapshot_id 는 위 결과에서)
SELECT count(*) FROM lakehouse.commerce.poc_smoke FOR VERSION AS OF 1234567890123456789;
```

MinIO 콘솔(http://localhost:9001, `.env`의 루트 계정)에서 `lakehouse/warehouse/commerce/poc_smoke/` 아래 `data/`와 `metadata/`를 열어 보면 INSERT마다 Parquet 파일과 `metadata.json`이 하나씩 늘어난 것이 보입니다. 파일이 올라간 것과 테이블에 반영된 것이 다른 단계라는 점([03 저장·조회 흐름](../../concepts/03-write-and-read-flow.md))을 여기서 눈으로 확인합니다.

## 2단계에서 직접 확인할 것

이벤트 발행은 홈랩의 admin 계정으로 합니다(홈랩 `app` 계정은 `sandbox.demo`만 허용).

```bash
cd ~/kafka-home-lab/kafka && docker compose exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9094 --producer.config /etc/kafka/secrets/admin.properties \
  --topic lakehouse.order.events
```

```json
{"eventId":"evt-0006","eventType":"ORDER_CREATED","orderId":"order-2003","customerId":"customer-3003","amount":9900,"occurredAt":"2026-09-13T11:00:00+09:00"}
```

30초 안에 Trino에서 보입니다.

```sql
SELECT eventid, eventtype, orderid, amount FROM lakehouse.commerce.order_events ORDER BY eventid;
SELECT snapshot_id, committed_at, summary FROM lakehouse.commerce."order_events$snapshots" ORDER BY committed_at DESC;
```

스냅샷 `summary`에 커넥터가 기록한 Kafka 오프셋이 들어 있습니다. 재시작 시 여기서부터 이어 읽습니다.

### 장애 재현 — 누락·중복 확인

```bash
cd ~/lakehouse-poc && docker compose kill connect       # 적재 중 강제 종료
# 이벤트 3건 더 발행 (위 producer)
docker compose --profile connect up -d connect            # 재시작
```

재시작 후 1분 안에 3건이 추가되고, 다음 쿼리가 0행이어야 합니다.

```sql
SELECT eventid, count(*) FROM lakehouse.commerce.order_events GROUP BY eventid HAVING count(*) > 1;
```

### 유지보수 작업 체험

```sql
ALTER TABLE lakehouse.commerce.order_events EXECUTE optimize;                                      -- 작은 파일 병합
ALTER TABLE lakehouse.commerce.order_events EXECUTE expire_snapshots(retention_threshold => '0d');  -- 스냅샷 만료 (PoC 라 0d)
SELECT count(*) FROM lakehouse.commerce."order_events$files";                                       -- 파일 수가 줄었는지
```

## 구축하면서 확인한 것 (2026-09-13, Iceberg Sink 1.9.2 · Connect 7.9.1 · Kafka 4.0)

| 현상 | 원인 | 반영한 곳 |
| --- | --- | --- |
| 태스크가 `GroupAuthorizationException: cg-control-<UUID>`로 FAILED | 워커의 제어 토픽 컨슈머 그룹이 `cg-control-` + **매번 다른 UUID**. 커넥터 이름 기반 접두사로 준 ACL이 맞지 않음 | `setup.sh`: `--group cg-control- --resource-pattern-type prefixed` |
| 커넥터·태스크는 RUNNING인데 커밋마다 `Commit timeout reached ... committed to 0 table(s)` | 워커는 `put()`이 불릴 때만 제어 토픽을 `poll(0)` 함. 데이터가 뜸하면 `put()`이 `offset.flush.interval.ms`(기본 60초)마다만 불려 제어 컨슈머가 그룹에 못 붙음 | compose: `CONNECT_OFFSET_FLUSH_INTERVAL_MS: 1000` |
| 컨슈머 그룹 이름 | 데이터 `connect-order-events-sink`, 코디네이터 `connect-order-events-sink-coord`, 워커 제어 `cg-control-<UUID>` | ACL 3종 (앞 둘은 `connect-order-events-sink` 접두사로 묶임) |
| 재시작 후 이어 읽는 근거 | 스냅샷 `summary`의 `kafka.connect.offsets.<제어토픽>.<그룹>` 키에 커밋 시점 오프셋이 저장됨 | README 2단계 확인 항목 |
| 재구축 시 Connect가 `No resolvable bootstrap urls`로 계속 재시작 | compose v2.19가 external 네트워크(`kafka-home-lab-net`) 연결을 빠뜨린 채 컨테이너를 만든 경우가 있음. `docker inspect`에 네트워크가 하나만 보임 | `setup.sh`: 기동 후 연결 여부를 확인해 `docker network connect` 후 재시작 |

Connect 문제를 볼 때는 로그 레벨을 올리면 워커·코디네이터가 주고받는 제어 이벤트(`START_COMMIT`, `DATA_WRITTEN`, `DATA_COMPLETE`, `COMMIT_COMPLETE`)가 보입니다.

```bash
curl -X PUT -H 'Content-Type: application/json' -d '{"level":"DEBUG"}' \
  http://localhost:8083/admin/loggers/org.apache.iceberg.connect
```

장애 재현 결과: 5건 적재 후 `kill connect` → 3건 추가 발행 → 재시작. 60초 안에 8건이 되었고 `eventid` 중복은 0건이었습니다.

## 운영과 다른 점

| 항목 | 이 PoC | 운영 |
| --- | --- | --- |
| MinIO | 단일 노드, 단일 디스크 | MNMD + Erasure Coding ([MinIO 02](../../../minio/concepts/02-deployment-topology.md)) |
| Catalog | REST 테스트 픽스처 + sqlite | 운영용 REST 구현체 + 백업되는 DB |
| Trino | coordinator = worker 1대 | coordinator 1 + worker N |
| Kafka | 홈랩 1노드 RF1 | 3노드 RF3, `min.insync.replicas=2` |
| 커밋 주기 | 30초 (빨리 보려고) | 1~5분 + 일 단위 병합 |
| 이벤트 형식 | JSON, 스키마 없음 | Avro + Schema Registry 또는 스키마 있는 JSON |
| 테이블 자동 생성 | 켬 (`auto-create-enabled`) | 끔. 테이블은 DDL로 관리, 파티션 지정 |

## 정리

```bash
cd ~/lakehouse-poc && docker compose --profile connect down -v && rm -rf ~/lakehouse-poc
```

홈랩 브로커에 만든 `connect` 계정·토픽·ACL은 남습니다. 지우려면 홈랩에서 실행합니다.

```bash
cd ~/kafka-home-lab/kafka
A="--bootstrap-server localhost:9094 --command-config /etc/kafka/secrets/admin.properties"
docker compose exec -T kafka /opt/kafka/bin/kafka-acls.sh $A --remove --force --allow-principal User:connect \
  --topic lakehouse-connect-configs --topic lakehouse-connect-offsets --topic lakehouse-connect-status \
  --topic lakehouse.order.events --topic lakehouse-control-iceberg
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh $A --delete --topic 'lakehouse.*'
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh $A --delete --topic 'lakehouse-.*'
docker compose exec -T kafka /opt/kafka/bin/kafka-configs.sh $A --alter --delete-config SCRAM-SHA-512 \
  --entity-type users --entity-name connect
```

## 구축 결과 디렉터리

```text
~/lakehouse-poc/
├── .env                      # 자격증명 (커밋 금지)
├── docker-compose.yml
├── catalog-data/catalog.db   # REST Catalog 의 등록 정보 (sqlite)
├── minio/lakehouse-policy.json
├── trino/catalog/lakehouse.properties
└── connect/order-events-sink.json (+ .order-events-sink.rendered.json: 자격증명 치환본)
```

## 관련 문서

- [02 구성요소 정의와 역할](../../concepts/02-components.md)
- [06 데이터 정합성과 운영 원칙](../../concepts/06-consistency-and-operations.md)
- [Iceberg — Kafka Connect](https://iceberg.apache.org/docs/latest/kafka-connect/), [Trino — Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)
