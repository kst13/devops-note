# 주문·결제 샘플 — Lakehouse PoC 끝단 시연

주문·결제 화면에서 결제를 실행하면 이벤트가 Kafka에 발행되고, Kafka Connect Iceberg Sink가 MinIO의 Iceberg 테이블에 적재하고, 같은 화면에서 Trino 조회 결과(결제 성공률, 취소·환불, 주문·결제 불일치 후보, 반영 지연)로 돌아오는 것까지를 한 프로젝트로 묶은 샘플입니다. 발표자료 8장(활용 사례 ①)과 14장(PoC 단계)을 실제 코드로 보여 줍니다.

```text
Vue 3 (Vite :5173)
  ├─ 파이프라인 화면 ── GET /api/pipeline/status ──▶ Spring Boot ── Kafka Admin · Connect REST · Trino ──▶ 각 단계 상태
  ├─ 주문·결제 화면 ── POST /api/orders, /api/payments ──▶ Spring Boot (:8090)
  │                                                          ├─ H2 (주문·결제 상태의 기준 = 운영 DB 역할)
  │                                                          └─ KafkaTemplate ──▶ 홈랩 Kafka (SASL_SSL :9094)
  │                                                                                order.events / payment.events
  └─ Lakehouse 조회 화면 ── GET /api/analytics/* ──▶ Spring Boot ── Trino JDBC ──▶ Trino (:8080)
                                                                                    └─ Iceberg commerce.order_events / payment_events
                                                                                       (MinIO + REST Catalog)
                                                          ▲
                                       Kafka Connect Iceberg Sink × 2 (lakehouse-poc, 30초 커밋)
```

- 백엔드가 쓰기(Kafka)와 읽기(Trino)를 모두 담당합니다. 프런트는 Trino를 직접 호출하지 않습니다.
- H2가 운영 DB 역할입니다. 주문·결제의 현재 상태는 H2가 기준이고, Lakehouse는 이벤트 이력입니다.
- Outbox·Debezium은 범위 밖입니다. DB 저장 후 직접 발행하며, 운영 전환 시 [Transactional Outbox](../../concepts/06-consistency-and-operations.md)로 바꿉니다.

## 전제

| 항목 | 준비 |
| --- | --- |
| JDK 21, Node 22 | 호스트 JDK가 25면 `JAVA_HOME`을 21로 지정해 Gradle을 실행 |
| Kafka | [Kafka 홈랩](../../../kafka/examples/home-lab/README.md)이 떠 있을 것 (`kafka-home-lab`, SASL_SSL :9094) |
| Lakehouse (2·3단계) | [로컬 PoC](../local-poc/README.md)가 `setup.sh --connect`로 떠 있을 것 (`lakehouse-poc`) |
| 포트 | 8090(백엔드), 5173(프런트) 미사용 |

## 1단계 — 화면에서 결제하면 Kafka에 이벤트가 발행된다

```bash
cd scripts && ./kafka-setup.sh          # 홈랩에 order-service 계정, 토픽 2개, ACL. backend/.env 생성
cd ../backend && set -a && . ./.env && set +a && ./gradlew bootRun     # :8090
cd ../frontend && npm install && npm run dev                            # :5173
```

브라우저에서 http://localhost:5173/order 를 엽니다.

1. **주문 생성** — `order.events`에 `ORDER_CREATED`가 발행됩니다. 프리셋 버튼으로 정상·한도 초과·포인트 부족 케이스를 바로 만들 수 있습니다.
2. **결제** — 목록에서 주문을 선택하고 결제수단을 골라 결제합니다. `payment.events`에 `PAYMENT_REQUESTED` 뒤에 `PAYMENT_COMPLETED` 또는 `PAYMENT_FAILED`가 발행됩니다.
3. **결제 이벤트 발행 생략** 체크 — H2에는 결제 완료로 저장하지만 이벤트를 발행하지 않습니다. 3단계에서 "주문·결제 불일치 후보"로 잡히는 데이터를 만드는 장치입니다.
4. **주문 취소** — 결제 완료 주문만 가능. `ORDER_CANCELLED`와 `PAYMENT_REFUNDED`가 발행됩니다.

가짜 결제(PG) 규칙: 금액 1,000,000원 이상 → `LIMIT_EXCEEDED`, `POINT`이고 50,000원 초과 → `INSUFFICIENT_POINT`. 3단계 조회에 실패 데이터가 있어야 성공률이 100%가 아닌 숫자로 나옵니다.

확인:

```bash
cd ~/kafka-home-lab/kafka && docker compose exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9094 --consumer.config /etc/kafka/secrets/admin.properties \
  --topic payment.events --from-beginning --property print.key=true
```

키가 `order_id`이므로 같은 주문의 이벤트가 같은 파티션에 순서대로 들어갑니다.

## 2단계 — Kafka → Iceberg (MinIO)

```bash
cd scripts && ./lakehouse-setup.sh           # 테이블 DDL 2개 + Iceberg Sink 커넥터 2개 등록
./lakehouse-setup.sh --reset                 # 커넥터·컨슈머 그룹·테이블을 지우고 처음부터 (토픽은 그대로라 재적재됨)
```

- 테이블은 DDL로 만듭니다(`iceberg.tables.auto-create-enabled=false`). 운영에서 파티션·타입을 통제하는 방식과 같습니다.
- 커넥터는 토픽별로 하나씩입니다. 커밋 주기 30초.
- `occurred_at`은 JSON 문자열이라 `varchar`로 받습니다. 조회 SQL에서 `from_iso8601_timestamp`로 변환합니다. 운영에서는 Connect SMT(`TimestampConverter`)로 `timestamptz`로 바꾸고 `day(occurred_at)` 파티션을 겁니다.

확인:

```sql
-- cd ~/lakehouse-poc && docker compose exec -it trino trino
SELECT event_type, count(*) FROM lakehouse.commerce.payment_events GROUP BY 1;
SELECT event_type, count(*) FROM lakehouse.commerce.order_events GROUP BY 1;
```

발행 건수와 같아야 합니다. "발행 생략"한 결제는 여기 없습니다.

## 3단계 — 화면에서 Trino 조회

http://localhost:5173/analytics 에 다음이 보입니다. 15초마다 자동 갱신됩니다.

| 화면 | SQL | 의미 |
| --- | --- | --- |
| ① 결제 성공률 | `PAYMENT_REQUESTED` 대비 `PAYMENT_COMPLETED`, 일별·수단별 | 발표자료 8장 첫 번째 조회 |
| 실패 사유 | `PAYMENT_FAILED`를 사유·수단별 집계 | |
| ② 취소·환불 현황 | `ORDER_CREATED` 대비 `ORDER_CANCELLED`, `PAYMENT_REFUNDED` 금액 | 두 번째 조회 |
| ③ 주문·결제 불일치 후보 | 주문은 있는데 유예 시간이 지나도 결제 이벤트가 없는 주문 | 세 번째 조회. "발행 생략"한 주문이 잡힘 |
| ④ 반영 지연 | 앱 발행 시각(`occurred_at`) → 데이터 파일 기록 시각(`$file_modified_time`) | 15장 반영 지연 목표의 실측치 |
| 주문 타임라인 | 주문 ID로 두 토픽 이력을 시간순 병합 | H2 상태와 나란히 비교 |

조회 API(`/api/analytics/*`)의 SQL은 서버에 고정되어 있고, 모두 `event_id` 중복 제거 CTE 위에서 돕니다. 재시작·되감기로 같은 이벤트가 두 번 적재될 수 있기 때문입니다([06 운영 원칙](../../concepts/06-consistency-and-operations.md) 3장). Trino가 없으면 조회 API만 503을 돌려주고 주문·결제는 정상 동작합니다.

```bash
curl -s 'localhost:8090/api/analytics/payment-success-rate?days=7' | jq
curl -s 'localhost:8090/api/analytics/mismatches?graceSeconds=60' | jq
```

## 파이프라인 화면 — 이벤트가 단계를 옮겨 가는 것을 보기

http://localhost:5173/pipeline 은 다섯 단계의 현재 상태를 5초마다 읽어 한 줄로 보여 줍니다.

```text
① 앱·H2        ② Kafka              ③ Kafka Connect     ④ Iceberg / MinIO            ⑤ Trino
주문·결제 수  →  토픽별 메시지 수   →  커넥터·태스크 상태  →  테이블별 레코드·파일·스냅샷  →  테이블별 행 수
                파티션 끝 오프셋                            마지막 커밋 시각
                커넥터 그룹 lag                             MinIO 경로
```

| 단계 | 어디서 읽나 | 보여 주는 것 |
| --- | --- | --- |
| ① 앱 | H2 | 주문·결제 건수, 발행 토픽 |
| ② Kafka | AdminClient (`order-service` 계정) | 토픽별 파티션 끝 오프셋 합계, 커넥터 컨슈머 그룹(`connect-<커넥터>`)의 커밋 오프셋과 **lag** |
| ③ Kafka Connect | Connect REST `/connectors/<name>/status` | 커넥터·태스크 RUNNING 여부 |
| ④ Iceberg / MinIO | Trino 메타데이터 테이블 `$snapshots`·`$files` | 스냅샷 수, 마지막 커밋 시각, 파일 수·크기·레코드 수, MinIO 경로 |
| ⑤ Trino | `count(*)` | 조회 화면이 보는 행 수 |

"테스트 결제 1건 발행" 버튼은 주문 생성 + 결제를 한 번에 실행해 이벤트 3건을 보냅니다. 아래 "변화 로그"에 이전 조회와 달라진 숫자가 순서대로 찍힙니다. 실측 순서는 다음과 같습니다.

```text
+0초   ① 앱 주문·결제 +1  →  ② Kafka 메시지 +3, lag 1·2      (Trino 행 수 그대로)
+13초  ② lag 0  →  ④ 스냅샷 +1, 파일 +1, 마지막 커밋 갱신  →  ⑤ Trino 행 +3
```

lag이 0이 되고 스냅샷이 늘어난 뒤에야 Trino 행 수가 바뀝니다. 이것이 4장 "반영 지연의 원인과 규모"를 눈으로 보는 방법입니다. 단계 하나가 죽어 있으면 그 카드만 빨갛게 표시되고 나머지는 계속 보입니다.

## 구축하면서 확인한 것 (2026-09-15)

| 현상 | 원인 | 반영 |
| --- | --- | --- |
| Trino에서 `occurred_at`이 `1.789456E9`로 보임 | Spring Kafka `JsonSerializer`의 기본 ObjectMapper가 `OffsetDateTime`을 epoch 초로 직렬화 | 이벤트 레코드가 ISO-8601 `String`을 직접 든다. 조회 CTE는 epoch 문자열도 `try_cast`로 받는다 |
| 결제 Sink 태스크 `GroupAuthorizationException: connect-payment-events-sink-app` | 초기 PoC의 ACL이 `connect-order-events-sink` 접두사만 허용 | `kafka-setup.sh`가 `connect-` 접두사 그룹 ACL을 추가 |
| `CREATE TABLE IF NOT EXISTS`가 지나가고 컬럼이 `orderid`처럼 보임 | 초기 PoC가 auto-create로 만든 카멜케이스 스키마 테이블이 남아 있었음 | `lakehouse-setup.sh --reset`이 옛 커넥터·그룹·테이블을 지운다 |
| 카탈로그 뷰 대신 CTE | REST fixture(sqlite) 카탈로그의 뷰 지원에 기대지 않기 위해 | `AnalyticsController.DEDUP_CTE` |

## 디렉터리

```text
order-payment-sample/
├── backend/        Spring Boot 3.5 · Java 21 · Gradle (Kotlin DSL)
│   ├── src/main/java/com/osstem/sample/orderpayment/
│   │   ├── order/       주문 엔티티·서비스·API
│   │   ├── payment/     결제 엔티티·가짜 PG·서비스·API
│   │   ├── event/       OrderEvent · PaymentEvent · EventPublisher
│   │   ├── analytics/   Trino JDBC 조회 API
│   │   ├── pipeline/    단계별 상태 API (Kafka Admin · Connect REST · Trino 메타데이터)
│   │   └── config/      설정 바인딩, CORS, 예외
│   ├── src/main/resources/application.yml
│   └── .env             kafka-setup.sh 가 생성 (커밋 금지)
├── frontend/       Vue 3 · Vite · vue-router
│   └── src/pages/   OrderPage.vue · AnalyticsPage.vue · PipelinePage.vue
└── scripts/
    ├── kafka-setup.sh               1단계: 계정·토픽·ACL
    ├── lakehouse-setup.sh           2단계: 테이블 DDL + 커넥터 등록 (--reset)
    └── iceberg-sink.template.json   커넥터 설정 템플릿
```

## 정리

```bash
cd scripts && ./lakehouse-setup.sh --reset      # 커넥터·테이블 제거 (선택)
cd ~/kafka-home-lab/kafka
A="--bootstrap-server localhost:9094 --command-config /etc/kafka/secrets/admin.properties"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh $A --delete --topic order.events --topic payment.events
docker compose exec -T kafka /opt/kafka/bin/kafka-configs.sh $A --alter --delete-config SCRAM-SHA-512 --entity-type users --entity-name order-service
```

## 관련 문서

- [로컬 PoC](../local-poc/README.md) — 이 샘플이 붙는 MinIO·Catalog·Trino·Connect
- [03 데이터 적재 및 조회 흐름](../../concepts/03-write-and-read-flow.md), [08 단계별 구축과 PoC 계획](../../concepts/08-rollout-plan.md)
- [Kafka 사용 가이드: 프로듀서](../../../kafka/usage-guide/03-producer.md) — `acks=all`, 멱등, 압축 설정의 근거
