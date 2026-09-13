# 데이터는 어떻게 저장되고 조회되는가

> 주문 이벤트 하나가 Kafka에 들어와 Trino에서 조회되기까지의 경로입니다. **파일 저장과 테이블 커밋은 다른 단계**이고, 커밋이 끝나야 조회됩니다.

## 1. 이벤트 하나의 여정

```json
{
  "eventId": "evt-1001",
  "eventType": "ORDER_CREATED",
  "orderId": "order-2001",
  "customerId": "customer-3001",
  "amount": 35000,
  "occurredAt": "2026-09-13T10:00:00+09:00"
}
```

```text
주문 서비스 ──▶ Kafka · order.events (P0 offset 40)
                      │ ① 읽기 (오프셋 추적)
                      ▼
          Writer (Kafka Connect Iceberg Sink)
            ② 변환   ③ 배치로 묶어 Parquet 생성
            ④ 파일 업로드          ⑤ 테이블 커밋
               │                      │
               ▼                      ▼
        MinIO · lakehouse        Iceberg Catalog
        data/*.parquet           order_events → 현재 metadata 위치
        metadata/*.json                 ▲
               ▲                        │ 테이블 위치 확인
               │ 파일 읽기               │
               └──────── Trino ─────────┘
```

1. **수집.** Writer가 Kafka에서 이벤트를 읽습니다. 읽은 위치(오프셋)를 추적합니다.
2. **변환.** 필드와 타입을 테이블 컬럼에 맞춥니다.
3. **파일 생성.** 일정량을 모아 Parquet 파일을 만듭니다.
4. **저장.** MinIO에 파일을 올립니다. 아직 테이블에는 보이지 않습니다.
5. **커밋.** Iceberg 테이블에 새 스냅샷을 확정합니다. 이 순간부터 Trino에서 보입니다.

Iceberg Kafka Connect Sink는 커밋 간격을 `iceberg.control.commit.interval-ms`(기본 5분)로 정합니다. 이 값이 곧 **반영 지연**입니다. 짧게 잡으면 작은 파일이 많아지고, 길게 잡으면 조회가 늦어집니다. 작은 파일 문제는 병합 작업으로 따로 풉니다([06 운영](06-consistency-and-operations.md)).

## 2. 조회 흐름

```sql
SELECT customer_id, SUM(amount) AS total_amount
FROM lakehouse.commerce.order_events
WHERE event_type = 'ORDER_CREATED'
  AND occurred_at >= TIMESTAMP '2026-09-01 00:00:00'
GROUP BY customer_id;
```

1. Coordinator가 Catalog에서 `commerce.order_events`의 현재 메타데이터 위치를 얻습니다.
2. 메타데이터의 매니페스트에서 파일별 통계(컬럼 최소·최대값)를 보고 `occurred_at` 조건에 걸리지 않는 파일을 **읽지 않습니다**.
3. Worker들이 남은 Parquet 파일에서 필요한 컬럼(`customer_id`, `amount`, `event_type`, `occurred_at`)만 읽어 부분 집계합니다.
4. Coordinator가 결과를 합쳐 반환합니다.

Trino가 MinIO 뒤에 직렬로 붙는 구조가 아닙니다. Catalog에서 테이블을 찾고, 메타데이터로 읽을 파일을 결정한 뒤, 저장소의 파일을 읽습니다. 파티션과 파일 배치가 조회 조건과 맞아야 2단계에서 파일을 많이 걸러냅니다.

## 3. 이벤트 이력 테이블과 최신 상태 테이블

같은 주문 이벤트라도 어떤 테이블을 만들지에 따라 적재 방식과 조회 의미가 달라집니다.

```text
주문 A의 이벤트: ① 생성 → ② 결제 완료 → ③ 취소

order_events (이력)            orders_current (상태)
A · CREATED   · v1             A · CANCELLED · v3
A · PAID      · v2             (v2가 늦게 와도 v3를 덮지 않음)
A · CANCELLED · v3
"어떤 일이 있었나?"              "지금 어떤 상태인가?"
```

| 구분 | 이벤트 이력 테이블 | 최신 상태 테이블 |
| --- | --- | --- |
| 행의 의미 | 일어난 사건 하나 | 엔티티 하나의 현재 상태 |
| 적재 방식 | append만. 단순하고 검증 쉬움 | upsert. `entityVersion`으로 역순 방어 필요 |
| 답하는 질문 | 어떤 일이 있었나? 처리 소요 시간은? | 지금 어떤 상태인가? |
| 주의 | 생성·결제·취소 행을 단순 합산하면 매출이 틀림 | 과거 상태를 잃음. 이력에서 재구성 가능 |
| 권장 | **먼저 구축.** 흐름 이해·검증에 유리 | 이력 위에서 뷰 또는 주기 작업으로 파생 |

이력이 있으면 상태는 언제든 다시 만들 수 있습니다. 반대는 불가능합니다. 첫 테이블은 append만 하는 이벤트 이력으로 시작합니다.

## 4. 테이블 3단계

Iceberg 안에서 데이터를 관리하는 논리적 구분입니다. 별도 제품이 아닙니다.

| 단계 | 예시 | 목적 |
| --- | --- | --- |
| ① 수집 이력 | `order_events`, `payment_events`, `refund_events` | 문제 추적과 재처리. 각 의미대로 보존 |
| ② 정제 데이터 | `orders`, `payment_attempts`, `refunds` | 중복·타입·상태·식별자 정리. 일관된 업무 단위 |
| ③ 분석 테이블 | `daily_payment_summary`, `order_payment_mismatch`, `unified_transaction` | 반복 조회를 쉽게 제공 |

한 주문에 결제 시도와 환불이 여러 건 붙습니다. 단순 조인 후 합산하면 중복 집계됩니다. ②에서 주문·결제 시도·환불을 각각의 단위로 두고, ③에서 집계 규칙을 정합니다. 수집 이력도 무기한 보관은 아닙니다. 데이터별 보관·삭제 기준을 적용합니다.

## 5. 직접 확인하기

[로컬 PoC](../examples/local-poc/README.md)에서 다음을 눈으로 확인할 수 있습니다.

```sql
-- 커밋된 스냅샷 목록 (Trino)
SELECT snapshot_id, committed_at, operation
FROM lakehouse.commerce."order_events$snapshots";

-- 스냅샷이 참조하는 데이터 파일과 레코드 수
SELECT file_path, record_count, file_size_in_bytes
FROM lakehouse.commerce."order_events$files";

-- 과거 시점 조회
SELECT count(*) FROM lakehouse.commerce.order_events FOR VERSION AS OF <snapshot_id>;
```

```bash
# MinIO에 실제로 생긴 파일 (mc)
mc ls -r local/lakehouse/warehouse/commerce/order_events/
```

## 관련 문서

- [02 구성요소 정의와 역할](02-components.md)
- [06 데이터 정합성과 운영 원칙](06-consistency-and-operations.md) — 커밋 주기와 파일 병합, 재처리
- [Kafka: 프로듀서·토픽·컨슈머 핵심 정리](../../kafka/concepts/16-producer-topic-consumer-summary.md) — 오프셋·커밋·멱등 처리

## 참고한 공식 문서

- [Iceberg — Kafka Connect (커밋 간격, 테이블 자동 생성)](https://iceberg.apache.org/docs/latest/kafka-connect/)
- [Trino — Iceberg connector (metadata tables, time travel)](https://trino.io/docs/current/connector/iceberg.html)
