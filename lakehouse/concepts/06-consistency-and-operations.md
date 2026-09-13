# 데이터 정합성과 운영 원칙

> 수집 방식, 이벤트 규약, 재처리, 권한, 유지보수를 처음부터 정합니다. 나중에 붙이면 이미 쌓인 데이터를 고칠 수 없습니다.

## 1. 수집 방식 — 초기 적재 + 변경 수집

기존 데이터는 초기 적재(배치)로, 이후 변경은 **Transactional Outbox** 또는 CDC로 받습니다. 애플리케이션이 DB 저장과 Kafka 발행을 각각 실행하면 하나만 성공하는 문제가 생깁니다.

```text
Outbox
주문 서비스 ──▶ ┌ 한 DB 트랜잭션 ─────────────┐
               │ orders (업무 테이블)          │
               │ outbox (이벤트 행)            │ ──▶ Debezium Outbox Router ──▶ Kafka
               └─────────────────────────────┘
               둘 다 성공하거나 둘 다 실패 → 발행 누락 없음

CDC
레거시 시스템 ──▶ orders (앱 수정 없음) ──▶ binlog / WAL ──▶ Debezium + 규약 변환 ──▶ Kafka
```

| 항목 | Transactional Outbox | CDC (테이블 직접 캡처) |
| --- | --- | --- |
| 원리 | 업무 변경과 발행할 이벤트를 같은 트랜잭션으로 outbox 테이블에 기록 | DB 로그를 읽어 테이블 변경을 그대로 이벤트로 전달 |
| 이벤트 형태 | 앱이 설계한 업무 이벤트. 규약 적용 쉬움 | 행의 before/after. 업무 의미는 변환 단계에서 부여 |
| 앱 수정 | 필요 (outbox 쓰기 코드) | 불필요 (DB 접근 권한만) |
| 어울리는 곳 | 코드를 고칠 수 있는 서비스 | 수정이 어려운 레거시·패키지 |
| 주의 | outbox 정리 작업 필요 | 스키마 변경이 그대로 노출. 트랜잭션 경계 복원 어려움 |

권장: 신규·수정 가능한 서비스는 Outbox, 레거시는 CDC. 둘 다 아래 이벤트 규약으로 변환해 Kafka에 넣습니다. CDC를 모두 나중 단계로 미루지 않습니다. Flink는 나중에 도입할 수 있지만 안정적인 수집 방식은 1단계부터 필요합니다.

## 2. 이벤트 규약 — 중복·순서·삭제

`eventId`로 중복을 제거하는 것만으로는 부족합니다. "주문 생성 → 결제 완료 → 주문 취소" 순서로 발생한 이벤트가 재처리 과정에서 결제 완료가 나중에 적용되면 취소 상태가 되돌아갈 수 있습니다.

| 항목 | 목적 |
| --- | --- |
| `eventId` | 동일 이벤트 중복 처리 방지 |
| `entityId` | 동일 주문·메시지 식별 |
| `entityVersion` | 오래된 변경이 최신 상태를 덮어쓰는 것 방지 |
| `eventType` | 생성·변경·취소·삭제 구분. DB 정리용 삭제와 실제 삭제 요청도 여기서 구분 |
| `occurredAt`, `ingestedAt` | 발생 시각과 수집 시각 구분 |
| `schemaVersion` | 이벤트 형식 변경 관리 |
| `tenantId`, `sourceSystem` | 조직 범위와 출처 식별 |

이벤트 이력 테이블은 append만 하므로 중복 이벤트가 두 행이 될 수 있습니다. 조회 시 `eventId` 기준 최신 1건만 취하는 뷰를 두거나, 정제 단계에서 중복을 제거합니다. 최신 상태 테이블은 `entityVersion`이 큰 경우에만 갱신합니다.

## 3. 재처리와 장애 복구

```text
Kafka offset 40 41 42 43 44 ──▶ Writer: 42까지 파일 생성·커밋 ──▶ Iceberg 스냅샷 N (커밋 오프셋 = 42)
                                   │
                            Writer 중단 (43·44는 읽었지만 커밋 전)
                                   │
                            재시작: 스냅샷 N의 오프셋(42)부터 다시 읽음
                                   │
                            스냅샷 N+1 (43·44 반영, 오프셋 = 44)
```

- Iceberg Kafka Connect Sink는 커밋한 오프셋을 테이블 스냅샷과 함께 기록합니다. 재시작하면 마지막 커밋 다음부터 읽으므로 누락이 없습니다. Kafka 보관 기간 안이면 되감아 재적재할 수 있습니다.
- 커넥터의 exactly-once와 별개로, 원본 시스템이 같은 업무 이벤트를 다른 메시지로 중복 발행하는 문제는 `eventId`와 업무 규칙으로 다룹니다.
- Kafka 보관 기간 밖은 Iceberg 스냅샷과 초기 적재 절차로 복구합니다. Kafka 보관 기간과 스냅샷 유지 기간이 곧 **재처리 가능 창**입니다.

Uber의 운영 경험이 강조하는 점은 "도구 연결"이 아니라 **"장애 후 어디서부터 다시 읽고, 어디까지 저장됐다고 판단할 것인가"** 입니다([07 기업 사례](07-industry-cases.md)).

## 4. 접근 권한과 삭제 전파

- Trino 카탈로그·스키마 단위 권한, 조회 감사 로그. 개인정보 컬럼은 마스킹 뷰로 제공합니다.
- 삭제 요청은 `eventType=DELETED` 이벤트로 세 계층에 전파합니다. Iceberg에서는 해당 행을 삭제한 뒤 스냅샷 만료로 과거 파일까지 정리합니다. 백업에 남은 데이터의 처리 기간도 정책에 넣습니다.
- Catalog의 등록 정보와 객체 저장소의 파일은 함께 백업·복구합니다.

## 5. 파일 유지보수

Iceberg 테이블은 쌓기만 하면 느려집니다. 정기 작업을 처음부터 일정에 넣습니다.

| 항목 | 기준 (초기 제안) | 왜 |
| --- | --- | --- |
| 파티션 | `days(occurred_at)`. 이벤트 이력은 일자 기준 | 기간 조회가 대부분이라 읽을 파일을 날짜로 잘라냄. 숨은 파티션이라 쿼리에 파티션 컬럼을 쓸 필요 없음 |
| 파일 크기 | 목표 128~512MB. 작은 파일은 병합 | Writer가 자주 커밋할수록 작은 파일이 늘고, 파일 수만큼 열기 비용이 늘어 Trino가 느려짐 |
| 병합 (compaction) | 매일, 전날 파티션 대상 `rewrite_data_files` | 반영 지연은 짧게, 조회는 큰 파일로. 둘을 분리 |
| 스냅샷 만료 | 7일 유지 후 `expire_snapshots` | 스냅샷이 참조하는 파일은 지워지지 않음. 만료해야 공간 회수. 되감기 가능 기간과 맞춤 |
| 고아 파일 | 주 1회 `remove_orphan_files`, 보관 기준 **3일 이상** | 실패한 커밋이 남긴 파일 정리. 기준을 쓰기 작업 시간보다 짧게 잡으면 **기록 중인 파일까지 지울 수 있음**(공식 문서 경고) |
| 매니페스트 | 병합과 함께 `rewrite_manifests` | 메타데이터 파일도 작게 쪼개지면 계획 수립이 느려짐 |
| 오래된 메타데이터 | `write.metadata.previous-versions-max`로 개수 제한 | 잦은 커밋마다 metadata.json이 늘어남 |

Trino에서는 다음처럼 실행합니다.

```sql
ALTER TABLE lakehouse.commerce.order_events EXECUTE optimize(file_size_threshold => '128MB');
ALTER TABLE lakehouse.commerce.order_events EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE lakehouse.commerce.order_events EXECUTE remove_orphan_files(retention_threshold => '3d');
```

병합은 데이터를 바꾸지 않고 파일 배치만 바꿉니다. 새 스냅샷이 생기고 조회는 중단 없이 이어집니다. 파티션을 너무 잘게 나누면 쿼리 계획 시간이 늘어납니다. 고객별 조회와 월별 전체 집계는 요구하는 배치가 다르므로 실제 조회 조건으로 설계합니다.

## 6. 처음부터 볼 운영 지표

| 지표 | 왜 |
| --- | --- |
| 이벤트 발생 → Trino 조회까지 걸리는 시간 | 반영 지연 SLA |
| Kafka Consumer Lag와 마지막 성공 커밋 시각 | Writer가 멈췄는지 |
| 원본 대비 적재 건수와 중복 이벤트 수 | 정합성 |
| 파일 수·크기와 유지보수 작업 실패 | 조회 성능 저하의 선행 신호 |
| 대표 쿼리의 응답 시간과 읽은 데이터량 | 파티션·파일 배치가 조회 조건과 맞는지 |

이 목록은 기업들의 공통 표준이 아니라, 확인한 사례를 우리 구성에 적용한 제안입니다.

## 관련 문서

- [03 데이터는 어떻게 저장되고 조회되는가](03-write-and-read-flow.md)
- [05 콜드 데이터 이관](05-cold-data-migration.md) — 삭제 정책 구분
- [Kafka: 프로듀서·토픽·컨슈머 핵심 정리](../../kafka/concepts/16-producer-topic-consumer-summary.md) — 커밋·멱등 처리 원리

## 참고한 공식 문서

- [Iceberg — Maintenance](https://iceberg.apache.org/docs/latest/maintenance/)
- [Iceberg — Kafka Connect](https://iceberg.apache.org/docs/latest/kafka-connect/)
- [Trino — Iceberg connector (ALTER TABLE EXECUTE)](https://trino.io/docs/current/connector/iceberg.html)
- [Debezium — Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)
