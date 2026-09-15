# Lakehouse PoC 샘플 프로젝트 설계 (주문·결제)

## 배경

Lakehouse 계층(`lakehouse/`)의 PoC는 지금 `setup.sh --connect`로 콘솔 프로듀서가 넣은 JSON이 Iceberg 테이블에 적재되는 것까지 검증했다. 실제 애플리케이션이 이벤트를 발행하고, 그 결과를 화면에서 조회하는 끝단 시연이 없다. 발표자료 14장의 PoC 단계(수집·보관 검증 → 활용 사례 ① 구현)를 실제 코드로 보여 주는 샘플이 필요하다.

## 목표

- 주문·결제 화면에서 결제를 실행하면 이벤트가 Kafka에 발행되고, Iceberg 테이블에 적재되어, 같은 화면에서 SQL 결과로 조회된다.
- 발표자료 8장의 세 조회(결제 성공률, 취소·환불 현황, 주문·결제 불일치 후보)를 화면으로 보여 준다.
- 반영 지연(결제 → 조회 가능)을 실측해 화면에 표시한다.

## 비목표 (YAGNI)

- Transactional Outbox·Debezium. PoC는 DB 저장 후 직접 발행하고, 운영 전환 시 Outbox로 바꾼다는 점을 README에 명시한다.
- 실제 PG 연동, 인증·권한, 배포 파이프라인.
- Avro·Schema Registry. JSON으로 시작한다.
- Flink, Serving·Search 계층.

## 단계

| 단계 | 산출물 | 완료 기준 |
| --- | --- | --- |
| 1 | Spring Boot 백엔드 + Vue 3 프런트. 주문 생성·결제 화면. 홈랩 Kafka `order.events`·`payment.events` 토픽에 JSON 발행 | 화면에서 결제 → 콘솔 컨슈머로 두 토픽에 이벤트 확인 |
| 2 | 기존 `lakehouse-poc` 커넥터에 두 토픽 추가, 테이블 DDL로 생성 | Trino에서 `commerce.order_events`·`commerce.payment_events` 조회 |
| 3 | 백엔드 조회 API(Trino JDBC) + Vue 조회 화면 3종 + 반영 지연 표시 | 결제 후 화면에서 성공률·불일치 후보가 갱신됨 |

## 아키텍처

```text
Vue 3 (Vite, :5173)
  ├─ 주문·결제 화면 ── POST /api/orders, /api/payments ──▶ Spring Boot (:8090)
  │                                                          ├─ H2 (주문·결제 상태의 기준)
  │                                                          └─ KafkaTemplate ──▶ 홈랩 Kafka (SASL_SSL :9094)
  │                                                                                  order.events / payment.events
  └─ 조회 화면 ── GET /api/analytics/* ──▶ Spring Boot ── Trino JDBC ──▶ Trino (:8080)
                                                                             └─ Iceberg commerce.* (MinIO + REST Catalog)
                                                          ▲
                                             Kafka Connect Iceberg Sink (lakehouse-poc, 2단계)
```

- 백엔드가 쓰기(Kafka)와 읽기(Trino) 양쪽을 담당한다. 프런트는 Trino를 직접 호출하지 않는다.
- 운영 DB 역할은 H2가 맡는다. 주문·결제의 현재 상태는 H2가 기준이고, Lakehouse는 이벤트 이력이다.

## 이벤트 규약

필드명은 스네이크케이스. Iceberg Sink가 JSON 키를 컬럼명으로 쓰고, Trino가 소문자로 정규화하기 때문이다.

공통 필드: `event_id`(UUID), `event_type`, `order_id`, `customer_id`, `occurred_at`(ISO-8601, 오프셋 포함), `schema_version`(1), `source_system`(`order-payment-sample`).

| 토픽 | event_type | 추가 필드 | 키 |
| --- | --- | --- | --- |
| `order.events` | `ORDER_CREATED`, `ORDER_CANCELLED` | `amount`, `item_name` | `order_id` |
| `payment.events` | `PAYMENT_REQUESTED`, `PAYMENT_COMPLETED`, `PAYMENT_FAILED`, `PAYMENT_REFUNDED` | `payment_id`, `amount`, `method`(CARD/BANK/POINT), `failure_reason`(실패 시) | `order_id` |

키를 `order_id`로 두어 같은 주문의 이벤트가 파티션 안에서 순서를 지킨다.

## 시나리오 (3단계 조회가 의미 있으려면 실패·불일치가 있어야 한다)

결제 화면의 가짜 결제 서비스는 다음 규칙으로 동작한다.

- 금액이 1,000,000 이상이면 `PAYMENT_FAILED`(`LIMIT_EXCEEDED`).
- 결제수단 `POINT`이고 금액이 50,000 초과면 `PAYMENT_FAILED`(`INSUFFICIENT_POINT`).
- 화면의 "결제 이벤트 발행 생략" 토글을 켜면 H2에는 결제 완료로 저장하지만 `payment.events`를 발행하지 않는다. 주문·결제 불일치 후보를 만들기 위한 장치다.
- 결제 완료 주문은 "취소" 버튼으로 `ORDER_CANCELLED` + `PAYMENT_REFUNDED`를 발행한다.

## 백엔드

- Java 21, Spring Boot 3.5, Gradle Kotlin DSL(기존 샘플과 통일). 의존성: web, data-jpa, h2, spring-kafka, trino-jdbc, validation.
- 패키지 `com.osstem.sample.orderpayment`: `order`(엔티티·서비스·컨트롤러), `payment`, `event`(이벤트 DTO·프로듀서), `analytics`(Trino 조회), `config`.
- `application.yml` 프로파일: `local`(기본, 홈랩 SASL_SSL, 비밀은 환경변수), 3단계에 `analytics.trino.url` 추가.
- Kafka 설정은 usage-guide 표준: `acks=all`, `enable.idempotence=true`, `compression-type=lz4`, JsonSerializer(타입 헤더 끄기).
- 분석 API는 SQL을 서버에 고정하고 파라미터만 받는다. 조회 시 `event_id` 중복 제거 뷰를 쓴다.

## 프런트

- Vue 3 + Vite, 라우터 2개: `/order`(주문·결제), `/analytics`(조회). 상태 관리 라이브러리 없이 `fetch`.
- 조회 화면: 결제 성공률(일별·수단별 표), 취소·환불 현황(건수·금액), 불일치 후보 목록, 최근 결제의 반영 지연(결제 시각 대비 조회에 나타난 시각).
- 개발 서버 프록시로 `/api` → `localhost:8090`.

## 2단계 연결

- 홈랩에 `order-service` 계정, `order.events`·`payment.events` 토픽(파티션 3), ACL(쓰기). `connect` 계정에 두 토픽 읽기 ACL.
- 테이블은 DDL로 생성(`partitioning = ARRAY['day(occurred_at)']`). 커넥터는 `iceberg.tables=commerce.order_events,commerce.payment_events`에 `iceberg.tables.route-field=... ` 대신 토픽별 커넥터 2개로 단순화.
- `occurred_at`은 JSON 문자열이므로 Sink가 varchar로 넣는다. DDL에서 varchar로 두고 조회 뷰에서 `from_iso8601_timestamp`로 변환한다.

## 위치와 문서

- `lakehouse/examples/order-payment-sample/` 아래 `backend/`, `frontend/`, `README.md`, `scripts/`(홈랩 계정·토픽·ACL, 커넥터 등록, 테이블 DDL).
- README는 1·2·3단계 실행 순서와 확인 방법을 담고, `lakehouse/README.md` 예제 목록에 추가한다.

## 검증

- 1단계: 화면에서 결제 3건(성공·실패·생략) 후 콘솔 컨슈머로 `order.events` 3건, `payment.events` 2건 확인.
- 2단계: Trino에서 두 테이블 건수가 발행 건수와 일치.
- 3단계: 조회 화면에 성공률·불일치 후보 1건이 보이고 반영 지연이 커밋 주기(30초) 안.
- `./gradlew test`, `npm run build` 통과. 저장소 `npm run lint && npm test`, `git diff --check` 통과.
