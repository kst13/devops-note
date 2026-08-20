# 토픽 명명 규칙과 이벤트 데이터

> 토픽 이름은 한 번 정하면 사실상 바꿀 수 없습니다(바꾸려면 새 토픽 + 데이터 마이그레이션 + 모든 앱 수정). 그래서 처음부터 규칙에 맞춰 짓고, 규칙은 자동으로 검증합니다.

## 1. 표준 패턴 — 3단계 고정

```text
<도메인>.<엔티티>.<이벤트>     예) commerce.order.created, billing.payment.completed, inventory.stock.updated
```

| 자리 | 의미 | 예 |
| --- | --- | --- |
| 도메인 | 업무 영역 (조직이 아니라 비즈니스 기준) | `commerce`, `billing`, `inventory` |
| 엔티티 | 이벤트의 주체가 되는 대상 | `order`, `payment`, `stock` |
| 이벤트 | 무슨 일이 일어났나 — **과거형** | `created`, `paid`, `cancelled` |

- **소문자만** 사용하고 계층은 점(`.`)으로 구분합니다. 한 자리 안에서 단어를 이어야 하면 하이픈(`-`)을 씁니다(`flash-sale`).
- **엔티티 자리는 항상 채웁니다(3단계 고정)** — 도메인에 엔티티가 하나뿐이어도 생략하지 않습니다(`order.created` ❌ → `commerce.order.created` ✅). 자리 수가 일정해야 이름만 보고 도메인/엔티티를 파싱할 수 있고, ACL 접두사 패턴·이름 자동 검증(정규식)이 단순해집니다.
- **이벤트는 과거형**으로 씁니다. 토픽에 실리는 것은 "이미 일어난 사실"이지 명령이 아니기 때문입니다. `create-order`(명령형)가 아니라 `order.created`(사실)입니다.
- 임시/실험 토픽은 `sandbox.<이름>` 접두사를 씁니다. 운영 토픽과 한눈에 구분되고, 정리 대상 식별이 쉽습니다.

## 2. 이름에 넣지 않는 것 (안티패턴)

| 넣지 않는 것 | 이유 | 나쁜 예 |
| --- | --- | --- |
| 환경(dev/prod) | 환경은 **클러스터로 분리**하므로 이름은 동일해야 합니다. 이름이 환경마다 다르면 앱 설정이 갈라지고, 운영에서 `dev.` 토픽을 구독하는 사고가 납니다 | `prod.order.created` |
| 팀·서비스명 | 조직 개편·서비스 개명 때마다 이름이 낡습니다. "누가 쓰는가"가 아니라 **"무슨 데이터인가"** 로 짓습니다 | `orderteam.created` |
| 파티션 수·보존 기간 등 설정값 | 설정은 바뀝니다. 바뀌는 메타데이터는 이름 밖(설정·카탈로그)에 둡니다 | `order.created.p6.7d` |

## 3. 이벤트별 토픽 vs 엔티티별 토픽

- 기본은 이벤트별 토픽(`commerce.order.created`, `commerce.order.paid`)입니다. 구독이 정밀해집니다.
- 단, **순서가 필요한 이벤트는 한 토픽에** 담아야 합니다. 이벤트별로 나누면 같은 주문의 이벤트가 여러 토픽으로 갈라지는데, **토픽 사이에는 순서 보장이 없습니다.** 한 주문의 상태 흐름(생성→결제→배송)을 순서대로 처리해야 하면 `commerce.order.events` 하나에 담고, key=orderId로 같은 파티션에 모으고, 이벤트 타입은 값이나 헤더로 구분합니다. 순서 보장의 원리는 [03 프로듀서](03-producer.md)를 참고하세요.

## 4. 파생 토픽과 특수 접두사

- 재시도: `<토픽>.retry`, 실패 격리(DLQ): `<토픽>.dlq` — 접미사는 이 두 개로 고정합니다. Spring Kafka `@RetryableTopic`을 쓰면 기본 접미사(`-retry-N`, `-dlt`)를 규칙에 맞게 바꿉니다.

```java
@RetryableTopic(
        attempts = "3",
        retryTopicSuffix = ".retry",
        dltTopicSuffix = ".dlq"
)
@KafkaListener(topics = "commerce.order.created", groupId = "notification-service")
public void handle(OrderCreatedEvent event, Acknowledgment ack) { /* ... */ }
```

- CDC(DB 변경 캡처) 토픽은 도메인 이벤트와 구분해 `cdc.<db>.<table>`(예: `cdc.orders.orders`)로 짓습니다. Debezium 기본값(`<server>.<schema>.<table>`)을 쓸 때도 접두사가 `cdc.` 계열이 되도록 서버명을 정합니다.
- 버전: **호환이 깨지는 스키마 변경일 때만** `.v2` 접미사로 새 토픽을 만듭니다(`commerce.order.created.v2`). 필드 추가처럼 호환되는 변경은 이름을 유지합니다. 버전 토픽이 난립하면 컨슈머 이관이 끝나지 않습니다.

## 5. 기술 제약과 검증

- 허용 문자 `[a-z0-9._-]`, 길이 249자 이하(실무는 50자 이내 권장).
- `__` 접두사 금지 — Kafka 내부 토픽(`__consumer_offsets` 등)과 충돌합니다.
- 한 이름에서 `.`과 `_` 혼용 금지 — Kafka가 메트릭 이름에서 `.`을 `_`로 바꾸므로 `a.b`와 `a_b`의 JMX 메트릭이 충돌합니다(생성 시 경고가 뜹니다).

CI나 토픽 생성 파이프라인에서 쓸 수 있는 검증 정규식:

```text
^(sandbox\.[a-z0-9.-]+|cdc\.[a-z0-9-]+\.[a-z0-9-]+|[a-z0-9-]+\.[a-z0-9-]+\.[a-z0-9-]+(\.v[0-9]+)?(\.retry|\.dlq)?)$
```

## 6. 이벤트 데이터(값) 설계

메시지 값(value)은 **JSON**을 권장합니다. 예시 이벤트(`commerce.order.created`):

```json
{
  "orderId": "ORD-20260729-0001",
  "customerId": 10234,
  "items": [
    { "sku": "A-100", "qty": 2 },
    { "sku": "B-250", "qty": 1 }
  ],
  "totalAmount": 51000,
  "createdAt": "2026-07-29T09:15:00Z"
}
```

이 이벤트에 대응하는 DTO (Java 21 record):

```java
public record OrderCreatedEvent(
        String orderId,
        Long customerId,
        List<OrderItem> items,
        BigDecimal totalAmount,
        Instant createdAt
) {
    public record OrderItem(String sku, int qty) {}
}
```

설계 원칙:

- **식별자를 반드시 포함**합니다(`orderId`). 컨슈머의 멱등 판정([04 컨슈머](04-consumer.md))과 key 지정([03 프로듀서](03-producer.md))에 쓰입니다.
- **시각은 ISO-8601 + UTC**(`2026-07-29T09:15:00Z`)로 통일합니다. 타임존이 섞이면 컨슈머마다 해석이 달라집니다.
- **필드 추가는 자유, 삭제·이름 변경·타입 변경은 호환이 깨지는 변경**입니다. 깨지는 변경은 `.v2` 새 토픽으로 처리합니다(위 4장).
- 이벤트에는 컨슈머가 판단에 필요한 만큼만 담습니다. 전체 DB 행을 그대로 싣지 않습니다 — 스키마가 DB에 종속되고, 변경 파급이 커집니다.
