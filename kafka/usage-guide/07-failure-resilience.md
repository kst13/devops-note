# Kafka 장애에 대비하는 애플리케이션 설계

> Kafka 클러스터가 전부 멈춰도 여러분의 서비스가 데이터를 잃지 않고 버티게 만드는 방법입니다. 핵심은 하나입니다 — **클러스터 정지를 "유실 사고"가 아니라 "지연 사고"로 만드는 것.** 클러스터 자체의 복구는 플랫폼팀이 하지만([복구 절차](../troubleshooting/cluster-total-outage.md)), 정지 시간 동안 발생한 데이터를 지키는 것은 애플리케이션의 몫입니다.

## TL;DR

- ✅ 중요한 이벤트는 **outbox 패턴**으로 DB에 먼저 기록하고 릴레이가 Kafka로 보내세요
- ✅ 컨슈머는 이미 [멱등 처리](04-consumer.md)가 표준입니다 — 복구 후 재전송·재처리가 반드시 발생하기 때문입니다
- ✅ 프로듀서 예외를 삼키지 마세요 — 잡아서 보관하거나, 최소한 로그·알림을 남기세요
- ✅ Kafka 의존 기능과 핵심 기능을 분리해 **저하 모드**를 설계하세요

## Kafka가 죽으면 여러분의 앱에는 무슨 일이 생기나

| 역할 | 벌어지는 일 | 데이터 위험 |
| --- | --- | --- |
| 프로듀서 | 브로커가 안 보이면 메모리 버퍼(`buffer.memory`, 기본 32MB)에 쌓다가, 버퍼가 차면 `max.block.ms`(기본 60초) 대기 후 **예외 발생** | 예외를 처리하지 않으면 **그 메시지는 유실** |
| 컨슈머 | 폴링해도 결과가 없을 뿐 오류 없이 대기 | 없음 — 읽던 위치는 클러스터에 저장되어 복구 시 이어짐 |
| 이미 토픽에 있던 데이터 | 클러스터 복구와 함께 그대로 돌아옴 (RF3) | 없음 |

즉 위험 구간은 단 하나, **정지 시간 동안 프로듀서가 만드는 새 데이터**입니다.

## Outbox 패턴 — 프로듀서 유실의 정석 해법

Kafka에 직접 쓰지 않고, 서비스의 DB 트랜잭션 안에서 outbox 테이블에 먼저 기록합니다. 별도 릴레이가 outbox를 읽어 Kafka로 보내고, 성공한 행만 지웁니다.

```text
[서비스] --(하나의 DB 트랜잭션)--> [업무 테이블 + outbox 테이블]
                                        │
[릴레이] --(outbox 폴링)--> Kafka 전송 --> 성공 시 행 삭제
```

Kafka가 죽으면 outbox에 쌓일 뿐이고, 복구되면 밀린 것을 순서대로 내보냅니다. 부수 효과로 "DB는 커밋됐는데 이벤트는 발행 안 됨" 같은 이중 쓰기 불일치도 사라집니다 — 업무 데이터와 이벤트가 같은 트랜잭션이기 때문입니다.

```sql
CREATE TABLE outbox (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  topic         VARCHAR(200) NOT NULL,
  message_key   VARCHAR(200),
  payload       JSON NOT NULL,
  created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

```java
@Transactional
public void placeOrder(Order order) {
    orderRepository.save(order);                       // 업무 데이터
    outboxRepository.save(OutboxRecord.of(             // 이벤트 — 같은 트랜잭션
        "commerce.order.created", order.getId(), toJson(order)));
}
```

릴레이는 `@Scheduled` 폴링으로 직접 구현하거나 Debezium(outbox 테이블 CDC)을 사용합니다. 릴레이 전송이 재시도될 수 있으므로 **컨슈머의 멱등 처리가 전제**입니다 — [04 컨슈머](04-consumer.md)의 표준을 그대로 지키면 됩니다.

모든 이벤트에 outbox가 필요한 것은 아닙니다. 유실되면 안 되는 이벤트(주문, 결제, 상태 변경)에 적용하고, 유실을 감수할 수 있는 것(조회 로그, 메트릭)은 아래 폴백으로 충분합니다.

## Outbox까지 안 간다면: 최소한의 폴백

프로듀서 콜백에서 실패를 감지해 보관하거나, 그것도 어려우면 반드시 로그·알림이라도 남깁니다. 가장 나쁜 코드는 예외를 조용히 삼키는 코드입니다.

```java
kafkaTemplate.send("commerce.order.created", key, payload)
    .whenComplete((result, ex) -> {
        if (ex != null) {
            fallbackStore.append(key, payload);   // 로컬 DB/파일에 보관 후 배치 재전송
            alertService.notify("Kafka 전송 실패", ex);
        }
    });
```

## 저하 모드 — Kafka 죽음이 서비스 죽음이 되지 않게

Kafka에 의존하는 기능과 핵심 기능을 분리해 두면, 정지 시간에도 서비스의 본질은 유지됩니다.

- 주문 접수(핵심, DB 기반)는 계속 받고 — 알림 발송·분석 이벤트(Kafka 기반)만 멈췄다가 복구 후 outbox로 따라잡기
- 전송 실패가 반복되면 서킷 브레이커로 Kafka 경로를 잠시 끊어, 스레드가 `max.block.ms` 대기에 묶여 서비스 전체가 느려지는 2차 피해를 차단

"Kafka가 죽으면 우리 서비스의 어떤 기능이 멈추는가"를 답할 수 있어야 하고, 그 답이 "전부"라면 의존 범위를 좁히는 설계 변경이 필요하다는 신호입니다.

## 복구 후에 벌어지는 일

클러스터가 돌아오면 outbox·폴백에 쌓인 메시지가 한꺼번에 전송되고, 컨슈머는 밀린 데이터를 따라잡습니다. 이때 두 가지를 기억하세요.

- **중복은 반드시 옵니다.** 재전송·재처리 과정에서 같은 메시지를 두 번 받는 것은 정상이며, 멱등 처리가 되어 있으면 아무 문제가 없습니다.
- **lag가 크게 보이는 것은 정상입니다.** 밀린 것을 따라잡는 중이므로, lag가 줄어드는 추세인지만 확인하면 됩니다.

궁금한 점은 DevOps 담당(플랫폼팀)에게 문의하세요.
