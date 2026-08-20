# 컨슈머 (메시지 받기)

> 핵심은 두 가지입니다. **처리 완료 후 수동 커밋**, 그리고 **같은 메시지를 두 번 받아도 안전한(멱등) 처리**. Kafka는 "최소 한 번(at-least-once)" 전달이 기본이라, 중복 수신은 예외 상황이 아니라 **정상 동작**입니다.

## 1. 컨슈머 그룹이 동작하는 방식 (최소 배경지식)

- 같은 `group.id`를 가진 컨슈머들이 **컨슈머 그룹**을 이루고, 토픽의 파티션을 나눠 맡습니다. **한 파티션은 그룹 내 한 컨슈머만** 읽습니다.
- 그래서 병렬 처리 상한 = 파티션 수입니다. 파티션 6개인 토픽에 컨슈머를 7개 띄우면 1개는 유휴 상태가 됩니다.
- 그룹은 파티션별로 **오프셋(어디까지 처리했는지)** 을 커밋해 둡니다. 컨슈머가 죽거나 재배포되면 다른 컨슈머가 그 파티션을 이어받아 **마지막 커밋 지점부터** 다시 읽습니다 — 여기서 중복 수신이 생깁니다.

## 2. 왜 수동 커밋인가

- 자동 커밋(`enable.auto.commit=true`)은 **처리 완료와 무관하게** 주기적으로 커밋합니다. 메시지를 받아서 처리하던 중에 커밋이 먼저 나가고 앱이 죽으면, 재시작한 컨슈머는 그 메시지를 건너뜁니다 → **유실**.
- 수동 커밋은 순서를 뒤집습니다: **처리 성공 → 커밋**. 처리 중에 죽으면 커밋이 없으므로 재시작 후 같은 메시지를 다시 받습니다 → 유실 대신 **중복**. 중복은 멱등 처리로 흡수할 수 있지만, 유실은 복구할 수 없습니다.

## 3. 리스너 코드 — 수동 커밋 + 멱등 처리

```java
@Component
public class OrderCreatedListener {

    private final NotificationService notificationService;
    private final ProcessedEventRepository processedRepo;   // 멱등 판정용 (DB 등)

    public OrderCreatedListener(NotificationService notificationService,
                                ProcessedEventRepository processedRepo) {
        this.notificationService = notificationService;
        this.processedRepo = processedRepo;
    }

    @KafkaListener(topics = "commerce.order.created", groupId = "notification-service")
    public void handle(OrderCreatedEvent event, Acknowledgment ack) {
        // 1) 같은 메시지를 두 번 받아도 안전하게 (재시도·리밸런싱으로 중복 수신 가능)
        if (processedRepo.existsById(event.orderId())) {
            ack.acknowledge();        // 이미 처리 → 커밋하고 넘어감
            return;
        }

        // 2) 실제 처리
        notificationService.sendOrderConfirmation(event);

        // 3) 처리 완료 기록 + 수동 커밋
        processedRepo.save(new ProcessedEvent(event.orderId()));
        ack.acknowledge();            // ★ 처리 성공 후에만 커밋 (자동 커밋 X)
    }
}
```

`group-id`는 서비스별로 고유하게 정합니다. 같은 토픽을 여러 서비스가 구독하면 서비스마다 그룹이 달라야 각자 전체 메시지를 받습니다(그룹이 같으면 나눠 받게 됩니다).

## 4. 멱등 처리 패턴

중복 판정 기준은 이벤트 안의 **식별자**([01 토픽 명명 규칙](01-topic-naming.md) 6장에서 식별자를 필수로 넣는 이유)입니다. 상황에 맞는 패턴을 고르세요.

| 패턴 | 방법 | 어울리는 경우 |
| --- | --- | --- |
| 처리 이력 테이블 | 위 코드처럼 `processed_event(id)` 저장 후 존재 여부 확인. 비즈니스 변경과 **같은 트랜잭션**으로 저장해야 정확 | 외부 호출·알림 등 되돌릴 수 없는 부수효과가 있을 때 |
| 자연스러운 멱등(upsert) | `INSERT ... ON CONFLICT UPDATE`, `save()` 덮어쓰기 | 이벤트가 "최신 상태"를 담고 있을 때 |
| 상태 전이 검증 | 현재 상태보다 과거인 이벤트는 무시 (`이미 PAID인데 CREATED가 다시 옴` → skip) | 상태 머신이 있는 엔티티(주문 등). 중복과 역순을 동시에 방어 |

## 5. 리밸런싱 — 중복이 생기는 대표 경로

그룹에 컨슈머가 들어오거나 나가면(배포, 스케일 인/아웃, 장애) 파티션 배정을 다시 하는 **리밸런싱**이 일어납니다. 이때 "처리는 끝났지만 커밋 전"이던 메시지를 새 담당 컨슈머가 다시 받습니다.

- 배포할 때마다 리밸런싱이 일어난다고 생각하고, 멱등 처리를 전제로 두세요.
- 처리가 느려서 `max.poll.interval.ms`(기본 5분)를 넘기면 브로커가 컨슈머를 죽은 것으로 보고 강제 리밸런싱합니다. 한 번의 `poll()`로 받는 양(`max.poll.records`)을 줄이거나 처리를 빠르게 만드세요. 이 상황은 "처리 → 리밸런싱 → 재수신 → 또 5분 초과"의 **무한 중복 루프**가 되기 쉽습니다.

## 6. 병렬 처리

- 처리량을 늘리려면 컨슈머 인스턴스(또는 `@KafkaListener(concurrency = "N")`)를 늘리되, **파티션 수가 상한**입니다. 파티션보다 많은 컨슈머는 놀게 되고, 그 이상이 필요하면 파티션 증설을 요청합니다([02 토픽 요청하기](02-topic-request.md)).
- 리스너 안에서 받은 메시지를 별도 스레드풀에 던지지 마세요. 같은 파티션(=같은 key) 메시지의 처리 순서가 뒤섞이고, 커밋 시점도 애매해집니다. 병렬화는 **파티션 단위**로 하는 것이 Kafka의 방식입니다.

## 7. DON'T

- `ack.acknowledge()`를 처리 **전에** 호출하기 — 자동 커밋과 같은 유실 문제가 생깁니다.
- 리스너 안에서 멀티스레드로 순서 섞기.
- 여러 서비스가 같은 `group.id` 공유 — 메시지를 나눠 받아 서로 일부만 처리하게 됩니다.
- 멱등 처리 생략 — "우리는 중복 안 생기던데요"는 리밸런싱 없는 평시 이야기입니다. 배포·장애 때 반드시 생깁니다.
