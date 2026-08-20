# 프로듀서 (메시지 보내기)

> 핵심은 세 가지입니다. **유실 방지는 `acks=all` + 멱등**, **순서는 key**, **효율은 압축 + 배치**. 앞의 둘은 설정([05 접속 설정](05-connection-config.md))으로 한 번에 끝나고, key만 코드에서 챙기면 됩니다.

## 1. 왜 `acks=all` + `enable.idempotence=true`인가

- `acks`는 "브로커가 어디까지 저장했을 때 성공으로 볼 것인가"입니다.
  - `acks=0`: 응답을 기다리지 않음 → 네트워크 오류만 나도 유실. 금지.
  - `acks=1`: 리더만 저장하면 성공 → 리더가 복제 전에 죽으면 **성공 응답을 받은 메시지가 유실**됩니다.
  - `acks=all`: ISR(동기화된 복제본)들이 저장해야 성공 → 클러스터 표준인 `min.insync.replicas=2`와 조합하면 **브로커 1대가 죽어도 무손실**입니다.
- `enable.idempotence=true`는 재시도로 인한 **중복과 순서 역전**을 막습니다. 전송 실패로 프로듀서가 재시도할 때, 브로커가 시퀀스 번호로 "이미 받은 배치"를 걸러내고(중복 방지), 앞 배치가 실패하면 뒤 배치도 함께 거절해(순서 보존) 재시도하게 합니다.
- 유실이 허용되는 대량 로그·메트릭이면 그 토픽만 `acks=1`을 쓸 수도 있지만, **기본은 `acks=all`** 입니다.

## 2. 순서가 중요하면 key를 지정하세요

Kafka의 순서 보장은 **"같은 파티션 안에서만"** 성립합니다. key를 지정하면 `hash(key) % 파티션수`로 파티션이 정해지므로, **같은 key의 메시지는 항상 같은 파티션에 순서대로** 쌓입니다.

- key는 **순서를 지켜야 하는 단위**로 정합니다. 주문 이벤트라면 orderId, 사용자 상태라면 userId.
- key가 없으면 배치 단위로 파티션에 분산돼(sticky partitioner) 순서 개념이 없습니다.
- 순서가 필요한 이벤트는 **한 토픽**에 있어야 합니다 — 토픽 사이에는 순서 보장이 없습니다([01 토픽 명명 규칙](01-topic-naming.md) 3장).
- 추가 조건: 같은 key의 이벤트는 **한 프로듀서 인스턴스**에서 발생 순서대로 보내야 합니다. 인스턴스 A가 CREATED, B가 PAID를 보내면 도착 순서를 보장할 수 없습니다.

## 3. 전송 코드

```java
@Service
public class OrderEventProducer {

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(OrderCreatedEvent event) {
        // key = orderId  →  같은 주문의 이벤트는 항상 같은 파티션 (그 안에서 순서 보장)
        kafkaTemplate.send("commerce.order.created", event.orderId(), event)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    // 전송 실패 로깅/재처리 (acks=all 이라 여기 오면 실제 실패)
                    log.error("commerce.order.created 발행 실패: {}", event.orderId(), ex);
                }
            });
    }
}
```

- `send(topic, key, value)` 3인자 버전을 쓰세요. key 없는 2인자 버전은 순서가 필요 없는 토픽에서만.
- 비동기 콜백에서 실패를 감지하면 **뒤 이벤트 전송을 중단**하는 것까지가 순서 보장입니다. CREATED가 최종 실패했는데 PAID를 계속 보내면 안 됩니다.
- 메시지마다 `send().get()`으로 동기 대기하지 마세요. 처리량이 급락하고 배치가 1건이 되어 압축 효과도 사라집니다(아래 4장).

## 4. 압축해서 보내기 (`compression-type: lz4`)

프로듀서가 **배치 단위로 압축**해서 보내고, 브로커는 받은 그대로 저장하며(`compression.type=producer` 기본값), 컨슈머 라이브러리가 자동으로 풉니다. 애플리케이션 코드는 압축 여부를 모릅니다 — 설정 한 줄로 네트워크·디스크·복제 트래픽이 줄어듭니다(JSON 이벤트는 보통 3~5배).

| 코덱 | 압축률 | CPU | 언제 |
| --- | --- | --- | --- |
| `lz4` | 중간 | 매우 낮음 | **기본 추천** — 처리량 우선 |
| `zstd` | 높음 | 낮음~중간 | 네트워크/디스크 비용이 중요할 때 |
| `snappy` | 중간 | 낮음 | lz4와 비슷 (오래된 클라이언트 호환) |
| `gzip` | 높음 | 높음 | CPU 여유가 크고 대역폭이 귀할 때만 |

배치 튜닝과 주의점:

- 압축은 **레코드 배치**(같은 파티션으로 가는 메시지 묶음)에 걸립니다. `batch-size`(기본 16KB)를 64KB로 키우고 `linger.ms`를 10~20ms 주면 배치가 커져 압축률이 좋아집니다. 배치가 1건이면 압축 이득이 거의 없습니다.
- 브로커·토픽의 `compression.type`은 기본값 `producer`로 둡니다. 다른 코덱을 지정하면 브로커가 풀어서 다시 압축하느라 CPU를 씁니다.
- `max.request.size`·`message.max.bytes` 한도는 **압축 후 크기** 기준입니다.
- 압축은 암호화가 아닙니다. 전송 보안은 SASL_SSL([05 접속 설정](05-connection-config.md))이 담당합니다.

설정 위치는 [05 접속 설정](05-connection-config.md)의 `application.yml`에 모두 들어 있습니다.

## 5. DON'T

- `acks=0`으로 중요한 데이터 보내기 — 유실됩니다.
- 순서가 필요한 토픽에 key 없이 보내기 — 파티션에 흩어져 순서가 깨집니다.
- 같은 key의 이벤트를 여러 서비스 인스턴스에서 나눠 보내기 — 도착 순서 보장이 없습니다.
- 메시지마다 `send().get()` 동기 전송 — 처리량 급락 + 압축 무력화.
