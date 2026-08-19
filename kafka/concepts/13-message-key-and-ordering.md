# 메시지 키와 순서 보장: 파티션, 전역 순서, 선착순 처리

> "순서가 필요하면 키를 지정하라"는 말의 **정확한 의미**와, 그것만으로 부족한 경우를 정리합니다. 3파티션 토픽에 주문 1~10이 들어오는 예시로 파티션 배정 → 브로커 로그 → 컨슈머 처리까지 따라가고, 전역 순서가 필요한 경우와 선착순(재고 차감) 설계까지 다룹니다. 샘플 코드는 Java + Spring Kafka 기준입니다.

## TL;DR

- Kafka의 순서 보장 단위는 **토픽이 아니라 파티션**입니다. 전역 순서는 없습니다.
- 키를 지정하면 `hash(key) % 파티션 수`로 파티션이 정해지므로 **같은 키 → 같은 파티션 → 순서 유지**. 키가 없으면 흩어집니다.
- Kafka는 **보낸 순서를 보존**할 뿐 정렬해 주지 않습니다. 순서는 프로듀서·브로커·컨슈머 세 층이 각자 지켜야 성립합니다.
- 전역 순서가 정말 필요하면 **파티션 1개**뿐입니다. 대개는 "순서가 필요한 최소 단위"를 찾아 그것을 키로 잡는 것이 답입니다.
- 선착순은 **상품(재고 단위)을 키**로 잡아 오프셋 순서를 순위로 씁니다. Redis를 붙인다고 더 공정해지지는 않고, 더 빠르고 단순해질 뿐입니다.

---

## 1. 원리: 순서 보장은 파티션 안에서만

파티션은 뒤에만 붙일 수 있는(append-only) 로그입니다. 도착한 순서대로 오프셋 0, 1, 2…가 매겨지고 절대 재정렬되지 않습니다. 하지만 파티션이 여러 개면 파티션 사이에는 어떤 순서도 없습니다.

프로듀서가 키를 지정하면 기본 파티셔너가 `murmur2(key) & 0x7fffffff % 파티션 수`로 파티션을 고릅니다. 그래서:

- **같은 키 → 항상 같은 파티션 → 그 키의 메시지는 보낸 순서대로** 쌓입니다.
- 키가 없으면(null) 라운드로빈/sticky 방식으로 흩어져 순서가 깨집니다. **순서가 필요한 이벤트에 키를 빼먹는 것이 가장 흔한 실수**입니다.
- 서로 다른 키 사이의 순서(주문 7 vs 주문 42)는 보장되지 않지만, 보통 필요하지도 않습니다.

그래서 **키 설계 = 순서 설계**입니다. 순서가 필요한 단위(주문 ID, 사용자 ID, 계좌번호 등)를 키로 잡습니다.

### 같은 키는 "언제 보내도" 같은 파티션인가

네 — **파티션 수가 그대로인 한** 항상 같습니다. 파티션 선택은 어딘가에 기록해 둔 상태가 아니라 **순수 계산**이기 때문입니다.

```text
partition = murmur2(keyBytes) & 0x7fffffff  %  파티션 수
```

- 브로커나 프로듀서가 "order-1은 P1"이라고 기억하는 게 아닙니다. 키 바이트가 같고 파티션 수가 같으면 결과가 같을 뿐입니다. 그래서 어떤 프로듀서 인스턴스에서, 며칠 뒤에, 다른 서비스에서 보내도 `order-1`은 같은 파티션입니다. 프로듀서·브로커 재시작, 리더 변경도 무관합니다(리더는 파티션을 어느 브로커가 담당하느냐일 뿐, 파티션 번호는 그대로).
- **보관 기간(retention)과도 무관합니다.** `retention.ms`가 지나 데이터가 전부 삭제돼도, 로그 압축으로 옛 값이 정리돼도, 토픽이 비어 있어도 `order-1`은 같은 파티션으로 갑니다. 삭제는 이미 저장된 세그먼트를 지우는 것이지 파티션 구조를 건드리지 않습니다(파티션은 0건이 되어도 그대로 존재하고 오프셋도 이어서 증가).

| 상황 | `order-1`의 파티션 |
| --- | --- |
| 보관 기간 지나서 데이터 전부 삭제됨 / 로그 압축됨 / 토픽이 빈 상태 | **그대로** |
| 프로듀서·브로커 재시작, 리더 변경 | 그대로 |
| 토픽을 삭제하고 같은 이름·같은 파티션 수로 재생성 | 그대로 (계산식이 같으므로) |
| **파티션 수 변경** (증설, 다른 수로 재생성) | **바뀔 수 있음** — 기존 데이터는 옮겨지지 않아 같은 주문의 옛 이벤트와 새 이벤트가 갈라짐 |
| 키 문자열이 미묘하게 다름 (`order-1` vs `order-01` vs `ORDER-1`, 공백) | 다른 키 → 다른 파티션일 수 있음 |
| 키 직렬화 방식이 다름 (String vs JSON `"order-1"` 따옴표 포함) | 해시는 **직렬화된 바이트**로 계산 → 다른 파티션일 수 있음 |
| 다른 파티셔너 (커스텀, 또는 librdkafka 기본 crc32처럼 언어별 기본 해시가 다름) | Java 와 다른 언어 프로듀서가 섞이면 같은 키가 다른 파티션 → `murmur2_random` 등으로 통일 |
| 키 null / 파티션 번호 직접 지정 | 해시를 쓰지 않음 |

즉 파티션 배정을 바꾸는 요인은 **키 바이트, 파티션 수, 파티셔너 알고리즘** 셋뿐이고, 데이터가 있고 없고는 거기 들어가지 않습니다. 실무 함의: 순서가 중요한 토픽은 파티션 수를 처음에 넉넉히 정하고 바꾸지 않으며, 여러 서비스·언어가 같은 토픽에 쓸 때는 키 포맷·직렬화·파티셔너를 통일합니다.

한 가지 구분할 것: "데이터가 사라져도 같은 파티션"이라 **순서는 유지**되지만, 이미 지워진 이벤트를 컨슈머가 다시 읽을 수는 없습니다. `order-1`의 CREATED가 보관 기간이 지나 삭제된 뒤 PAID가 들어오면 새로 붙는 컨슈머는 PAID만 봅니다. 상태 재구성이 필요한 토픽은 `cleanup.policy=compact`(키별 최신 값 보존)를 쓰거나 보관 기간을 충분히 길게 잡습니다 — 이건 파티션 배정이 아니라 보존 정책의 문제입니다.

## 2. 예시: 3파티션 토픽에 주문 1~10

키를 `order-1`~`order-10` 문자열로 보내면 기본 파티셔너는 실제로 다음과 같이 배정합니다(위 해시식으로 계산한 값).

| 파티션 | 배정된 주문 |
| --- | --- |
| P0 | order-2, order-3, order-6, order-10 |
| P1 | order-1, order-7, order-9 |
| P2 | order-4, order-5, order-8 |

4/3/3으로 균등하지 않아도 정상입니다. 중요한 건 같은 주문은 항상 같은 파티션이라는 점입니다.

### 프로듀서가 보낸 순서 (시간순, 주문끼리 섞여 있음)

```text
t01 order-1  CREATED     t11 order-7  CREATED     t21 order-8  PAID
t02 order-2  CREATED     t12 order-3  PAID        t22 order-9  PAID
t03 order-3  CREATED     t13 order-8  CREATED     t23 order-3  SHIPPED
t04 order-1  PAID        t14 order-9  CREATED     t24 order-10 PAID
t05 order-4  CREATED     t15 order-5  PAID        t25 order-5  SHIPPED
t06 order-2  PAID        t16 order-2  SHIPPED     t26 order-7  SHIPPED
t07 order-5  CREATED     t17 order-10 CREATED     t27 order-6  SHIPPED
t08 order-1  SHIPPED     t18 order-7  PAID        t28 order-8  SHIPPED
t09 order-6  CREATED     t19 order-4  SHIPPED     t29 order-9  SHIPPED
t10 order-4  PAID        t20 order-6  PAID        t30 order-10 SHIPPED
```

`t01`~`t30`은 도착 순서이고 세 칸은 지면 배치일 뿐입니다("동시"나 "서버 1/2/3"이 아닙니다).

### 브로커에 쌓인 모습 (파티션별 로그, 오프셋 순)

```text
P0 (order-2,3,6,10)                 P1 (order-1,7,9)                 P2 (order-4,5,8)
off 0  order-2  CREATED  (t02)      off 0  order-1  CREATED (t01)    off 0  order-4  CREATED (t05)
off 1  order-3  CREATED  (t03)      off 1  order-1  PAID    (t04)    off 1  order-5  CREATED (t07)
off 2  order-2  PAID     (t06)      off 2  order-1  SHIPPED (t08)    off 2  order-4  PAID    (t10)
off 3  order-6  CREATED  (t09)      off 3  order-7  CREATED (t11)    off 3  order-8  CREATED (t13)
off 4  order-3  PAID     (t12)      off 4  order-9  CREATED (t14)    off 4  order-5  PAID    (t15)
off 5  order-2  SHIPPED  (t16)      off 5  order-7  PAID    (t18)    off 5  order-4  SHIPPED (t19)
off 6  order-10 CREATED  (t17)      off 6  order-9  PAID    (t22)    off 6  order-8  PAID    (t21)
off 7  order-6  PAID     (t20)      off 7  order-7  SHIPPED (t26)    off 7  order-5  SHIPPED (t25)
off 8  order-3  SHIPPED  (t23)      off 8  order-9  SHIPPED (t29)    off 8  order-8  SHIPPED (t28)
off 9  order-10 PAID     (t24)
off 10 order-6  SHIPPED  (t27)
off 11 order-10 SHIPPED  (t30)
```

P0만 보면 order-2는 off 0→2→5, order-3은 1→4→8, order-6은 3→7→10, order-10은 6→9→11로 **각 주문의 CREATED→PAID→SHIPPED가 오프셋 증가 순서 그대로** 유지됩니다.

### 컨슈머 그룹(컨슈머 3개)이 읽는 모습

```text
consumer-A ← P0 : order-2, 3, 6, 10 을 오프셋 0→11 순서로
consumer-B ← P1 : order-1, 7, 9   을 오프셋 0→8  순서로
consumer-C ← P2 : order-4, 5, 8   을 오프셋 0→8  순서로
```

- consumer-A가 order-2 SHIPPED(off 5)를 처리할 때는 CREATED(off 0)·PAID(off 2)를 이미 끝낸 뒤입니다 → **주문 안 순서 보장**.
- consumer-B가 order-1을 다 끝냈을 때 consumer-A는 아직 order-2 PAID를 처리 중일 수 있습니다 → **주문 사이 순서는 무관**, 3개가 병렬로 돕니다.

### 같은 조건에서 깨지는 시나리오

| 상황 | 결과 |
| --- | --- |
| 키 없이 전송 | t01~t30이 라운드로빈으로 흩어짐. order-1의 CREATED(P0)·PAID(P1)·SHIPPED(P2)를 서로 다른 컨슈머가 동시에 읽어 SHIPPED가 먼저 처리될 수 있음 |
| 컨슈머 4개 | 파티션이 3개뿐이라 하나는 놀고 있음. 순서는 유지, 처리량은 늘지 않음 |
| 컨슈머 1개 | P0·P1·P2를 혼자 읽음. 순서는 유지, 병렬성 없음 |
| 파티션을 4개로 증설 | `hash % 4`로 바뀜 → order-3의 이후 이벤트가 P3으로 감. 기존 P0에 남은 PAID보다 P3의 SHIPPED가 먼저 소비될 수 있음 |
| 컨슈머가 레코드를 스레드풀에 던짐 | off 2(order-2 PAID)와 off 5(order-2 SHIPPED)를 다른 스레드가 처리 → 순서 역전 가능 |

### 직접 재현하기

[1노드 KRaft 예제](../examples/compose-1node-kraft/README.md)에서 3파티션 토픽을 만든 뒤 `order-N:STATUS` 형식으로 붙여 넣으면 됩니다. 키 문자열이 정확히 `order-1`~`order-10`이면 위 표와 같은 파티션 번호가 나옵니다(키를 `1`, `2`처럼 바꾸면 해시가 달라져 배정도 달라집니다).

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9094 \
  --command-config /etc/kafka/secrets/client-sasl-ssl.properties \
  --create --topic order-events --partitions 3 --replication-factor 1

# 키 있는 프로듀서: "키:값" 형식으로 입력
docker compose exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9094 \
  --producer.config /etc/kafka/secrets/client-sasl-ssl.properties \
  --topic order-events --property parse.key=true --property key.separator=:

# 파티션·오프셋·키를 함께 출력하는 컨슈머
docker compose exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9094 \
  --consumer.config /etc/kafka/secrets/client-sasl-ssl.properties \
  --topic order-events --from-beginning \
  --property print.partition=true --property print.offset=true --property print.key=true
```

## 3. CREATED→PAID→SHIPPED는 어떻게 보장되나

Kafka는 **보낸 순서를 파티션 안에서 그대로 보존할 뿐, 비즈니스 순서를 정렬해 주지 않습니다.** 세 층이 각자 자기 몫을 지켜야 성립합니다.

### 프로듀서 — 순서대로, 같은 파티션으로, 재시도에도 안 뒤바뀌게

| 조건 | 왜 필요한가 |
| --- | --- |
| 애플리케이션이 실제 발생 순서대로 `send()` 호출 | Kafka는 도착 순서를 기록할 뿐입니다. 앱이 PAID를 먼저 보내면 파티션에도 PAID가 먼저 쌓입니다 |
| 같은 키(주문 ID) | 같은 파티션으로 가야 하나의 로그에 순서대로 append됩니다 |
| 한 프로듀서 인스턴스에서 전송 | 인스턴스 A가 CREATED, B가 PAID를 거의 동시에 보내면 누가 먼저 브로커에 도착할지 보장이 없습니다. 같은 주문의 이벤트는 한 인스턴스(또는 DB 순서를 따르는 아웃박스 릴레이 하나)에서 나가야 합니다 |
| `enable.idempotence=true` (Kafka 3.x 기본) | 배치 1(CREATED)이 재시도되는 사이 배치 2(PAID)가 먼저 성공하면 순서가 뒤집힙니다. 멱등 프로듀서는 시퀀스 번호로 브로커가 순서를 검사해 앞 배치가 실패하면 뒤 배치도 거절하고 함께 재시도합니다 (`max.in.flight.requests.per.connection ≤ 5` 조건) |
| `acks=all` + 실패 시 뒤 이벤트 중단 | CREATED 전송이 최종 실패했는데 PAID를 계속 보내면 안 됩니다. 비동기 `send()`라면 콜백에서 실패를 감지해 후속 전송을 막습니다 |

```java
@Service
@RequiredArgsConstructor
public class OrderEventPublisher {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public void publish(OrderEvent event) {
        // 키 = 주문 ID → 같은 주문의 이벤트는 항상 같은 파티션, 따라서 순서 보장
        kafkaTemplate.send("order-events", event.orderId(), event);
    }
}
```

```yaml
spring:
  kafka:
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      properties:
        enable.idempotence: true                    # 재전송해도 중복·순서 역전 없음
        max.in.flight.requests.per.connection: 5    # 멱등성 켜면 5 이하까지 순서 유지
```

### 브로커 — 파티션은 append-only 로그

도착한 순서대로 오프셋이 매겨지고 절대 재정렬되지 않습니다. 리더가 바뀌어도 팔로워는 리더의 로그를 그대로 복제하므로 순서는 유지됩니다.

### 컨슈머 — 순서대로 읽고, 순서대로 처리하고, 처리한 뒤 커밋

| 조건 | 왜 필요한가 |
| --- | --- |
| 한 파티션은 그룹 내 한 컨슈머만 읽음 | Kafka가 보장합니다. 파티션 안 순서 = 그 컨슈머가 받는 순서 |
| 파티션 안에서 순차 처리 | `poll()`로 받은 레코드를 별도 스레드풀에 던지면 PAID가 CREATED보다 먼저 끝날 수 있습니다. 파티션당 단일 스레드로 처리하거나, 병렬화하더라도 키 단위로 같은 워커에 고정합니다 |
| 처리 완료 후 커밋 | CREATED 처리 도중 죽었는데 오프셋이 이미 커밋돼 있으면, 재시작 시 PAID부터 읽습니다. 처리 → 커밋 순서(`AckMode.MANUAL` 등)를 지킵니다 |
| 재처리 시 멱등하게 | 처리 후 커밋 전에 죽으면 CREATED가 두 번 옵니다. 순서는 맞지만 중복이 생기므로 컨슈머는 멱등해야 합니다 |

### Kafka가 못 막는 것과 실무 방어선

- **파티션 수 변경** — `hash % N`이 바뀌어 같은 주문이 다른 파티션으로 가기 시작합니다. 순서가 중요한 토픽은 파티션 수를 처음부터 넉넉히 정합니다.
- **여러 프로듀서/여러 토픽** — 주문 서비스는 CREATED, 결제 서비스는 PAID를 다른 토픽에 보내면 순서 개념 자체가 없습니다.
- **핫 파티션** — 키가 한쪽으로 몰리면(대형 고객 하나) 특정 파티션만 커집니다. 순서 단위를 너무 크게 잡지 않는 것도 키 설계의 일부입니다.

그래서 컨슈머는 보통 **"이 상태 전이가 유효한가"** 를 한 번 더 확인합니다.

```java
@KafkaListener(topics = "order-events", concurrency = "3")
public void on(ConsumerRecord<String, OrderEvent> record) {
    Order order = orderRepository.findById(record.key()).orElse(null);
    OrderEvent event = record.value();

    // 이미 지나간 상태(중복·역순)면 무시하고, 아직 앞 단계가 안 왔으면 보류/재시도
    if (order != null && order.status().isAtOrAfter(event.status())) {
        return;                              // 멱등: 재처리·역순 무시
    }
    if (!OrderStatus.canTransition(order == null ? null : order.status(), event.status())) {
        throw new OutOfOrderException();     // 재시도 큐/DLT로 → 앞 이벤트가 오면 성공
    }
    orderRepository.apply(event);
}
```

이벤트에 **버전 번호(또는 발생 시각)** 를 넣어 `event.version <= order.version`이면 버리는 방식도 흔합니다.

**한 줄 요약**: 같은 키 + 한 프로듀서에서 발생 순서대로 + 멱등 프로듀서 → 파티션 로그에 순서대로 append → 파티션당 단일 컨슈머가 순차 처리 후 커밋. 이 사슬 중 하나라도 끊기면 순서가 깨지므로 컨슈머는 상태 전이 검증으로 한 번 더 방어합니다.

## 4. 주문 1~10 사이의 순서(전역 순서)가 필요하다면

**전역 순서가 필요하면 그 순서 단위 전체가 한 파티션에 들어가야 합니다.** Kafka는 파티션 밖의 순서를 보장할 방법이 없기 때문에, 선택지는 "파티션을 하나로 모으느냐" 아니면 "순서 요구사항을 다시 정의하느냐" 둘 중 하나입니다.

| 요구 | 답 |
| --- | --- |
| 정말로 모든 주문 이벤트가 하나의 시간축 위에 있어야 함 | **파티션 1개** (RF는 3 유지). 처리량 한계를 받아들임 |
| "1~10 순서"가 사실은 고객/매장/재고 단위 안의 순서 | 그 단위를 **키**로 잡고 파티션은 여러 개 |
| 이미 다중 파티션 토픽이고 못 바꿈 | 시퀀스 번호 + 컨슈머 재정렬 (권장하지 않음) |
| 순서 없이도 맞는 상태를 만들 수 있음 | 버전/스냅샷 이벤트로 **순서 의존을 제거** |

### 선택지 1. 파티션 1개 토픽

```bash
kafka-topics.sh --create --topic order-events --partitions 1 --replication-factor 3
```

- t01~t30 발생 순서가 그대로 오프셋 순서가 됩니다. 컨슈머 그룹에 컨슈머를 몇 개 띄우든 하나만 읽고 나머지는 대기합니다.
- 복제 팩터는 그대로 두므로 **가용성은 잃지 않습니다.** 잃는 건 병렬성(처리량)뿐입니다.
- 이때도 3절의 조건(한 프로듀서에서 발생 순서대로, 멱등 프로듀서, 컨슈머 단일 스레드 순차 처리 후 커밋)은 그대로 필요합니다.
- 파티션 하나는 보통 초당 수천~수만 건은 문제없습니다. "주문 이벤트 전역 순서" 수준의 트래픽이면 대개 충분합니다.

### 선택지 2. 키를 "순서 단위"만큼 크게

| 순서가 필요한 범위 | 키 예시 | 결과 |
| --- | --- | --- |
| 정말 모든 주문 사이 | 고정 문자열 `"orders"` | 전부 한 파티션 → 사실상 선택지 1과 같고, 나머지 파티션은 낭비 |
| 같은 고객의 주문 사이 | `customer-42` | 고객별로 순서 보장, 고객 간 병렬 |
| 같은 매장/테넌트의 주문 사이 | `store-7` | 매장별 순서, 매장 간 병렬 |

실무에서 "1~10 순서가 지켜져야 한다"는 요구를 파고들면 대부분 "같은 고객/같은 매장/같은 재고 SKU 안에서"로 좁혀집니다. 그러면 그 단위를 키로 잡아 순서와 병렬성을 둘 다 얻습니다.

### 선택지 3. 컨슈머가 순서를 복원

프로듀서가 각 이벤트에 전역 시퀀스 번호(DB 시퀀스, 아웃박스 PK 등)를 붙이고 컨슈머가 시퀀스 순으로 다시 정렬합니다. 느린 파티션 하나가 전체를 막고, 버퍼 메모리·재시작 시 상태 복구가 필요해 **복잡하고 느립니다.** 어차피 시퀀스를 발급하는 곳이 단일 지점이므로 그 지점에서 파티션 1개 토픽으로 보내는 게 훨씬 단순합니다.

### 선택지 4. 순서 요구를 없애는 설계

- 이벤트에 버전/타임스탬프를 넣고 "내가 가진 것보다 오래된 이벤트는 무시" (last-writer-wins)
- "상태 변경 명령" 대신 **최종 상태 스냅샷**(`status=SHIPPED, version=3`)을 보내기 — 도착 순서가 뒤바뀌어도 버전으로 판단
- 3절의 상태 전이 검증

Kafka에서 전역 순서와 파티션 병렬성은 트레이드오프이지 둘 다 얻는 옵션은 없습니다. 설계 질문은 "어떻게 전역 순서를 보장하지?"가 아니라 **"순서가 정말 필요한 최소 단위가 무엇인가?"** 입니다.

## 5. 선착순 처리 (재고·쿠폰·좌석)

선착순은 "이 상품에 대한 요청 중 **먼저 도착한 N개**만 성공"이니, 순서 단위는 주문이 아니라 **상품(재고 단위)** 입니다. 키를 상품 ID로 잡고 파티션 로그의 오프셋 순서를 곧 도착 순위로 씁니다.

```text
사용자 요청 ──▶ API 서버(여러 대) ──▶ Kafka topic: flash-sale-requests (key = itemId)
                                          │
                                          ▼  파티션당 컨슈머 1개, 오프셋 순서로 처리
                                       재고 차감 (DB / Redis) ──▶ 결과 이벤트/알림
```

### 예시 — 상품 A(재고 3), B(재고 2), C(재고 5)

API 서버 3대로 들어온 요청을 도착 순서로 나열하면:

```text
t01 item-A u101   t06 item-A u106   t11 item-C u111
t02 item-B u102   t07 item-B u107   t12 item-A u112
t03 item-A u103   t08 item-C u108   t13 item-B u113
t04 item-C u104   t09 item-A u109
t05 item-A u105   t10 item-A u110
```

키 해시로 파티션에 배정되고(설명용 배정), 컨슈머는 오프셋 순으로 재고를 차감합니다.

```text
P0 (item-A, 재고 3)         P1 (item-B, 재고 2)        P2 (item-C, 재고 5)
off 0 u101 → 성공 (남 2)    off 0 u102 → 성공 (남 1)   off 0 u104 → 성공 (남 4)
off 1 u103 → 성공 (남 1)    off 1 u107 → 성공 (남 0)   off 1 u108 → 성공 (남 3)
off 2 u105 → 성공 (남 0)    off 2 u113 → 매진          off 2 u111 → 성공 (남 2)
off 3 u106 → 매진
off 4 u109 → 매진
off 5 u110 → 매진
off 6 u112 → 매진
```

- item-A는 u101·u103·u105가 당첨. **API 서버가 여러 대여도** P0에 append된 순서가 유일한 기준입니다.
- 컨슈머는 오프셋 순서로만 재고를 차감하므로 **경쟁 조건(race)이 없습니다.** 재고 차감에 락이 필요 없어지는 것이 이 설계의 가장 큰 장점입니다.

### "동시"에 들어오면?

API 서버 3대에 u101, u106, u109가 같은 순간 들어왔다고 해도, 세 서버의 프로듀서 요청은 모두 P0 리더 브로커로 가고 리더는 **하나씩 순서대로** append합니다. 어느 서버의 요청이 먼저 리더에 닿느냐(네트워크 지연, 배칭 타이밍, 서버 부하)에 따라 오프셋이 정해집니다.

```text
09:00:00.000  API-1 ← u101,  API-2 ← u106,  API-3 ← u109   (모두 item-A)

P0
off 0  u106   ← API-2 요청이 리더에 가장 먼저 도착
off 1  u101
off 2  u109
```

**파티션 입장에서 "동시"는 존재하지 않습니다.** 반드시 하나의 전순서가 만들어지고 그것이 선착순 순위입니다. 같은 서버 안에서는 `send()` 호출 순서, 서버 사이에서는 브로커 도착 순서가 기준입니다.

### 반드시 지켜야 할 것

| 계층 | 조건 | 이유 |
| --- | --- | --- |
| 프로듀서 | 키 = 상품 ID, `enable.idempotence=true`, `acks=all` | 같은 파티션·재시도 시 순서 유지·유실 없음 |
| 프로듀서 | 요청마다 고유 requestId 를 메시지에 포함 | 컨슈머 재처리 시 중복 당첨 방지 |
| 프로듀서 | `send()` 성공 콜백 후 사용자에게 "접수됨" 응답 | 브로커에 기록된 것만 접수로 인정 |
| 토픽 | **파티션 수 절대 변경 금지** (이벤트 진행 중) | 증설하면 같은 상품이 두 파티션에 갈라져 순서·재고 계산이 깨짐 |
| 컨슈머 | 파티션당 단일 스레드 순차 처리 | 병렬 처리하면 오프셋 순서 ≠ 처리 순서 |
| 컨슈머 | 재고 차감 + requestId 기록을 한 트랜잭션으로, 처리 후 커밋 | 죽었다 재시작해도 이미 처리한 요청은 건너뜀(멱등) |
| 컨슈머 | 컨슈머 수 ≤ 파티션 수 | 남는 컨슈머는 놀 뿐, 처리량은 파티션 수로 결정 |

```java
@KafkaListener(topics = "flash-sale-requests", concurrency = "3")
@Transactional
public void on(ConsumerRecord<String, SaleRequest> rec, Acknowledgment ack) {
    SaleRequest req = rec.value();
    if (processedRequestRepo.exists(req.requestId())) { ack.acknowledge(); return; } // 재처리 멱등

    // 이 파티션은 이 스레드만 읽으므로 상품 재고에 대한 경쟁이 없음 → 락 불필요
    boolean won = stockRepo.decrementIfAvailable(req.itemId());        // UPDATE ... SET qty=qty-1 WHERE qty>0
    processedRequestRepo.save(req.requestId(), won ? WON : SOLD_OUT);
    resultPublisher.send("flash-sale-results", req.userId(), new SaleResult(req.requestId(), won));
    ack.acknowledge();                                                  // 처리 완료 후 커밋
}
```

### 공정성의 현실적 정의

- Kafka가 보장하는 건 **브로커 파티션에 도착한 순서**입니다. 사용자가 버튼을 누른 절대 시각 순서는 어떤 시스템도 알 수 없습니다.
- 그래서 "선착순 = 파티션 append 순서"로 정의하고 그 정의를 흔들지 않는 데 집중합니다. 프로듀서 배칭(`linger.ms`)은 같은 파티션 안 순서를 바꾸지 않으므로 괜찮습니다.
- 사용자에게는 동기 응답으로 당첨 여부를 주지 않고 **"접수됨 → 결과는 알림/조회"** 로 처리하는 것이 자연스럽습니다.

### 한 파티션 한계를 넘을 때

인기 상품 하나에 요청이 몰리면 그 상품의 파티션이 병목입니다(핫 파티션). 보통 파티션 하나로 초당 수만 건은 처리되므로 대부분 충분하지만, 부족하면:

- **재고 샤딩**: 재고 100개를 `item-A#0`(34), `item-A#1`(33), `item-A#2`(33) 세 키로 나눠 병렬 처리. 샤드마다 독립 선착순이라 엄밀한 전역 선착순은 아니게 되지만 처리량은 3배.
- **API 앞단에서 1차 컷**: Redis `DECR`/Lua로 재고 이상 요청은 즉시 "매진" 응답하고 통과한 요청만 Kafka로. 선착순 판정은 Redis가 하고 Kafka는 DB를 보호하는 버퍼(트래픽 완충, 재처리 로그)가 됩니다. 실무에서 가장 흔한 조합입니다.

## 6. Redis를 쓰면 더 공정해지나?

**더 공정해지지는 않고, 더 빠르고 단순해집니다.** 공정성은 "누가 결정하느냐"가 아니라 **"결정 지점이 하나인가, 그 앞단의 지연 편차가 얼마나 되는가"** 로 정해집니다.

| | Redis 방식 | Kafka 방식 |
| --- | --- | --- |
| 직렬화 지점 | Redis 단일 스레드 (`DECR`/Lua) | 파티션 로그 (append 순서) |
| "선착순"의 정의 | Redis에 명령이 도착한 순서 | 파티션에 append된 순서 |
| 사용자가 버튼 누른 실제 시각 | 모름 | 모름 |

둘 다 하나의 지점에 도착한 순서를 선착순으로 정의하고, 그 앞의 경로(사용자 네트워크 → LB → API 서버)의 지연 편차는 동일하게 남습니다.

| 관점 | Redis가 유리 | Kafka가 유리 |
| --- | --- | --- |
| 지연 | 결정까지 홉이 하나 적음(API → Redis). Kafka는 배칭 → 브로커 → 컨슈머 | |
| 응답 | 동기 응답으로 즉시 당첨/매진 표시 → 공정하다고 **느끼는** 경험 | |
| 단순함 | 원자 연산 하나로 끝. 컨슈머·오프셋·리밸런스 없음 | |
| 증거 | | 파티션 로그가 "누가 몇 번째로 도착했는지"의 영구 기록. 민원·감사에 오프셋 제시 가능. Redis 카운터는 순서를 기록하지 않음 |
| 내구성 | | Redis는 failover 시 복제 지연만큼 카운터가 되돌아가 초과 당첨 가능. Kafka는 `acks=all`이면 기록이 사라지지 않고 재처리해도 같은 결과 |
| 뒷단 보호 | | 폭주 트래픽을 흡수해 DB를 천천히 처리시킴 |

실제로 불공정을 만드는 것은 어느 방식이든 같습니다: LB가 어느 서버로 보내느냐와 그 서버의 상태, 클라이언트 재시도(**requestId/userId 중복 차단**이 없으면 "빠른 손"이 아니라 "많은 요청"이 이김), 봇·매크로(유입 제어 문제).

그래서 보통은 이렇게 조합합니다.

```text
API 서버 ─▶ Redis (인당 1회 체크 + 재고 원자 차감) ─▶ 즉시 응답 (당첨/매진)
              │ 통과한 요청만
              ▼
           Kafka (key=itemId) ─▶ 컨슈머 ─▶ DB 확정·주문 생성·알림
                                       (Redis 결과와 대조, 재처리 멱등)
```

판정은 Redis(빠르고 동기), 기록과 후처리는 Kafka(내구성·감사·버퍼). Kafka 컨슈머는 Redis 판정을 신뢰하되 DB에서 재고를 다시 검증해 Redis 장애 시 초과 당첨을 최종 방어합니다.

## 관련 문서

- [Kafka 핵심 개념: 토픽, 파티션, 브로커, 컨트롤러](01-kafka-basics.md)
- [Producer와 복제](02-producer-and-replication.md) — `acks`, 멱등 프로듀서
- [Consumer와 Consumer Group](03-consumer-and-consumer-group.md) — 파티션 배정, 커밋
- [Kafka 사용 원칙 (개발자용)](12-usage-principles.md) — 이 클러스터의 프로듀서·컨슈머 기본 설정
