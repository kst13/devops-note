# Kafka 사용 원칙 (개발자용)

> 이 클러스터를 안전하게 함께 쓰기 위한 규칙입니다. **처음 Kafka를 쓰는 개발자는 이 문서만 읽어도** 시작할 수 있게 정리했습니다. 샘플 코드는 **Java + Spring Boot(Spring Kafka)** 기준입니다. 배경이 궁금하면 각 항목의 링크를 따라가세요.

## TL;DR — 이것만 지키면 됩니다

- ✅ 토픽은 **직접 만들지 말고 요청**하세요 (아래 "토픽 요청하기").
- ✅ 프로듀서는 **`acks=all` + `enable.idempotence=true`**.
- ✅ 순서가 중요하면 **메시지 key**를 지정하세요.
- ✅ 컨슈머는 **수동 커밋**(처리 후 `ack.acknowledge()`).
- ✅ **자기 서비스 계정**으로만 접속하고, `bootstrap.servers`에 **브로커 3대를 모두** 적으세요.
- ✅ 컨슈머는 **같은 메시지를 두 번 받아도 안전**하게(멱등) 처리하세요.

---

## 1. 핵심 원칙 (운영 정책)

| 원칙 | 의미 |
| --- | --- |
| 안전 기본값 | 모든 토픽은 **RF3 + `min.insync.replicas=2`**, 프로듀서는 **`acks=all`** → 브로커 1대 죽어도 무손실 |
| 최소 권한 | 서비스마다 **별도 계정**, 자기 토픽만 접근. 토픽 생성 권한은 관리자만 |
| 거버넌스 | **토픽 자동 생성 금지**. 토픽은 명명 규칙에 맞춰 명시적으로 생성 |
| 관측 | 컨슈머 **lag**·복제 상태를 모니터링. 문제는 조기에 |

이 원칙은 여러분이 실수로 데이터를 잃거나 남의 토픽을 건드리는 걸 막기 위한 것입니다.

## 2. 토픽 명명 규칙과 이벤트 데이터

```text
<도메인>.<이벤트>          예) order.created, payment.completed, inventory.updated
```

- 소문자 + 점(`.`)으로 구분, 도메인 먼저. 임시/실험 토픽은 `sandbox.<이름>`.

메시지 값(value)은 **JSON**을 권장합니다. 예시 이벤트(`order.created`):

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

## 3. 토픽 요청하기 (직접 만들지 마세요)

토픽은 아무나 만들 수 없습니다(계정에 생성 권한이 없습니다). 새 토픽이 필요하면 아래 정보를 담아 요청하세요.

| 항목 | 값 |
| --- | --- |
| 토픽명 | `order.created` |
| 파티션 수 | 6 (동시에 몇 개 컨슈머가 병렬 처리할지 기준) |
| 보관 기간 | 7일 |
| 용도 | 주문 생성 이벤트 발행 |
| 요청 방법 | 팀 프로세스에 맞게 지정 (예: `ows-infra` 리포에 토픽 정의 PR / 티켓) |

→ 관리자가 표준 설정(RF3, `min.insync.replicas=2`)으로 생성하고, 여러분 계정에 read/write 권한을 부여합니다.

> **파티션 수**는 나중에 늘릴 순 있어도 **줄일 수 없고**, 늘리면 key 순서가 흐트러집니다. 처음에 여유 있게 잡으세요.

## 4. 프로듀서 (메시지 보내기)

`acks=all` + 멱등은 **설정**으로(아래 6번 `application.yml`), 순서는 **key**로 챙깁니다.

```java
@Service
public class OrderEventProducer {

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(OrderCreatedEvent event) {
        // key = orderId  →  같은 주문의 이벤트는 항상 같은 파티션 (그 안에서 순서 보장)
        kafkaTemplate.send("order.created", event.orderId(), event)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    // 전송 실패 로깅/재처리 (acks=all 이라 여기 오면 실제 실패)
                    log.error("order.created 발행 실패: {}", event.orderId(), ex);
                }
            });
    }
}
```

- **순서가 중요하면 반드시 key를 지정**하세요(`send(topic, key, value)`). key가 없으면 여러 파티션으로 흩어져 순서가 깨집니다.
- 유실이 허용되는 대량 로그·메트릭이면 그 토픽만 `acks=1`을 쓸 수도 있지만, **기본은 `acks=all`** 입니다.

DON'T: `acks=0`으로 중요한 데이터 보내기(유실됨).

## 5. 컨슈머 (메시지 받기)

**수동 커밋** + **멱등 처리**가 핵심입니다.

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

    @KafkaListener(topics = "order.created", groupId = "notification-service")
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

- **처리 완료 후 수동 커밋**하세요. 자동 커밋은 처리 전에 커밋돼 장애 시 메시지를 놓칠 수 있습니다.
- 컨슈머는 **같은 메시지를 두 번 받을 수 있다**고 가정하고 **멱등하게** 처리하세요(위 `orderId` 중복 체크).
- 병렬 처리를 늘리려면 **파티션 수**를 늘려야 합니다(같은 `group.id` 컨슈머가 파티션보다 많으면 초과분은 유휴).

DON'T: `ack.acknowledge()`를 처리 **전에** 호출하기 / 리스너 안에서 멀티스레드로 순서 섞기.

## 6. 접속 설정 (`application.yml`)

Spring Boot는 `application.yml`에 모아둡니다. **프로듀서/컨슈머 설정은 두 버전이 동일**하고, **접속·보안 부분만** 다릅니다. 비밀번호·키는 **환경변수/Secret Manager로 주입**(하드코딩 금지).

### ① PLAINTEXT (로컬·사설망, 인증·암호화 없음)

```yaml
spring:
  kafka:
    bootstrap-servers: kafka1:9092,kafka2:9092,kafka3:9092   # 포트 9092, 보안 설정 없음
    producer:
      acks: all
      properties:
        enable.idempotence: true
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    consumer:
      group-id: notification-service          # 서비스별 고유
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "com.osstem.*"
    listener:
      ack-mode: manual
```

### ② SASL_SSL (운영, SCRAM 인증 + TLS 암호화)

```yaml
spring:
  kafka:
    bootstrap-servers: kafka1:9094,kafka2:9094,kafka3:9094   # 포트 9094
    properties:                                              # ★ 보안 블록 추가 (아래만 다름)
      security.protocol: SASL_SSL
      sasl.mechanism: SCRAM-SHA-512
      sasl.jaas.config: >
        org.apache.kafka.common.security.scram.ScramLoginModule required
        username="${KAFKA_USER}" password="${KAFKA_PASSWORD}";
      ssl.truststore.location: /app/secrets/truststore.jks
      ssl.truststore.password: ${KAFKA_TRUSTSTORE_PASSWORD}
      ssl.endpoint.identification.algorithm: https
    producer:
      acks: all
      properties:
        enable.idempotence: true
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    consumer:
      group-id: notification-service          # 서비스별 고유
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        isolation.level: read_committed
        spring.json.trusted.packages: "com.osstem.*"   # JSON 역직렬화 허용 패키지
    listener:
      ack-mode: manual                        # 수동 커밋 (위 5번 코드와 짝)
```

**차이는 딱 두 곳**: 포트(`9092` → `9094`)와 `spring.kafka.properties`의 보안 블록(SASL_SSL은 추가).

- `${KAFKA_USER}`/`${KAFKA_PASSWORD}` 계정은 **관리자에게 발급**받습니다. 남의 계정을 쓰지 마세요.

### 프로파일로 분리 (권장)

로컬은 PLAINTEXT, 스테이징/운영은 SASL_SSL로 **코드 변경 없이** 전환하려면 Spring Profile로 나눕니다.

```text
application.yml            # 공통(producer/consumer/listener 설정)
application-local.yml      # PLAINTEXT (bootstrap 9092, 보안 없음)
application-stage.yml      # SASL_SSL  (bootstrap 9094 + properties 보안 블록)
```

```bash
java -jar app.jar --spring.profiles.active=local   # 로컬
java -jar app.jar --spring.profiles.active=stage   # 스테이징
```

- 공통 설정(acks=all, ack-mode 등)은 `application.yml`에 두고 **접속·보안만 프로파일별 파일**에 둡니다.

## 7. 자주 하는 실수

| 증상 | 원인 | 해결 |
| --- | --- | --- |
| 메시지가 가끔 유실 | `acks=1`/`acks=0`, 자동 커밋 | `acks=all`, `ack-mode: manual` |
| 순서가 뒤바뀜 | key 없이 전송 | 순서 단위(orderId 등)를 key로 |
| 같은 메시지 중복 처리 | 재시도·리밸런싱 | 컨슈머를 멱등하게(중복 체크) |
| 접속 실패/권한 오류 | 계정·ACL·truststore | 발급받은 계정·인증서 확인 |
| 컨슈머 늘려도 안 빨라짐 | 파티션 수 < 컨슈머 수 | 파티션 증설 요청 |
| `JsonDeserializer` 오류 | 신뢰 패키지 미설정 | `spring.json.trusted.packages` 지정 |

## 더 읽을거리

- 개념: [Kafka 핵심 개념](01-kafka-basics.md) · [Producer와 복제](02-producer-and-replication.md) · [Consumer와 Consumer Group](03-consumer-and-consumer-group.md)
- 실무 Q&A(순서 시뮬레이션·멱등 등): [09-concepts-qna](09-concepts-qna.md)
- 보안(왜 계정·TLS가 필요한가): [10-security-tls-and-auth](10-security-tls-and-auth.md)
- 명령어: [운영 치트시트](../commands/kafka-operations-cheatsheet.md)

궁금한 점이나 토픽 요청은 DevOps 담당(플랫폼팀)에게 문의하세요.
