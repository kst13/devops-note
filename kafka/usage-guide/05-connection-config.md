# 접속 설정 (`application.yml`)

> Spring Boot 설정은 `application.yml`에 모아둡니다. **프로듀서/컨슈머 설정은 어느 환경이든 동일**하고, **접속·보안 부분만** 다릅니다. 비밀번호·키는 **환경변수/Secret Manager로 주입**하세요(하드코딩 금지).

## 1. 공통 원칙

- `bootstrap.servers`에는 **브로커 3대를 모두** 적습니다. 이 목록은 "최초 접속용 후보"라서 1대만 적어도 동작은 하지만, 하필 그 1대가 죽어 있으면 앱이 시작조차 못 합니다. 3대를 적으면 살아 있는 아무 브로커로든 접속합니다.
- 계정(`${KAFKA_USER}`/`${KAFKA_PASSWORD}`)은 **관리자에게 발급**받습니다. 남의 계정을 쓰지 마세요 — ACL이 자기 토픽에만 열려 있고, 계정을 공유하면 사고 추적이 안 됩니다.
- 아래 두 레시피의 **차이는 딱 두 곳**: 포트(`9092` → `9094`)와 `spring.kafka.properties`의 보안 블록(SASL_SSL에만 있음).

## 2. 레시피 ① PLAINTEXT (로컬·사설망, 인증·암호화 없음)

```yaml
spring:
  kafka:
    bootstrap-servers: kafka1:9092,kafka2:9092,kafka3:9092   # 포트 9092, 보안 설정 없음
    producer:
      acks: all
      compression-type: lz4                   # 배치 단위 압축 (none|gzip|snappy|lz4|zstd)
      batch-size: 65536                       # 배치를 키워야 압축률이 나옴 (기본 16384)
      properties:
        enable.idempotence: true
        linger.ms: 20                         # 배치를 채울 시간을 조금 줌
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

## 3. 레시피 ② SASL_SSL (운영, SCRAM 인증 + TLS 암호화)

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
      compression-type: lz4                   # 배치 단위 압축 (none|gzip|snappy|lz4|zstd)
      batch-size: 65536                       # 배치를 키워야 압축률이 나옴 (기본 16384)
      properties:
        enable.idempotence: true
        linger.ms: 20                         # 배치를 채울 시간을 조금 줌
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
      ack-mode: manual                        # 수동 커밋 (컨슈머 코드와 짝)
```

## 4. 항목별 설명 — 왜 이 값인가

| 설정 | 값 | 이유 |
| --- | --- | --- |
| `producer.acks` | `all` | ISR 저장까지 확인해야 성공 — 브로커 1대 장애에도 무손실 ([03 프로듀서](03-producer.md) 1장) |
| `producer.properties.enable.idempotence` | `true` | 재시도 시 중복·순서 역전 방지 |
| `producer.compression-type` | `lz4` | 네트워크·디스크 3~5배 절감, CPU 부담 거의 없음 ([03 프로듀서](03-producer.md) 4장) |
| `producer.batch-size` / `linger.ms` | `65536` / `20` | 배치를 키워 압축률·처리량 확보. linger는 배치가 찰 시간을 주는 대기 상한 |
| `consumer.enable-auto-commit` | `false` | 처리 완료 후에만 커밋 — 유실 방지 ([04 컨슈머](04-consumer.md) 2장) |
| `listener.ack-mode` | `manual` | `Acknowledgment.acknowledge()` 호출 시점에 커밋 |
| `consumer.group-id` | 서비스명 | 서비스별 고유. 공유하면 메시지를 나눠 받는 사고 |
| `spring.json.trusted.packages` | `"com.osstem.*"` | `JsonDeserializer`가 역직렬화를 허용할 패키지. 미설정 시 예외 |
| `consumer.properties.isolation.level` | `read_committed` | 트랜잭션 프로듀서가 쓰다 중단한(abort) 메시지를 읽지 않음 |
| `sasl.mechanism` | `SCRAM-SHA-512` | 비밀번호를 평문 전송하지 않는 챌린지-응답 인증 |
| `ssl.truststore.*` | 발급받은 JKS | 브로커 인증서를 신뢰하기 위한 저장소 |
| `ssl.endpoint.identification.algorithm` | `https` | 접속한 호스트와 인증서의 호스트명 일치 검증(중간자 방지) |

## 5. 프로파일로 분리 (권장)

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

- 공통 설정(acks=all, compression-type, ack-mode 등)은 `application.yml`에 두고 **접속·보안만 프로파일별 파일**에 둡니다.
- 운영 계정·truststore 비밀번호는 배포 파이프라인의 Secret으로 주입하고, 저장소에는 절대 커밋하지 않습니다.
