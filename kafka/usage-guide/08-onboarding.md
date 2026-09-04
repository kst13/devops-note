# 개발자 온보딩 체크리스트

> 처음 Kafka를 쓰는 서비스가 **0에서 "연결 성공"까지** 가는 절차입니다. 문서의 예시 주소(`10.0.0.x`)는 계정 발급 시 전달되는 **실제 접속 정보**로 바꿔 읽으세요.

전체 흐름:

```text
① 신청 → ② 발급물 수령 → ③ application.yml 설정 → ④ 연결 확인 → ⑤ 토픽 신청 → ⑥ 코드 작성
```

## 1. 준비물 3종 — 무엇이 필요하고 어디서 받나

Kafka 접속은 사내 DB 접속과 비슷합니다 — **주소, 계정, 그리고 보안 연결에 필요한 파일** 세 가지가 있어야 합니다. 전부 플랫폼팀이 발급하며(아래 2장에서 한 번에 신청), 여기서는 각각이 무엇이고 왜 필요한지부터 설명합니다.

### ① 접속 정보 (주소·포트)

| 대상 | 예 | 용도 |
| --- | --- | --- |
| Kafka 브로커 **3대** | `10.0.0.11:9094`, `10.0.0.12:9094`, `10.0.0.13:9094` | 메시지를 보내고 받는 본체 |
| Schema Registry **2대** | `10.0.0.11:8081`, `10.0.0.13:8081` | 메시지 형식(스키마) 관리 — Avro 도입 시에만 사용 |

- 브로커가 3대인 이유: 같은 데이터를 3벌로 복제해 **1대가 죽어도 서비스가 계속되는** 구성입니다. 설정에는 **반드시 3대를 모두** 적으세요 — 목록은 "최초 접속 후보"라서, 1대만 적으면 하필 그 서버 점검 중일 때 앱이 시작을 못 합니다.
- 포트는 용도별로 나뉘어 있고 앱이 쓰는 건 **`:9094` 하나**입니다. (9092·9093은 서버끼리 쓰는 내부 포트라 몰라도 됩니다.)

### ② 서비스 계정 (아이디/비밀번호)

- DB 계정처럼 **아이디/비밀번호로 인증**합니다(SCRAM 방식). 계정은 **개인이 아니라 서비스 단위로 1개** — 계정명이 곧 서비스명입니다.
- 계정마다 **신청한 토픽에만** 권한이 열립니다. 다른 팀 토픽은 계정이 있어도 접근이 거부됩니다(사고 범위 격리). 그래서 남의 계정을 빌려 쓰면 안 됩니다 — 권한도 다르고, 문제 발생 시 추적도 안 됩니다.

### ③ truststore 파일

- 브로커와의 통신은 **TLS로 암호화**됩니다(HTTPS와 같은 원리). 이때 "내가 접속한 상대가 진짜 우리 Kafka 서버인지"를 확인하는 **인증서 보관함 파일**이 `truststore.jks`입니다.
- 브라우저는 공인 인증서라 이 과정이 자동이지만, 우리 클러스터는 **사내 자체 발급 인증서**라 이 파일을 직접 받아 앱 서버에 배치해야 합니다. 파일 자체에도 비밀번호가 걸려 있어 함께 발급됩니다.
- 파일이 없거나 경로·비밀번호가 틀리면 접속 시 `SSLHandshakeException`이 납니다(4장 원인표).

### ④ 방화벽 개방 (준비물이라기보다 통로)

사내망은 기본적으로 서버 간 통신이 막혀 있어서, **앱 서버 → 브로커 3대 `:9094`, Schema Registry 2대 `:8081`** 경로를 열어야 합니다. 위 세 가지가 다 있어도 방화벽이 닫혀 있으면 `Connection refused`/타임아웃만 나요. 별도로 신청할 필요는 없고 — 2장의 신청에 앱 서버 IP를 적으면 함께 처리됩니다.

## 2. 신청하기 (플랫폼팀에 한 번에)

Kafka는 사내 공용 인프라라 **DB처럼 계정을 발급받아 쓰는 구조**입니다. 아무나 접속하거나 토픽을 만들 수 없게 잠겨 있고(사고·무관리 토픽 방지), 필요한 권한을 신청하면 플랫폼팀이 열어줍니다. 신청은 **서비스 단위로 한 번**이면 됩니다 — 개인별로 신청하는 것이 아닙니다.

### 무엇을 적어서 신청하나

| 항목 | 적는 방법 | 예 |
| --- | --- | --- |
| 서비스명 | 배포되는 애플리케이션 이름. 이 이름이 그대로 **계정명**이 되고, 컨슈머의 `group.id`도 이걸 씁니다 | `notification-service` |
| 앱 서버 IP/대역 | 이 서비스가 **배포되어 도는 서버**의 IP(개발·스테이징·운영 모두). 여기서 브로커(`:9094`)·Schema Registry(`:8081`)로 가는 방화벽이 열립니다. 개인 PC에서 붙어 개발하려면 PC 대역도 포함 | `10.20.0.0/24` 또는 IP 목록 |
| 필요한 토픽과 권한 | **이미 있는 토픽**을 쓸 거면: 토픽명 + 읽기(구독)인지 쓰기(발행)인지. **새 토픽**이 필요하면: [02 토픽 요청하기](02-topic-request.md)의 양식(토픽명·파티션 수·보관 기간·용도)을 함께 첨부 | `commerce.order.created` 읽기 |
| 담당자 | 발급물 전달과 장애 시 연락받을 사람 | 이름/연락처 |

> 어떤 토픽이 있는지 모르겠으면 그냥 "주문 생성 이벤트를 받아서 알림을 보내려 한다"처럼 **하려는 일**을 적으세요. 토픽 매칭은 플랫폼팀이 함께 봐줍니다.

작성 예시:

```text
[Kafka 사용 신청]
- 서비스명: notification-service
- 용도: 주문 생성 이벤트를 구독해 고객에게 알림 발송
- 앱 서버: 개발 10.20.1.15 / 운영 10.30.1.20~21
- 필요 토픽: commerce.order.created (읽기)
- 담당자: 홍길동 (내선 1234)
```

### 무엇을 받게 되나

| 발급물 | 무엇 | 어디에 쓰나 (3장의 설정 위치) |
| --- | --- | --- |
| 계정/비밀번호 | 이 서비스 전용 SCRAM 계정 | `sasl.jaas.config`의 `username`/`password` |
| `truststore.jks` + 비밀번호 | 브로커의 TLS 인증서를 신뢰하기 위한 파일 | 앱 서버에 배치 후 `ssl.truststore.*` |
| 접속 주소 목록 | 브로커 3대(`:9094`), Schema Registry 2대(`:8081`)의 실제 주소 | `bootstrap-servers` (3대 모두 기재) |
| 권한(ACL) | 신청한 토픽에 대한 읽기/쓰기 권한 — 눈에 보이는 파일은 아니고 서버에 등록됨 | 별도 설정 불필요. 신청 안 한 토픽은 접근이 거부됩니다 |

비밀번호와 truststore 비밀번호는 **저장소에 커밋하지 말고** 환경변수/Secret으로 주입합니다(3장).

### 나중에 다시 신청이 필요한 경우

- 새 토픽이 필요할 때, 이미 쓰는 토픽에 반대 방향 권한(읽기→쓰기)이 필요할 때
- 앱 서버가 추가·변경될 때 (방화벽 재신청)
- 신청 안 한 토픽에 접근하면 `TopicAuthorizationException`이 납니다 — 4장의 원인표 참고

## 3. 백엔드 세팅 (Spring Boot)

의존성 추가:

```text
implementation "org.springframework.kafka:spring-kafka"
```

발급받은 값을 넣은 최소 `application.yml` — `${...}`는 **환경변수/Secret으로 주입**하고 저장소에 커밋하지 않습니다:

```yaml
spring:
  kafka:
    bootstrap-servers: 10.0.0.11:9094,10.0.0.12:9094,10.0.0.13:9094   # 발급받은 3대 모두
    properties:
      security.protocol: SASL_SSL
      sasl.mechanism: SCRAM-SHA-512
      sasl.jaas.config: >
        org.apache.kafka.common.security.scram.ScramLoginModule required
        username="${KAFKA_USER}" password="${KAFKA_PASSWORD}";
      ssl.truststore.location: /app/secrets/truststore.jks             # 발급받은 파일 배치 경로
      ssl.truststore.password: ${KAFKA_TRUSTSTORE_PASSWORD}
      ssl.endpoint.identification.algorithm: https
    producer:
      acks: all
      compression-type: lz4
      properties:
        enable.idempotence: true
    consumer:
      group-id: notification-service        # 서비스명과 동일하게
      enable-auto-commit: false
    listener:
      ack-mode: manual
```

- 프로듀서/컨슈머 각 항목이 왜 이 값인지, 로컬(PLAINTEXT)·운영 프로파일 분리는 [05 접속 설정](05-connection-config.md) 참고.
- Schema Registry를 쓰는 경우 `schema.registry.url`에 **발급받은 두 주소를 모두** 나열합니다(`http://10.0.0.11:8081,http://10.0.0.13:8081`).

## 4. 연결 확인 — 가장 짧은 방법

앱에 임시로 아래를 넣고 기동하면 접속 여부가 바로 판별됩니다 (확인 후 제거):

```java
@Bean
CommandLineRunner kafkaConnectivityCheck(KafkaAdmin kafkaAdmin) {
    return args -> System.out.println("Kafka 연결 성공: " +
            kafkaAdmin.describeTopics());   // 접속 실패 시 여기서 예외 발생
}
```

Schema Registry 확인은 앱 서버에서:

```bash
curl http://10.0.0.11:8081/subjects   # [] 또는 스키마 목록이 나오면 정상. {}가 나오면 경로에 /subjects를 붙였는지 확인
```

실패 시 예외별 원인:

| 예외/증상 | 원인 | 확인 |
| --- | --- | --- |
| `Connection refused` / 타임아웃 | 방화벽 미개방, 주소·포트 오기 | 신청한 대역에 앱 서버 IP가 포함됐는지, `:9094`인지 |
| `SaslAuthenticationException` | 계정/비밀번호 불일치 | 발급값과 주입된 환경변수 대조 |
| `SSLHandshakeException` | truststore 경로·비밀번호 오류 | 파일 배치 경로와 `ssl.truststore.*` 값 |
| `No subject alternative ... matching` | 발급 주소가 아닌 다른 호스트명으로 접속 | `bootstrap-servers`를 발급받은 주소 그대로 |
| `TopicAuthorizationException` | 해당 토픽 ACL 없음 | 토픽 신청·권한(read/write) 확인 |

## 5. 다음 단계

- 토픽이 더 필요하면 → [02 토픽 요청하기](02-topic-request.md)
- 메시지 보내기 → [03 프로듀서](03-producer.md) · 받기 → [04 컨슈머](04-consumer.md)
- 개발 중 문제가 생기면 → [06 자주 하는 실수](06-common-mistakes.md), 장애 대비 설계 → [07 장애 대비](07-failure-resilience.md)

막히면 발급 안내에 적힌 플랫폼팀 채널로 문의하세요 — 4장의 예외명을 함께 보내주시면 처리가 빠릅니다.
