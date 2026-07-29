# Kafka 사용 원칙 (개발자용)

> 이 클러스터를 안전하게 함께 쓰기 위한 규칙입니다. **처음 Kafka를 쓰는 개발자는 이 문서만 읽어도** 시작할 수 있게 정리했습니다. 배경이 궁금하면 각 항목의 링크를 따라가세요.

## TL;DR — 이것만 지키면 됩니다

- ✅ 토픽은 **직접 만들지 말고 요청**하세요 (아래 "토픽 요청하기").
- ✅ 프로듀서는 **`acks=all` + `enable.idempotence=true`**.
- ✅ 순서가 중요하면 **메시지 key**를 지정하세요.
- ✅ 컨슈머는 **`enable.auto.commit=false`** 로 두고 **처리 후 수동 커밋**.
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

## 2. 토픽 명명 규칙

```text
<도메인>.<이벤트>          예) order.created, payment.completed, inventory.updated
```

- 소문자 + 점(`.`)으로 구분, 도메인 먼저.
- 임시/실험 토픽은 `sandbox.<이름>` 처럼 접두사를 붙이세요.

## 3. 토픽 요청하기 (직접 만들지 마세요)

토픽은 아무나 만들 수 없습니다(계정에 생성 권한이 없습니다). 새 토픽이 필요하면:

```text
1. 토픽 요청 (이름 / 파티션 수 / 보관 기간 / 용도)  → <요청 방법: 예) 인프라 리포에 PR, 티켓 등>
2. 관리자가 표준 설정(RF3, min.insync.replicas=2)으로 생성 + 여러분 계정에 read/write 권한 부여
3. 생성 완료되면 애플리케이션에서 사용
```

> `<요청 방법>` 부분은 우리 팀 프로세스에 맞게 채워 넣으세요(예: `ows-infra` 리포에 토픽 정의 PR).

**파티션 수 정할 때**: "동시에 몇 개 컨슈머가 병렬 처리해야 하나"가 기준입니다. 나중에 늘릴 순 있어도 **줄일 수 없고**, 늘리면 key 순서가 흐트러지니 처음에 여유 있게 잡으세요.

## 4. 프로듀서 (메시지 보내기)

```properties
acks=all                 # ISR 전체 확인 후 성공 (무손실). 3.0+ 기본값
enable.idempotence=true  # 재시도 중복 방지. 3.0+ 기본값
```

- **순서가 중요하면 key를 지정**하세요. 같은 key는 같은 파티션 → 그 안에서 순서 보장.
  ```java
  producer.send(new ProducerRecord<>("order.created", orderId, payload)); // key=orderId
  ```
- 유실이 허용되는 대량 로그·메트릭이라면 `acks=1`을 쓸 수도 있지만, **기본은 `acks=all`** 입니다.

DON'T: `acks=0`으로 중요한 데이터 보내기 (유실됨).

## 5. 컨슈머 (메시지 받기)

```properties
group.id=<your-service>          # 서비스별 고유 그룹 ID
enable.auto.commit=false         # 자동 커밋 끄기
isolation.level=read_committed   # 트랜잭션 사용 시
```

- **처리 완료 후 수동 커밋**하세요. 자동 커밋은 처리 전에 커밋돼 장애 시 메시지를 놓칠 수 있습니다.
- 컨슈머는 **같은 메시지를 두 번 받을 수 있다**고 가정하고(재시도·리밸런싱), **멱등하게** 처리하세요(예: 주문ID로 중복 체크).
- 병렬 처리를 늘리려면 **파티션 수**를 늘려야 합니다(컨슈머만 늘리면 남는 컨슈머는 놉니다).

DON'T: 여러 인스턴스가 **같은 group.id**로 뜨는 걸 잊고 파티션보다 많은 컨슈머를 띄우기(초과분은 유휴).

## 6. 접속 설정 템플릿 (복붙용)

```properties
# --- 공통 ---
bootstrap.servers=kafka1:9094,kafka2:9094,kafka3:9094   # 3대 모두 나열 (환경에 맞게 교체)
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="<your-service>" password="<from-secret-manager>";
ssl.truststore.location=/path/truststore.jks
ssl.truststore.password=<from-secret-manager>
ssl.endpoint.identification.algorithm=https

# --- 프로듀서 추가 ---
acks=all
enable.idempotence=true

# --- 컨슈머 추가 ---
group.id=<your-service>
enable.auto.commit=false
```

- **비밀번호·키는 코드/저장소에 하드코딩하지 말고** Secret Manager로 주입하세요.
- 계정(`<your-service>`)은 관리자에게 발급받습니다. 남의 계정을 쓰지 마세요.

## 7. 자주 하는 실수

| 증상 | 원인 | 해결 |
| --- | --- | --- |
| 메시지가 가끔 유실 | `acks=1`/`acks=0`, 자동 커밋 | `acks=all`, 수동 커밋 |
| 순서가 뒤바뀜 | key 없이 전송 | 순서 단위를 key로 |
| 같은 메시지 중복 처리 | 재시도·리밸런싱 | 컨슈머를 멱등하게 |
| 접속 실패/권한 오류 | 계정·ACL·truststore | 발급받은 계정·인증서 확인 |
| 컨슈머 늘려도 안 빨라짐 | 파티션 수 < 컨슈머 수 | 파티션 증설 요청 |

## 더 읽을거리

- 개념: [Kafka 핵심 개념](01-kafka-basics.md) · [Producer와 복제](02-producer-and-replication.md) · [Consumer와 Consumer Group](03-consumer-and-consumer-group.md)
- 실무 Q&A(순서 시뮬레이션·멱등 등): [09-concepts-qna](09-concepts-qna.md)
- 보안(왜 계정·TLS가 필요한가): [10-security-tls-and-auth](10-security-tls-and-auth.md)
- 명령어: [운영 치트시트](../commands/kafka-operations-cheatsheet.md)

궁금한 점이나 토픽 요청은 DevOps 담당(플랫폼팀)에게 문의하세요.
