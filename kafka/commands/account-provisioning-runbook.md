# 계정·권한 발급 런북 (관리자용)

> 개발팀의 Kafka 사용 신청([08 온보딩](../usage-guide/08-onboarding.md) 2장)을 받은 관리자가 수행하는 절차입니다. 개발자용 온보딩 문서의 "뒷면"에 해당합니다. 예시 주소(`10.0.0.x`)는 실제 값으로 바꿔 사용하세요.

작업 요약: **계정 생성 1건 + ACL 1~2건 + 확인 1건 + 방화벽·발급 전달**. 모든 명령은 브로커 서버 **아무 한 대**의 compose 디렉터리에서 실행하면 클러스터 전체에 반영됩니다.

문서 전체에서 아래 축약을 사용합니다:

```bash
KAFKA_BIN="docker compose exec kafka /opt/kafka/bin"
OPTS="--bootstrap-server 10.0.0.11:9094 --command-config /etc/kafka/secrets/client.properties"
```

## 1. 신청 내용 검토

| 확인 | 기준 |
| --- | --- |
| 서비스명 | 소문자 케밥 케이스(예: `notification-service`). 계정명·그룹명으로 그대로 사용 |
| 토픽 | 기존 토픽이면 이름이 정확한지, 신규면 [명명 규칙](../usage-guide/01-topic-naming.md)(3단계) 준수 여부 |
| 권한 방향 | 읽기(구독)인지 쓰기(발행)인지 — 신청한 방향만 부여 |
| 앱 서버 IP | 방화벽 신청 대상. 개발·운영 환경 구분 확인 |

## 2. (신규 토픽 신청이 포함된 경우) 토픽 생성

RF3·`min.insync.replicas=2`는 클러스터 기본값이 적용되므로 파티션 수와 보존 기간만 지정합니다.

```bash
$KAFKA_BIN/kafka-topics.sh $OPTS --create \
  --topic commerce.order.created \
  --partitions 6 \
  --config retention.ms=604800000    # 7일
```

## 3. 계정 생성 (SCRAM)

```bash
openssl rand -hex 16                 # 비밀번호 생성 — 발급 전달에도 사용

$KAFKA_BIN/kafka-configs.sh $OPTS --alter \
  --add-config 'SCRAM-SHA-512=[password=생성한값]' \
  --entity-type users --entity-name notification-service
```

- 계정명 = 신청서의 서비스명. 이 이름이 ACL 주체(`User:notification-service`)이자 개발자의 `username`이 됩니다.
- 같은 명령을 다시 실행하면 **비밀번호 변경**이 됩니다(재발급 절차와 동일).

## 4. ACL 부여 — 신청한 방향만

**읽기(구독) 세트** — 토픽 Read + 컨슈머 그룹 Read가 한 쌍입니다. 그룹 ACL을 빼먹으면 구독이 실패합니다(그룹명 = 서비스명이 표준):

```bash
$KAFKA_BIN/kafka-acls.sh $OPTS --add \
  --allow-principal User:notification-service \
  --operation Read --operation Describe --topic commerce.order.created

$KAFKA_BIN/kafka-acls.sh $OPTS --add \
  --allow-principal User:notification-service \
  --operation Read --group notification-service
```

**쓰기(발행) 세트** — 그룹 ACL 없이 토픽 쪽만:

```bash
$KAFKA_BIN/kafka-acls.sh $OPTS --add \
  --allow-principal User:order-service \
  --operation Write --operation Describe --topic commerce.order.created
```

참고:

- 멱등 프로듀서(`enable.idempotence=true`)도 토픽 Write면 충분합니다 — Kafka 2.8+부터 Write에 멱등 쓰기 권한이 포함됩니다.
- `@RetryableTopic`을 쓰는 컨슈머 서비스는 `<토픽>.retry`·`<토픽>.dlq` 토픽의 읽기/쓰기 ACL을 추가로 부여합니다.
- Schema Registry(Avro)를 쓰는 서비스는 별도 브로커 ACL이 필요 없습니다 — SR 접근은 8081 방화벽으로만 제어됩니다.

## 5. 등록 확인

```bash
$KAFKA_BIN/kafka-acls.sh $OPTS --list --principal User:notification-service
```

출력을 신청서와 대조해 **의도한 권한만** 있는지 확인합니다.

## 6. 방화벽·발급물 전달

1. **방화벽**: 신청서의 앱 서버 IP → 브로커 3대 `:9094` (+ SR 사용 시 2대 `:8081`) 개방 신청.
2. **배포용 truststore**: 브로커의 keystore(개인키)는 절대 전달 금지. CA 인증서로 만든 배포용 truststore를 공용으로 사용합니다(최초 1회 생성, 모든 서비스에 동일 파일):

```bash
keytool -importcert -alias devops-kafka-ca -file ca.crt \
  -keystore client.truststore.jks -storepass '배포용비밀번호' -noprompt
```

3. **발급 안내 전달** — 비밀번호는 안전한 채널로(메신저 평문 금지, Secret 등록 대행 권장):

```text
[Kafka 발급 안내 — notification-service]
- 접속: 10.0.0.11:9094, 10.0.0.12:9094, 10.0.0.13:9094 (SASL_SSL)
- 계정: notification-service / (비밀번호 별도 전달)
- truststore: client.truststore.jks 첨부 / (비밀번호 별도 전달)
- 권한: commerce.order.created 읽기, 컨슈머 그룹 notification-service
- 설정 방법: usage-guide 08 온보딩 문서 3장
```

## 7. 변경·회수

| 상황 | 명령 |
| --- | --- |
| 비밀번호 재발급 | 3장의 계정 생성 명령 재실행 (덮어쓰기) |
| 권한 회수 | `kafka-acls.sh $OPTS --remove --allow-principal User:서비스명 --operation ... --topic ...` |
| 계정 삭제 | `kafka-configs.sh $OPTS --alter --delete-config 'SCRAM-SHA-512' --entity-type users --entity-name 서비스명` |
| 서비스 폐기 | ACL 전체 제거 → 계정 삭제 → 방화벽 회수 → 그 서비스만 쓰던 토픽 정리 검토 |

## 자동화 메모

이 런북의 3~6장은 Admin Portal의 자동화 후보입니다 — 신청 폼 제출 → 계정·ACL 생성 → 발급 안내 생성까지 이어지면 관리자 작업은 검토·승인만 남습니다.
