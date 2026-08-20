# 자주 하는 실수 (증상 → 원인 → 확인 → 해결)

> 실제로 가장 자주 접수되는 문제들입니다. 각 항목을 증상 → 원인 → 확인 방법 → 해결 순으로 정리했습니다. 아래 CLI 명령은 관리자 또는 읽기 권한이 있는 계정 기준입니다.

## 1. 메시지가 가끔 유실된다

- **원인**: 프로듀서 `acks=1`/`acks=0`(리더만 저장하고 성공 처리), 또는 컨슈머 자동 커밋(처리 전에 커밋).
- **확인**: `application.yml`에서 `producer.acks`와 `consumer.enable-auto-commit` 값을 확인. 유실 시점이 브로커 재시작/배포 시각과 겹치는지 확인.
- **해결**: `acks: all` + `enable.idempotence: true`, 컨슈머는 `enable-auto-commit: false` + `ack-mode: manual`([03 프로듀서](03-producer.md), [04 컨슈머](04-consumer.md)).

## 2. 순서가 뒤바뀐다

- **원인**: key 없이 전송해 메시지가 여러 파티션으로 흩어짐. 또는 순서가 필요한 이벤트가 여러 토픽/여러 프로듀서 인스턴스로 갈라짐.
- **확인**: 컨슈머에서 파티션 번호를 함께 로깅해 보면, 같은 주문의 이벤트가 서로 다른 파티션에서 오는 것이 보입니다.
- **해결**: 순서 단위(orderId 등)를 key로 지정하고, 순서가 필요한 이벤트는 한 토픽·한 프로듀서 인스턴스에서 발행([03 프로듀서](03-producer.md) 2장).

## 3. 같은 메시지가 두 번 처리된다

- **원인**: 재시도·리밸런싱에 의한 중복 수신은 Kafka의 **정상 동작**입니다(최소 한 번 전달).
- **확인**: 중복 발생 시각이 배포·스케일 조정·컨슈머 장애 시각과 겹치는지 확인.
- **해결**: 컨슈머를 멱등하게 — 처리 이력 테이블, upsert, 상태 전이 검증 중 택 1([04 컨슈머](04-consumer.md) 4장). "중복이 안 생기게" 하는 것이 아니라 "중복이 와도 안전하게"가 목표입니다.

## 4. 접속 실패 / 권한 오류가 난다

- **원인**: 계정·비밀번호 오류, ACL 없음(남의 토픽 접근), truststore 미설정, 포트 혼동(9092 vs 9094).
- **확인**: 예외 메시지 구분 — `SaslAuthenticationException`(계정), `TopicAuthorizationException`(ACL), `SSLHandshakeException`(truststore), `Connection refused`/타임아웃(주소·포트·방화벽).
- **해결**: 발급받은 자기 서비스 계정인지, 토픽 read/write 권한을 요청했는지, truststore 경로·비밀번호가 맞는지 확인([05 접속 설정](05-connection-config.md)).

## 5. 컨슈머를 늘렸는데 처리량이 안 늘어난다

- **원인**: 컨슈머 수가 파티션 수를 초과 — 같은 그룹에서 한 파티션은 한 컨슈머만 읽으므로 초과분은 유휴.
- **확인**: 그룹 상태에서 배정 현황을 봅니다. 배정된 파티션이 없는 멤버가 보이면 초과 상태입니다.

```bash
kafka-consumer-groups.sh --bootstrap-server kafka1:9094 \
  --command-config client.properties \
  --describe --group notification-service
```

- **해결**: 파티션 증설을 요청합니다([02 토픽 요청하기](02-topic-request.md)). 단, 증설하면 key→파티션 배정이 바뀌어 그 시점의 순서 연속성이 끊기는 점을 감안하세요.

## 6. `JsonDeserializer` 오류가 난다

- **원인**: 역직렬화 신뢰 패키지 미설정, 또는 프로듀서·컨슈머의 DTO 패키지 불일치.
- **확인**: 예외에 `The class ... is not in the trusted packages` 문구가 있는지 확인.
- **해결**: `spring.json.trusted.packages: "com.osstem.*"` 지정([05 접속 설정](05-connection-config.md) 4장).

## 7. 네트워크·디스크 사용량이 크다

- **원인**: 압축 없이 전송, 또는 압축을 켰지만 배치가 작아 효과가 없음(건별 동기 전송 등).
- **확인**: 프로듀서 설정에서 `compression-type` 유무, `batch-size`/`linger.ms` 값, 코드에서 `send().get()` 동기 호출 여부.
- **해결**: `compression-type: lz4` + `batch-size: 65536` + `linger.ms: 20`, 비동기 전송([03 프로듀서](03-producer.md) 4장).

## 8. 압축을 켰는데 브로커 CPU가 올라간다

- **원인**: 토픽/브로커의 `compression.type`이 프로듀서 코덱과 달라, 브로커가 **풀었다가 다시 압축**함.
- **확인**: 토픽 설정 조회로 `compression.type`이 `producer`(기본)인지 확인.

```bash
kafka-configs.sh --bootstrap-server kafka1:9094 \
  --command-config client.properties \
  --entity-type topics --entity-name commerce.order.created --describe
```

- **해결**: 토픽 `compression.type`을 기본값 `producer`로 되돌립니다. 프로듀서가 정한 코덱 그대로 저장하는 것이 가장 쌉니다.

## 9. 컨슈머 lag이 계속 늘어난다

- **원인**: 처리 속도 < 유입 속도. 또는 특정 파티션만 몰리는 핫 파티션(한 key에 트래픽 집중), 5분 이상 걸리는 처리로 인한 리밸런싱 루프.
- **확인**: 위 5번의 `--describe` 출력에서 파티션별 LAG 열을 봅니다 — 전체가 고르게 늘면 처리량 부족, 한 파티션만 늘면 핫 파티션입니다. 로그에 리밸런싱이 반복되면 `max.poll.interval.ms` 초과를 의심하세요.
- **해결**: 처리량 부족이면 컨슈머·파티션 증설, 핫 파티션이면 key 단위 재설계(순서 단위를 더 잘게), 리밸런싱 루프면 `max.poll.records` 축소 또는 처리 시간 단축([04 컨슈머](04-consumer.md) 5장).
