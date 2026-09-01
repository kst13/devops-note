# Kafka 장애 시나리오 러너

문서로 정리한 Kafka 장애 시나리오를 로컬 3브로커 클러스터에서 실제로 재현하고, 메시지 유실 여부를 기계적으로 판정해 리포트로 남기는 CLI 도구입니다. "브로커 1대가 죽어도 유실이 없다", "2대가 죽으면 쓰기가 실패한다", "전체 정지에도 폴백으로 데이터를 지킨다"를 말이 아니라 실행 결과로 증명합니다.

원리는 단순합니다. 프로듀서가 순번(seq)이 박힌 메시지를 계속 보내고 컨슈머가 수신 순번을 기록하는 동안, 러너가 `docker stop/start`로 장애를 주입합니다. 마지막에 "성공으로 기록된 순번 집합 ⊆ 수신 순번 집합"이라는 불변식을 검사해 PASS/FAIL을 판정합니다.

## 전제 조건

- Docker (Compose v2 포함)
- JDK 21 이상, Maven 3.9 이상
- 포트 9092, 9192, 9292 미사용 상태

## 실행 방법

```bash
# 1) 로컬 3브로커 클러스터 기동 (최초 20초 정도 대기)
docker compose up -d

# 2) 빌드
mvn -q package -DskipTests

# 3) 시나리오 실행 (종료 코드: 0=PASS, 1=FAIL, 2=사용법 오류)
java -jar target/scenario-runner.jar normal-roundtrip
java -jar target/scenario-runner.jar broker-1-down
java -jar target/scenario-runner.jar broker-2-down
java -jar target/scenario-runner.jar total-outage
```

시나리오가 끝나면 러너가 컨테이너를 모두 start 상태로 되돌리므로 연속 실행이 가능합니다. 각 실행은 이전 데이터를 지우기 위해 테스트 토픽(`test.scenario.events`)을 삭제 후 재생성합니다.

## 시나리오

| 시나리오 | 절차 | PASS 기준 |
| --- | --- | --- |
| normal-roundtrip | 1만 건 전송 → 전량 소비 | 유실 0, 중복 0 |
| broker-1-down | 부하 중 브로커 1대 정지 60초 → 복구 | 전송 지속(재시도 허용), 유실 0, ISR 완전 회복 |
| broker-2-down | 부하 중 2대 정지 60초 → 복구 → 실패분 재전송 | 정지 구간 쓰기 실패가 발생하는 것이 정상(min.insync.replicas=2), 최종 유실 0 |
| total-outage | 부하 중 3대 정지 → 전체 재기동 → 폴백 재전송 | 쿼럼·리더 자력 복구, 재전송 포함 최종 유실 0 |

broker-2-down과 total-outage는 "실패가 0건이면 오히려 FAIL"로 판정합니다 — 실패가 없다는 것은 장애 주입 자체가 동작하지 않았다는 뜻이기 때문입니다.

## 리포트 해석

리포트는 `reports/<시나리오>-<시각>.md`로 저장됩니다.

- **전송 성공** — acks=all 응답을 받아 성공으로 기록된 메시지 수
- **미전송 잔여** — 재전송 후에도 전달되지 않은 메시지 수 (0이 아니면 항상 FAIL)
- **유실** — 성공으로 기록됐는데 수신되지 않은 수 (0이 아니면 항상 FAIL — 가장 심각)
- **중복** — 같은 순번이 두 번 이상 수신된 수. 재시도·재전송이 있는 시나리오에서는 정상이며, 컨슈머 멱등 처리가 필요한 이유를 보여줍니다

## 관련 문서

- [클러스터 전체 정지와 복구 절차](../../troubleshooting/cluster-total-outage.md) — 운영자 관점의 복구
- [장애에 대비하는 애플리케이션 설계](../../usage-guide/07-failure-resilience.md) — 이 러너의 폴백 재전송이 검증하는 패턴
- [프로듀서 acks와 복제 원리](../../concepts/02-producer-and-replication.md)

## 정리

```bash
docker compose down    # 클러스터와 데이터 제거
```

이 클러스터는 시나리오 검증 전용입니다. 운영 구성은 [SASL_SSL 3노드 예제](../compose-3node-kraft/README.md)를 사용합니다.
