# Kafka 시나리오 러너 설계

- 날짜: 2026-09-02
- 상태: 승인됨
- 범위: `kafka/examples/scenario-runner/` — 정상·비정상 시나리오를 자동 재현·검증하는 CLI 애플리케이션

## 목표

문서로 정리한 Kafka 장애 시나리오(troubleshooting/cluster-total-outage.md, usage-guide/07-failure-resilience.md)를 로컬 3노드 클러스터에서 실제로 재현하고, 메시지 유실 여부를 기계적으로 판정해 리포트로 남긴다. "브로커 1대가 죽어도 유실이 없다", "2대가 죽으면 쓰기가 실패한다", "전체 정지에도 폴백 버퍼로 데이터를 지킨다"를 말이 아니라 실행 결과로 증명하는 도구다.

## 결정 사항

- 대상 환경: 로컬 `kafka/examples/compose-3node-kraft-plaintext` 클러스터 (bootstrap 서버는 설정으로 변경 가능)
- 형태: CLI 시나리오 러너 — 시나리오 이름을 인자로 받아 주입→관측→판정→리포트까지 자동 실행
- 스택: Java 21, Spring Boot 3, Spring Kafka, Gradle (usage-guide 표준 스택과 동일)
- 1차 시나리오: 핵심 4개 (정상 왕복, 브로커 1대 정지, 2대 정지, 전체 정지→복구)
- 장애 주입: `docker stop/start`를 ProcessBuilder로 호출 (PLAINTEXT compose의 컨테이너 이름 기준)

## 아키텍처

단일 Spring Boot 비웹 애플리케이션. 시나리오 실행 중 프로듀서·컨슈머가 백그라운드 스레드로 계속 돌면서 장애 주입 전·중·후의 동작을 관찰한다.

```text
java -jar runner.jar <scenario>
        │
ScenarioRunner ──> FaultInjector (docker stop/start, AdminClient 상태 폴링)
   │    │
   │    ├─ LoadGenerator   seq 박힌 메시지 연속 생산 (acks=all, 멱등)
   │    ├─ VerifierConsumer 수신 seq 기록
   │    └─ Ledger          보낸 것/받은 것/실패한 것 대조표
   │
   └─> Judge ──> Reporter (콘솔 + reports/<시나리오>-<시각>.md)
```

### 구성 요소 책임

| 구성 요소 | 책임 | 의존 |
| --- | --- | --- |
| ScenarioRunner | CLI 진입, 시나리오 선택·수명 관리, 사전 점검, finally 원상 복구 | 전체 |
| LoadGenerator | `{seq, sentAt, scenarioId}` JSON을 지정 속도로 전송. 성공/실패를 Ledger에 기록. 실패분은 시나리오 ④에서 로컬 버퍼에 보관 | KafkaTemplate, Ledger |
| VerifierConsumer | 별도 그룹으로 처음부터 소비, 수신 seq를 Ledger에 기록 | Spring Kafka listener, Ledger |
| Ledger | sent-ok / sent-fail / received 집합. 스레드 안전 | 없음 (순수 자료구조) |
| FaultInjector | 컨테이너 stop/start, 클러스터 상태 대기(리더 존재, URP=0, 쿼럼) | docker CLI, AdminClient |
| Judge | 시나리오별 기대 결과와 Ledger 대조 → PASS/FAIL과 근거 | Ledger |
| Reporter | 콘솔 요약과 Markdown 리포트 파일 생성 | Judge 결과 |

## 시나리오 명세

공통: 테스트 토픽 `test.scenario.events` (파티션 3, RF3, `min.insync.replicas=2`)를 시작 시 AdminClient로 생성(있으면 재사용). 프로듀서는 `acks=all` + `enable.idempotence=true` + `retries` 기본값.

| 시나리오 | 절차 | PASS 기준 |
| --- | --- | --- |
| ① normal-roundtrip | 1만 건 전송 → 전량 소비 대기 | 유실 0, 중복 0, 처리량·지연 통계 출력 |
| ② broker-1-down | 부하 시작 → 브로커 1대 stop → 60초 유지 → start → URP=0 대기 | 정지 구간 포함 전송 성공 지속(재시도 허용), 유실 0, 복구 후 URP=0 |
| ③ broker-2-down | 부하 시작 → 2대 stop → 60초 유지 → start → 회복 대기 | 정지 구간 전송이 `NotEnoughReplicas`류로 실패하는 것이 기대 동작(그게 PASS). 실패 seq는 Ledger에 기록되고, 복구 후 재전송 시 최종 유실 0 |
| ④ total-outage | 부하 시작 → 3대 stop → 실패분 로컬 버퍼 보관 → 전체 start → 쿼럼·리더 회복 대기 → 버퍼 재전송 | 클러스터 자력 복구 확인, 재전송 포함 최종 유실 0 — usage-guide 07의 폴백 패턴 검증 |

판정 불변식: "성공으로 기록된 seq 집합 ⊆ 수신 seq 집합"(유실 0). 중복은 ①에서만 FAIL 사유, ③④는 재전송 특성상 허용하고 개수만 리포트.

## 오류 처리

- 사전 점검: 시나리오 시작 전 컨테이너 3개 존재·기동 상태, 토픽 접근 가능 여부 확인. 실패 시 즉시 종료(장애 주입 전).
- 원상 복구: 시나리오가 예외로 중단돼도 finally에서 세 컨테이너 모두 start.
- docker 명령 실패(권한 등)는 명령 출력 포함해 오류 보고.

## 테스트

- Ledger·Judge는 순수 로직 — 단위 테스트 (유실/중복 판정 케이스).
- FaultInjector의 docker 호출은 명령 문자열 생성까지만 단위 테스트, 실제 호출은 통합 실행에서.
- 시나리오 전체는 compose 클러스터를 띄운 상태의 실행이 통합 테스트를 겸한다. README에 실행 절차 문서화.

## 저장소 통합

- `kafka/examples/scenario-runner/README.md`가 sync-content에 의해 예제 문서로 노출된다 — 목적, 실행 절차, 시나리오 표, 리포트 해석을 담는다.
- 빌드 산출물(build/), 리포트(reports/)는 gitignore 처리.
- 커밋 후 `cd web && npm run lint && npm test` 통과 확인.

## 범위 밖 (이후 확장 후보)

- ISR 축소(네트워크 지연 주입), 디스크 만수(작은 볼륨) 시나리오
- 실서버 3대 대상 모드(SSH 주입), SASL_SSL 클러스터 대상
- 웹 대시보드, 정기 실행(CI)
