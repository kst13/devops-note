# kafka-admin 3단계 착수 문서 (조치 + 알림 발송)

2026-08-07 작성. 1·2단계 완료 시점의 상태를 기록하고, 3단계를 바로 시작할 수 있도록 범위·이월 항목·선결 결정사항을 정리한다.

## 현재 상태 (3단계의 전제)

- 저장소: `~/Documents/kafka-admin` — origin(사내 GitLab), kkk(GitHub) 모두 `436f162`까지 푸시됨
- 스택: Spring Boot 4.1.0 / Java 21 / kafka-clients 4.2.x(BOM) / JPA+H2 / Vue 3+TS / Nginx+compose
- 완료된 것:
  - **1단계**: 세션 로그인(ADMIN/DEVELOPER), 클러스터/토픽/그룹·랙 조회, 브로커 무응답 503, web/was 분리 배포
  - **2단계**: 60초 지표 수집(랙/URP/디스크/브로커수)→H2 이력(7일), 임계치·수집실패 알림 이력(30분 쿨다운), 인증서 D-30 감시(SASL_SSL), 랙 추이 차트/알림 화면. `AlertNotifier` 포트는 Noop 구현만 존재
- 테스트: 백엔드 29건(Testcontainers 싱글턴 컨테이너 포함), 프론트 vitest 6건. 전부 통과 상태
- 원 설계 문서: `docs/superpowers/specs/2026-08-06-kafka-admin-site-design.md`

## 3단계 범위

원 설계의 "구축 단계 3"(조치) + 2단계에서 미룬 알림 발송.

### A. ops 모듈 — 조치 실행기 (핵심)

설계 원칙(원 스펙의 안전장치 절, 그대로 유효):

- 모든 위험 작업은 **반드시 ops 모듈 경유** — 컨트롤러가 kafka 모듈을 직접 호출해 조치하는 경로 금지
- 실행 전 **영향 요약 + 확인 절차** → 실행 → **감사 로그**(누가·언제·무엇을·파라미터·결과) 강제
- 자동 복구 없음: 감지 → 알림 → 사람 확인 → 실행의 반자동 구조
- ADMIN 역할만 조치 실행 가능 (DEVELOPER는 조회·신청만)

조치 목록 (AdminClient API 기준):

| 조치 | API | 비고 |
|---|---|---|
| 컨슈머 그룹 오프셋 리셋 | `alterConsumerGroupOffsets` | earliest/latest/특정 오프셋/타임스탬프. 그룹 활성 상태면 불가 → "컨슈머 먼저 중지" 안내 UI 필수 |
| 토픽 설정 변경 | `incrementalAlterConfigs` | retention.ms 등. 디스크 급증 대응 원클릭 |
| 파티션 증가 | `createPartitions` | 감소 불가 경고 표시 |
| preferred 리더 선출 | `electLeaders(PREFERRED)` | 리더 쏠림 해소 |
| unclean 리더 선출 | 동적 설정 + `electLeaders(UNCLEAN)` | **데이터 유실 감수** — 2차 확인 절차 필요 (선결 결정 #4) |
| 파티션 재할당 | `alterPartitionReassignments` | 재할당 진행률 조회(`listPartitionReassignments`) 포함 |
| 복제 스로틀 | 동적 설정 (leader/follower.replication.throttled.*) | 재할당·복구 폭풍 대응, 설정+해제 한 쌍 |

감사 로그: `AuditLog` 엔티티(actor, action, target, params JSON, result, error, executedAt) + 조회 API/화면. H2 저장, 보존 정책은 선결 결정 #3.

### B. AlertNotifier 발송 구현체

- 2단계에서 인터페이스(`AlertNotifier.send(AlertEvent)`)만 만들어둠 — 구현체 1개를 꽂으면 끝나는 구조
- 웹훅(범용 JSON POST) 우선 권장: URL만 env로 받으면 Slack/Teams/사내 메신저 대부분 커버
- 발송 실패가 수집 루프를 깨지 않도록 비동기/예외 격리 필수

### C. 이월 하드닝 (3단계에 포함 권장)

최종 리뷰들이 3단계로 미룬 항목 중 우선순위 높은 것:

1. **시작 시 인증서 즉시 점검** — 현재 매일 03시(KST)까지 최대 24시간 공백. 기동 시 1회 `runDailyCheck()` 실행
2. **runDaily 트랜잭션 분리** — 인증서 점검(네트워크 I/O)을 @Transactional 밖으로, 보존 삭제만 트랜잭션
3. **404/503 에러 구분** — 존재하지 않는 토픽/그룹 요청 시 UnknownTopicOrPartitionException → 404 (지금은 전부 503 "브로커 접속 불가")
4. **로그아웃 버튼** — UI에 없음. client.ts의 빈 응답 처리(204 외)도 함께
5. **securityProtocol fail-fast** — 오타 값이 조용히 PLAINTEXT로 흐르는 문제, 기동 시 검증
6. **/api/metrics hours 상한** (예: 168) 및 groupId `encodeURIComponent`

낮은 우선순위 (여유 있으면): AlertEvent id를 화면 row key로 노출, KIP-848(`group.protocol=consumer`) 컨슈머 IT 추가, KafkaFutures 실패 경로 단위 테스트, GroupQueryServiceIT `@Timeout`, 트렌드 x축 시간 간격 반영, Dockerfile 비루트 USER/HEALTHCHECK, Nginx TLS 종단.

### 범위 밖 (4단계로 유지)

SSH 브로커 재시작, 토픽 신청/승인 워크플로우, 메시지 열람. Cruise Control 연동은 필요성이 확인될 때 검토.

## 시작 전 결정할 것 (3단계 킥오프 질문)

1. **알림 채널**: 웹훅 URL을 확보할 수 있는 사내 메신저가 무엇인지 (Slack/Teams/기타). 메일(SMTP)도 필요한지
2. **조치 우선순위**: 7종 전부 한 번에 vs 실전 빈도 높은 것부터(오프셋 리셋 + retention 변경 + 파티션 증가 먼저, 리더 선출·재할당·스로틀은 후반)
3. **감사 로그 보존**: 기간 제한 없이 보존(권장, 규모 작음) vs N일
4. **unclean 리더 선출 노출 여부**: 데이터 유실 감수 조치라 UI에서 아예 뺄지, 2차 확인(예: 토픽명 타이핑)을 걸고 넣을지
5. **확인 절차 UX**: 모든 조치 공통 — 영향 요약 모달 + 확인 버튼이면 충분한지, 파괴적 조치(unclean, 재할당)만 대상명 타이핑 확인을 추가할지

## 진행 방법

이 문서 검토 후 "3단계 진행"이라고 하면:

1. 위 5개 결정사항 확인 (한 번에 질문)
2. 구현 계획 작성 → `docs/superpowers/plans/` (1·2단계와 같은 형식: 태스크별 TDD, 코드 포함)
3. 서브에이전트 방식 실행 (태스크별 구현→리뷰→수정 루프, 최종 전체 리뷰, 실브로커 E2E)

### 계획 작성 시 주의 (1·2단계에서 배운 것)

- H2 예약어(`VALUE` 등) 컬럼명 금지, named in-memory DB 공유 → 테스트 데이터는 고유 접두어
- Boot 4.x 테스트 슬라이스는 모듈 분리: `spring-boot-webmvc-test`, `spring-boot-security-test` (import `org.springframework.boot.webmvc.test.autoconfigure.*`)
- Testcontainers는 싱글턴 컨테이너 패턴 (@Container 금지 — 컨텍스트 캐시와 충돌)
- 네트워크 코드는 connect/read 타임아웃 모두 명시
- 조치 API의 Testcontainers IT는 위험 조치일수록 우선 작성 (오프셋 리셋, 재할당)
