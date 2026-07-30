# Kafka 운영 관리자 웹 애플리케이션 설계

작성일: 2026-07-30

## 배경과 목표

여러 애플리케이션이 하나의 Kafka 클러스터를 공유하는 MSA 환경에서, Kafka를 유지보수·운영하기 위한 사내 관리자 웹 애플리케이션을 자체 구축한다.

두 가지 목적을 동시에 만족시킨다.

- **운영자**: 클러스터가 지금 정상인지 판단하고 장애 원인을 좁힌다.
- **개발팀**: 자기 팀 토픽·컨슈머 상태를 직접 확인하고, 토픽 생성·설정 변경을 신청한다. 운영자 수작업 요청을 줄이는 것이 목표다.

## 대상 환경

| 항목 | 값 |
|---|---|
| Kafka | 4.x, KRaft 전용 |
| 클러스터 | 단일 prod 클러스터, 브로커 3대 |
| 보안 | SASL_SSL + mTLS |
| 스테이징 클러스터 | 없음 |

## 범위

### 포함

- 클러스터·브로커·토픽·컨슈머 그룹 현재 상태 조회
- ACL 조회 (읽기 전용)
- 메시지 내용 조회, 테스트 메시지 발행
- 토픽 생성 신청, 토픽 설정 변경 신청 및 승인 워크플로
- 토픽 소유권(팀) 관리
- 웹앱 자체 계정·역할 관리
- 감사 로그

### 제외

| 제외 항목 | 이유 |
|---|---|
| 시계열 지표(처리량, GC, 디스크 추이 그래프) | 현재 상태 진단에 집중. 메트릭 수집·보관 설계 부담을 지지 않는다 |
| 이상 상태 외부 알림(Slack·이메일) | 1차 범위에서 제외. 화면 내 경고 배지로만 표시 |
| 파티션 수 증가 | 되돌릴 수 없고 키 기반 순서를 깨뜨린다. CLI로 수행 |
| 컨슈머 그룹 오프셋 리셋 | 파괴적. CLI로 수행 |
| 토픽 삭제 | 파괴적. CLI로 수행 |
| ACL 부여·해제 | 조회만 지원. 변경은 CLI로 수행 |
| 다중 클러스터 관리 | 관리 대상이 단일 클러스터 |
| 사내 SSO/LDAP 연동 | 웹앱 자체 계정 DB로 시작 |

## 아키텍처

```
React SPA  ──HTTPS/세션──▶  Spring Boot API  ──┬── AdminClient ──▶ Kafka (SASL_SSL + mTLS)
                                  │            ├── Consumer  ──▶ (메시지 조회)
                                  │            └── Producer  ──▶ (테스트 발행)
                                  │
                                  └── PostgreSQL (웹앱 고유 데이터만)
```

기술 스택은 Spring Boot(Java) API + React SPA다. Kafka `AdminClient`가 공식 Java 라이브러리이므로 mTLS·SASL_SSL 설정, JAAS, 키스토어 로드가 모두 보장된 경로로 처리된다.

### 웹앱 인증 방식

**서버 측 세션 + `HttpOnly`·`Secure` 쿠키**를 쓴다. JWT를 쓰지 않는 이유는 관리자 도구에서 즉시 무효화(계정 비활성화, 강제 로그아웃)가 필요하고, 세션이 그것을 추가 구조 없이 제공하기 때문이다.

비밀번호는 bcrypt로 저장한다. `ADMIN`이 계정을 생성할 때 임시 비밀번호를 발급하고, 첫 로그인 시 변경을 강제한다. 최소 길이 12자만 검증하고 복잡도 규칙은 두지 않는다.

### 데이터 신선도 정책

조회는 요청 시점에 `AdminClient`로 실시간 질의한다. **Kafka 상태를 DB에 저장하지 않는다.** DB에는 Kafka가 알지 못하는 데이터만 담는다 — 사용자·역할, 토픽 소유권, 승인 요청, 감사 로그. 이 경계를 지키면 "DB와 실제 클러스터가 어긋남" 부류의 버그가 구조적으로 발생할 수 없다.

예외는 컨슈머 그룹 랙 집계 뷰 하나다. 이 뷰는 `listConsumerGroups → describeConsumerGroups → listConsumerGroupOffsets → listOffsets` 4단 호출이라 그룹이 수십 개면 수 초가 걸린다. 이 집계 결과에만 5~10초 TTL 인메모리 캐시를 둔다. 그룹 상세 화면은 캐시를 쓰지 않고 항상 실시간으로 조회한다.

### Kafka 접속 자격증명

웹앱은 **단일 서비스 계정 principal**(예: `CN=kafka-admin-web`)로 Kafka에 접속한다. Kafka는 사용자별 권한 위임을 지원하지 않으므로(사용자마다 mTLS 인증서가 필요) 이것이 유일하게 현실적인 구조다.

필요 권한:

| 리소스 | 권한 |
|---|---|
| Cluster | DESCRIBE, DESCRIBE_CONFIGS |
| Topic | CREATE, DESCRIBE, DESCRIBE_CONFIGS, ALTER_CONFIGS, READ, WRITE |
| Group | DESCRIBE |

Topic의 READ는 메시지 조회, WRITE는 테스트 발행에 필요하다. ACL 조회(`describeAcls`)는 Cluster DESCRIBE로 충족되므로 별도 권한을 추가하지 않는다.

키스토어·트러스트스토어 경로와 패스워드는 환경변수로만 주입한다. DB나 코드에 두지 않는다.

이 구조의 결과로 **Kafka 브로커 로그에는 모든 작업이 이 한 계정으로 기록된다.** "누가 했는지"의 유일한 증거는 웹앱 감사 로그다. 따라서 감사 로그는 부가 기능이 아니라 필수 컴포넌트다.

### 백엔드 모듈 경계

각 모듈은 `KafkaAdminGateway` 인터페이스에만 의존한다. 이 인터페이스가 Kafka 클라이언트 라이브러리를 격리하는 유일한 지점이며, 덕분에 나머지 모듈 전부를 Kafka 없이 가짜 게이트웨이로 테스트할 수 있다.

| 모듈 | 책임 | 의존 |
|---|---|---|
| `kafka` | AdminClient·Consumer·Producer 래핑, `KafkaAdminGateway` 구현. Kafka 타입은 이 모듈 안에서만 다루고 밖으로는 자체 DTO만 노출 | — |
| `cluster` | 브로커·컨트롤러·로그디렉터리 조회, 클러스터 건강도 판정 | gateway |
| `topic` | 토픽 목록·상세·설정 조회, 생성·설정변경 실행 | gateway, ownership |
| `consumergroup` | 그룹·랙 조회, 랙 집계 캐시 | gateway |
| `message` | 메시지 조회, 테스트 발행 | gateway, audit |
| `request` | 승인 워크플로 상태 기계 | topic, iam, audit |
| `ownership` | 토픽 ↔ 소유 팀 매핑 | — |
| `iam` | 사용자·팀·역할, 로그인 | — |
| `audit` | 감사 로그 기록·조회 | — |

## 역할 모델

| 역할 | 메타데이터 조회 | 메시지 조회 | 테스트 발행 | 신청 | 승인·실행 | 사용자·소유권 관리 |
|---|---|---|---|---|---|---|
| `DEVELOPER` | 전체 | 자기 팀 토픽만 | 화이트리스트 토픽만 | 가능 | 불가 | 불가 |
| `OPERATOR` | 전체 | 전체 | 전체 (화이트리스트 밖은 재확인) | 가능 | 가능 | 불가 |
| `ADMIN` | 전체 | 전체 | 전체 | 가능 | 가능 | 가능 |

계정 생성 시 기본 역할은 `DEVELOPER`다.

메타데이터 조회는 역할과 무관하게 전체 공개한다. 클러스터 상태를 숨기면 협업이 되지 않는다. 반면 **메시지 내용은 소유권으로 제한한다.** 다른 팀 토픽의 페이로드에는 개인정보나 결제 정보가 있을 수 있고, 그것은 "조회 자유"의 대상이 아니다.

팀 소속이 없는 계정은 "자기 팀 토픽"이 비어 있어 메시지 조회 결과가 자동으로 없다. 별도 차단 로직이 필요하지 않다.

`OPERATOR`는 자기가 낸 신청을 자기가 승인할 수 없다(self-approval 금지). 기본값은 금지이며, 운영자가 1인인 조직을 위해 `ADMIN`이 `app_settings`에서 해제할 수 있다. 기본값이 느슨한 쪽이 더 위험하므로 금지를 기본으로 둔다.

## 화면과 기능

### 1. 클러스터 개요

첫 화면. "지금 정상인가"에 한 눈에 답하는 것이 유일한 목적이다.

- 브로커 카드 3개: id, host:port, rack, 온라인 여부, 컨트롤러 표시
- 위험 지표: `offline partitions`, `under-replicated partitions`, `min.insync.replicas` 미달 토픽 수. 0이 아니면 경고색
- 브로커별 디스크 사용량 (`describeLogDirs` 현재값 합계)
- 클러스터 id, Kafka 버전

`offline partitions > 0`은 이미 쓰기 실패가 발생하고 있다는 뜻이고, `under-replicated > 0`은 서비스는 되지만 브로커 하나만 더 정지하면 무손실이 깨진다는 뜻이다. 두 지표를 같은 비중으로 나열하지 않고 심각도 순으로 배치한다.

### 2. 토픽 목록

이름, 파티션 수, RF, `min.insync.replicas`, retention, 총 크기, 소유 팀, 배지. 소유 팀·배지 종류·소유권 미지정으로 필터.

배지는 두 종류다.

**현재 이상 상태 배지** — 지금 문제가 있는 토픽:

| 배지 | 조건 |
|---|---|
| `OFFLINE` | 리더가 없는 파티션 보유 |
| `UNDER_REPLICATED` | ISR 크기 < RF인 파티션 보유 |
| `BELOW_MIN_ISR` | ISR 크기 < `min.insync.replicas`인 파티션 보유 |

**설정 위험 배지** — 지금은 정상이나 장애 시 문제가 되는 설정:

| 배지 | 조건 | 의미 |
|---|---|---|
| `RF_1` | RF = 1 | 브로커 1대 정지로 데이터 소실 |
| `NO_DURABILITY` | RF ≥ 2 이고 `min.insync.replicas` = 1 | 무손실 보장 없음. `acks=all`이 무의미 |
| `NO_HEADROOM` | `min.insync.replicas` = RF | 브로커 1대 정지 시 즉시 쓰기 중단 |

3노드 클러스터에서 이 설정 조합들은 장애 시 결과가 크게 달라지는 지점이고, 목록에서 바로 보이지 않으면 아무도 확인하지 않는다.

### 3. 토픽 상세

- 파티션별 leader / replicas / ISR / 시작·끝 오프셋 / 추정 건수
- 설정 전체. 브로커 기본값 대비 override된 항목 강조
- 이 토픽을 구독하는 컨슈머 그룹 목록
- 리더 편중 여부(특정 브로커가 리더를 과점하고 있는지)

추정 건수는 `끝 오프셋 - 시작 오프셋`이다. 트랜잭션 마커와 압축(`cleanup.policy=compact`)으로 실제 건수와 어긋날 수 있으므로 화면에 "추정"임을 명시한다.

### 4. 컨슈머 그룹 목록

group id, 상태(`Stable` / `PreparingRebalance` / `CompletingRebalance` / `Empty` / `Dead`), 멤버 수, 구독 토픽, 총 랙. 총 랙 내림차순 기본 정렬. 랙 집계 TTL 캐시 적용 대상.

### 5. 컨슈머 그룹 상세

파티션별 current offset / end offset / 랙, 각 파티션 담당 멤버(client id, host).

랙이 특정 파티션에만 쏠렸는지(키 편중) 전체적으로 밀렸는지(컨슈머 처리량 부족)를 구분할 수 있게 파티션 단위로 표시한다. 이 구분이 대응 방법을 결정한다.

### 6. ACL 조회 (읽기 전용)

principal별 리소스 권한 목록. 부여·해제는 범위에서 제외했지만, "이 애플리케이션이 왜 접근 거부되는가"는 ACL 조회 없이 진단이 불가능하므로 읽기는 포함한다.

### 7. 내 팀 토픽

토픽 목록에 소유권 필터가 적용된 뷰. 별도 화면이 아니라 목록 화면의 저장된 필터로 구현한다.

### 8. 메시지 브라우저

토픽·파티션 선택 후 세 가지 시작점을 지원한다.

- 최신에서 역순 N건
- 특정 오프셋부터
- 특정 타임스탬프부터

키·값·헤더·타임스탬프·오프셋 표시, JSON 자동 포맷.

가드레일:

- 1회 최대 조회 건수: 기본 100, 상한 1000
- 값 표시 크기 상한: 기본 64KB. 초과분은 잘라내고 잘렸음을 표시
- 전용 Consumer를 요청마다 생성하고 반드시 닫는다
- **`group.id`를 부여하지 않는다.** 실제 컨슈머 그룹의 오프셋을 절대 변경하지 않기 위함이다

상한이 없으면 대용량 토픽 조회 한 번으로 웹앱 힙이 고갈된다.

모든 메시지 조회는 감사 로그에 기록한다(사용자, 토픽, 파티션, 오프셋 범위, 건수, 시각).

### 9. 테스트 메시지 발행

키·값·헤더 입력, 파티션 지정 옵션.

단일 prod 클러스터 구성에서 가장 위험한 기능이다. 통제 방식:

- **발행 허용 토픽을 화이트리스트로 관리한다.** 목록에 없는 토픽에는 발행 버튼을 렌더링하지 않는다
- 화이트리스트는 `ADMIN`이 관리하고, 접미사 패턴(`*.test`, `*.dev` 등)을 허용한다
- `OPERATOR`는 화이트리스트 밖 토픽에도 발행할 수 있으나, 토픽명 재입력 확인을 거친다
- 모든 발행은 감사 로그에 기록한다

### 10. 토픽 생성 신청

입력: 이름, 파티션 수, RF, `min.insync.replicas`, retention, `cleanup.policy`, 소유 팀, 용도, 예상 처리량.

제출 전 검증:

- 명명 규칙 부합. 규칙은 `app_settings.topic_name_pattern`에 정규식으로 두고 `ADMIN`이 변경한다. 기본값 `^[a-z0-9]+(\.[a-z0-9-]+)+$` — 점으로 구분된 최소 2단 소문자 이름(예: `order.created`). 사내 규칙이 확정되면 이 값만 바꾼다
- 이름 중복 (실제 클러스터 조회)
- RF ≤ 브로커 수
- `min.insync.replicas` ≤ RF
- RF = 3일 때 `min.insync.replicas` 권장값 2 안내 (차단이 아닌 안내)

### 11. 토픽 설정 변경 신청

변경 가능 항목만 노출한다(`retention.ms`, `retention.bytes`, `cleanup.policy`, `max.message.bytes` 등). 현재값 → 요청값을 diff로 표시한다.

파티션 수와 RF는 이 화면에 존재하지 않는다(범위 제외).

### 12. 승인 대기 목록 / 상세

신청 내용, 신청자, 검증 결과, 승인·반려 처리. 반려는 사유를 필수로 받는다. 승인 즉시 백엔드가 `AdminClient`로 실행하고 결과를 요청에 기록한다.

### 13. 감사 로그

사용자·기간·행위 유형·대상 토픽으로 검색. 조회 전용이며 UI에 삭제·수정 경로를 두지 않는다.

감사 대상 행위는 다음으로 한정한다.

| `action` | 대상 |
|---|---|
| `LOGIN`, `LOGIN_FAILED`, `LOGOUT` | 사용자 |
| `MESSAGE_READ` | 토픽 (파티션, 오프셋 범위, 건수를 `detail`에) |
| `MESSAGE_PRODUCE` | 토픽 (키, 헤더, 파티션을 `detail`에. 값 본문은 저장하지 않음) |
| `REQUEST_SUBMIT`, `REQUEST_APPROVE`, `REQUEST_REJECT`, `REQUEST_CANCEL`, `REQUEST_RETRY` | 변경 요청 |
| `TOPIC_CREATE`, `TOPIC_CONFIG_ALTER` | 토픽 (실제 Kafka 실행 결과) |
| `USER_CREATE`, `USER_UPDATE`, `USER_DISABLE`, `ROLE_CHANGE` | 사용자 |
| `OWNERSHIP_CHANGE`, `WHITELIST_CHANGE`, `SETTING_CHANGE` | 각 대상 |

**메타데이터 조회(토픽 목록, 컨슈머 그룹, 클러스터 상태)는 감사 대상이 아니다.** 전 직원에게 열려 있는 정보이고, 기록하면 감사 로그가 조회 트래픽으로 가득 차서 정작 중요한 행위가 묻힌다. 메시지 내용 조회는 제한된 권한이므로 기록한다.

`MESSAGE_PRODUCE`에 값 본문을 저장하지 않는 이유는 감사 로그가 페이로드 저장소가 되면 그 자체가 유출 경로가 되기 때문이다.

### 14. 관리 화면 (`ADMIN`)

사용자·팀·역할 관리, 토픽 소유권 매핑, 발행 화이트리스트, self-approval 정책.

기존 토픽에 소유권을 처음 부여할 때를 위해, 토픽 목록에서 소유권 미지정 토픽을 필터링해 일괄 지정할 수 있게 한다. 그렇지 않으면 도입 첫날 수십 개 토픽을 하나씩 처리해야 한다.

## 데이터 모델 (PostgreSQL)

| 테이블 | 주요 컬럼 |
|---|---|
| `teams` | id, name, description |
| `users` | id, username, password_hash, display_name, team_id, role, enabled, created_at, last_login_at |
| `topic_ownership` | id, topic_name (unique), team_id, note, updated_by, updated_at |
| `change_requests` | id, type, status, payload (jsonb), requester_id, reason, reviewer_id, review_comment, requested_at, reviewed_at, executed_at, execution_error |
| `audit_logs` | id, actor_id, actor_username, action, target_type, target_name, detail (jsonb), result, ip_address, occurred_at |
| `produce_whitelist` | id, pattern, created_by, created_at |
| `app_settings` | key, value |

`change_requests.type`: `CREATE_TOPIC`, `ALTER_TOPIC_CONFIG`

`app_settings` 키 목록:

| 키 | 기본값 | 용도 |
|---|---|---|
| `self_approval_allowed` | `false` | 자기 신청 자기 승인 허용 여부 |
| `topic_name_pattern` | `^[a-z0-9]+(\.[a-z0-9-]+)+$` | 토픽 명명 규칙 |
| `message_read_max_count` | `100` | 메시지 1회 조회 기본 건수 |
| `message_value_max_bytes` | `65536` | 메시지 값 표시 크기 상한 |
| `admin_client_timeout_ms` | `10000` | AdminClient 호출 타임아웃 |
| `lag_cache_ttl_seconds` | `10` | 랙 집계 캐시 TTL |
| `feature_message_enabled` | `false` | 메시지 조회·발행 기능 활성화 |
| `feature_write_enabled` | `false` | 승인·실행 기능 활성화 |

`feature_*` 두 개는 도입 순서(아래)의 단계별 활성화에 쓰인다. 기본값이 `false`이므로 최초 배포는 자동으로 읽기 전용이 된다.

설계상 중요한 두 가지:

**`audit_logs`는 `users`에 FK를 걸지 않고 `actor_username`을 비정규화해 함께 저장한다.** 감사 증거는 행위자 계정이 사라진 뒤에도 남아야 한다. 같은 이유로 사용자는 삭제하지 않고 `enabled=false`로 비활성화만 한다.

**`change_requests.payload`는 jsonb다.** 토픽 생성과 설정 변경의 필드가 다르고, 앞으로 승인 대상 작업이 늘어날 수 있다. 저장은 느슨하게 하되 읽을 때는 타입별 Java 레코드로 역직렬화해 검증한다. 스키마 없는 데이터가 로직까지 흘러들어가지 않도록 경계를 한 곳으로 모은다.

## 승인 워크플로

```
PENDING ──승인──▶ APPROVED ──실행성공──▶ EXECUTED
   │                  │
   │                  └──실행실패──▶ EXECUTION_FAILED ──재실행──▶ EXECUTED
   ├──반려──▶ REJECTED
   └──신청자 취소──▶ CANCELLED
```

### 승인과 실행을 같은 트랜잭션에 넣지 않는다

Kafka 토픽 생성은 롤백이 불가능하다. 실행 순서는 다음과 같다.

1. `status=APPROVED` 커밋
2. `AdminClient`로 실행
3. `status=EXECUTED` 또는 `EXECUTION_FAILED` + 에러 메시지 커밋

트랜잭션 안에서 Kafka를 호출하면, 커밋이 실패했을 때 **Kafka에는 토픽이 생성됐지만 DB는 `PENDING`** 인 상태가 된다. 아무도 인지하지 못하는 불일치다.

위 순서에서 최악의 경우는 `APPROVED`에서 멈춘 요청인데, 이는 화면에 "실행 결과 미확인"으로 표시되어 운영자가 인지하고 재실행할 수 있다. 복구 불가능한 조용한 실패보다 복구 가능한 눈에 보이는 실패를 택한다.

### 재실행은 멱등적이다

실행 전에 실제 Kafka 상태를 조회한다. 토픽이 이미 존재하면 생성을 건너뛰고 `EXECUTED`로 정리한다.

### 동시 승인 방지

`UPDATE change_requests SET status='APPROVED' ... WHERE id=? AND status='PENDING'`의 영향 행 수로 판정한다. 0행이면 "이미 처리된 요청입니다"를 반환한다. 별도 락 테이블이나 버전 컬럼은 두지 않는다.

## 에러 처리

**Kafka 연결 실패 시 빈 목록을 반환하지 않는다.** 조회 화면은 "클러스터에 연결할 수 없습니다"와 마지막 성공 시각을 명시적으로 표시한다. 빈 목록은 "토픽이 하나도 없다"로 읽히고, 장애 대응 중 이 오독은 치명적이다.

**모든 `AdminClient` 호출에 명시적 타임아웃(기본 10초)을 설정한다.** 타임아웃은 즉시 실패로 보고한다. 브로커가 응답하지 않을 때의 무한 대기는 웹앱 스레드를 고갈시켜 Kafka 장애를 웹앱 장애로 확산시킨다.

**부분 실패는 부분 성공으로 표시한다.** 브로커 3대 중 1대가 응답하지 않으면 확보한 정보는 표시하고 "브로커 2 정보 조회 실패"를 함께 명시한다. 전체를 에러 화면으로 대체하면 필요한 정보를 확인할 수 없다.

`TopicExistsException`, `PolicyViolationException`, `InvalidConfigurationException`, `TopicAuthorizationException` 등은 한국어 안내 메시지로 변환한다. 스택 트레이스는 서버 로그에만 남기고 응답 본문에 포함하지 않는다.

**웹앱 자신의 클라이언트 인증서 만료를 감시한다.** 만료 30일 전부터 화면 상단에 경고를 표시한다. 인증서 만료로 Kafka 접속이 끊기는 장애는 원인 파악에 유독 오래 걸린다.

## 테스트 전략

**단위 테스트 — 가짜 `KafkaAdminGateway`로 로직 전부를 커버한다.** 클러스터 건강도 판정, RF·`min.insync.replicas` 위험 조합 판별, 승인 상태 기계 전이(허용 전이와 금지 전이 모두), 권한 판정, self-approval 차단, 발행 화이트리스트 패턴 매칭. Kafka 없이 실행되므로 빠르고 CI에서 안정적이다.

**통합 테스트 — Testcontainers Kafka(단일 브로커, PLAINTEXT)로 게이트웨이 구현체를 검증한다.** 토픽 생성, 설정 변경, 랙 계산, 메시지 조회, `group.id` 없는 Consumer가 실제로 오프셋을 커밋하지 않는지 확인. RF=3 관련 판정 로직은 단위 테스트로 커버하고 컨테이너를 3대 띄우지 않는다.

**권한 테스트 — API 레벨.** `DEVELOPER`가 타 팀 토픽 메시지를 조회하면 403, 화이트리스트 밖 토픽에 발행하면 403, 자기 신청을 자기가 승인하면 409.

**SASL_SSL + mTLS 경로는 자동 테스트하지 않는다.** Testcontainers로 인증서 체인까지 재현하는 비용이 얻는 것보다 크다. 대신 수동 검증 절차를 문서로 남긴다.

## 도입 순서

스테이징 클러스터가 없고 대상이 prod 하나뿐이다. 따라서 단계를 나눈다.

**1단계 — 읽기 전용 배포.** `feature_message_enabled=false`, `feature_write_enabled=false` 상태로 배포한다. 조회 결과가 실제 클러스터와 일치하는지, 클러스터에 부하를 주지 않는지 먼저 확인한다. SASL_SSL + mTLS 접속 검증도 이 단계에서 수행한다.

**2단계 — 소유권 정리.** 기존 토픽에 소유 팀을 일괄 매핑하고 사용자·팀·역할을 등록한다.

**3단계 — 메시지 조회·발행 활성화.** 발행 화이트리스트를 먼저 등록한 뒤 `feature_message_enabled=true`.

**4단계 — 승인 워크플로 활성화.** `feature_write_enabled=true`.

첫 배포부터 쓰기를 여는 것은 검증되지 않은 코드로 prod에 손대는 일이다.

구현 계획도 이 4단계를 그대로 따른다. 각 단계가 독립적으로 배포 가능한 단위이므로, 계획을 단계별로 나누면 중간에 멈춰도 그때까지의 결과가 쓸 수 있는 상태로 남는다.
