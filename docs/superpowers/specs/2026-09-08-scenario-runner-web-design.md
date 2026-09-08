# scenario-runner Web UI 설계

## 배경

`kafka/examples/scenario-runner`는 현재 CLI 전용 도구다. 장애 시나리오 검증(`normal-roundtrip`, `broker-1-down` 등)과 usage-guide 코드 실습(`sample-produce`, `sample-avro-*`, `sample-schema-evolution`)을 명령으로 실행하고, 출력은 터미널과 `reports/*.md`로만 본다.

여기에 **웹 제어판**을 붙여, 브라우저에서 샘플 명령을 실행하고 결과를 화면에서 보게 한다. 학습·데모 상황에서 터미널 없이 usage-guide의 프로듀서/컨슈머·스키마 진화 흐름을 보여줄 수 있게 하는 것이 목적이다.

## 목표와 비목표

**목표**
- 브라우저에서 `sample-*` 명령을 실행하고 출력·종료코드를 화면에서 확인
- 기존 CLI 사용 방식은 그대로 유지 (웹은 추가 기능)
- jar 하나로 배포 — 별도 프론트 빌드 체인 없음

**비목표 (YAGNI)**
- 장애 시나리오(`broker-*`, `total-outage`)의 웹 실행 — 안전상 API에서 거부
- 실시간 스트리밍(SSE/WebSocket) — 샘플은 수 초라 요청-완료 후 일괄 표시로 충분
- 시나리오 리포트 시각화, 인증, 원격 접근

## 핵심 결정

1. **목적 = 실행+결과 제어판** (읽기 전용 대시보드나 정적 설명 페이지가 아님)
2. **제어 범위 = 샘플 명령만** — 웹은 docker를 죽이는 명령을 실행할 수 없다
3. **프론트 = 단일 HTML 페이지** (Spring 정적 서빙, 빌드 도구 없음)

## 아키텍처

기존 CLI를 해치지 않고 `web`라는 새 명령을 추가한다. 이 명령일 때만 웹 서버가 켜진다.

```text
java -jar scenario-runner.jar sample-produce 6   # 기존 CLI — 변경 없음
java -jar scenario-runner.jar web                # 신규 — 웹 서버 기동 (127.0.0.1)
        │
        ├─ GET  /              → 단일 HTML 페이지 (resources/static/index.html)
        ├─ GET  /api/commands  → 웹에서 실행 가능한 샘플 명령 목록 (JSON)
        └─ POST /api/run       → {command, count} 실행 → {output, exitCode} 반환
```

- 현재 `spring.main.web-application-type: none`이므로, `web` 명령일 때만 서블릿 웹 컨텍스트로 기동한다. 다른 명령은 지금처럼 비웹으로 실행돼 CLI 동작·종료코드가 그대로 유지된다.
- 프론트는 정적 HTML/JS 한 장. `fetch`로 `/api/*`를 호출해 결과를 `<pre>`에 표시한다.

## 컴포넌트

| 컴포넌트 | 역할 | 의존 |
| --- | --- | --- |
| `web/WebCommand` | `Command` 구현. `web` 실행 시 서블릿 웹 컨텍스트를 띄우고 대기 | Spring Boot |
| `web/RunnerController` | `/api/commands`, `/api/run` 2개 엔드포인트 | CommandRegistry |
| `web/CommandInvoker` | 명령 실행을 감싸 표준출력 문자열 + 종료코드를 캡처 | 기존 sample 명령 |
| `resources/static/index.html` | 명령 선택·count 입력·실행·출력 표시 | 없음 (바닐라 JS) |

- 기존 `Command` 인터페이스와 `CommandRegistry`를 재사용한다. 샘플 명령 자체는 수정하지 않는다.
- `CommandInvoker`는 `System.out`을 일시적으로 캡처해 명령의 출력을 문자열로 모은다(명령들이 `System.out.printf`로 출력하므로). 실행은 순차·단건으로 제한한다.

## 안전 설계 (가장 중요한 속성)

웹이 브로커를 죽이는 것을 **서버 코드에서** 막는다 — UI에서 감추는 것으로는 부족하다.

- `/api/run`은 **화이트리스트**에 있는 `sample-*` 명령만 허용한다. 목록: `sample-produce`, `sample-consume`, `sample-avro-produce`, `sample-avro-consume`, `sample-schema-evolution`.
- 시나리오 명령(`normal-roundtrip`, `broker-1-down`, `broker-2-down`, `total-outage`)과 미등록 명령은 **400으로 거부**한다.
- 웹 서버는 `127.0.0.1`에만 바인딩한다. 원격 접근 불가.
- `count` 인자는 정수·상한(예: 1000)으로 검증한다.

## 데이터 흐름

```text
[브라우저] 명령 선택 + count 입력 → 실행 클릭
   │ POST /api/run {command:"sample-produce", count:6}
   ▽
[RunnerController] 화이트리스트 검사 (실패 시 400)
   │ 통과 시
   ▽
[CommandInvoker] 출력 캡처하며 command.run(args) 실행
   │ {output:"전송 OK key=... partition=...", exitCode:0}
   ▽
[브라우저] <pre>에 출력 표시, 종료코드로 성공/실패 배지
```

## 오류 처리

- 화이트리스트 위반 → 400 + 사유 메시지
- `count` 형식 오류 → 400
- 명령 실행 중 예외 → 500 + 예외 메시지(근본 원인). 종료코드 1로 표기
- Kafka/SR 미기동 상태에서 실행 → 명령이 이미 내는 안내 메시지(예: "직렬화 실패 — Schema Registry …")가 그대로 output에 담겨 화면에 표시됨

## 테스트

docker 없이 도는 단위·MockMvc 수준으로 한정한다.

- **안전 속성(필수)**: `/api/run`에 `broker-1-down` 등 시나리오 명령을 보내면 400으로 거부되는지
- `/api/commands`가 샘플 명령만 반환하는지 (시나리오 명령이 목록에 없는지)
- `/api/run`에 정상 샘플 명령 + count → 200, 응답에 output·exitCode 필드가 있는지 (명령 실행은 mock 또는 Kafka 불요한 경로로)
- `count` 형식·상한 검증

## 파일 변경

**신규**
- `src/main/java/dev/devopsnote/kafkarunner/web/WebCommand.java`
- `src/main/java/dev/devopsnote/kafkarunner/web/RunnerController.java`
- `src/main/java/dev/devopsnote/kafkarunner/web/CommandInvoker.java`
- `src/main/resources/static/index.html`
- 테스트: `web/RunnerControllerTest.java`

**수정**
- `build.gradle.kts` — `spring-boot-starter-web` 의존 추가
- `README.md` — `web` 명령 사용법 추가

**변경 없음**
- 기존 sample/scenario 명령, `application.yml`의 비웹 기본값, CLI 진입점 동작
