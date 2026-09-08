# scenario-runner Web UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** scenario-runner에 `web` 실행 모드를 추가해, 브라우저에서 안전한 `sample-*` 명령만 실행하고 출력·종료코드를 화면에서 본다.

**Architecture:** 기존 CLI는 그대로 두고, `main`에서 첫 인자가 `web`이면 서블릿 웹 컨텍스트로 기동한다(그 외에는 현재처럼 비웹). 웹은 REST 엔드포인트 2개(`/api/commands`, `/api/run`)와 정적 HTML 한 장으로 구성하며, 실행 가능한 명령은 서버 코드의 화이트리스트로 `sample-*`만 허용한다.

**Tech Stack:** Java 21, Spring Boot 3.5.5 (spring-boot-starter-web 추가), Gradle Kotlin DSL, 바닐라 HTML/JS.

**Spec:** `docs/superpowers/specs/2026-09-08-scenario-runner-web-design.md`

## Global Constraints

- 작업 디렉터리: `kafka/examples/scenario-runner/`. 빌드·테스트는 `./gradlew`.
- 기존 CLI 동작·종료코드(0/1/2)와 `application.yml`의 `spring.main.web-application-type: none` 기본값은 변경하지 않는다.
- 웹 서버는 `127.0.0.1`에만 바인딩한다.
- 화이트리스트: `sample-produce`, `sample-consume`, `sample-avro-produce`, `sample-avro-consume`, `sample-schema-evolution`. 그 외(시나리오·미등록)는 거부한다.
- 실운영 IP·호스트는 코드에 넣지 않는다.
- 커밋 메시지 말미: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

## File Structure

- `web/CommandInvoker.java` — 명령 하나를 실행하며 표준출력을 문자열로 캡처하고 종료코드를 반환. Kafka·docker와 무관한 순수 실행 래퍼.
- `web/RunnerController.java` — `/api/commands`(화이트리스트 명령 목록), `/api/run`(실행). 화이트리스트 검증이 여기 있다.
- `resources/static/index.html` — 명령 선택·count 입력·실행·출력 표시. 바닐라 JS.
- `RunnerApplication.java`(수정) — `main`에서 `web` 분기, `ApplicationRunner`가 `web`를 건너뜀.
- `build.gradle.kts`(수정) — `spring-boot-starter-web` 추가.
- `README.md`(수정) — `web` 사용법.

---

## Task 1: 웹 스타터 추가 (CLI 회귀 없음 확인)

**Files:**
- Modify: `build.gradle.kts` (dependencies 블록)
- Test: 기존 `src/test/java/.../RunnerApplicationTest.java` 재사용(회귀 확인)

**Interfaces:**
- Consumes: 없음
- Produces: 클래스패스에 Spring MVC. 기본 웹 타입은 `application.yml`의 `none`이 유지.

- [ ] **Step 1: 의존성 추가**

`build.gradle.kts`의 `dependencies { }`에 한 줄 추가(기존 `spring-boot-starter` 아래):

```kotlin
    implementation("org.springframework.boot:spring-boot-starter-web")
```

- [ ] **Step 2: 빌드·기존 테스트가 그대로 통과하는지 확인**

Run: `./gradlew test`
Expected: PASS (기존 테스트 전부). starter-web 추가만으로 CLI 동작이 바뀌지 않아야 한다 — `application.yml`에 `spring.main.web-application-type: none`이 있어 CLI 실행 시 서버가 뜨지 않는다.

- [ ] **Step 3: 커밋**

```bash
git add build.gradle.kts
git commit -m "Add spring-boot-starter-web to scenario-runner

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Task 2: CommandInvoker — 출력 캡처 실행 래퍼

**Files:**
- Create: `src/main/java/dev/devopsnote/kafkarunner/web/CommandInvoker.java`
- Test: `src/test/java/dev/devopsnote/kafkarunner/web/CommandInvokerTest.java`

**Interfaces:**
- Consumes: `dev.devopsnote.kafkarunner.command.Command` (`int run(List<String>)`, `System.out`으로 출력).
- Produces:
  - `record InvocationResult(String output, int exitCode)`
  - `CommandInvoker.invoke(Command command, List<String> args) -> InvocationResult` — `command.run`을 실행하며 그 사이 `System.out` 출력을 문자열로 모아 담는다. 예외가 나면 exitCode 1, output에 근본 원인 메시지를 담는다. `synchronized`로 직렬 실행(전역 `System.out` 교체 때문).

- [ ] **Step 1: 실패 테스트 작성**

```java
package dev.devopsnote.kafkarunner.web;

import static org.assertj.core.api.Assertions.assertThat;

import dev.devopsnote.kafkarunner.command.Command;
import java.util.List;
import org.junit.jupiter.api.Test;

class CommandInvokerTest {
    static Command printing(String text, int code) {
        return new Command() {
            public String name() { return "p"; }
            public String description() { return "p"; }
            public int run(List<String> args) { System.out.print(text + args); return code; }
        };
    }

    @Test
    void capturesStdoutAndExitCode() {
        var result = new CommandInvoker().invoke(printing("hello", 0), List.of("6"));
        assertThat(result.output()).contains("hello").contains("6");
        assertThat(result.exitCode()).isEqualTo(0);
    }

    @Test
    void propagatesNonZeroExitCode() {
        var result = new CommandInvoker().invoke(printing("x", 1), List.of());
        assertThat(result.exitCode()).isEqualTo(1);
    }

    @Test
    void exceptionBecomesExitCodeOneWithRootMessage() {
        Command boom = new Command() {
            public String name() { return "b"; }
            public String description() { return "b"; }
            public int run(List<String> args) { throw new RuntimeException(new IllegalStateException("root cause")); }
        };
        var result = new CommandInvoker().invoke(boom, List.of());
        assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.output()).contains("root cause");
    }

    @Test
    void restoresSystemOutAfterRun() {
        var original = System.out;
        new CommandInvoker().invoke(printing("x", 0), List.of());
        assertThat(System.out).isSameAs(original);
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew test --tests '*CommandInvokerTest'`
Expected: FAIL (CommandInvoker 없음 — 컴파일 에러)

- [ ] **Step 3: 최소 구현**

```java
package dev.devopsnote.kafkarunner.web;

import dev.devopsnote.kafkarunner.command.Command;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** 명령 하나를 실행하며 표준출력을 문자열로 캡처한다. 전역 System.out 을 교체하므로 직렬 실행한다. */
public class CommandInvoker {

    public record InvocationResult(String output, int exitCode) {}

    public synchronized InvocationResult invoke(Command command, List<String> args) {
        PrintStream original = System.out;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PrintStream capture = new PrintStream(buffer, true, StandardCharsets.UTF_8);
        System.setOut(capture);
        try {
            int code = command.run(args);
            return new InvocationResult(buffer.toString(StandardCharsets.UTF_8), code);
        } catch (Exception e) {
            return new InvocationResult(buffer.toString(StandardCharsets.UTF_8)
                + "\n실행 실패: " + rootMessage(e), 1);
        } finally {
            System.setOut(original);
            capture.flush();
        }
    }

    private static String rootMessage(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage();
    }
}
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew test --tests '*CommandInvokerTest'`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add src/main/java/dev/devopsnote/kafkarunner/web/CommandInvoker.java \
        src/test/java/dev/devopsnote/kafkarunner/web/CommandInvokerTest.java
git commit -m "Add CommandInvoker capturing stdout and exit code

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Task 3: RunnerController — API 2개 + 화이트리스트 (핵심 안전 속성)

**Files:**
- Create: `src/main/java/dev/devopsnote/kafkarunner/web/RunnerController.java`
- Test: `src/test/java/dev/devopsnote/kafkarunner/web/RunnerControllerTest.java`

**Interfaces:**
- Consumes: `CommandRegistry.find(String) -> Optional<Command>`; `CommandInvoker.invoke(...) -> InvocationResult`.
- Produces:
  - `GET /api/commands` → `200` JSON 배열 `[{"name":..., "description":...}]` — 화이트리스트 명령만.
  - `POST /api/run` (body `{"command": "...", "count": 6}`) → `200` `{"output":..., "exitCode":...}`. 화이트리스트 밖·미등록 → `400`. `count`는 선택(정수, 1~1000).
  - 상수 `RunnerController.SAMPLE_COMMANDS` (LinkedHashSet, 순서 고정).

- [ ] **Step 1: 실패 테스트 작성**

```java
package dev.devopsnote.kafkarunner.web;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.command.CommandRegistry;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class RunnerControllerTest {
    MockMvc mvc;

    static Command named(String name) {
        return new Command() {
            public String name() { return name; }
            public String description() { return name + " desc"; }
            public int run(List<String> args) { System.out.print("ran " + name); return 0; }
        };
    }

    @BeforeEach
    void setup() {
        // 시나리오(broker-1-down) + 샘플(sample-produce)이 모두 등록된 레지스트리
        var registry = new CommandRegistry(
            List.of(named("broker-1-down")),
            List.of(named("sample-produce")));
        mvc = MockMvcBuilders.standaloneSetup(new RunnerController(registry, new CommandInvoker())).build();
    }

    @Test
    void listsOnlySampleCommands() throws Exception {
        mvc.perform(get("/api/commands"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[?(@.name=='sample-produce')]").exists())
            .andExpect(jsonPath("$[?(@.name=='broker-1-down')]").doesNotExist());
    }

    @Test
    void runsWhitelistedSample() throws Exception {
        mvc.perform(post("/api/run").contentType("application/json")
                .content("{\"command\":\"sample-produce\",\"count\":6}"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.exitCode").value(0))
            .andExpect(jsonPath("$.output").value(org.hamcrest.Matchers.containsString("ran sample-produce")));
    }

    @Test
    void rejectsScenarioCommand() throws Exception {
        mvc.perform(post("/api/run").contentType("application/json")
                .content("{\"command\":\"broker-1-down\",\"count\":1}"))
            .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsUnknownCommand() throws Exception {
        mvc.perform(post("/api/run").contentType("application/json")
                .content("{\"command\":\"sample-nope\"}"))
            .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsOutOfRangeCount() throws Exception {
        mvc.perform(post("/api/run").contentType("application/json")
                .content("{\"command\":\"sample-produce\",\"count\":99999}"))
            .andExpect(status().isBadRequest());
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew test --tests '*RunnerControllerTest'`
Expected: FAIL (RunnerController 없음)

- [ ] **Step 3: 최소 구현**

```java
package dev.devopsnote.kafkarunner.web;

import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.command.CommandRegistry;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 웹 제어판 API. 안전을 위해 sample-* 화이트리스트 명령만 실행할 수 있다. */
@RestController
@RequestMapping("/api")
public class RunnerController {
    /** 웹에서 실행 허용하는 명령 — 브로커를 죽이지 않는 것만. 순서 고정. */
    public static final Set<String> SAMPLE_COMMANDS = new LinkedHashSet<>(List.of(
        "sample-produce", "sample-consume",
        "sample-avro-produce", "sample-avro-consume", "sample-schema-evolution"));

    private final CommandRegistry registry;
    private final CommandInvoker invoker;

    public RunnerController(CommandRegistry registry, CommandInvoker invoker) {
        this.registry = registry;
        this.invoker = invoker;
    }

    public record CommandInfo(String name, String description) {}
    public record RunRequest(String command, Integer count) {}

    @GetMapping("/commands")
    public List<CommandInfo> commands() {
        return SAMPLE_COMMANDS.stream()
            .map(registry::find)
            .flatMap(java.util.Optional::stream)
            .map(c -> new CommandInfo(c.name(), c.description()))
            .toList();
    }

    @PostMapping("/run")
    public ResponseEntity<?> run(@RequestBody RunRequest req) {
        if (req.command() == null || !SAMPLE_COMMANDS.contains(req.command())) {
            return ResponseEntity.badRequest().body("허용되지 않은 명령: " + req.command());
        }
        if (req.count() != null && (req.count() < 1 || req.count() > 1000)) {
            return ResponseEntity.badRequest().body("count 범위 오류(1~1000): " + req.count());
        }
        Command command = registry.find(req.command()).orElse(null);
        if (command == null) {
            return ResponseEntity.badRequest().body("등록되지 않은 명령: " + req.command());
        }
        List<String> args = req.count() == null ? List.of() : List.of(String.valueOf(req.count()));
        return ResponseEntity.ok(invoker.invoke(command, args));
    }
}
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew test --tests '*RunnerControllerTest'`
Expected: PASS (특히 `rejectsScenarioCommand` — 이 도구의 핵심 안전 속성)

- [ ] **Step 5: 커밋**

```bash
git add src/main/java/dev/devopsnote/kafkarunner/web/RunnerController.java \
        src/test/java/dev/devopsnote/kafkarunner/web/RunnerControllerTest.java
git commit -m "Add web control-panel API with sample-only whitelist

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Task 4: web 모드 기동 + 정적 페이지

**Files:**
- Modify: `src/main/java/dev/devopsnote/kafkarunner/RunnerApplication.java`
- Create: `src/main/resources/static/index.html`
- Test: 수동 확인(브라우저) — 자동 테스트는 Task 3가 API를 커버함

**Interfaces:**
- Consumes: `SpringApplicationBuilder`, `WebApplicationType`.
- Produces: `java -jar build/libs/scenario-runner.jar web` → `http://127.0.0.1:8088` 에서 페이지·API 제공. 다른 명령은 기존과 동일.

- [ ] **Step 1: main 분기 + ApplicationRunner 스킵 구현**

`RunnerApplication.java`를 아래로 수정한다. 핵심: 첫 인자가 `web`이면 서블릿 웹으로 기동(서버 유지, `System.exit` 호출 안 함)하고, `ApplicationRunner.run`은 `web`를 만나면 아무것도 하지 않는다(웹 모드에서도 러너 콜백이 실행되므로 "알 수 없는 명령"을 찍지 않도록).

```java
package dev.devopsnote.kafkarunner;

import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.command.CommandRegistry;
import dev.devopsnote.kafkarunner.command.UsageException;
import dev.devopsnote.kafkarunner.sample.SampleKafkaConfig;
import java.util.List;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/** CLI 진입점. 첫 인자로 명령을 고른다. 첫 인자가 web 이면 로컬 웹 제어판을 띄운다. */
@SpringBootApplication
@EnableConfigurationProperties(RunnerProperties.class)
public class RunnerApplication implements ApplicationRunner, ExitCodeGenerator {
    static final String WEB = "web";

    private final CommandRegistry registry;
    private int exitCode = 0;

    public RunnerApplication(CommandRegistry registry) { this.registry = registry; }

    public static void main(String[] args) {
        SampleKafkaConfig.trustGeneratedAvroClasses(); // Avro 로딩 전에 설정
        if (args.length > 0 && args[0].equals(WEB)) {
            // 127.0.0.1 전용 서블릿 웹으로 기동하고 서버를 유지한다(종료 코드로 내려가지 않음)
            new SpringApplicationBuilder(RunnerApplication.class)
                .web(WebApplicationType.SERVLET)
                .properties("server.address=127.0.0.1", "server.port=8088")
                .run(args);
            System.out.println("웹 제어판: http://127.0.0.1:8088  (종료: Ctrl+C)");
            return;
        }
        System.exit(SpringApplication.exit(SpringApplication.run(RunnerApplication.class, args)));
    }

    @Override public void run(ApplicationArguments args) throws Exception {
        List<String> nonOption = args.getNonOptionArgs();
        if (!nonOption.isEmpty() && nonOption.get(0).equals(WEB)) {
            return; // 웹 모드 — 서버가 요청을 처리하므로 CLI 콜백은 아무것도 하지 않는다
        }
        if (nonOption.isEmpty()) {
            System.out.print(registry.usage());
            exitCode = 2;
            return;
        }
        String name = nonOption.get(0);
        Command command = registry.find(name).orElse(null);
        if (command == null) {
            System.out.print("알 수 없는 명령: " + name + "\n\n" + registry.usage());
            exitCode = 2;
            return;
        }
        try {
            exitCode = command.run(nonOption.subList(1, nonOption.size()));
        } catch (UsageException e) {
            System.out.print("인자 오류: " + e.getMessage() + "\n\n" + registry.usage());
            exitCode = 2;
        }
    }

    @Override public int getExitCode() { return exitCode; }
}
```

- [ ] **Step 2: 기존 테스트가 여전히 통과하는지 확인**

Run: `./gradlew test`
Expected: PASS. `RunnerApplicationTest`는 `ApplicationRunner.run`을 직접 호출하는데 `web` 인자를 쓰지 않으므로 영향 없음.

- [ ] **Step 3: 정적 페이지 작성**

`src/main/resources/static/index.html`:

```html
<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>scenario-runner 제어판</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 780px; margin: 40px auto; padding: 0 16px; }
  h1 { font-size: 20px; }
  .row { display: flex; gap: 8px; align-items: center; margin: 16px 0; }
  select, input, button { font-size: 14px; padding: 6px 10px; }
  pre { background: #f4f5f7; border: 1px solid #ddd; border-radius: 6px; padding: 14px; white-space: pre-wrap; min-height: 60px; }
  .badge { font-weight: 700; padding: 2px 10px; border-radius: 999px; }
  .ok { background: #e3f7ec; color: #1a7f45; }
  .fail { background: #fdecec; color: #b42318; }
  .desc { color: #666; font-size: 13px; }
</style>
</head>
<body>
  <h1>scenario-runner 제어판 <span class="desc">(로컬 · sample 명령 전용)</span></h1>
  <div class="row">
    <select id="command"></select>
    <label>count <input id="count" type="number" value="6" min="1" max="1000" style="width:70px"></label>
    <button id="run">실행</button>
    <span id="status"></span>
  </div>
  <p class="desc" id="cmdDesc"></p>
  <pre id="output">명령을 선택하고 실행하세요.</pre>
<script>
const $ = (id) => document.getElementById(id);
async function loadCommands() {
  const res = await fetch("/api/commands");
  const list = await res.json();
  $("command").innerHTML = list.map(c => `<option value="${c.name}">${c.name}</option>`).join("");
  updateDesc(list);
  $("command").onchange = () => updateDesc(list);
}
function updateDesc(list) {
  const cur = list.find(c => c.name === $("command").value);
  $("cmdDesc").textContent = cur ? cur.description : "";
}
$("run").onclick = async () => {
  $("status").innerHTML = "실행 중…";
  $("output").textContent = "";
  try {
    const res = await fetch("/api/run", {
      method: "POST", headers: {"Content-Type": "application/json"},
      body: JSON.stringify({ command: $("command").value, count: Number($("count").value) })
    });
    if (!res.ok) {
      $("status").innerHTML = '<span class="badge fail">거부</span>';
      $("output").textContent = await res.text();
      return;
    }
    const data = await res.json();
    const ok = data.exitCode === 0;
    $("status").innerHTML = `<span class="badge ${ok ? "ok" : "fail"}">exit ${data.exitCode}</span>`;
    $("output").textContent = data.output || "(출력 없음)";
  } catch (e) {
    $("status").innerHTML = '<span class="badge fail">오류</span>';
    $("output").textContent = String(e);
  }
};
loadCommands();
</script>
</body>
</html>
```

- [ ] **Step 4: 웹 모드 수동 확인**

```bash
docker compose up -d           # 클러스터 준비 (curl localhost:8081/subjects 가 [] 면 완료)
./gradlew bootJar
java -jar build/libs/scenario-runner.jar web &
curl -s localhost:8088/api/commands        # sample-* 5개가 나오면 OK
curl -s -X POST localhost:8088/api/run -H 'Content-Type: application/json' \
  -d '{"command":"sample-produce","count":3}'   # {"output":"...","exitCode":0}
curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8088/api/run \
  -H 'Content-Type: application/json' -d '{"command":"broker-1-down"}'   # 400
```

Expected: 명령 목록에 시나리오 없음, 샘플 실행 200, 시나리오 400. 브라우저 `http://127.0.0.1:8088`에서 드롭다운·실행·출력 확인 후 서버 종료.

- [ ] **Step 5: 커밋**

```bash
git add src/main/java/dev/devopsnote/kafkarunner/RunnerApplication.java \
        src/main/resources/static/index.html
git commit -m "Add web mode bootstrap and control-panel page

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Task 5: README에 web 사용법 추가

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: 없음
- Produces: 없음

- [ ] **Step 1: 실행 방법 절에 web 모드 추가**

`README.md`의 "실행 방법" 절(빌드 다음)에 아래 블록을 추가한다:

```markdown
### 웹 제어판 (선택)

`sample-*` 명령을 브라우저에서 실행하고 출력을 화면에서 볼 수 있습니다. 장애 시나리오는 웹에서 실행할 수 없습니다(서버가 거부) — 브로커를 stop/start 하는 명령은 CLI 전용입니다.

```bash
java -jar build/libs/scenario-runner.jar web   # http://127.0.0.1:8088 (로컬 전용)
```
```

- [ ] **Step 2: 문서 링크·정합성 확인**

Run: `git diff README.md`
Expected: web 절이 추가되고 기존 CLI 설명은 그대로. jar 경로가 `build/libs/scenario-runner.jar`로 일치.

- [ ] **Step 3: 커밋**

```bash
git add README.md
git commit -m "Document scenario-runner web control panel

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Self-Review (작성자 확인 완료)

- **Spec coverage**: web 명령(Task 4)·API 2개(Task 3)·화이트리스트(Task 3)·127.0.0.1(Task 4)·단일 HTML(Task 4)·CLI 불변(Task 1·4)·안전 테스트(Task 3)·README(Task 5) 모두 태스크로 커버됨. 스트리밍·시나리오 웹실행·인증은 스펙의 비목표라 의도적으로 제외.
- **Placeholder scan**: 모든 코드 스텝에 실제 코드 포함. TODO/TBD 없음.
- **Type consistency**: `CommandInvoker.invoke -> InvocationResult(output, exitCode)`가 Task 2 정의와 Task 3 사용에서 일치. `CommandRegistry.find(String) -> Optional<Command>`, `SAMPLE_COMMANDS` 상수명 일치. `RunnerApplication.WEB` 분기 일관.
