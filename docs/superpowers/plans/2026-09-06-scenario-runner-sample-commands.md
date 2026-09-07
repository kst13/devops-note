# scenario-runner 사용 예시 명령 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** scenario-runner jar에 `sample-*` 명령 5개를 추가해, usage-guide의 Spring Kafka 코드(JSON)와 Schema Registry+Avro 흐름(전송·수신·스키마 진화)을 로컬 클러스터에서 실행할 수 있게 한다.

**Architecture:** 기존 `RunnerApplication`의 시나리오 맵을 `Command` 인터페이스 + `CommandRegistry`로 일반화하고, 기존 시나리오 흐름은 `ScenarioCommand`가 그대로 감싼다. 샘플 명령은 `sample/` 패키지의 Spring `@Component`로 추가되며 `KafkaTemplate`, `@KafkaListener(autoStartup=false)`, Confluent `KafkaAvroSerializer`를 쓴다. compose에 Schema Registry 컨테이너를 추가하고, Avro 클래스는 `avro-maven-plugin`이 `src/main/avro/*.avsc`에서 생성한다.

**Tech Stack:** Java 21, Spring Boot 3.5.5, Spring Kafka 3.3.9(kafka-clients 3.9.1), Confluent kafka-avro-serializer 7.9.9, Avro 1.12.2, `confluentinc/cp-schema-registry:7.9.9`, Maven 3.9, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-09-06-scenario-runner-sample-commands-design.md`

**작업 디렉터리:** 모든 `mvn`/`docker compose`/`java -jar` 명령은 `kafka/examples/scenario-runner/`에서 실행한다. `git` 명령은 저장소 루트 기준 경로를 쓴다.

**주의 사항 (전 작업 공통)**

- 워킹 트리에 이 작업과 무관한 미커밋 변경이 있다(`AGENTS.md`, `CLAUDE.md`, `kafka/examples/home-lab/README.md`, `kafka/examples/home-lab/setup.sh`). 커밋할 때 반드시 파일을 명시해서 `git add`하고, `git add -A`나 `git add .`를 쓰지 않는다.
- 스펙과의 차이 한 곳: 스펙의 `SampleTopics.java`는 만들지 않는다. 이미 `FaultInjector.ensureTopic(topic, partitions)`가 같은 일(RF3, minISR2, 이미 있으면 통과)을 하므로 재사용한다.
- 커밋 메시지는 짧은 명령형 영어 제목이며 아래 트레일러를 붙인다.

```text
Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
```

---

## 파일 구조

| 경로 (`kafka/examples/scenario-runner/` 기준) | 역할 | 작업 |
| --- | --- | --- |
| `src/main/java/.../command/Command.java` | CLI 명령 인터페이스 | 1 |
| `src/main/java/.../command/ScenarioCommand.java` | 기존 시나리오 흐름을 Command로 감쌈 | 1 |
| `src/main/java/.../command/CommandRegistry.java` | 이름 → Command 매핑, 사용법 문자열 | 1 |
| `src/main/java/.../command/CommandConfig.java` | `CommandRegistry` 빈 정의 | 1 |
| `src/main/java/.../RunnerApplication.java` | 디스패치와 종료 코드만 담당하도록 축소 | 1 |
| `src/main/java/.../RunnerProperties.java` | `sampleTopic`, `sampleAvroTopic` 추가 | 1 |
| `src/main/resources/application.yml` | 샘플 토픽, `schema.registry.url` | 1, 2 |
| `src/test/java/.../command/CommandRegistryTest.java` | 등록 순서·미등록·중복 | 1 |
| `src/test/java/.../RunnerApplicationTest.java` | CLI 종료 코드 규약 | 1 |
| `pom.xml` | Confluent 저장소, Avro 의존성, avro-maven-plugin | 2 |
| `docker-compose.yml` | `schema-registry` 서비스 | 2 |
| `src/main/avro/OrderCreated.avsc` | v1 스키마(코드 생성 대상) | 2 |
| `src/main/resources/schemas/order-created-v2.avsc` | 호환 스키마 | 3 |
| `src/main/resources/schemas/order-created-incompatible.avsc` | 비호환 스키마 | 3 |
| `src/test/java/.../sample/SchemaEvolutionTest.java` | Avro 로컬 호환성 검사 | 3 |
| `src/main/java/.../sample/OrderCreatedEvent.java` | JSON 단계 record | 4 |
| `src/main/java/.../sample/SampleEvents.java` | 샘플 이벤트 생성 | 4, 5 |
| `src/main/java/.../sample/SampleArgs.java` | `[count]` 인자 파싱 | 4 |
| `src/main/java/.../sample/SampleKafkaConfig.java` | 샘플용 KafkaTemplate/리스너 팩토리/SR 클라이언트 빈 | 4, 5 |
| `src/main/java/.../sample/SampleProduceCommand.java` | `sample-produce` | 4 |
| `src/main/java/.../sample/SampleConsumeCommand.java` | `sample-consume` | 4 |
| `src/test/java/.../RunnerContextTest.java` | Spring 컨텍스트에서 명령 9개 등록 확인 | 4, 5, 6 |
| `src/main/java/.../sample/SampleAvroProduceCommand.java` | `sample-avro-produce` | 5 |
| `src/main/java/.../sample/SampleAvroConsumeCommand.java` | `sample-avro-consume` | 5 |
| `src/main/java/.../sample/SchemaEvolutionCommand.java` | `sample-schema-evolution` | 6 |
| `README.md` | 사용 예시 절, 전제 조건, Confluent 저장소 안내 | 7 |
| `../../usage-guide/03-producer.md`, `04-consumer.md`, `08-onboarding.md` | 실행해 보기 링크 | 7 |

`...` = `dev/devopsnote/kafkarunner`.

---

### Task 1: Command 인터페이스와 디스패치 리팩터

기존 4개 시나리오의 동작은 바뀌지 않는다. `RunnerApplication`이 명령 이름으로 `Command`를 찾아 실행하고 종료 코드만 다루게 만든다.

**Files:**
- Create: `src/main/java/dev/devopsnote/kafkarunner/command/Command.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/command/ScenarioCommand.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/command/CommandRegistry.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/command/CommandConfig.java`
- Modify: `src/main/java/dev/devopsnote/kafkarunner/RunnerApplication.java`
- Modify: `src/main/java/dev/devopsnote/kafkarunner/RunnerProperties.java`
- Modify: `src/main/resources/application.yml`
- Test: `src/test/java/dev/devopsnote/kafkarunner/command/CommandRegistryTest.java`
- Test: `src/test/java/dev/devopsnote/kafkarunner/RunnerApplicationTest.java`

- [ ] **Step 1: 실패하는 테스트 작성 — CommandRegistryTest**

```java
package dev.devopsnote.kafkarunner.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.devopsnote.kafkarunner.RunnerProperties;
import java.util.List;
import org.junit.jupiter.api.Test;

class CommandRegistryTest {
    private static final RunnerProperties PROPS = new RunnerProperties(
        "localhost:9092", "test.scenario.events", 3, List.of("kafka1", "kafka2", "kafka3"), "reports",
        "commerce.order.created", "commerce.order.created.avro");

    record FakeCommand(String name) implements Command {
        @Override public String description() { return "fake"; }
        @Override public int run(List<String> args) { return 0; }
    }

    @Test
    void registersScenariosFirstThenSamples() {
        var registry = new CommandRegistry(PROPS, List.of(new FakeCommand("sample-x")));
        assertThat(registry.names()).containsExactly(
            "normal-roundtrip", "broker-1-down", "broker-2-down", "total-outage", "sample-x");
    }

    @Test
    void unknownNameIsEmpty() {
        var registry = new CommandRegistry(PROPS, List.of());
        assertThat(registry.find("nope")).isEmpty();
        assertThat(registry.find("broker-1-down")).isPresent();
    }

    @Test
    void rejectsDuplicateNames() {
        assertThatThrownBy(() -> new CommandRegistry(PROPS, List.of(new FakeCommand("normal-roundtrip"))))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("normal-roundtrip");
    }

    @Test
    void usageListsScenariosAndSamplesInSeparateGroups() {
        var registry = new CommandRegistry(PROPS, List.of(new FakeCommand("sample-x")));
        String usage = registry.usage();
        assertThat(usage).contains("장애 시나리오:").contains("사용 예시:");
        assertThat(usage.indexOf("장애 시나리오:")).isLessThan(usage.indexOf("사용 예시:"));
        assertThat(usage).contains("normal-roundtrip").contains("sample-x");
    }
}
```

- [ ] **Step 2: 실패하는 테스트 작성 — RunnerApplicationTest**

```java
package dev.devopsnote.kafkarunner;

import static org.assertj.core.api.Assertions.assertThat;

import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.command.CommandRegistry;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;

class RunnerApplicationTest {
    private static final RunnerProperties PROPS = new RunnerProperties(
        "localhost:9092", "t", 3, List.of("kafka1", "kafka2", "kafka3"), "reports", "s", "s.avro");

    static Command command(String name, int exitCode) {
        return new Command() {
            @Override public String name() { return name; }
            @Override public String description() { return name; }
            @Override public int run(List<String> args) {
                if (!args.isEmpty() && args.get(0).equals("bad")) throw new IllegalArgumentException("bad arg");
                return exitCode;
            }
        };
    }

    static int exitCodeFor(String... args) throws Exception {
        var app = new RunnerApplication(new CommandRegistry(PROPS, List.of(command("ok", 0), command("fail", 1))));
        app.run(new DefaultApplicationArguments(args));
        return app.getExitCode();
    }

    @Test void noArgsIsUsageError() throws Exception { assertThat(exitCodeFor()).isEqualTo(2); }
    @Test void unknownCommandIsUsageError() throws Exception { assertThat(exitCodeFor("nope")).isEqualTo(2); }
    @Test void commandExitCodeIsPropagated() throws Exception {
        assertThat(exitCodeFor("ok")).isEqualTo(0);
        assertThat(exitCodeFor("fail")).isEqualTo(1);
    }
    @Test void illegalArgumentIsUsageError() throws Exception { assertThat(exitCodeFor("ok", "bad")).isEqualTo(2); }
}
```

- [ ] **Step 3: 테스트가 컴파일 실패로 실패하는지 확인**

Run: `mvn -q test -Dtest='CommandRegistryTest,RunnerApplicationTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — `package dev.devopsnote.kafkarunner.command does not exist`, `RunnerProperties` 생성자 인자 수 불일치.

- [ ] **Step 4: RunnerProperties와 application.yml에 샘플 토픽 추가**

`src/main/java/dev/devopsnote/kafkarunner/RunnerProperties.java` 전체를 다음으로 교체:

```java
package dev.devopsnote.kafkarunner;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "runner")
public record RunnerProperties(String bootstrapServers, String topic, int partitions,
                               List<String> containers, String reportDir,
                               String sampleTopic, String sampleAvroTopic) {}
```

`src/main/resources/application.yml`의 `runner:` 블록을 다음으로 교체(나머지는 그대로):

```yaml
runner:
  bootstrap-servers: localhost:9092,localhost:9192,localhost:9292
  topic: test.scenario.events
  partitions: 3
  containers: [kafka1, kafka2, kafka3]
  report-dir: reports
  # sample-* 명령이 쓰는 토픽 — usage-guide 03·04 의 예시 토픽과 같은 이름
  sample-topic: commerce.order.created
  sample-avro-topic: commerce.order.created.avro
```

- [ ] **Step 5: Command 인터페이스 작성**

`src/main/java/dev/devopsnote/kafkarunner/command/Command.java`:

```java
package dev.devopsnote.kafkarunner.command;

import java.util.List;

/** CLI 첫 인자로 선택되는 실행 단위. 반환값은 프로세스 종료 코드 (0 성공/PASS, 1 실패/FAIL).
 *  인자 형식 오류는 IllegalArgumentException 을 던지면 RunnerApplication 이 종료 코드 2 로 바꾼다. */
public interface Command {
    String name();
    String description();
    int run(List<String> args) throws Exception;
}
```

- [ ] **Step 6: ScenarioCommand 작성 (기존 RunnerApplication.run 의 시나리오 흐름을 그대로 이동)**

`src/main/java/dev/devopsnote/kafkarunner/command/ScenarioCommand.java`:

```java
package dev.devopsnote.kafkarunner.command;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.ledger.Judge;
import dev.devopsnote.kafkarunner.ledger.Ledger;
import dev.devopsnote.kafkarunner.report.Reporter;
import dev.devopsnote.kafkarunner.scenario.Broker1Down;
import dev.devopsnote.kafkarunner.scenario.Broker2Down;
import dev.devopsnote.kafkarunner.scenario.NormalRoundtrip;
import dev.devopsnote.kafkarunner.scenario.Scenario;
import dev.devopsnote.kafkarunner.scenario.TotalOutage;
import java.util.List;

/** 장애 시나리오 하나를 Command 로 감싼다: 토픽 초기화 → 시나리오 실행 → 판정 → 리포트. */
public class ScenarioCommand implements Command {
    private final Scenario scenario;
    private final RunnerProperties props;

    public ScenarioCommand(Scenario scenario, RunnerProperties props) {
        this.scenario = scenario;
        this.props = props;
    }

    /** 등록 순서가 곧 사용법 출력 순서. */
    public static List<Command> all(RunnerProperties props) {
        return List.of(
            new ScenarioCommand(new NormalRoundtrip(), props),
            new ScenarioCommand(new Broker1Down(), props),
            new ScenarioCommand(new Broker2Down(), props),
            new ScenarioCommand(new TotalOutage(), props));
    }

    @Override public String name() { return scenario.name(); }
    @Override public String description() { return scenario.description(); }

    @Override public int run(List<String> args) throws Exception {
        FaultInjector injector = new FaultInjector(props.bootstrapServers(), props.containers());
        Ledger ledger = new Ledger();
        try {
            injector.ensureAllRunning();
            injector.resetTopic(props.topic(), props.partitions());
            scenario.run(props, injector, ledger);
        } finally {
            injector.startAll(); // 어떤 경우에도 클러스터 원상 복구
        }
        var result = new Judge().judge(ledger, scenario.expectation());
        new Reporter().write(props.reportDir(), scenario.name(), scenario.description(), result);
        return result.pass() ? 0 : 1;
    }
}
```

- [ ] **Step 7: CommandRegistry와 CommandConfig 작성**

`src/main/java/dev/devopsnote/kafkarunner/command/CommandRegistry.java`:

```java
package dev.devopsnote.kafkarunner.command;

import dev.devopsnote.kafkarunner.RunnerProperties;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** 명령 이름 → Command. 장애 시나리오 4개가 먼저, 그 뒤에 Spring 빈으로 등록된 샘플 명령. */
public class CommandRegistry {
    private final Map<String, Command> commands = new LinkedHashMap<>();

    public CommandRegistry(RunnerProperties props, List<Command> sampleCommands) {
        ScenarioCommand.all(props).forEach(this::add);
        sampleCommands.forEach(this::add);
    }

    private void add(Command command) {
        if (commands.putIfAbsent(command.name(), command) != null) {
            throw new IllegalStateException("중복 명령 이름: " + command.name());
        }
    }

    public Optional<Command> find(String name) { return Optional.ofNullable(commands.get(name)); }

    public List<String> names() { return List.copyOf(commands.keySet()); }

    public String usage() {
        var sb = new StringBuilder("사용법: java -jar scenario-runner.jar <명령> [인자]\n\n장애 시나리오:\n");
        commands.values().stream().filter(c -> c instanceof ScenarioCommand).forEach(c -> appendLine(sb, c));
        sb.append("\n사용 예시:\n");
        commands.values().stream().filter(c -> !(c instanceof ScenarioCommand)).forEach(c -> appendLine(sb, c));
        return sb.toString();
    }

    private static void appendLine(StringBuilder sb, Command c) {
        sb.append(String.format("  %-26s %s%n", c.name(), c.description()));
    }
}
```

`src/main/java/dev/devopsnote/kafkarunner/command/CommandConfig.java`:

```java
package dev.devopsnote.kafkarunner.command;

import dev.devopsnote.kafkarunner.RunnerProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CommandConfig {
    /** Command 빈이 하나도 없어도 동작해야 하므로 List 주입 대신 ObjectProvider 를 쓴다.
     *  orderedStream() 은 @Order 를 존중한다 — 샘플 명령이 사용법에 나오는 순서. */
    @Bean
    public CommandRegistry commandRegistry(RunnerProperties props, ObjectProvider<Command> commands) {
        return new CommandRegistry(props, commands.orderedStream().toList());
    }
}
```

- [ ] **Step 8: RunnerApplication을 디스패치 전용으로 교체**

`src/main/java/dev/devopsnote/kafkarunner/RunnerApplication.java` 전체:

```java
package dev.devopsnote.kafkarunner;

import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.command.CommandRegistry;
import java.util.List;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/** CLI 진입점. 첫 인자로 명령을 고르고 나머지 인자를 넘긴 뒤 종료 코드만 다룬다 (0 성공, 1 실패, 2 사용법 오류). */
@SpringBootApplication
@EnableConfigurationProperties(RunnerProperties.class)
public class RunnerApplication implements ApplicationRunner, ExitCodeGenerator {
    private final CommandRegistry registry;
    private int exitCode = 0;

    public RunnerApplication(CommandRegistry registry) { this.registry = registry; }

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(RunnerApplication.class, args)));
    }

    @Override public void run(ApplicationArguments args) throws Exception {
        List<String> nonOption = args.getNonOptionArgs();
        if (nonOption.isEmpty()) {
            System.out.print(registry.usage());
            exitCode = 2;
            return;
        }
        String name = nonOption.get(0);
        Command command = registry.find(name).orElse(null);
        if (command == null) {
            System.out.println("알 수 없는 명령: " + name + "\n\n" + registry.usage());
            exitCode = 2;
            return;
        }
        try {
            exitCode = command.run(nonOption.subList(1, nonOption.size()));
        } catch (IllegalArgumentException e) {
            System.out.println("인자 오류: " + e.getMessage() + "\n\n" + registry.usage());
            exitCode = 2;
        }
    }

    @Override public int getExitCode() { return exitCode; }
}
```

- [ ] **Step 9: 전체 테스트 통과 확인**

Run: `mvn -q test`
Expected: `Tests run: 11, Failures: 0, Errors: 0` 근처 (기존 Judge 3 + Ledger + FaultInjector 1 + 신규 8). BUILD SUCCESS.

- [ ] **Step 10: jar 사용법 출력 확인 (Docker 불필요)**

Run: `mvn -q package -DskipTests && java -jar target/scenario-runner.jar; echo "exit=$?"`
Expected: `장애 시나리오:` 아래 4개 명령, `사용 예시:` 아래 비어 있음, `exit=2`.

- [ ] **Step 11: 커밋**

```bash
cd /Users/maro/Documents/devops-note
git add kafka/examples/scenario-runner/src/main/java/dev/devopsnote/kafkarunner/command \
        kafka/examples/scenario-runner/src/main/java/dev/devopsnote/kafkarunner/RunnerApplication.java \
        kafka/examples/scenario-runner/src/main/java/dev/devopsnote/kafkarunner/RunnerProperties.java \
        kafka/examples/scenario-runner/src/main/resources/application.yml \
        kafka/examples/scenario-runner/src/test/java/dev/devopsnote/kafkarunner/command \
        kafka/examples/scenario-runner/src/test/java/dev/devopsnote/kafkarunner/RunnerApplicationTest.java
git commit -m "Introduce command registry in scenario runner

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: 빌드·인프라 구성 (Confluent 의존성, Avro 코드 생성, SR 컨테이너)

코드 생성과 컨테이너 정의라 단위 테스트 대상이 아니다. 검증은 컴파일 결과 파일과 compose 설정 검사로 한다.

**Files:**
- Modify: `pom.xml`
- Modify: `docker-compose.yml`
- Modify: `src/main/resources/application.yml`
- Create: `src/main/avro/OrderCreated.avsc`

- [ ] **Step 1: v1 스키마 파일 작성**

`src/main/avro/OrderCreated.avsc`:

```json
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "dev.devopsnote.kafkarunner.sample.avro",
  "doc": "주문 생성 이벤트 v1 — usage-guide 의 OrderCreatedEvent 와 같은 필드",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "customerId", "type": "string"},
    {"name": "amount", "type": "long"},
    {"name": "createdAt", "type": "string", "doc": "ISO-8601 문자열"}
  ]
}
```

- [ ] **Step 2: pom.xml 수정**

`<properties>` 블록에 두 줄 추가:

```xml
    <confluent.version>7.9.9</confluent.version>
    <avro.version>1.12.2</avro.version>
```

`<dependencies>` 안, `spring-kafka` 의존성 바로 뒤에 추가:

```xml
    <!-- Avro 런타임: avro-maven-plugin 과 같은 버전으로 고정해 생성 코드와 맞춘다 -->
    <dependency>
      <groupId>org.apache.avro</groupId>
      <artifactId>avro</artifactId>
      <version>${avro.version}</version>
    </dependency>
    <!-- Schema Registry 연동 serializer (kafka-schema-registry-client 포함). Maven Central 에 없어 아래 Confluent 저장소가 필요 -->
    <dependency>
      <groupId>io.confluent</groupId>
      <artifactId>kafka-avro-serializer</artifactId>
      <version>${confluent.version}</version>
    </dependency>
```

`</dependencies>` 바로 뒤, `<build>` 앞에 추가:

```xml
  <repositories>
    <repository>
      <id>confluent</id>
      <url>https://packages.confluent.io/maven/</url>
    </repository>
  </repositories>
```

`<plugins>` 안, `spring-boot-maven-plugin` 뒤에 추가:

```xml
      <plugin>
        <groupId>org.apache.avro</groupId>
        <artifactId>avro-maven-plugin</artifactId>
        <version>${avro.version}</version>
        <executions>
          <execution>
            <phase>generate-sources</phase>
            <goals><goal>schema</goal></goals>
            <configuration>
              <sourceDirectory>${project.basedir}/src/main/avro</sourceDirectory>
              <outputDirectory>${project.build.directory}/generated-sources/avro</outputDirectory>
              <!-- CharSequence 대신 String getter 가 나오도록 -->
              <stringType>String</stringType>
            </configuration>
          </execution>
        </executions>
      </plugin>
```

- [ ] **Step 3: 컴파일과 코드 생성 확인**

Run: `mvn -q compile && ls target/generated-sources/avro/dev/devopsnote/kafkarunner/sample/avro/ && grep -c "public java.lang.String getOrderId" target/generated-sources/avro/dev/devopsnote/kafkarunner/sample/avro/OrderCreated.java`
Expected: `OrderCreated.java` 출력, grep 결과 `1`. 최초 실행은 Confluent 저장소에서 의존성을 받느라 1~3분 걸릴 수 있다.

만약 `io.confluent:kafka-avro-serializer:7.9.9` 해석에 실패하면 `curl -sI https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/7.9.9/kafka-avro-serializer-7.9.9.pom | head -1`로 네트워크를 확인한다. 200이 아니면 사내 미러/프록시 문제이므로 사용자에게 보고하고 중단한다.

- [ ] **Step 4: docker-compose.yml에 Schema Registry 추가**

파일 맨 위 주석 3줄을 다음으로 교체:

```yaml
# 시나리오 러너 전용 로컬 3브로커 클러스터 + Schema Registry (PLAINTEXT, 단일 머신)
# 실서버 배포용이 아니다 — 장애 시나리오 재현·검증과 sample-* 사용 예시 전용.
# 브로커 간/쿼럼은 컨테이너 DNS(kafka1/2/3), 러너는 localhost:9092/9192/9292 로 접속.
# Schema Registry 는 브로커의 INTERNAL 리스너(29092)로 붙고 러너는 localhost:8081 로 접속.
```

`services:` 블록 끝(`kafka3` 정의 뒤)에 추가:

```yaml
  # 장애 시나리오는 runner.containers(kafka1/2/3)만 stop/start 하므로 이 컨테이너는 시나리오에 영향을 주지 않는다.
  # total-outage 로 브로커가 전부 내려가면 SR 은 에러 로그를 내며 대기하다 복구 후 다시 붙는다 —
  # "SR 은 브로커에 의존하지만 브로커는 SR 을 모른다"의 재현.
  schema-registry:
    image: confluentinc/cp-schema-registry:7.9.9
    container_name: schema-registry
    restart: "no"
    depends_on: [kafka1, kafka2, kafka3]
    ports: ["8081:8081"]
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka1:29092,kafka2:29092,kafka3:29092
      # _schemas 를 RF3 로 만들어 브로커 1대 정지 중에도 SR 이 동작하게 한다
      SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR: 3
      SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: backward
```

- [ ] **Step 5: application.yml에 schema.registry.url 추가**

`spring.kafka` 블록의 `bootstrap-servers` 줄 바로 아래에 추가(들여쓰기 4칸, `producer:`와 같은 레벨):

```yaml
    properties:
      # Avro serializer/deserializer 와 SchemaRegistryClient 가 읽는다 (usage-guide 08 과 같은 키)
      schema.registry.url: http://localhost:8081
```

결과 `spring.kafka` 블록:

```yaml
  kafka:
    bootstrap-servers: ${runner.bootstrap-servers}
    properties:
      # Avro serializer/deserializer 와 SchemaRegistryClient 가 읽는다 (usage-guide 08 과 같은 키)
      schema.registry.url: http://localhost:8081
    producer:
      acks: all
      properties:
        enable.idempotence: true
    consumer:
      auto-offset-reset: earliest
      enable-auto-commit: false
```

- [ ] **Step 6: compose 문법 검사와 기존 테스트 재확인**

Run: `docker compose config --quiet && echo COMPOSE_OK && mvn -q test`
Expected: `COMPOSE_OK`, BUILD SUCCESS (테스트 수 변화 없음).

- [ ] **Step 7: 커밋**

```bash
cd /Users/maro/Documents/devops-note
git add kafka/examples/scenario-runner/pom.xml \
        kafka/examples/scenario-runner/docker-compose.yml \
        kafka/examples/scenario-runner/src/main/resources/application.yml \
        kafka/examples/scenario-runner/src/main/avro/OrderCreated.avsc
git commit -m "Add Schema Registry and Avro build setup to scenario runner

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: 진화용 스키마 파일과 로컬 호환성 테스트

SR 없이도 스키마 파일이 의도대로(v2 호환, incompatible 비호환) 작성됐는지 Avro API로 검증한다.

**Files:**
- Create: `src/main/resources/schemas/order-created-v2.avsc`
- Create: `src/main/resources/schemas/order-created-incompatible.avsc`
- Test: `src/test/java/dev/devopsnote/kafkarunner/sample/SchemaEvolutionTest.java`

- [ ] **Step 1: 실패하는 테스트 작성**

```java
package dev.devopsnote.kafkarunner.sample;

import static org.assertj.core.api.Assertions.assertThat;

import dev.devopsnote.kafkarunner.sample.avro.OrderCreated;
import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.junit.jupiter.api.Test;

/** BACKWARD 호환 = 새 스키마(reader)로 옛 데이터(writer=v1)를 읽을 수 있다. SR 이 하는 검사와 같은 규칙을 로컬에서 확인한다. */
class SchemaEvolutionTest {
    private static final Schema V1 = OrderCreated.getClassSchema();

    static Schema load(String name) throws IOException {
        try (InputStream in = SchemaEvolutionTest.class.getResourceAsStream("/schemas/" + name)) {
            assertThat(in).as("resource /schemas/" + name).isNotNull();
            return new Schema.Parser().parse(in);
        }
    }

    @Test
    void v2IsBackwardCompatibleWithV1() throws IOException {
        var result = SchemaCompatibility.checkReaderWriterCompatibility(load("order-created-v2.avsc"), V1);
        assertThat(result.getType()).isEqualTo(SchemaCompatibilityType.COMPATIBLE);
    }

    @Test
    void incompatibleSchemaBreaksBackwardCompatibility() throws IOException {
        var result = SchemaCompatibility.checkReaderWriterCompatibility(load("order-created-incompatible.avsc"), V1);
        assertThat(result.getType()).isEqualTo(SchemaCompatibilityType.INCOMPATIBLE);
    }

    @Test
    void evolvedSchemasKeepTheRecordName() throws IOException {
        // 이름이 다르면 SR 이 "다른 타입"으로 보고 호환 검사 자체가 달라진다
        assertThat(load("order-created-v2.avsc").getFullName()).isEqualTo(V1.getFullName());
        assertThat(load("order-created-incompatible.avsc").getFullName()).isEqualTo(V1.getFullName());
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `mvn -q test -Dtest=SchemaEvolutionTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 3개 실패, 메시지에 `resource /schemas/order-created-v2.avsc` … `Expecting actual not to be null`.

- [ ] **Step 3: v2 스키마 작성 (기본값 있는 필드 추가 → BACKWARD 호환)**

`src/main/resources/schemas/order-created-v2.avsc`:

```json
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "dev.devopsnote.kafkarunner.sample.avro",
  "doc": "주문 생성 이벤트 v2 — couponCode 추가 (기본값 null 이라 v1 데이터를 읽을 수 있다 = BACKWARD 호환)",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "customerId", "type": "string"},
    {"name": "amount", "type": "long"},
    {"name": "createdAt", "type": "string", "doc": "ISO-8601 문자열"},
    {"name": "couponCode", "type": ["null", "string"], "default": null}
  ]
}
```

- [ ] **Step 4: 비호환 스키마 작성 (기본값 없는 필드 추가 → BACKWARD 위반)**

`src/main/resources/schemas/order-created-incompatible.avsc`:

```json
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "dev.devopsnote.kafkarunner.sample.avro",
  "doc": "고의로 비호환인 스키마 — channel 에 기본값이 없어 v1 데이터를 읽을 수 없다. SR 이 409 로 거부해야 한다",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "customerId", "type": "string"},
    {"name": "amount", "type": "long"},
    {"name": "createdAt", "type": "string", "doc": "ISO-8601 문자열"},
    {"name": "channel", "type": "string"}
  ]
}
```

- [ ] **Step 5: 통과 확인**

Run: `mvn -q test`
Expected: BUILD SUCCESS, 신규 3개 포함 전부 통과.

- [ ] **Step 6: 커밋**

```bash
cd /Users/maro/Documents/devops-note
git add kafka/examples/scenario-runner/src/main/resources/schemas \
        kafka/examples/scenario-runner/src/test/java/dev/devopsnote/kafkarunner/sample/SchemaEvolutionTest.java
git commit -m "Add evolved order schemas with compatibility test

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: JSON 단계 — sample-produce / sample-consume

usage-guide 03·04·05의 코드와 같은 조합(`KafkaTemplate` + `JsonSerializer`, `@KafkaListener` + 수동 ack + trusted packages). 실제 전송·수신은 Docker가 필요하므로 단위 테스트는 "Spring 컨텍스트가 뜨고 명령이 등록된다"까지 확인하고, 동작은 Step 9에서 실행으로 검증한다.

**Files:**
- Create: `src/main/java/dev/devopsnote/kafkarunner/sample/OrderCreatedEvent.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/sample/SampleEvents.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/sample/SampleArgs.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/sample/SampleKafkaConfig.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/sample/SampleProduceCommand.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/sample/SampleConsumeCommand.java`
- Test: `src/test/java/dev/devopsnote/kafkarunner/RunnerContextTest.java`
- Test: `src/test/java/dev/devopsnote/kafkarunner/sample/SampleArgsTest.java`

- [ ] **Step 1: 실패하는 테스트 작성 — SampleArgsTest**

```java
package dev.devopsnote.kafkarunner.sample;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class SampleArgsTest {
    @Test void defaultWhenMissing() { assertThat(SampleArgs.count(List.of(), 10)).isEqualTo(10); }
    @Test void parsesFirstArg() { assertThat(SampleArgs.count(List.of("25"), 10)).isEqualTo(25); }
    @Test void rejectsNonNumber() {
        assertThatThrownBy(() -> SampleArgs.count(List.of("ten"), 10))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ten");
    }
    @Test void rejectsZeroOrNegative() {
        assertThatThrownBy(() -> SampleArgs.count(List.of("0"), 10)).isInstanceOf(IllegalArgumentException.class);
    }
}
```

- [ ] **Step 2: 실패하는 테스트 작성 — RunnerContextTest**

```java
package dev.devopsnote.kafkarunner;

import static org.assertj.core.api.Assertions.assertThat;

import dev.devopsnote.kafkarunner.command.CommandRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/** Spring 컨텍스트가 브로커·SR 없이 뜨고(팩토리는 연결을 지연한다) 샘플 명령이 @Order 순서로 등록되는지. */
@SpringBootTest
class RunnerContextTest {
    @Autowired CommandRegistry registry;

    @Test
    void registersScenarioAndSampleCommands() {
        assertThat(registry.names()).containsExactly(
            "normal-roundtrip", "broker-1-down", "broker-2-down", "total-outage",
            "sample-produce", "sample-consume");
    }
}
```

- [ ] **Step 3: 실패 확인**

Run: `mvn -q test -Dtest='SampleArgsTest,RunnerContextTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `SampleArgsTest` 컴파일 에러(`SampleArgs` 없음). 컴파일 에러 때문에 `RunnerContextTest`는 실행되지 않는다 — 정상.

- [ ] **Step 4: 작은 유틸 3개 작성**

`src/main/java/dev/devopsnote/kafkarunner/sample/OrderCreatedEvent.java`:

```java
package dev.devopsnote.kafkarunner.sample;

/** usage-guide 03·04 의 OrderCreatedEvent — JSON 단계용. createdAt 은 ISO-8601 문자열(Jackson 시간 모듈 의존 없음). */
public record OrderCreatedEvent(String orderId, String customerId, long amount, String createdAt) {}
```

`src/main/java/dev/devopsnote/kafkarunner/sample/SampleEvents.java`:

```java
package dev.devopsnote.kafkarunner.sample;

import java.time.Instant;

/** 샘플 이벤트 생성. orderId 를 3개로 돌려 "같은 key → 같은 파티션"이 출력에서 보이게 한다. */
final class SampleEvents {
    private SampleEvents() {}

    static String orderId(int i) { return "ORD-" + (1000 + i % 3); }

    static OrderCreatedEvent json(int i) {
        return new OrderCreatedEvent(orderId(i), "CUST-" + (i % 5 + 1), 10_000L * i, Instant.now().toString());
    }
}
```

`src/main/java/dev/devopsnote/kafkarunner/sample/SampleArgs.java`:

```java
package dev.devopsnote.kafkarunner.sample;

import dev.devopsnote.kafkarunner.command.UsageException;
import java.util.List;

/** sample-* 명령의 선택 인자 [count] 파싱. 잘못된 값은 UsageException → 종료 코드 2. */
final class SampleArgs {
    private SampleArgs() {}

    static int count(List<String> args, int defaultValue) {
        if (args.isEmpty()) return defaultValue;
        int count;
        try { count = Integer.parseInt(args.get(0)); }
        catch (NumberFormatException e) { throw new UsageException("count 는 정수여야 합니다: " + args.get(0)); }
        if (count <= 0) throw new UsageException("count 는 1 이상이어야 합니다: " + count);
        return count;
    }
}
```

- [ ] **Step 5: SampleKafkaConfig 작성 (JSON 부분)**

`src/main/java/dev/devopsnote/kafkarunner/sample/SampleKafkaConfig.java`:

```java
package dev.devopsnote.kafkarunner.sample;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

/** sample-* 명령 전용 Kafka 빈. 접속·acks·idempotence·auto-offset-reset 은 application.yml 의 spring.kafka.* 에서 오고,
 *  여기서는 명령마다 다른 serializer/deserializer 만 지정한다. 러너 내부(load/)는 부하 제어를 위해 kafka-clients 를 직접 쓴다. */
@Configuration
public class SampleKafkaConfig {
    static final String JSON_LISTENER_FACTORY = "sampleJsonListenerFactory";

    private final KafkaProperties kafka;

    public SampleKafkaConfig(KafkaProperties kafka) { this.kafka = kafka; }

    /** usage-guide 05 와 같은 조합: String key + JsonSerializer. */
    @Bean
    public KafkaTemplate<String, OrderCreatedEvent> sampleJsonTemplate() {
        Map<String, Object> props = kafka.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    /** usage-guide 04·05: JsonDeserializer + trusted packages + 수동 ack. */
    @Bean(JSON_LISTENER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> sampleJsonListenerFactory() {
        Map<String, Object> props = kafka.buildConsumerProperties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "dev.devopsnote.kafkarunner.sample"); // 미설정 시 역직렬화 예외
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderCreatedEvent.class.getName());
        var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
```

- [ ] **Step 6: SampleProduceCommand 작성**

`src/main/java/dev/devopsnote/kafkarunner/sample/SampleProduceCommand.java`:

```java
package dev.devopsnote.kafkarunner.sample;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** usage-guide 03 의 OrderEventProducer 를 CLI 로: send(topic, key, value) + 비동기 콜백. */
@Component
@Order(1)
public class SampleProduceCommand implements Command {
    private static final Logger log = LoggerFactory.getLogger(SampleProduceCommand.class);
    private final KafkaTemplate<String, OrderCreatedEvent> template;
    private final RunnerProperties props;

    public SampleProduceCommand(KafkaTemplate<String, OrderCreatedEvent> sampleJsonTemplate, RunnerProperties props) {
        this.template = sampleJsonTemplate;
        this.props = props;
    }

    @Override public String name() { return "sample-produce"; }
    @Override public String description() { return "OrderCreatedEvent N건 전송 (JSON, KafkaTemplate) — 기본 10건"; }

    @Override public int run(List<String> args) throws Exception {
        int count = SampleArgs.count(args, 10);
        String topic = props.sampleTopic();
        // compose 가 자동 생성을 꺼 두었으므로(운영과 동일) 토픽을 먼저 만든다
        new FaultInjector(props.bootstrapServers(), props.containers()).ensureTopic(topic, props.partitions());

        var latch = new CountDownLatch(count);
        var failures = new AtomicInteger();
        for (int i = 1; i <= count; i++) {
            OrderCreatedEvent event = SampleEvents.json(i);
            // key = orderId → 같은 주문은 항상 같은 파티션 (출력에서 확인)
            template.send(topic, event.orderId(), event).whenComplete((result, ex) -> {
                if (ex != null) {
                    failures.incrementAndGet();
                    log.error("전송 실패 key={}", event.orderId(), ex); // acks=all 이라 여기 오면 실제 실패
                } else {
                    var meta = result.getRecordMetadata();
                    System.out.printf("전송 OK  key=%s  partition=%d  offset=%d%n", event.orderId(), meta.partition(), meta.offset());
                }
                latch.countDown();
            });
        }
        if (!latch.await(30, TimeUnit.SECONDS)) {
            System.out.println("30초 안에 전송 콜백이 모두 오지 않았습니다 — 브로커 상태를 확인하세요");
            return 1;
        }
        template.flush();
        System.out.printf("완료: %d건 전송, 실패 %d건  (topic=%s)%n", count, failures.get(), topic);
        return failures.get() == 0 ? 0 : 1;
    }
}
```

- [ ] **Step 7: SampleConsumeCommand 작성**

`src/main/java/dev/devopsnote/kafkarunner/sample/SampleConsumeCommand.java`:

```java
package dev.devopsnote.kafkarunner.sample;

import dev.devopsnote.kafkarunner.command.Command;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** usage-guide 04 의 @KafkaListener 를 CLI 로. 리스너는 autoStartup=false 라 이 명령이 켤 때만 그룹에 참여한다
 *  (장애 시나리오 실행 중 샘플 컨슈머가 브로커에 그룹을 만드는 것을 막는다). */
@Component
@Order(2)
public class SampleConsumeCommand implements Command {
    static final String LISTENER_ID = "sample-json";
    static final String GROUP_ID = "notification-service";
    static final long IDLE_TIMEOUT_MS = 30_000;

    private final KafkaListenerEndpointRegistry registry;
    private final AtomicInteger received = new AtomicInteger();
    private volatile long lastReceivedAt;

    public SampleConsumeCommand(KafkaListenerEndpointRegistry registry) { this.registry = registry; }

    @Override public String name() { return "sample-consume"; }
    @Override public String description() { return "@KafkaListener 로 N건 수신 (JSON, 수동 ack) — 기본 10건, 30초 무수신 시 종료"; }

    @KafkaListener(id = LISTENER_ID, topics = "${runner.sample-topic}", groupId = GROUP_ID,
                   containerFactory = SampleKafkaConfig.JSON_LISTENER_FACTORY, autoStartup = "false")
    public void onOrderCreated(ConsumerRecord<String, OrderCreatedEvent> record, Acknowledgment ack) {
        OrderCreatedEvent event = record.value();
        System.out.printf("수신  partition=%d  offset=%d  key=%s  customer=%s  amount=%d%n",
            record.partition(), record.offset(), record.key(), event.customerId(), event.amount());
        ack.acknowledge(); // 처리 완료 후 커밋 — 처리 전에 커밋하면 장애 시 메시지를 잃는다
        received.incrementAndGet();
        lastReceivedAt = System.currentTimeMillis();
    }

    @Override public int run(List<String> args) throws Exception {
        int count = SampleArgs.count(args, 10);
        MessageListenerContainer container = registry.getListenerContainer(LISTENER_ID);
        received.set(0);
        lastReceivedAt = System.currentTimeMillis();
        System.out.printf("group=%s 로 구독 시작 (목표 %d건)%n", GROUP_ID, count);
        container.start();
        try {
            while (received.get() < count) {
                if (System.currentTimeMillis() - lastReceivedAt > IDLE_TIMEOUT_MS) {
                    System.out.printf("%d초 동안 신규 메시지 없음 — 종료 (이미 커밋된 오프셋 이후만 읽습니다. 새로 보내려면 sample-produce)%n",
                        IDLE_TIMEOUT_MS / 1000);
                    break;
                }
                Thread.sleep(200);
            }
        } finally {
            container.stop();
        }
        System.out.printf("완료: %d건 수신  (group=%s)%n", received.get(), GROUP_ID);
        return 0;
    }
}
```

- [ ] **Step 8: 테스트 통과 확인**

Run: `mvn -q test`
Expected: BUILD SUCCESS. `RunnerContextTest`는 컨텍스트를 띄우며 사용법을 출력한다(인자 없음 → 종료 코드 2를 기록만 하고 JVM은 종료하지 않음). 브로커 없이도 통과해야 한다 — `DefaultKafkaProducerFactory`와 리스너 컨테이너(autoStartup=false)는 연결을 지연하기 때문이다.

컨텍스트 로드가 실패하면 예외 메시지를 읽는다. `NoSuchBeanDefinitionException: KafkaTemplate<Object, Object>` 계열이면 Boot 자동설정이 우리 템플릿과 충돌한 것이므로, `SampleKafkaConfig`의 각 `@Bean`에 이름을 명시하고 주입 지점에서 `@Qualifier`를 쓴다.

- [ ] **Step 9: 실행 검증 (Docker)**

먼저 포트 점유를 확인한다. 이 머신에는 `kafka-home-lab`, `sr-home-lab` 컨테이너가 떠 있을 수 있고 9092/8081을 쓴다.

Run: `docker ps --format '{{.Names}} {{.Ports}}' | grep -E '9092|8081' || echo NO_CONFLICT`
Expected: `NO_CONFLICT`. 충돌 컨테이너가 있으면 **사용자에게 알리고 중지 동의를 받은 뒤** 진행한다 (사용자의 home-lab 환경이므로 임의로 내리지 않는다).

```bash
docker compose up -d && sleep 25
curl -s localhost:8081/subjects; echo
mvn -q package -DskipTests
java -jar target/scenario-runner.jar sample-produce 6; echo "exit=$?"
java -jar target/scenario-runner.jar sample-consume 6; echo "exit=$?"
```

Expected:
- `curl` → `[]` (SR 기동 완료. 빈 배열이 아니면 10초 더 기다린다)
- produce: `전송 OK key=ORD-1001 partition=…` 6줄. `ORD-1000/1001/1002` 각각이 항상 같은 partition 번호. `완료: 6건 전송, 실패 0건`, `exit=0`
- consume: `수신 …` 6줄, `완료: 6건 수신`, `exit=0`
- 다시 `sample-consume 1`을 실행하면 30초 뒤 `신규 메시지 없음` 후 `완료: 0건 수신`, `exit=0` (오프셋이 커밋됐다는 증거)

- [ ] **Step 10: 커밋**

```bash
cd /Users/maro/Documents/devops-note
git add kafka/examples/scenario-runner/src/main/java/dev/devopsnote/kafkarunner/sample \
        kafka/examples/scenario-runner/src/test/java/dev/devopsnote/kafkarunner/sample/SampleArgsTest.java \
        kafka/examples/scenario-runner/src/test/java/dev/devopsnote/kafkarunner/RunnerContextTest.java
git commit -m "Add JSON produce and consume sample commands

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Avro 단계 — sample-avro-produce / sample-avro-consume

JSON 단계와 같은 구조에서 serializer만 Confluent Avro로 바뀐다. 코드 어디에도 SR REST 호출이 없는데 스키마가 등록되는 것을 전송 후 조회로 보여준다.

**Files:**
- Modify: `src/main/java/dev/devopsnote/kafkarunner/sample/SampleKafkaConfig.java`
- Modify: `src/main/java/dev/devopsnote/kafkarunner/sample/SampleEvents.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/sample/SampleAvroProduceCommand.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/sample/SampleAvroConsumeCommand.java`
- Modify: `src/test/java/dev/devopsnote/kafkarunner/RunnerContextTest.java`

- [ ] **Step 1: RunnerContextTest 기대값에 두 명령 추가 (실패하는 테스트)**

`containsExactly(...)` 인자를 다음으로 교체:

```java
            "normal-roundtrip", "broker-1-down", "broker-2-down", "total-outage",
            "sample-produce", "sample-consume", "sample-avro-produce", "sample-avro-consume");
```

- [ ] **Step 2: 실패 확인**

Run: `mvn -q test -Dtest=RunnerContextTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `Expecting … to contain exactly … but could not find … "sample-avro-produce", "sample-avro-consume"`.

- [ ] **Step 3: SampleKafkaConfig에 Avro 빈 추가**

import 추가:

```java
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Objects;
```

상수 추가(`JSON_LISTENER_FACTORY` 아래):

```java
    static final String AVRO_LISTENER_FACTORY = "sampleAvroListenerFactory";
    static final String SCHEMA_REGISTRY_URL_KEY = "schema.registry.url";
```

클래스 끝(마지막 `}` 앞)에 추가:

```java
    /** Avro + Schema Registry. serializer 가 SR 에 스키마를 등록·조회하므로 앱 코드에는 SR 호출이 없다.
     *  schema.registry.url 은 spring.kafka.properties 에서 buildProducerProperties() 로 함께 들어온다.
     *  값 타입을 Object 로 두어 생성 클래스(SpecificRecord)와 GenericRecord 를 한 템플릿으로 보낸다. */
    @Bean
    public KafkaTemplate<String, Object> sampleAvroTemplate() {
        Map<String, Object> props = kafka.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    /** Avro 컨슈머 설정. ConsumerFactory 를 빈으로 노출하면 Boot 기본 팩토리와 제네릭이 충돌하므로 static 으로 공유한다. */
    static Map<String, Object> avroConsumerProps(KafkaProperties kafka) {
        Map<String, Object> props = kafka.buildConsumerProperties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        // true: writer 스키마의 이름으로 생성 클래스(OrderCreated)를 찾아 역직렬화. false 면 GenericRecord
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return props;
    }

    @Bean(AVRO_LISTENER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, Object> sampleAvroListenerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, Object>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(avroConsumerProps(kafka)));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    /** 스키마 id·버전 조회와 sample-schema-evolution 의 직접 등록용. 생성 시점에는 접속하지 않는다. */
    @Bean
    public SchemaRegistryClient schemaRegistryClient() {
        String url = Objects.requireNonNull(kafka.getProperties().get(SCHEMA_REGISTRY_URL_KEY),
            "spring.kafka.properties.schema.registry.url 이 필요합니다");
        return new CachedSchemaRegistryClient(url, 100);
    }
```

- [ ] **Step 4: SampleEvents에 Avro 생성 메서드 추가**

import 추가: `import dev.devopsnote.kafkarunner.sample.avro.OrderCreated;`

`json(int i)` 아래에 추가:

```java
    static OrderCreated avro(int i) {
        return OrderCreated.newBuilder()
            .setOrderId(orderId(i))
            .setCustomerId("CUST-" + (i % 5 + 1))
            .setAmount(10_000L * i)
            .setCreatedAt(Instant.now().toString())
            .build();
    }
```

- [ ] **Step 5: SampleAvroProduceCommand 작성**

`src/main/java/dev/devopsnote/kafkarunner/sample/SampleAvroProduceCommand.java`:

```java
package dev.devopsnote.kafkarunner.sample;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.sample.avro.OrderCreated;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Avro 전송. 코드는 JSON 단계와 같고 serializer 만 다르다 — SR 등록은 KafkaAvroSerializer 안에서 일어난다. */
@Component
@Order(3)
public class SampleAvroProduceCommand implements Command {
    private static final Logger log = LoggerFactory.getLogger(SampleAvroProduceCommand.class);
    private final KafkaTemplate<String, Object> template;
    private final SchemaRegistryClient schemaRegistry;
    private final RunnerProperties props;

    public SampleAvroProduceCommand(KafkaTemplate<String, Object> sampleAvroTemplate,
                                    SchemaRegistryClient schemaRegistry, RunnerProperties props) {
        this.template = sampleAvroTemplate;
        this.schemaRegistry = schemaRegistry;
        this.props = props;
    }

    @Override public String name() { return "sample-avro-produce"; }
    @Override public String description() { return "OrderCreated(Avro) N건 전송 — serializer 가 SR 에 스키마 등록, 기본 10건"; }

    @Override public int run(List<String> args) throws Exception {
        int count = SampleArgs.count(args, 10);
        String topic = props.sampleAvroTopic();
        new FaultInjector(props.bootstrapServers(), props.containers()).ensureTopic(topic, props.partitions());

        var latch = new CountDownLatch(count);
        var failures = new AtomicInteger();
        for (int i = 1; i <= count; i++) {
            OrderCreated event = SampleEvents.avro(i);
            template.send(topic, event.getOrderId(), event).whenComplete((result, ex) -> {
                if (ex != null) {
                    failures.incrementAndGet();
                    log.error("전송 실패 key={}", event.getOrderId(), ex);
                } else {
                    var meta = result.getRecordMetadata();
                    System.out.printf("전송 OK  key=%s  partition=%d  offset=%d  serializedValueSize=%dB%n",
                        event.getOrderId(), meta.partition(), meta.offset(), meta.serializedValueSize());
                }
                latch.countDown();
            });
        }
        if (!latch.await(30, TimeUnit.SECONDS)) {
            System.out.println("30초 안에 전송 콜백이 모두 오지 않았습니다 — 브로커/SR 상태를 확인하세요");
            return 1;
        }
        template.flush();

        // 앱 코드는 SR 을 호출한 적이 없지만 serializer 가 등록해 두었다 — 확인만 한다
        String subject = topic + "-value";
        var latest = schemaRegistry.getLatestSchemaMetadata(subject);
        System.out.printf("SR 확인: subject=%s  version=%d  schemaId=%d  (메시지에는 이 id 4바이트만 실린다)%n",
            subject, latest.getVersion(), latest.getId());
        System.out.printf("완료: %d건 전송, 실패 %d건  (topic=%s)%n", count, failures.get(), topic);
        return failures.get() == 0 ? 0 : 1;
    }
}
```

- [ ] **Step 6: SampleAvroConsumeCommand 작성**

`src/main/java/dev/devopsnote/kafkarunner/sample/SampleAvroConsumeCommand.java`:

```java
package dev.devopsnote.kafkarunner.sample;

import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.sample.avro.OrderCreated;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** Avro 수신. deserializer 가 메시지 앞 5바이트에서 schema id 를 읽어 SR 에서 스키마를 받아온다(캐시됨). */
@Component
@Order(4)
public class SampleAvroConsumeCommand implements Command {
    static final String LISTENER_ID = "sample-avro";
    static final String GROUP_ID = "notification-service-avro";
    static final long IDLE_TIMEOUT_MS = 30_000;

    private final KafkaListenerEndpointRegistry registry;
    private final AtomicInteger received = new AtomicInteger();
    private volatile long lastReceivedAt;

    public SampleAvroConsumeCommand(KafkaListenerEndpointRegistry registry) { this.registry = registry; }

    @Override public String name() { return "sample-avro-consume"; }
    @Override public String description() { return "@KafkaListener 로 OrderCreated(Avro) N건 수신 — 기본 10건, 30초 무수신 시 종료"; }

    @KafkaListener(id = LISTENER_ID, topics = "${runner.sample-avro-topic}", groupId = GROUP_ID,
                   containerFactory = SampleKafkaConfig.AVRO_LISTENER_FACTORY, autoStartup = "false")
    public void onOrderCreated(ConsumerRecord<String, OrderCreated> record, Acknowledgment ack) {
        OrderCreated event = record.value();
        System.out.printf("수신  partition=%d  offset=%d  key=%s  customer=%s  amount=%d%n",
            record.partition(), record.offset(), record.key(), event.getCustomerId(), event.getAmount());
        ack.acknowledge();
        received.incrementAndGet();
        lastReceivedAt = System.currentTimeMillis();
    }

    @Override public int run(List<String> args) throws Exception {
        int count = SampleArgs.count(args, 10);
        MessageListenerContainer container = registry.getListenerContainer(LISTENER_ID);
        received.set(0);
        lastReceivedAt = System.currentTimeMillis();
        System.out.printf("group=%s 로 구독 시작 (목표 %d건)%n", GROUP_ID, count);
        container.start();
        try {
            while (received.get() < count) {
                if (System.currentTimeMillis() - lastReceivedAt > IDLE_TIMEOUT_MS) {
                    System.out.printf("%d초 동안 신규 메시지 없음 — 종료%n", IDLE_TIMEOUT_MS / 1000);
                    break;
                }
                Thread.sleep(200);
            }
        } finally {
            container.stop();
        }
        System.out.printf("완료: %d건 수신  (group=%s)%n", received.get(), GROUP_ID);
        return 0;
    }
}
```

- [ ] **Step 7: 테스트 통과 확인**

Run: `mvn -q test`
Expected: BUILD SUCCESS. `RunnerContextTest`가 명령 8개 순서대로 통과.

- [ ] **Step 8: 실행 검증 (Docker, Task 4 Step 9 의 클러스터가 떠 있는 상태)**

```bash
mvn -q package -DskipTests
java -jar target/scenario-runner.jar sample-avro-produce 6; echo "exit=$?"
curl -s localhost:8081/subjects; echo
curl -s localhost:8081/subjects/commerce.order.created.avro-value/versions; echo
java -jar target/scenario-runner.jar sample-avro-consume 6; echo "exit=$?"
```

Expected:
- produce: `전송 OK …` 6줄(`serializedValueSize`는 수십 바이트대 — 스키마 전체가 아니라 id만 실린 크기), `SR 확인: subject=commerce.order.created.avro-value version=1 schemaId=1`, `exit=0`
- `/subjects` → `["commerce.order.created.avro-value"]`
- `/versions` → `[1]`
- consume: `수신 …` 6줄, `exit=0`

- [ ] **Step 9: 커밋**

```bash
cd /Users/maro/Documents/devops-note
git add kafka/examples/scenario-runner/src/main/java/dev/devopsnote/kafkarunner/sample \
        kafka/examples/scenario-runner/src/test/java/dev/devopsnote/kafkarunner/RunnerContextTest.java
git commit -m "Add Avro produce and consume sample commands

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: sample-schema-evolution

한 명령으로 네 장면을 재현한다: ① v2 호환 등록 성공 ② v2 메시지 전송 ③ v1 클래스로 수신 성공 ④ 비호환 등록 409 거부.

**Files:**
- Create: `src/main/java/dev/devopsnote/kafkarunner/sample/SchemaEvolutionCommand.java`
- Modify: `src/test/java/dev/devopsnote/kafkarunner/RunnerContextTest.java`

- [ ] **Step 1: RunnerContextTest 기대값에 명령 추가 (실패하는 테스트)**

`containsExactly(...)` 인자를 다음으로 교체:

```java
            "normal-roundtrip", "broker-1-down", "broker-2-down", "total-outage",
            "sample-produce", "sample-consume", "sample-avro-produce", "sample-avro-consume",
            "sample-schema-evolution");
```

- [ ] **Step 2: 실패 확인**

Run: `mvn -q test -Dtest=RunnerContextTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `could not find … "sample-schema-evolution"`.

- [ ] **Step 3: SchemaEvolutionCommand 작성**

`src/main/java/dev/devopsnote/kafkarunner/sample/SchemaEvolutionCommand.java`:

```java
package dev.devopsnote.kafkarunner.sample;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.sample.avro.OrderCreated;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** 스키마 진화를 한 번에 재현: v2 호환 등록 → v2 전송 → v1 클래스로 수신 → 비호환 등록 거부(409). */
@Component
@Order(5)
public class SchemaEvolutionCommand implements Command {
    private final SchemaRegistryClient schemaRegistry;
    private final KafkaTemplate<String, Object> template;
    private final KafkaProperties kafka;
    private final RunnerProperties props;

    public SchemaEvolutionCommand(SchemaRegistryClient schemaRegistry, KafkaTemplate<String, Object> sampleAvroTemplate,
                                  KafkaProperties kafka, RunnerProperties props) {
        this.schemaRegistry = schemaRegistry;
        this.template = sampleAvroTemplate;
        this.kafka = kafka;
        this.props = props;
    }

    @Override public String name() { return "sample-schema-evolution"; }
    @Override public String description() { return "v2 호환 등록 → v2 전송 → v1 클래스로 수신 → 비호환 스키마 409 거부 재현"; }

    @Override public int run(List<String> args) throws Exception {
        String topic = props.sampleAvroTopic();
        String subject = topic + "-value";
        new FaultInjector(props.bootstrapServers(), props.containers()).ensureTopic(topic, props.partitions());

        // sample-avro-produce 를 먼저 안 돌렸어도 되도록 v1 을 여기서도 등록 (같은 스키마면 기존 id 반환)
        int v1Id = schemaRegistry.register(subject, new AvroSchema(OrderCreated.getClassSchema()));
        System.out.printf("v1 등록 확인  subject=%s  schemaId=%d%n", subject, v1Id);

        // ① 호환 스키마 등록 — 필드 추가 + 기본값 → BACKWARD 통과
        AvroSchema v2 = loadSchema("order-created-v2.avsc");
        int v2Id = schemaRegistry.register(subject, v2);
        System.out.printf("① v2 등록 성공  schemaId=%d  (couponCode 는 기본값 null → 옛 데이터를 읽을 수 있어 BACKWARD 호환)%n", v2Id);

        try (Consumer<String, Object> consumer = newConsumerAtEnd(topic)) {
            // ② v2 로 한 건 전송 — GenericRecord 라 생성 클래스 없이도 새 스키마로 보낼 수 있다
            GenericRecord record = new GenericData.Record(v2.rawSchema());
            record.put("orderId", "ORD-2001");
            record.put("customerId", "CUST-9");
            record.put("amount", 55_000L);
            record.put("createdAt", Instant.now().toString());
            record.put("couponCode", "WELCOME10");
            template.send(topic, "ORD-2001", record).get(30, TimeUnit.SECONDS);
            System.out.println("② v2 메시지 전송  key=ORD-2001  couponCode=WELCOME10");

            // ③ v1 생성 클래스(reader 스키마)로 v2 메시지(writer 스키마) 수신
            OrderCreated got = pollOne(consumer, Duration.ofSeconds(30));
            if (got == null) {
                System.out.println("③ 30초 안에 메시지를 받지 못했습니다 — FAIL");
                return 1;
            }
            System.out.printf("③ v1 클래스로 수신 OK  orderId=%s  amount=%d  (writer=v2, reader=v1 — couponCode 는 reader 에 없어 무시됨)%n",
                got.getOrderId(), got.getAmount());
        }

        // ④ 비호환 스키마 등록 — 기본값 없는 필드 추가 → SR 이 거부
        AvroSchema incompatible = loadSchema("order-created-incompatible.avsc");
        try {
            int id = schemaRegistry.register(subject, incompatible);
            System.out.printf("④ 비호환 스키마가 등록되어 버렸습니다 (id=%d) — SR 호환성 설정을 확인하세요. FAIL%n", id);
            return 1;
        } catch (RestClientException e) {
            if (e.getStatus() != 409) throw e;
            System.out.printf("④ SR 거부  HTTP 409  (channel 에 기본값이 없어 옛 데이터를 읽을 수 없음)%n    %s%n", firstLine(e.getMessage()));
        }

        System.out.printf("subject=%s 버전 목록: %s  (비호환 스키마는 버전에 남지 않는다)%n", subject, schemaRegistry.getAllVersions(subject));
        return 0;
    }

    /** 전송 직전에 모든 파티션 끝에 위치시켜, 이번에 보내는 1건만 읽는다. */
    private Consumer<String, Object> newConsumerAtEnd(String topic) {
        var consumerProps = SampleKafkaConfig.avroConsumerProps(kafka);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "schema-evolution-" + System.currentTimeMillis());
        var consumer = new KafkaConsumer<String, Object>(consumerProps);
        List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
            .map(p -> new TopicPartition(topic, p.partition())).toList();
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        partitions.forEach(consumer::position); // seekToEnd 는 지연 평가라 position() 으로 확정
        return consumer;
    }

    private static OrderCreated pollOne(Consumer<String, Object> consumer, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            for (var record : consumer.poll(Duration.ofMillis(500))) {
                if (record.value() instanceof OrderCreated event) return event;
            }
        }
        return null;
    }

    private AvroSchema loadSchema(String name) throws IOException {
        try (InputStream in = getClass().getResourceAsStream("/schemas/" + name)) {
            if (in == null) throw new IllegalStateException("스키마 리소스 없음: /schemas/" + name);
            return new AvroSchema(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    private static String firstLine(String s) { return s == null ? "" : s.lines().findFirst().orElse(""); }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `mvn -q test`
Expected: BUILD SUCCESS. 명령 9개 순서대로 등록.

- [ ] **Step 5: 실행 검증 (Docker)**

```bash
mvn -q package -DskipTests
java -jar target/scenario-runner.jar sample-schema-evolution; echo "exit=$?"
curl -s localhost:8081/subjects/commerce.order.created.avro-value/versions; echo
java -jar target/scenario-runner.jar sample-schema-evolution; echo "exit=$?"
```

Expected:
- 1회차: `v1 등록 확인 … schemaId=1`, `① v2 등록 성공 schemaId=2`, `② v2 메시지 전송`, `③ v1 클래스로 수신 OK orderId=ORD-2001 amount=55000`, `④ SR 거부 HTTP 409`, `버전 목록: [1, 2]`, `exit=0`
- `/versions` → `[1,2]`
- 2회차: 같은 출력(v2는 이미 등록된 스키마라 같은 id 2), `exit=0` — 재실행에 안전

`③`에서 30초 타임아웃이 나면 `seekToEnd`가 전송 이후로 밀린 것이므로, `newConsumerAtEnd`의 `position` 호출이 `send` 앞에 있는지 확인한다.

- [ ] **Step 6: 커밋**

```bash
cd /Users/maro/Documents/devops-note
git add kafka/examples/scenario-runner/src/main/java/dev/devopsnote/kafkarunner/sample/SchemaEvolutionCommand.java \
        kafka/examples/scenario-runner/src/test/java/dev/devopsnote/kafkarunner/RunnerContextTest.java
git commit -m "Add schema evolution sample command

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: 문서, 회귀 검증, 사이트 빌드

**Files:**
- Modify: `README.md` (scenario-runner)
- Modify: `kafka/usage-guide/03-producer.md`, `kafka/usage-guide/04-consumer.md`, `kafka/usage-guide/08-onboarding.md`
- Modify (커밋하지 않음): `CLAUDE.md`, `AGENTS.md`

- [ ] **Step 1: README 첫 문단·전제 조건·실행 방법 갱신**

`README.md` 첫 문단(`# Kafka 장애 시나리오 러너` 아래 첫 단락) 끝에 문장 추가:

```markdown
장애 시나리오와 별개로, usage-guide 의 프로듀서/컨슈머 코드와 Schema Registry+Avro 흐름을 실제로 돌려보는 `sample-*` 명령도 같은 jar 에 들어 있습니다(아래 [사용 예시](#사용-예시)).
```

`## 전제 조건` 목록을 다음으로 교체:

```markdown
- Docker (Compose v2 포함)
- JDK 21 이상, Maven 3.9 이상
- 포트 9092, 9192, 9292(브로커), 8081(Schema Registry) 미사용 상태 — `home-lab` 예제를 띄워 두었다면 먼저 내려야 합니다
- Maven 이 `https://packages.confluent.io/maven/` 에 접근 가능해야 합니다 (Confluent Avro serializer 는 Maven Central 에 없음). 사내 미러를 쓰면 이 저장소를 허용 목록에 추가하세요
```

`## 실행 방법` 코드 블록의 `# 1)` 주석을 다음으로 교체:

```bash
# 1) 로컬 3브로커 클러스터 + Schema Registry 기동 (최초 30초 정도 대기, curl localhost:8081/subjects 가 [] 를 반환하면 준비 완료)
```

- [ ] **Step 2: README에 사용 예시 절 추가**

`## 리포트 해석` 절 끝(`## 관련 문서` 바로 앞)에 삽입:

````markdown
## 사용 예시

장애 시나리오와 같은 클러스터에서 usage-guide 의 코드를 실행해 봅니다. 시나리오와 달리 판정·리포트는 없고, 종료 코드는 0 정상 / 1 실패 / 2 인자 오류입니다. 코드는 `src/main/java/dev/devopsnote/kafkarunner/sample/` 에 있습니다. 러너 내부(`load/`)는 부하 제어를 위해 `kafka-clients` 를 직접 쓰지만, 샘플은 문서와 같은 Spring Kafka(`KafkaTemplate`, `@KafkaListener`) 스타일입니다.

### 1단계 — JSON 프로듀서/컨슈머

[03 프로듀서](../../usage-guide/03-producer.md), [04 컨슈머](../../usage-guide/04-consumer.md), [05 접속 설정](../../usage-guide/05-connection-config.md)의 코드 그대로입니다.

```bash
java -jar target/scenario-runner.jar sample-produce 6     # OrderCreatedEvent 6건 전송 (기본 10건)
java -jar target/scenario-runner.jar sample-consume 6     # notification-service 그룹으로 6건 수신
```

출력에서 볼 것:

- `전송 OK key=ORD-1001 partition=2 offset=…` — key 가 orderId 라서 같은 주문은 항상 같은 파티션에 갑니다. `ORD-1000/1001/1002` 각각의 partition 번호가 매번 같은지 확인하세요.
- `수신 partition=… offset=…` 뒤에 `ack.acknowledge()` 가 호출되어 오프셋이 커밋됩니다. 바로 다시 `sample-consume` 을 실행하면 30초 뒤 "신규 메시지 없음" 으로 끝나는데, 이것이 컨슈머 그룹이 오프셋을 기억한다는 뜻입니다. 새 메시지를 보려면 `sample-produce` 를 다시 실행하세요.
- 토픽은 명령이 직접 만듭니다. compose 가 운영처럼 자동 생성을 꺼 두었기 때문입니다.

### 2단계 — Avro + Schema Registry

[Schema Registry 개념](../../concepts/09-concepts-qna.md)에서 설명한 흐름을 실제로 확인합니다. 설정 차이는 serializer 클래스와 `schema.registry.url` 뿐이고, 앱 코드에는 SR 호출이 없습니다.

```bash
java -jar target/scenario-runner.jar sample-avro-produce 6
curl -s localhost:8081/subjects/commerce.order.created.avro-value/versions   # → [1]
java -jar target/scenario-runner.jar sample-avro-consume 6
java -jar target/scenario-runner.jar sample-schema-evolution
```

출력에서 볼 것:

- `sample-avro-produce` 의 `serializedValueSize` 는 수십 바이트입니다. 스키마 전체가 아니라 `magic 1B + schema id 4B + 페이로드` 만 실리기 때문입니다. 끝에 `SR 확인: … version=1 schemaId=1` 이 나오는데, 이 등록은 `KafkaAvroSerializer` 가 첫 전송 때 한 것입니다.
- `sample-avro-consume` 은 메시지 앞의 id 로 SR 에서 스키마를 받아 역직렬화합니다(id 별로 캐시되어 SR 이 잠시 죽어도 이미 본 스키마는 계속 처리됩니다).
- `sample-schema-evolution` 은 네 단계를 순서대로 출력합니다.
  1. `① v2 등록 성공` — `couponCode` 를 기본값 `null` 로 추가한 스키마는 BACKWARD 호환이라 등록됩니다.
  2. `② v2 메시지 전송` — `GenericRecord` 로 새 스키마 메시지를 보냅니다.
  3. `③ v1 클래스로 수신 OK` — 옛 생성 클래스로 새 메시지를 읽습니다. Avro 가 writer(v2)/reader(v1) 스키마를 대조해 모르는 필드를 버립니다. 컨슈머 배포 없이 프로듀서만 먼저 바꿔도 되는 이유입니다.
  4. `④ SR 거부 HTTP 409` — 기본값 없는 필드 `channel` 을 추가한 스키마는 옛 데이터를 읽을 수 없으므로 SR 이 거부합니다. 브로커는 이 검사를 하지 않습니다. SR 이 유일한 관문입니다.
- 스키마 파일: `src/main/avro/OrderCreated.avsc`(v1, 코드 생성), `src/main/resources/schemas/order-created-v2.avsc`, `order-created-incompatible.avsc`. 세 파일의 호환 관계는 `SchemaEvolutionTest` 가 SR 없이 검증합니다.

운영에서는 앱이 스키마를 마음대로 등록하지 못하게 `auto.register.schemas=false` 로 잠그고 CI 에서 미리 등록하는 방식을 권장합니다. 이 예제는 흐름을 보여주기 위해 기본값(자동 등록)을 씁니다.
````

- [ ] **Step 3: usage-guide 세 문서에 실행 링크 추가**

`kafka/usage-guide/03-producer.md` 파일 끝에 추가:

```markdown

## 실행해 보기

이 장의 코드는 [scenario-runner 사용 예시](../examples/scenario-runner/README.md#사용-예시)의 `sample-produce` 명령으로 로컬 클러스터에서 그대로 실행할 수 있습니다.
```

`kafka/usage-guide/04-consumer.md` 파일 끝에 추가:

```markdown

## 실행해 보기

이 장의 리스너는 [scenario-runner 사용 예시](../examples/scenario-runner/README.md#사용-예시)의 `sample-consume` 명령으로 로컬 클러스터에서 그대로 실행할 수 있습니다.
```

`kafka/usage-guide/08-onboarding.md`의 `schema.registry.url` 을 설명하는 줄(`- Schema Registry를 쓰는 경우 \`schema.registry.url\`에 …` 로 시작하는 목록 항목) 끝에 문장 추가:

```markdown
 Avro serializer 설정과 스키마 진화 동작은 [scenario-runner 사용 예시](../examples/scenario-runner/README.md#사용-예시)의 `sample-avro-*` 명령으로 로컬에서 확인할 수 있습니다.
```

- [ ] **Step 4: 링크 대상 확인**

Run: `cd /Users/maro/Documents/devops-note/kafka && ls concepts/09-concepts-qna.md usage-guide/03-producer.md usage-guide/04-consumer.md usage-guide/05-connection-config.md examples/scenario-runner/README.md && grep -c "사용 예시" examples/scenario-runner/README.md`
Expected: 5개 파일 모두 존재, grep 결과 `2` 이상.

- [ ] **Step 5: 회귀 검증 — SR 이 추가된 상태에서 기존 시나리오 PASS 유지**

```bash
cd /Users/maro/Documents/devops-note/kafka/examples/scenario-runner
java -jar target/scenario-runner.jar normal-roundtrip; echo "exit=$?"
java -jar target/scenario-runner.jar broker-1-down; echo "exit=$?"
docker ps --format '{{.Names}} {{.Status}}' | grep schema-registry
```

Expected: 두 시나리오 모두 `PASS`, `exit=0` (broker-1-down 은 약 2~3분). `schema-registry Up …` — 브로커 1대 정지·복구를 거쳐도 SR 컨테이너가 살아 있다.

- [ ] **Step 6: 사이트 빌드·테스트**

Run: `cd /Users/maro/Documents/devops-note/web && npm run lint && npm test`
Expected: lint 경고 없음, `# pass 2`, `# fail 0`.

- [ ] **Step 7: 클러스터 정리와 whitespace 검사**

```bash
cd /Users/maro/Documents/devops-note/kafka/examples/scenario-runner && docker compose down
cd /Users/maro/Documents/devops-note && git diff --check
```

Expected: 컨테이너 4개 제거, `git diff --check` 출력 없음.

- [ ] **Step 8: 커밋 (문서만)**

```bash
cd /Users/maro/Documents/devops-note
git add kafka/examples/scenario-runner/README.md \
        kafka/usage-guide/03-producer.md kafka/usage-guide/04-consumer.md kafka/usage-guide/08-onboarding.md
git commit -m "Document scenario runner sample commands

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

- [ ] **Step 9: CLAUDE.md / AGENTS.md 한 줄 갱신 — 커밋하지 않음**

두 파일의 Commands 절에서 scenario-runner 를 설명하는 문장(`kafka/examples/scenario-runner/` is a self-contained Java CLI … 로 시작)을 찾아, 그 문장 뒤에 추가:

```markdown
The same jar also ships `sample-*` commands (JSON produce/consume, Avro produce/consume, schema evolution) that need the `schema-registry` container from its compose file and the Confluent Maven repository declared in its `pom.xml`.
```

두 파일에는 이 작업과 무관한 미커밋 변경이 이미 있으므로 **커밋하지 않고** 사용자에게 "CLAUDE.md/AGENTS.md 에 한 줄을 추가했고, 기존 미커밋 변경과 함께 검토 후 커밋해 달라"고 보고한다.

---

## 셀프 리뷰 결과

- **스펙 커버리지:** 명령 구조(Task 1), 5개 샘플 명령(4·5·6), compose SR·pom·avsc(2), 진화 스키마와 로컬 테스트(3), 문서·링크·회귀·web 테스트(7). `SampleTopics.java` 는 `FaultInjector.ensureTopic` 재사용으로 대체(맨 위 주의 사항에 명시).
- **타입 일관성:** `RunnerProperties` 7필드 생성자를 Task 1 테스트 두 곳과 Task 4 이후 코드가 같은 순서로 쓴다. `SampleKafkaConfig.JSON_LISTENER_FACTORY` / `AVRO_LISTENER_FACTORY` 상수와 `@Bean` 이름, `@KafkaListener(containerFactory=…)` 가 일치한다. `avroConsumerProps(KafkaProperties)` 는 Task 5 에서 정의, Task 6 에서 사용. `Command.run` 은 모든 구현에서 `int run(List<String>) throws Exception`.
- **플레이스홀더:** 없음. 모든 코드 단계에 전체 코드, 모든 실행 단계에 명령과 기대 출력이 있다.

---

## 구현 중 변경 사항 (2026-09-06 ~ 09-08 실행 기록)

각 작업의 코드 리뷰에서 나온 수정이며, 위 작업 본문의 코드 블록보다 저장소의 실제 코드가 우선한다.

- **Task 1**: `command/UsageException extends IllegalArgumentException` 추가. `RunnerApplication` 은 이 타입만 종료 코드 2 로 바꾼다(일반 IAE 는 전파). `CommandRegistry(List<Command> scenarios, List<Command> samples)` 로 바꿔 `RunnerProperties` 와 `instanceof ScenarioCommand` 의존을 제거했고, 샘플이 없으면 `사용 예시:` 헤더를 생략한다. `SampleArgs` 는 `UsageException` 을 던지고 인자가 2개 이상이면 거부한다.
- **Task 2**: Confluent 8.3.1 → **7.9.9**. 8.x serializer 는 kafka-clients 4.1 의 `Monitorable` 을 요구해 Spring Boot 3.5 가 고정한 3.9.1 에서 클래스 로딩이 실패한다(`AvroSerdeClasspathTest` 로 고정). compose: 브로커 healthcheck(`kafka-broker-api-versions.sh`) + SR `depends_on: condition: service_healthy` + `restart: on-failure` + `SCHEMA_REGISTRY_KAFKASTORE_TOPIC_CONFIG_MIN_INSYNC_REPLICAS: 2`. SR 이 브로커 준비 전에 붙으면 `_schemas` 가 RF1 로 만들어져 영구히 쓰기 불가가 되기 때문이다. Confluent 저장소는 snapshots 비활성.
- **Task 3**: `SchemaEvolutionTest` 는 비호환 원인이 정확히 `READER_FIELD_MISSING_DEFAULT_VALUE`/`channel` 하나임을 단정하고, 진화 스키마가 v1 필드의 상위집합임을 고정한다. ③ 방향(옛 reader × 새 데이터)을 위한 `v1ReaderCanReadV2Writer` 추가.
- **Task 4**: `sample-consume N` 이 폴 배치 초과분까지 ack 하던 문제 → `volatile int target` 게이트(초과분은 출력·ack 하지 않아 다음 실행에서 다시 읽힘). 프로듀서 팩토리를 별도 `@Bean` 으로 두어 종료 시 close 되게 함. 라운드트립 후 `flush()` 제거. `RunnerContextTest` 가 리스너 컨테이너가 등록되어 있고 `autoStartup=false` 임을 단정.
- **Task 5**: Avro 1.12 의 생성 클래스 신뢰 목록(`ClassSecurityValidator`) 때문에 `KafkaAvroSerializer` 가 `SecurityException` 으로 막힘. 시스템 프로퍼티는 클래스 초기화 때 한 번만 읽혀 순서 의존적이므로 `SampleKafkaConfig.trustGeneratedAvroClasses()` 가 `ClassSecurityValidator.setGlobal(composite(...))` 로 등록하고 `RunnerApplication.main` 이 Spring 기동 전에 호출한다. `AvroRoundTripTest`(MockSchemaRegistryClient) 로 serializer 경로를 브로커 없이 검증. `SR 확인` 줄은 최신 버전이 아니라 **이번에 쓴 writer 스키마**의 version/id 를 출력. SR 접속 실패는 `SerializationException` 으로 `send()` 안에서 즉시 던져지므로 별도 catch 로 안내 메시지 출력.
- **Task 6**: ③ 은 이 실행이 보낸 key(`ORD-2001`)의 레코드만 받아들이고 writer schemaId 를 함께 출력한다(동시 실행 중 v1 메시지를 v2 로 오인하던 문제). ③ 방향이 FORWARD 이며 BACKWARD 설정만으로 보장되지 않는다는 주의 문구 추가. SR 실패는 바깥 try 하나로 통합, 전송 `.get()` 은 `TimeoutException` 까지 처리, 컨슈머 setup 실패 시 close, ④ 메시지는 `", details:"` 앞에서 잘라 200자 제한.
- **검증 환경**: 이 머신의 `sr-home-lab` 이 8081 을 점유해, 검증 시 SR 을 호스트 8082 로 띄우고(`docker compose -p scenario-runner` + 사본 compose) `--spring.kafka.properties.schema.registry.url=http://localhost:8082` 로 실행했다. 커밋된 compose/yml 은 8081 그대로다. Compose v2.19 는 `!override` 를 지원하지 않아 override 파일 대신 사본을 썼다.
- **Task 7**: usage-guide 08 의 링크는 문서 끝이 아니라 `schema.registry.url` 을 설명하는 항목 안에 문장으로 넣었다(맥락이 맞는 자리). 03·04 는 `## 실행해 보기` 절. 링크에 `#사용-예시` 조각을 붙이면 `normalizeDocumentLink` 가 `.md` 로 끝나지 않는다고 보고 인앱 이동으로 바꾸지 않으므로 조각 없이 둔다. README 의 ①~④ 설명은 렌더러가 중첩 목록을 지원하지 않아 최상위 bullet 로 평탄화했다.
