# Kafka 시나리오 러너 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 로컬 3브로커 클러스터에서 정상·비정상 시나리오 4개를 자동 재현하고 메시지 유실을 판정하는 CLI 러너를 `kafka/examples/scenario-runner/`에 구현한다.

**Architecture:** 단일 Spring Boot 비웹 앱. LoadGenerator(생산)·VerifierConsumer(소비)가 Ledger(원장)에 기록하고, FaultInjector가 docker stop/start로 장애를 주입하며, Judge가 원장을 시나리오 기대치와 대조해 PASS/FAIL을 내고 Reporter가 Markdown 리포트를 남긴다. 스펙: `docs/superpowers/specs/2026-09-02-kafka-scenario-runner-design.md`.

**Tech Stack:** Java 21(release 타깃, 로컬 JDK 25로 빌드), Spring Boot 3.5, Spring Kafka, Maven 3.9, apache/kafka:4.0.0 (Docker Compose), JUnit 5.

## Global Constraints

- 패키지 루트 `dev.devopsnote.kafkarunner`. 파일당 단일 책임.
- 커밋 메시지는 저장소 규약(짧은 명령형 영어, 예: `Add scenario runner ledger and judge`).
- `reports/`, `target/`은 커밋 금지 — Task 1의 .gitignore가 처리.
- README는 콘텐츠 파이프라인에 노출되므로 한국어·렌더러 서브셋 준수(H1 아래 요약 문단, 중첩 리스트 금지).
- 통합 실행(시나리오 실제 구동)은 compose 클러스터가 떠 있어야 한다. 각 통합 단계 전에 `docker compose ps`로 3컨테이너 Up 확인.
- 테스트 명령: `mvn -q test` (단위), 시나리오 실행: `java -jar target/scenario-runner.jar <시나리오>`.

## 파일 구조 (전체 지도)

```text
kafka/examples/scenario-runner/
├── README.md                      # Task 10
├── docker-compose.yml             # Task 2 — 로컬 3브로커 (kafka1/2/3)
├── .gitignore                     # Task 1
├── pom.xml                        # Task 1
└── src
    ├── main/java/dev/devopsnote/kafkarunner/
    │   ├── RunnerApplication.java           # Task 1(뼈대) → Task 7(디스패치)
    │   ├── RunnerProperties.java            # Task 5
    │   ├── ledger/Ledger.java               # Task 3
    │   ├── ledger/Judge.java                # Task 4
    │   ├── ledger/JudgeResult.java          # Task 4
    │   ├── ledger/Expectation.java          # Task 4
    │   ├── fault/FaultInjector.java         # Task 5
    │   ├── load/LoadGenerator.java          # Task 6
    │   ├── load/VerifierConsumer.java       # Task 6
    │   ├── report/Reporter.java             # Task 7
    │   └── scenario/
    │       ├── Scenario.java                # Task 7
    │       ├── NormalRoundtrip.java         # Task 7
    │       ├── Broker1Down.java             # Task 8
    │       ├── Broker2Down.java             # Task 8
    │       └── TotalOutage.java             # Task 9
    ├── main/resources/application.yml       # Task 1
    └── test/java/dev/devopsnote/kafkarunner/
        ├── ledger/LedgerTest.java           # Task 3
        ├── ledger/JudgeTest.java            # Task 4
        └── fault/FaultInjectorCommandTest.java  # Task 5
```

---

### Task 1: Maven 프로젝트 뼈대

**Files:**
- Create: `kafka/examples/scenario-runner/pom.xml`
- Create: `kafka/examples/scenario-runner/.gitignore`
- Create: `kafka/examples/scenario-runner/src/main/java/dev/devopsnote/kafkarunner/RunnerApplication.java`
- Create: `kafka/examples/scenario-runner/src/main/resources/application.yml`

- [ ] **Step 1: pom.xml 작성**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.5.5</version>
    <relativePath/>
  </parent>
  <groupId>dev.devopsnote</groupId>
  <artifactId>scenario-runner</artifactId>
  <version>0.1.0</version>
  <name>kafka-scenario-runner</name>
  <properties>
    <java.version>21</java.version>
    <maven.compiler.release>21</maven.compiler.release>
  </properties>
  <dependencies>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.kafka</groupId>
      <artifactId>spring-kafka</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-test</artifactId>
      <scope>test</scope>
    </dependency>
  </dependencies>
  <build>
    <finalName>scenario-runner</finalName>
    <plugins>
      <plugin>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-maven-plugin</artifactId>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 2: .gitignore 작성**

```text
target/
reports/
```

- [ ] **Step 3: RunnerApplication 뼈대**

```java
package dev.devopsnote.kafkarunner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RunnerApplication {
    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(RunnerApplication.class, args)));
    }
}
```

- [ ] **Step 4: application.yml**

```yaml
runner:
  bootstrap-servers: localhost:9092,localhost:9192,localhost:9292
  topic: test.scenario.events
  partitions: 3
  containers: [kafka1, kafka2, kafka3]
  report-dir: reports

spring:
  main:
    web-application-type: none
  kafka:
    bootstrap-servers: ${runner.bootstrap-servers}
    producer:
      acks: all
      properties:
        enable.idempotence: true
    consumer:
      auto-offset-reset: earliest
      enable-auto-commit: false

logging:
  level:
    org.apache.kafka: WARN
```

- [ ] **Step 5: 컴파일 확인 후 커밋**

Run: `cd kafka/examples/scenario-runner && mvn -q compile`
Expected: BUILD 성공 (경고 없이 종료 코드 0)

```bash
git add kafka/examples/scenario-runner
git commit -m "Add Kafka scenario runner project skeleton"
```

### Task 2: 로컬 3브로커 compose

**Files:**
- Create: `kafka/examples/scenario-runner/docker-compose.yml`

- [ ] **Step 1: compose 작성**

브로커 3개가 브리지 네트워크에서 컨테이너 DNS(kafka1/2/3)로 통신하고, 호스트에는 각각 9092/9192/9292로 노출된다. INTERNAL(브로커 간)·CONTROLLER(쿼럼)·EXTERNAL(호스트 클라이언트) 리스너 3개 구조. stop/start 시 데이터가 유지되도록 컨테이너를 remove하지 않는 운용을 전제한다(시나리오 러너는 stop/start만 사용).

```yaml
# 시나리오 러너 전용 로컬 3브로커 클러스터 (PLAINTEXT, 단일 머신)
# 실서버 배포용이 아니다 — 장애 시나리오 재현·검증 전용.
# 브로커 간/쿼럼은 컨테이너 DNS(kafka1/2/3), 러너는 localhost:9092/9192/9292 로 접속.
x-kafka-common: &kafka-common
  image: apache/kafka:4.0.0
  restart: "no"   # 러너가 stop/start 를 통제하므로 자동 재시작 금지
  environment: &kafka-env
    KAFKA_PROCESS_ROLES: broker,controller
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
    KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
    KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
    KAFKA_DEFAULT_REPLICATION_FACTOR: 3
    KAFKA_MIN_INSYNC_REPLICAS: 2
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
    KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE: "false"
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    # 로컬 검증 전용 고정 클러스터 ID (22자 base64 UUID)
    CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk

services:
  kafka1:
    <<: *kafka-common
    container_name: kafka1
    ports: ["9092:9092"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 1
      KAFKA_LISTENERS: INTERNAL://:29092,CONTROLLER://:9093,EXTERNAL://:9092
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka1:29092,EXTERNAL://localhost:9092
  kafka2:
    <<: *kafka-common
    container_name: kafka2
    ports: ["9192:9192"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 2
      KAFKA_LISTENERS: INTERNAL://:29092,CONTROLLER://:9093,EXTERNAL://:9192
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka2:29092,EXTERNAL://localhost:9192
  kafka3:
    <<: *kafka-common
    container_name: kafka3
    ports: ["9292:9292"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 3
      KAFKA_LISTENERS: INTERNAL://:29092,CONTROLLER://:9093,EXTERNAL://:9292
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka3:29092,EXTERNAL://localhost:9292
```

- [ ] **Step 2: 기동 검증**

Run:
```bash
cd kafka/examples/scenario-runner && docker compose up -d && sleep 20
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic smoke-test --partitions 3 --replication-factor 3
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic smoke-test
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic smoke-test
```
Expected: describe 출력에 파티션 3개, 각 `Isr: 1,2,3` (숫자 순서 무관)

- [ ] **Step 3: 커밋**

```bash
git add kafka/examples/scenario-runner/docker-compose.yml
git commit -m "Add local three-broker compose for scenario runner"
```

### Task 3: Ledger (TDD)

**Files:**
- Test: `src/test/java/dev/devopsnote/kafkarunner/ledger/LedgerTest.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/ledger/Ledger.java`

- [ ] **Step 1: 실패하는 테스트 작성**

```java
package dev.devopsnote.kafkarunner.ledger;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class LedgerTest {
    @Test
    void tracksSentAndReceivedSets() {
        Ledger ledger = new Ledger();
        ledger.recordSentOk(1); ledger.recordSentOk(2);
        ledger.recordSentFail(3);
        ledger.recordReceived(1);
        ledger.recordReceived(1); // 중복 수신
        assertThat(ledger.sentOk()).containsExactly(1L, 2L);
        assertThat(ledger.sentFail()).containsExactly(3L);
        assertThat(ledger.lostSeqs()).containsExactly(2L);      // 성공 기록됐지만 미수신
        assertThat(ledger.duplicateCount()).isEqualTo(1);       // 총수신 - 고유수신
    }
}
```

- [ ] **Step 2: 실패 확인** — Run: `mvn -q test` / Expected: 컴파일 실패(`Ledger` 없음)

- [ ] **Step 3: 구현**

```java
package dev.devopsnote.kafkarunner.ledger;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/** 보낸 것/받은 것의 대조표. 모든 판정의 유일한 근거. 스레드 안전. */
public class Ledger {
    private final Set<Long> sentOk = new ConcurrentSkipListSet<>();
    private final Set<Long> sentFail = new ConcurrentSkipListSet<>();
    private final Set<Long> received = new ConcurrentSkipListSet<>();
    private final AtomicLong receivedTotal = new AtomicLong();

    public void recordSentOk(long seq) { sentOk.add(seq); sentFail.remove(seq); } // 재전송 성공 반영
    public void recordSentFail(long seq) { if (!sentOk.contains(seq)) sentFail.add(seq); }
    public void recordReceived(long seq) { received.add(seq); receivedTotal.incrementAndGet(); }

    public Set<Long> sentOk() { return new TreeSet<>(sentOk); }
    public Set<Long> sentFail() { return new TreeSet<>(sentFail); }
    public Set<Long> received() { return new TreeSet<>(received); }
    public Set<Long> lostSeqs() { var lost = new TreeSet<>(sentOk); lost.removeAll(received); return lost; }
    public long duplicateCount() { return receivedTotal.get() - received.size(); }
}
```

- [ ] **Step 4: 통과 확인** — Run: `mvn -q test` / Expected: PASS
- [ ] **Step 5: 커밋** — `git add src && git commit -m "Add scenario runner message ledger"`

### Task 4: Judge (TDD)

**Files:**
- Test: `src/test/java/dev/devopsnote/kafkarunner/ledger/JudgeTest.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/ledger/Expectation.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/ledger/JudgeResult.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/ledger/Judge.java`

- [ ] **Step 1: 실패하는 테스트 작성**

```java
package dev.devopsnote.kafkarunner.ledger;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class JudgeTest {
    @Test
    void lossIsAlwaysFailure() {
        Ledger ledger = new Ledger();
        ledger.recordSentOk(1); // 수신 기록 없음 → 유실
        JudgeResult result = new Judge().judge(ledger, Expectation.strict());
        assertThat(result.pass()).isFalse();
        assertThat(result.reasons()).anyMatch(reason -> reason.contains("유실"));
    }

    @Test
    void duplicatesFailOnlyInStrictMode() {
        Ledger ledger = new Ledger();
        ledger.recordSentOk(1); ledger.recordReceived(1); ledger.recordReceived(1);
        assertThat(new Judge().judge(ledger, Expectation.strict()).pass()).isFalse();
        assertThat(new Judge().judge(ledger, Expectation.allowDuplicates()).pass()).isTrue();
    }

    @Test
    void expectedSendFailuresPassWhenPresent() {
        Ledger ledger = new Ledger();
        ledger.recordSentOk(1); ledger.recordReceived(1);
        ledger.recordSentFail(2);
        // 실패가 있어야 정상인 시나리오(2대 정지): 실패 0건이면 오히려 FAIL
        assertThat(new Judge().judge(ledger, Expectation.expectSendFailures()).pass()).isTrue();
        Ledger noFail = new Ledger();
        noFail.recordSentOk(1); noFail.recordReceived(1);
        assertThat(new Judge().judge(noFail, Expectation.expectSendFailures()).pass()).isFalse();
    }
}
```

- [ ] **Step 2: 실패 확인** — Run: `mvn -q test` / Expected: 컴파일 실패

- [ ] **Step 3: 구현**

```java
package dev.devopsnote.kafkarunner.ledger;

/** 시나리오별 기대 동작. */
public record Expectation(boolean duplicatesAllowed, boolean sendFailuresExpected) {
    public static Expectation strict() { return new Expectation(false, false); }
    public static Expectation allowDuplicates() { return new Expectation(true, false); }
    public static Expectation expectSendFailures() { return new Expectation(true, true); }
}
```

```java
package dev.devopsnote.kafkarunner.ledger;

import java.util.List;

public record JudgeResult(boolean pass, List<String> reasons,
                          long sentOk, long sentFail, long received, long lost, long duplicates) {}
```

```java
package dev.devopsnote.kafkarunner.ledger;

import java.util.ArrayList;
import java.util.List;

/** 원장과 기대치를 대조해 PASS/FAIL 판정. 불변식: 성공 기록 seq ⊆ 수신 seq. */
public class Judge {
    public JudgeResult judge(Ledger ledger, Expectation expectation) {
        List<String> reasons = new ArrayList<>();
        long lost = ledger.lostSeqs().size();
        if (lost > 0) reasons.add("유실 " + lost + "건: 성공으로 기록된 메시지가 수신되지 않음 " + preview(ledger));
        long duplicates = ledger.duplicateCount();
        if (!expectation.duplicatesAllowed() && duplicates > 0) reasons.add("중복 " + duplicates + "건 (허용 안 됨)");
        if (expectation.sendFailuresExpected() && ledger.sentFail().isEmpty())
            reasons.add("전송 실패가 발생해야 하는 시나리오인데 실패가 0건 — 장애 주입이 동작하지 않았을 가능성");
        return new JudgeResult(reasons.isEmpty(), reasons,
            ledger.sentOk().size(), ledger.sentFail().size(), ledger.received().size(), lost, duplicates);
    }

    private String preview(Ledger ledger) {
        var it = ledger.lostSeqs().stream().limit(5).toList();
        return "(예: " + it + ")";
    }
}
```

- [ ] **Step 4: 통과 확인** — Run: `mvn -q test` / Expected: PASS (테스트 4개)
- [ ] **Step 5: 커밋** — `git add src && git commit -m "Add scenario judge with per-scenario expectations"`

### Task 5: RunnerProperties + FaultInjector

**Files:**
- Create: `src/main/java/dev/devopsnote/kafkarunner/RunnerProperties.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/fault/FaultInjector.java`
- Test: `src/test/java/dev/devopsnote/kafkarunner/fault/FaultInjectorCommandTest.java`

- [ ] **Step 1: RunnerProperties**

```java
package dev.devopsnote.kafkarunner;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "runner")
public record RunnerProperties(String bootstrapServers, String topic, int partitions,
                               List<String> containers, String reportDir) {}
```

`RunnerApplication`에 `@EnableConfigurationProperties(RunnerProperties.class)` 추가.

- [ ] **Step 2: 명령 생성 테스트 작성 (실패 확인 포함)**

```java
package dev.devopsnote.kafkarunner.fault;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class FaultInjectorCommandTest {
    @Test
    void buildsDockerCommands() {
        assertThat(FaultInjector.command("stop", "kafka2")).containsExactly("docker", "stop", "kafka2");
        assertThat(FaultInjector.command("start", "kafka1")).containsExactly("docker", "start", "kafka1");
    }
}
```

- [ ] **Step 3: FaultInjector 구현**

핵심 메서드와 책임 (전체 구현):

```java
package dev.devopsnote.kafkarunner.fault;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** docker stop/start 로 장애를 주입하고, AdminClient 로 클러스터 상태를 관찰한다. */
public class FaultInjector {
    private static final Logger log = LoggerFactory.getLogger(FaultInjector.class);
    private final String bootstrapServers;
    private final List<String> containers;

    public FaultInjector(String bootstrapServers, List<String> containers) {
        this.bootstrapServers = bootstrapServers;
        this.containers = containers;
    }

    static List<String> command(String action, String container) {
        return List.of("docker", action, container);
    }

    public void stop(String container) { run(command("stop", container)); }
    public void start(String container) { run(command("start", container)); }
    public void startAll() { containers.forEach(this::start); }

    /** 시나리오 시작 전 사전 점검: 3컨테이너가 모두 Up 인지. */
    public void ensureAllRunning() {
        for (String container : containers) {
            String state = output(List.of("docker", "inspect", "-f", "{{.State.Running}}", container)).trim();
            if (!"true".equals(state)) {
                throw new IllegalStateException("컨테이너 " + container + " 가 실행 중이 아닙니다. docker compose up -d 후 재시도하세요.");
            }
        }
    }

    /** 테스트 토픽 생성 (이미 있으면 통과). */
    public void ensureTopic(String topic, int partitions) {
        try (Admin admin = admin()) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 3)
                    .configs(Map.of("min.insync.replicas", "2")))).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) throw new IllegalStateException(e);
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
    }

    /** 모든 파티션이 리더를 갖고 ISR 3개로 회복될 때까지 대기. */
    public void awaitFullIsr(String topic, Duration timeout) {
        awaitCondition(timeout, "ISR 완전 회복", admin -> {
            var desc = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
            return desc.partitions().stream().allMatch(p -> p.leader() != null && p.isr().size() == 3);
        });
    }

    /** 클러스터가 응답하고 모든 파티션에 리더가 있을 때까지 대기 (전체 정지 복구용). */
    public void awaitClusterReady(String topic, Duration timeout) {
        awaitCondition(timeout, "클러스터 응답·리더 존재", admin -> {
            var desc = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
            return desc.partitions().stream().allMatch(p -> p.leader() != null);
        });
    }

    private interface Check { boolean ok(Admin admin) throws Exception; }

    private void awaitCondition(Duration timeout, String what, Check check) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            try (Admin admin = admin()) {
                if (check.ok(admin)) { log.info("{} 확인 완료", what); return; }
            } catch (Exception e) { log.debug("{} 대기 중: {}", what, e.getMessage()); }
            sleep(2000);
        }
        throw new IllegalStateException(what + " 대기 시간 초과 (" + timeout + ")");
    }

    private Admin admin() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        return Admin.create(props);
    }

    private void run(List<String> cmd) {
        String out = output(cmd);
        log.info("$ {} -> {}", String.join(" ", cmd), out.trim());
    }

    private String output(List<String> cmd) {
        try {
            Process process = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            String out = new String(process.getInputStream().readAllBytes());
            if (!process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS) || process.exitValue() != 0) {
                throw new IllegalStateException("명령 실패: " + String.join(" ", cmd) + "\n" + out);
            }
            return out;
        } catch (java.io.IOException e) { throw new IllegalStateException(e); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
```

- [ ] **Step 4: 테스트 통과 확인** — Run: `mvn -q test` / Expected: PASS
- [ ] **Step 5: 커밋** — `git add src && git commit -m "Add fault injector with docker control and cluster waits"`

### Task 6: LoadGenerator + VerifierConsumer

**Files:**
- Create: `src/main/java/dev/devopsnote/kafkarunner/load/LoadGenerator.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/load/VerifierConsumer.java`

- [ ] **Step 1: LoadGenerator 구현**

별도 스레드에서 seq를 증가시키며 초당 약 200건 전송. 전송 결과 콜백에서 Ledger에 기록. `stopAndDrain()`은 전송 중단 후 in-flight 콜백까지 대기. 시나리오 ④를 위해 실패 seq 재전송 메서드 제공.

```java
package dev.devopsnote.kafkarunner.load;

import dev.devopsnote.kafkarunner.ledger.Ledger;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** seq 박힌 메시지를 연속 생산하고 성공/실패를 Ledger 에 기록한다. */
public class LoadGenerator implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(LoadGenerator.class);
    private final Producer<String, String> producer;
    private final Ledger ledger;
    private final String topic;
    private final AtomicLong seq = new AtomicLong();
    private final AtomicBoolean running = new AtomicBoolean();
    private Thread worker;

    public LoadGenerator(String bootstrapServers, String topic, Ledger ledger) {
        var props = new java.util.Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000"); // 장애 중 빠른 실패 판정
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.ledger = ledger;
    }

    public void start() {
        running.set(true);
        worker = Thread.ofPlatform().name("load-generator").start(() -> {
            while (running.get()) {
                sendOne(seq.incrementAndGet());
                try { Thread.sleep(5); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            }
        });
    }

    /** 지정 건수만 동기적으로 전송 (normal-roundtrip 용). */
    public void sendExactly(int count) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            long s = seq.incrementAndGet();
            producer.send(new ProducerRecord<>(topic, Long.toString(s), payload(s)), (meta, ex) -> {
                if (ex == null) ledger.recordSentOk(s); else ledger.recordSentFail(s);
                latch.countDown();
            });
        }
        latch.await(120, TimeUnit.SECONDS);
    }

    /** 실패로 기록된 seq 들을 재전송 (복구 후 폴백 재전송 검증용). */
    public void resend(Set<Long> seqs) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(seqs.size());
        for (long s : seqs) {
            producer.send(new ProducerRecord<>(topic, Long.toString(s), payload(s)), (meta, ex) -> {
                if (ex == null) ledger.recordSentOk(s); else ledger.recordSentFail(s);
                latch.countDown();
            });
        }
        latch.await(120, TimeUnit.SECONDS);
    }

    private void sendOne(long s) {
        try {
            producer.send(new ProducerRecord<>(topic, Long.toString(s), payload(s)), (meta, ex) -> {
                if (ex == null) ledger.recordSentOk(s); else ledger.recordSentFail(s);
            });
        } catch (Exception e) { ledger.recordSentFail(s); } // max.block.ms 초과 등 즉시 실패
    }

    private String payload(long s) {
        return "{\"seq\":" + s + ",\"sentAt\":\"" + java.time.Instant.now() + "\"}";
    }

    public void stopAndDrain() {
        running.set(false);
        if (worker != null) { try { worker.join(3000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }
        producer.flush();
    }

    @Override public void close() { producer.close(java.time.Duration.ofSeconds(10)); }
}
```

- [ ] **Step 2: VerifierConsumer 구현**

실행마다 고유 그룹 id(`verifier-<epoch>`), earliest 부터 소비하되 **이번 시나리오 seq 만** 원장에 기록하도록 payload의 seq를 파싱해 시작 seq 이후만 반영한다. 더 단순하게: 시나리오 시작 시 토픽을 지우고 새로 만들므로 전량 기록으로 충분 — 시나리오 시작 절차에 "토픽 삭제 후 재생성"을 포함한다 (FaultInjector.ensureTopic 앞에 deleteTopics 시도 추가).

```java
package dev.devopsnote.kafkarunner.load;

import dev.devopsnote.kafkarunner.ledger.Ledger;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** 별도 그룹으로 토픽 전체를 소비해 수신 seq 를 Ledger 에 기록한다. */
public class VerifierConsumer implements AutoCloseable {
    private final KafkaConsumer<String, String> consumer;
    private final Ledger ledger;
    private final AtomicBoolean running = new AtomicBoolean();
    private Thread worker;

    public VerifierConsumer(String bootstrapServers, String topic, Ledger ledger) {
        var props = new java.util.Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "verifier-" + System.currentTimeMillis());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        this.consumer = new KafkaConsumer<>(props);
        this.ledger = ledger;
        consumer.subscribe(List.of(topic));
    }

    public void start() {
        running.set(true);
        worker = Thread.ofPlatform().name("verifier-consumer").start(() -> {
            while (running.get()) {
                var records = consumer.poll(Duration.ofMillis(500));
                records.forEach(r -> ledger.recordReceived(Long.parseLong(r.key())));
            }
        });
    }

    /** 신규 수신이 quietMillis 동안 없을 때까지 대기 (따라잡기 완료 판정). */
    public void awaitQuiet(long quietMillis, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        long lastCount = -1; long quietSince = System.currentTimeMillis();
        while (System.currentTimeMillis() < deadline) {
            long now = ledger.received().size();
            if (now != lastCount) { lastCount = now; quietSince = System.currentTimeMillis(); }
            if (System.currentTimeMillis() - quietSince >= quietMillis) return;
            Thread.sleep(300);
        }
    }

    @Override public void close() {
        running.set(false);
        if (worker != null) { try { worker.join(3000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }
        consumer.close(Duration.ofSeconds(5));
    }
}
```

주의: `FaultInjector.ensureTopic`을 "삭제 후 재생성"으로 확장한다 — `resetTopic(String topic, int partitions)` 메서드 추가 (deleteTopics 후 존재하지 않을 때까지 대기, 그다음 createTopics).

```java
    /** 이전 실행 데이터를 지우기 위해 토픽을 삭제 후 재생성. */
    public void resetTopic(String topic, int partitions) {
        try (Admin admin = admin()) {
            try { admin.deleteTopics(List.of(topic)).all().get(); } catch (ExecutionException ignored) { }
            awaitCondition(Duration.ofSeconds(30), "토픽 삭제 완료",
                a -> !a.listTopics().names().get().contains(topic));
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
        ensureTopic(topic, partitions);
    }
```

- [ ] **Step 3: 컴파일 확인** — Run: `mvn -q test` / Expected: PASS
- [ ] **Step 4: 커밋** — `git add src && git commit -m "Add load generator and verifier consumer"`

### Task 7: Scenario 프레임 + NormalRoundtrip + Reporter + CLI 연결

**Files:**
- Create: `src/main/java/dev/devopsnote/kafkarunner/scenario/Scenario.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/scenario/NormalRoundtrip.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/report/Reporter.java`
- Modify: `src/main/java/dev/devopsnote/kafkarunner/RunnerApplication.java`

- [ ] **Step 1: Scenario 인터페이스**

```java
package dev.devopsnote.kafkarunner.scenario;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.ledger.Expectation;
import dev.devopsnote.kafkarunner.ledger.Ledger;

public interface Scenario {
    String name();
    String description();
    Expectation expectation();
    /** 장애 주입·부하·대기를 수행하고 원장을 채운다. */
    void run(RunnerProperties props, FaultInjector injector, Ledger ledger) throws Exception;
}
```

- [ ] **Step 2: NormalRoundtrip 구현**

```java
package dev.devopsnote.kafkarunner.scenario;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.ledger.Expectation;
import dev.devopsnote.kafkarunner.ledger.Ledger;
import dev.devopsnote.kafkarunner.load.LoadGenerator;
import dev.devopsnote.kafkarunner.load.VerifierConsumer;

/** ① 정상 왕복: 1만 건 전송 → 전량 수신 → 유실 0 / 중복 0. */
public class NormalRoundtrip implements Scenario {
    @Override public String name() { return "normal-roundtrip"; }
    @Override public String description() { return "정상 상태에서 1만 건 왕복 — 유실 0, 중복 0 확인"; }
    @Override public Expectation expectation() { return Expectation.strict(); }

    @Override public void run(RunnerProperties props, FaultInjector injector, Ledger ledger) throws Exception {
        try (var generator = new LoadGenerator(props.bootstrapServers(), props.topic(), ledger);
             var verifier = new VerifierConsumer(props.bootstrapServers(), props.topic(), ledger)) {
            verifier.start();
            generator.sendExactly(10_000);
            generator.stopAndDrain();
            verifier.awaitQuiet(3_000, 60_000);
        }
    }
}
```

- [ ] **Step 3: Reporter 구현**

```java
package dev.devopsnote.kafkarunner.report;

import dev.devopsnote.kafkarunner.ledger.JudgeResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/** 콘솔 요약 + Markdown 리포트 저장. */
public class Reporter {
    public Path write(String reportDir, String scenario, String description, JudgeResult result) throws Exception {
        String stamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        Path dir = Path.of(reportDir);
        Files.createDirectories(dir);
        Path file = dir.resolve(scenario + "-" + stamp + ".md");
        String verdict = result.pass() ? "PASS" : "FAIL";
        StringBuilder md = new StringBuilder();
        md.append("# 시나리오 리포트: ").append(scenario).append("\n\n")
          .append("- 판정: **").append(verdict).append("**\n")
          .append("- 설명: ").append(description).append("\n")
          .append("- 전송 성공: ").append(result.sentOk())
          .append(" / 전송 실패: ").append(result.sentFail())
          .append(" / 수신: ").append(result.received())
          .append(" / 유실: ").append(result.lost())
          .append(" / 중복: ").append(result.duplicates()).append("\n");
        if (!result.reasons().isEmpty()) {
            md.append("\n## FAIL 사유\n\n");
            result.reasons().forEach(reason -> md.append("- ").append(reason).append("\n"));
        }
        Files.writeString(file, md.toString());
        System.out.println("[" + verdict + "] " + scenario + " — 리포트: " + file);
        result.reasons().forEach(reason -> System.out.println("  - " + reason));
        return file;
    }
}
```

- [ ] **Step 4: RunnerApplication에 디스패치 연결**

```java
package dev.devopsnote.kafkarunner;

import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.ledger.Judge;
import dev.devopsnote.kafkarunner.ledger.Ledger;
import dev.devopsnote.kafkarunner.report.Reporter;
import dev.devopsnote.kafkarunner.scenario.*;
import java.util.Map;
import java.util.function.Supplier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableConfigurationProperties(RunnerProperties.class)
public class RunnerApplication implements ApplicationRunner, ExitCodeGenerator {
    private final RunnerProperties props;
    private int exitCode = 0;

    public RunnerApplication(RunnerProperties props) { this.props = props; }

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(RunnerApplication.class, args)));
    }

    private static final Map<String, Supplier<Scenario>> SCENARIOS = Map.of(
        "normal-roundtrip", NormalRoundtrip::new,
        "broker-1-down", Broker1Down::new,
        "broker-2-down", Broker2Down::new,
        "total-outage", TotalOutage::new);

    @Override public void run(ApplicationArguments args) throws Exception {
        if (args.getNonOptionArgs().isEmpty()) {
            System.out.println("사용법: java -jar scenario-runner.jar <시나리오>\n시나리오: " + SCENARIOS.keySet());
            exitCode = 2; return;
        }
        String name = args.getNonOptionArgs().get(0);
        Supplier<Scenario> supplier = SCENARIOS.get(name);
        if (supplier == null) { System.out.println("알 수 없는 시나리오: " + name + " / 가능: " + SCENARIOS.keySet()); exitCode = 2; return; }

        Scenario scenario = supplier.get();
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
        exitCode = result.pass() ? 0 : 1;
    }

    @Override public int getExitCode() { return exitCode; }
    @Bean ExitCodeGenerator exitCodeGenerator() { return this; }
}
```

주의: 이 시점에는 `Broker1Down` 등이 없어 컴파일이 깨진다 — Task 8·9의 클래스가 생길 때까지 Map에는 `normal-roundtrip`만 넣고, Task 8·9에서 항목을 추가한다.

- [ ] **Step 5: 통합 실행 (클러스터 필요)**

Run:
```bash
docker compose ps   # 3컨테이너 Up 확인
mvn -q package -DskipTests && java -jar target/scenario-runner.jar normal-roundtrip
```
Expected: `[PASS] normal-roundtrip` 출력, `reports/normal-roundtrip-*.md` 생성, 종료 코드 0

- [ ] **Step 6: 커밋** — `git add src && git commit -m "Add scenario framework with normal roundtrip"`

### Task 8: Broker1Down + Broker2Down

**Files:**
- Create: `src/main/java/dev/devopsnote/kafkarunner/scenario/Broker1Down.java`
- Create: `src/main/java/dev/devopsnote/kafkarunner/scenario/Broker2Down.java`
- Modify: `RunnerApplication.java` (SCENARIOS 맵에 항목 추가)

- [ ] **Step 1: Broker1Down 구현**

```java
package dev.devopsnote.kafkarunner.scenario;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.ledger.Expectation;
import dev.devopsnote.kafkarunner.ledger.Ledger;
import dev.devopsnote.kafkarunner.load.LoadGenerator;
import dev.devopsnote.kafkarunner.load.VerifierConsumer;
import java.time.Duration;

/** ② 브로커 1대 정지: 부하 중 1대를 내려도 유실 0, 복구 후 ISR 완전 회복. */
public class Broker1Down implements Scenario {
    @Override public String name() { return "broker-1-down"; }
    @Override public String description() { return "부하 중 브로커 1대 정지 60초 — 무중단·무유실 검증"; }
    @Override public Expectation expectation() { return Expectation.allowDuplicates(); }

    @Override public void run(RunnerProperties props, FaultInjector injector, Ledger ledger) throws Exception {
        try (var generator = new LoadGenerator(props.bootstrapServers(), props.topic(), ledger);
             var verifier = new VerifierConsumer(props.bootstrapServers(), props.topic(), ledger)) {
            verifier.start();
            generator.start();
            Thread.sleep(10_000);                       // 정상 부하 10초
            injector.stop(props.containers().get(1));   // kafka2 정지
            Thread.sleep(60_000);                       // 정지 상태에서 부하 지속
            injector.start(props.containers().get(1));
            injector.awaitFullIsr(props.topic(), Duration.ofMinutes(3));
            Thread.sleep(10_000);                       // 복구 후 부하 10초
            generator.stopAndDrain();
            verifier.awaitQuiet(3_000, 120_000);
        }
    }
}
```

- [ ] **Step 2: Broker2Down 구현**

```java
package dev.devopsnote.kafkarunner.scenario;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.ledger.Expectation;
import dev.devopsnote.kafkarunner.ledger.Ledger;
import dev.devopsnote.kafkarunner.load.LoadGenerator;
import dev.devopsnote.kafkarunner.load.VerifierConsumer;
import java.time.Duration;

/** ③ 브로커 2대 정지: min.insync.replicas=2 위반으로 전송 실패가 나는 것이 정상.
 *  복구 후 실패분 재전송까지 하면 최종 유실 0. */
public class Broker2Down implements Scenario {
    @Override public String name() { return "broker-2-down"; }
    @Override public String description() { return "부하 중 2대 정지 — 쓰기 실패(기대 동작)와 복구 후 재전송 무유실 검증"; }
    @Override public Expectation expectation() { return Expectation.expectSendFailures(); }

    @Override public void run(RunnerProperties props, FaultInjector injector, Ledger ledger) throws Exception {
        try (var generator = new LoadGenerator(props.bootstrapServers(), props.topic(), ledger);
             var verifier = new VerifierConsumer(props.bootstrapServers(), props.topic(), ledger)) {
            verifier.start();
            generator.start();
            Thread.sleep(10_000);
            injector.stop(props.containers().get(1));   // kafka2
            injector.stop(props.containers().get(2));   // kafka3
            Thread.sleep(60_000);                       // 실패 축적 구간
            injector.start(props.containers().get(1));
            injector.start(props.containers().get(2));
            injector.awaitFullIsr(props.topic(), Duration.ofMinutes(3));
            generator.stopAndDrain();
            generator.resend(ledger.sentFail());        // 실패분 재전송 → 최종 무유실
            verifier.awaitQuiet(3_000, 120_000);
        }
    }
}
```

- [ ] **Step 3: SCENARIOS 맵에 두 항목 추가 후 통합 실행**

Run:
```bash
mvn -q package -DskipTests
java -jar target/scenario-runner.jar broker-1-down    # 약 2~4분 소요
java -jar target/scenario-runner.jar broker-2-down
```
Expected: 두 시나리오 모두 `[PASS]`. broker-2-down 리포트에 전송 실패 건수 > 0.

- [ ] **Step 4: 커밋** — `git add src && git commit -m "Add broker outage scenarios"`

### Task 9: TotalOutage

**Files:**
- Create: `src/main/java/dev/devopsnote/kafkarunner/scenario/TotalOutage.java`
- Modify: `RunnerApplication.java` (SCENARIOS 맵에 항목 추가)

- [ ] **Step 1: TotalOutage 구현**

```java
package dev.devopsnote.kafkarunner.scenario;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.ledger.Expectation;
import dev.devopsnote.kafkarunner.ledger.Ledger;
import dev.devopsnote.kafkarunner.load.LoadGenerator;
import dev.devopsnote.kafkarunner.load.VerifierConsumer;
import java.time.Duration;

/** ④ 전체 정지 → 복구: 3대 모두 정지, 실패분은 원장이 폴백 버퍼 역할.
 *  복구 후 재전송하면 최종 유실 0 — usage-guide 07 의 폴백 패턴 검증. */
public class TotalOutage implements Scenario {
    @Override public String name() { return "total-outage"; }
    @Override public String description() { return "전체 정지 → 자력 복구 → 폴백 재전송 — 최종 무유실 검증"; }
    @Override public Expectation expectation() { return Expectation.expectSendFailures(); }

    @Override public void run(RunnerProperties props, FaultInjector injector, Ledger ledger) throws Exception {
        try (var generator = new LoadGenerator(props.bootstrapServers(), props.topic(), ledger);
             var verifier = new VerifierConsumer(props.bootstrapServers(), props.topic(), ledger)) {
            verifier.start();
            generator.start();
            Thread.sleep(10_000);
            for (String container : props.containers()) injector.stop(container);  // 전체 정지
            Thread.sleep(30_000);                        // 정지 구간 — 실패가 원장(sentFail)에 쌓인다
            generator.stopAndDrain();                    // 부하 중단
            for (String container : props.containers()) injector.start(container); // 전체 재기동
            injector.awaitClusterReady(props.topic(), Duration.ofMinutes(5));      // 쿼럼·리더 회복 대기
            injector.awaitFullIsr(props.topic(), Duration.ofMinutes(3));
            generator.resend(ledger.sentFail());         // 폴백 버퍼 재전송
            verifier.awaitQuiet(3_000, 120_000);
        }
    }
}
```

- [ ] **Step 2: 통합 실행**

Run: `mvn -q package -DskipTests && java -jar target/scenario-runner.jar total-outage`
Expected: `[PASS] total-outage`, 리포트에 전송 실패 > 0 그리고 유실 0

- [ ] **Step 3: 커밋** — `git add src && git commit -m "Add total outage recovery scenario"`

### Task 10: README + 콘텐츠 파이프라인 검증

**Files:**
- Create: `kafka/examples/scenario-runner/README.md`

- [ ] **Step 1: README 작성**

구성: H1 `# Kafka 장애 시나리오 러너` + 요약 문단(무엇을 왜 검증하는가), `## 전제 조건`(Docker, JDK 21+, Maven), `## 실행 방법`(compose up → mvn package → java -jar 시나리오명), `## 시나리오` 표(4개: 이름, 절차, PASS 기준 — 스펙의 표 재사용), `## 리포트 해석`(원장 수치 의미), `## 관련 문서`(usage-guide 07, troubleshooting cluster-total-outage 상대 링크), `## 정리`(compose down). 렌더러 서브셋 준수.

- [ ] **Step 2: 웹 파이프라인 검증**

Run: `cd web && npm run sync-content && npm run lint && npm test`
Expected: 문서 수 +1 (scenario-runner README), 테스트 2/2 PASS

- [ ] **Step 3: 최종 커밋**

```bash
git add kafka/examples/scenario-runner/README.md web/app/data/content.generated.json
git commit -m "Add scenario runner guide and sync content"
```

## Self-Review 결과

- 스펙 커버리지: 구성 요소 7종 → Task 3~7, 시나리오 4개 → Task 7~9, 로컬 compose → Task 2, README·파이프라인 → Task 10. 누락 없음.
- 플레이스홀더: 없음 — 모든 코드 단계에 전체 코드 포함.
- 타입 일관성: `Expectation.strict()/allowDuplicates()/expectSendFailures()`, `FaultInjector.resetTopic/ensureTopic/awaitFullIsr/awaitClusterReady`, `LoadGenerator.start/sendExactly/resend/stopAndDrain` 시그니처가 사용처(Task 7~9)와 일치함을 확인.
- 주의: Task 7 Step 4의 SCENARIOS 맵은 Task 8·9 전까지 `normal-roundtrip` 항목만 유지(컴파일 보전).
