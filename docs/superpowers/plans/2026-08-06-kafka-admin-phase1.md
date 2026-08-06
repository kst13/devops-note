# Kafka 관리자 사이트 1단계 (골격 + 조회) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 로그인 후 클러스터/브로커/토픽/컨슈머 그룹/랙을 조회할 수 있는 Kafka 관리자 사이트 1단계를 새 저장소 `~/Documents/kafka-admin`에 구축한다.

**Architecture:** Nginx(web)가 Vue 정적 파일을 서빙하고 `/api/*`를 Spring Boot(was)로 프록시한다. was는 AdminClient로 브로커에 조회를 수행하고, 사용자 계정은 H2 파일 DB에 저장한다. 스펙: `docs/superpowers/specs/2026-08-06-kafka-admin-site-design.md` (devops-note 저장소).

**Tech Stack:** Java 21, Spring Boot 3.5.x(Gradle), kafka-clients(AdminClient), Spring Security(세션), JPA+H2(파일 모드), Vue 3 + Vite + TypeScript, Vitest, Testcontainers, Nginx, docker-compose.

## Global Constraints

- 코드 위치: `~/Documents/kafka-admin` (새 git 저장소, devops-note와 별개)
- Java 21, Spring Boot 4.1.0 (계획 당시 3.5.4였으나 start.spring.io가 4.x만 제공 — Task 1 실행 시점에 4.1.0으로 확정), Node.js 22, Vue 3
- AdminClient 타임아웃 5초 (`request.timeout.ms=5000`, `default.api.timeout.ms=5000`) — 스펙 "예외 처리"
- 브로커 무응답 시 API는 503 + `{"error": "..."}` 로 응답하고 앱은 계속 동작한다
- 비밀값(SCRAM 비밀번호, truststore 비밀번호, 초기 admin 비밀번호)은 환경변수/마운트 파일로만 주입. 코드·이미지·커밋 금지
- 역할은 `ADMIN`, `DEVELOPER` 두 가지. 1단계는 전부 조회라 두 역할 모두 접근 가능
- 커밋 메시지: 짧은 명령형 영어 (`feat: ...`, `test: ...`, `chore: ...`)
- 문서/주석 한국어

## 파일 구조

```
kafka-admin/
├── was/                                  # Spring Boot (Gradle)
│   ├── build.gradle
│   └── src/
│       ├── main/java/com/osstem/kafkaadmin/
│       │   ├── KafkaAdminApplication.java
│       │   ├── config/KafkaConnectionProperties.java   # 접속 설정 → 클라이언트 props
│       │   ├── config/KafkaClientConfig.java           # Admin 빈
│       │   ├── config/SecurityConfig.java
│       │   ├── kafka/KafkaFutures.java                 # KafkaFuture → 예외 변환 헬퍼
│       │   ├── kafka/KafkaUnavailableException.java
│       │   ├── kafka/ClusterQueryService.java
│       │   ├── kafka/TopicQueryService.java
│       │   ├── kafka/GroupQueryService.java
│       │   ├── kafka/dto/Dtos.java                     # record 모음
│       │   ├── api/QueryController.java                # 조회 REST API
│       │   ├── api/ApiExceptionHandler.java
│       │   └── auth/ (AppUser, UserRepository, AuthController, AdminUserSeeder)
│       ├── main/resources/application.yml
│       └── test/ (각 태스크의 테스트, resources/application.properties)
├── web/                                  # Vue 3 + Vite + TS
│   ├── nginx.conf
│   ├── Dockerfile
│   └── src/ (api/client.ts, lib/lag.ts, router/index.ts, views/*.vue)
├── deploy/
│   ├── docker-compose.yml
│   └── .env.example
└── README.md
```

---

### Task 1: 저장소 + Spring Boot 스캐폴드

**Files:**
- Create: `~/Documents/kafka-admin/` (git init), `was/` (start.spring.io 스캐폴드), `was/build.gradle`(수정), `.gitignore`, `README.md`
- Test: `was/src/test/java/com/osstem/kafkaadmin/KafkaAdminApplicationTests.java` (스캐폴드에 포함)

**Interfaces:**
- Produces: Gradle 프로젝트 루트 `was/`, 패키지 `com.osstem.kafkaadmin`, `./gradlew test`가 동작하는 상태

- [ ] **Step 1: 저장소 생성 + 스캐폴드 다운로드**

```bash
mkdir -p ~/Documents/kafka-admin && cd ~/Documents/kafka-admin && git init
curl -s https://start.spring.io/starter.tgz \
  -d type=gradle-project -d language=java -d bootVersion=3.5.4 -d javaVersion=21 \
  -d groupId=com.osstem -d artifactId=kafka-admin-was -d name=KafkaAdmin \
  -d packageName=com.osstem.kafkaadmin -d baseDir=was \
  -d dependencies=web,security,data-jpa,h2 | tar -xzf -
```

- [ ] **Step 2: build.gradle에 의존성 추가**

`was/build.gradle`의 `dependencies` 블록을 아래로 교체:

```groovy
dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-web'
    implementation 'org.springframework.boot:spring-boot-starter-security'
    implementation 'org.springframework.boot:spring-boot-starter-data-jpa'
    implementation 'org.apache.kafka:kafka-clients'
    runtimeOnly 'com.h2database:h2'
    testImplementation 'org.springframework.boot:spring-boot-starter-test'
    testImplementation 'org.springframework.security:spring-security-test'
    testImplementation 'org.springframework.boot:spring-boot-testcontainers'
    testImplementation 'org.testcontainers:kafka'
    testImplementation 'org.testcontainers:junit-jupiter'
    testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
}
```

- [ ] **Step 3: 설정 파일 작성**

`was/src/main/resources/application.yml` (기존 application.properties 삭제):

```yaml
spring:
  datasource:
    url: jdbc:h2:file:${APP_DATA_DIR:./data}/kafka-admin
  jpa:
    hibernate:
      ddl-auto: update
app:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9094}
    security-protocol: ${KAFKA_SECURITY_PROTOCOL:PLAINTEXT}
    sasl-jaas: ${KAFKA_SASL_JAAS:}
    truststore-location: ${KAFKA_TRUSTSTORE_LOCATION:}
    truststore-password: ${KAFKA_TRUSTSTORE_PASSWORD:}
  admin-initial-password: ${ADMIN_INITIAL_PASSWORD:}
```

`was/src/test/resources/application.properties` (테스트는 인메모리 DB):

```properties
spring.datasource.url=jdbc:h2:mem:test
```

루트 `.gitignore`:

```
was/build/
was/data/
web/node_modules/
web/dist/
.DS_Store
.env
```

루트 `README.md`:

```markdown
# kafka-admin

Kafka 3노드 클러스터(KRaft, SASL_SSL) 관리자 사이트.

- `was/` Spring Boot 백엔드 (`cd was && ./gradlew bootRun`)
- `web/` Vue 프론트 (`cd web && npm run dev`)
- `deploy/` docker-compose 배포 (`cd deploy && docker compose up -d --build`)

설계 문서: devops-note 저장소 `docs/superpowers/specs/2026-08-06-kafka-admin-site-design.md`
```

- [ ] **Step 4: 스모크 테스트 실행**

Run: `cd ~/Documents/kafka-admin/was && ./gradlew test`
Expected: `KafkaAdminApplicationTests > contextLoads()` PASS (Security 기본 설정이 있어도 컨텍스트 로드는 성공)

- [ ] **Step 5: Commit**

```bash
cd ~/Documents/kafka-admin && git add -A && git commit -m "chore: scaffold Spring Boot backend"
```

---

### Task 2: Kafka 접속 설정 (properties → 클라이언트 props)

**Files:**
- Create: `was/src/main/java/com/osstem/kafkaadmin/config/KafkaConnectionProperties.java`, `config/KafkaClientConfig.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/config/KafkaConnectionPropertiesTest.java`

**Interfaces:**
- Produces: `Admin` 스프링 빈. `KafkaConnectionProperties(String bootstrapServers, String securityProtocol, String saslJaas, String truststoreLocation, String truststorePassword)` record + `Map<String, Object> toClientProps()`

- [ ] **Step 1: 실패하는 테스트 작성**

```java
package com.osstem.kafkaadmin.config;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaConnectionPropertiesTest {

    @Test
    void plaintext_구성은_보안설정을_포함하지_않는다() {
        var props = new KafkaConnectionProperties("localhost:9092", "PLAINTEXT", "", "", "");
        Map<String, Object> p = props.toClientProps();
        assertThat(p).containsEntry("bootstrap.servers", "localhost:9092")
                .containsEntry("request.timeout.ms", 5000)
                .containsEntry("default.api.timeout.ms", 5000)
                .doesNotContainKeys("sasl.jaas.config", "ssl.truststore.location");
    }

    @Test
    void sasl_ssl_구성은_인증과_truststore_설정을_포함한다() {
        var props = new KafkaConnectionProperties("10.0.0.11:9094", "SASL_SSL",
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"pw\";",
                "/secrets/truststore.jks", "tspw");
        Map<String, Object> p = props.toClientProps();
        assertThat(p).containsEntry("security.protocol", "SASL_SSL")
                .containsEntry("sasl.mechanism", "SCRAM-SHA-512")
                .containsEntry("ssl.truststore.location", "/secrets/truststore.jks")
                .containsEntry("ssl.truststore.password", "tspw")
                .containsKey("sasl.jaas.config");
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew test --tests 'KafkaConnectionPropertiesTest'`
Expected: 컴파일 실패 (`KafkaConnectionProperties` 없음)

- [ ] **Step 3: 구현**

`config/KafkaConnectionProperties.java`:

```java
package com.osstem.kafkaadmin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.HashMap;
import java.util.Map;

// AdminClient 타임아웃 5초: 브로커가 죽어도 화면이 5초 안에 "접속 불가"로 응답하기 위한 값(스펙 예외 처리 절)
@ConfigurationProperties(prefix = "app.kafka")
public record KafkaConnectionProperties(
        String bootstrapServers,
        String securityProtocol,
        String saslJaas,
        String truststoreLocation,
        String truststorePassword) {

    public Map<String, Object> toClientProps() {
        Map<String, Object> p = new HashMap<>();
        p.put("bootstrap.servers", bootstrapServers);
        p.put("request.timeout.ms", 5000);
        p.put("default.api.timeout.ms", 5000);
        if ("SASL_SSL".equals(securityProtocol)) {
            p.put("security.protocol", "SASL_SSL");
            p.put("sasl.mechanism", "SCRAM-SHA-512");
            p.put("sasl.jaas.config", saslJaas);
            p.put("ssl.truststore.location", truststoreLocation);
            p.put("ssl.truststore.password", truststorePassword);
        }
        return p;
    }
}
```

`config/KafkaClientConfig.java`:

```java
package com.osstem.kafkaadmin.config;

import org.apache.kafka.clients.admin.Admin;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(KafkaConnectionProperties.class)
public class KafkaClientConfig {

    // Admin.create 는 지연 접속이라 브로커가 내려가 있어도 앱 기동은 성공한다
    @Bean(destroyMethod = "close")
    public Admin adminClient(KafkaConnectionProperties props) {
        return Admin.create(props.toClientProps());
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew test --tests 'KafkaConnectionPropertiesTest'`
Expected: 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: kafka connection properties and Admin bean"
```

---

### Task 3: ClusterQueryService (Testcontainers 기반)

**Files:**
- Create: `kafka/KafkaFutures.java`, `kafka/KafkaUnavailableException.java`, `kafka/dto/Dtos.java`, `kafka/ClusterQueryService.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/kafka/KafkaIntegrationTestBase.java`, `kafka/ClusterQueryServiceIT.java`

**Interfaces:**
- Consumes: Task 2의 `Admin` 빈
- Produces:
  - `KafkaFutures.await(KafkaFuture<T>) -> T` (실패 시 `KafkaUnavailableException`)
  - `ClusterQueryService.getClusterInfo() -> ClusterInfo`
  - DTO: `BrokerInfo(int id, String host, int port)`, `ClusterInfo(String clusterId, int controllerId, List<BrokerInfo> brokers)`
  - 통합 테스트 베이스 `KafkaIntegrationTestBase` (apache/kafka:4.0.0 컨테이너 1대, `app.kafka.*` 주입)

- [ ] **Step 1: 실패하는 테스트 작성**

`kafka/KafkaIntegrationTestBase.java` (테스트 소스):

```java
package com.osstem.kafkaadmin.kafka;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

// 싱글턴 컨테이너 패턴: @Container는 클래스마다 컨테이너를 재시작하는데,
// Spring 테스트 컨텍스트는 클래스 간 캐시되므로 두 번째 IT부터 죽은 컨테이너를
// 바라보는 Admin 빈을 재사용하게 된다. JVM당 1회 기동으로 이를 방지한다.
// (컨테이너는 Testcontainers의 Ryuk이 JVM 종료 후 정리한다)
@SpringBootTest
public abstract class KafkaIntegrationTestBase {

    protected static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:4.0.0"));

    static {
        KAFKA.start();
    }

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry registry) {
        registry.add("app.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("app.kafka.security-protocol", () -> "PLAINTEXT");
    }
}
```

`kafka/ClusterQueryServiceIT.java`:

```java
package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.ClusterInfo;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import static org.assertj.core.api.Assertions.assertThat;

class ClusterQueryServiceIT extends KafkaIntegrationTestBase {

    @Autowired ClusterQueryService service;

    @Test
    void 단일_브로커_클러스터_정보를_조회한다() {
        ClusterInfo info = service.getClusterInfo();
        assertThat(info.clusterId()).isNotBlank();
        assertThat(info.brokers()).hasSize(1);
        assertThat(info.controllerId()).isEqualTo(info.brokers().get(0).id());
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew test --tests 'ClusterQueryServiceIT'`
Expected: 컴파일 실패 (`ClusterQueryService`, `Dtos` 없음)

- [ ] **Step 3: 구현**

`kafka/KafkaUnavailableException.java`:

```java
package com.osstem.kafkaadmin.kafka;

public class KafkaUnavailableException extends RuntimeException {
    public KafkaUnavailableException(Throwable cause) {
        super("브로커 접속 불가: " + cause.getMessage(), cause);
    }
}
```

`kafka/KafkaFutures.java`:

```java
package com.osstem.kafkaadmin.kafka;

import org.apache.kafka.common.KafkaFuture;
import java.util.concurrent.ExecutionException;

public final class KafkaFutures {
    private KafkaFutures() {}

    public static <T> T await(KafkaFuture<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaUnavailableException(e);
        } catch (ExecutionException e) {
            throw new KafkaUnavailableException(e.getCause());
        }
    }
}
```

`kafka/dto/Dtos.java` (1단계 조회 DTO 전부를 여기에 모은다):

```java
package com.osstem.kafkaadmin.kafka.dto;

import java.util.List;
import java.util.Map;

public final class Dtos {
    private Dtos() {}

    public record BrokerInfo(int id, String host, int port) {}
    public record ClusterInfo(String clusterId, int controllerId, List<BrokerInfo> brokers) {}
    public record TopicSummary(String name, int partitionCount, int replicationFactor) {}
    public record PartitionInfo(int partition, int leader, List<Integer> replicas, List<Integer> isr) {}
    public record TopicDetail(String name, List<PartitionInfo> partitions, Map<String, String> configs) {}
    public record GroupSummary(String groupId, String state, int memberCount) {}
    public record PartitionLag(String topic, int partition, long committed, long end, long lag) {}
    public record GroupDetail(String groupId, String state, List<PartitionLag> lags, long totalLag) {}
}
```

`kafka/ClusterQueryService.java`:

```java
package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.BrokerInfo;
import com.osstem.kafkaadmin.kafka.dto.Dtos.ClusterInfo;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.stereotype.Service;
import java.util.Comparator;
import java.util.List;

@Service
public class ClusterQueryService {

    private final Admin admin;

    public ClusterQueryService(Admin admin) {
        this.admin = admin;
    }

    public ClusterInfo getClusterInfo() {
        DescribeClusterResult result = admin.describeCluster();
        List<BrokerInfo> brokers = KafkaFutures.await(result.nodes()).stream()
                .map(n -> new BrokerInfo(n.id(), n.host(), n.port()))
                .sorted(Comparator.comparingInt(BrokerInfo::id))
                .toList();
        return new ClusterInfo(
                KafkaFutures.await(result.clusterId()),
                KafkaFutures.await(result.controller()).id(),
                brokers);
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew test --tests 'ClusterQueryServiceIT'` (Docker 데몬 필요)
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: cluster query service with testcontainers base"
```

---

### Task 4: TopicQueryService

**Files:**
- Create: `kafka/TopicQueryService.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/kafka/TopicQueryServiceIT.java`

**Interfaces:**
- Consumes: `Admin` 빈, `KafkaFutures.await`, `Dtos.TopicSummary/PartitionInfo/TopicDetail`, `KafkaIntegrationTestBase`
- Produces: `TopicQueryService.listTopics() -> List<TopicSummary>`, `describeTopic(String name) -> TopicDetail` (configs에는 `retention.ms`, `min.insync.replicas`, `cleanup.policy`만 담는다)

- [ ] **Step 1: 실패하는 테스트 작성**

```java
package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicSummary;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

class TopicQueryServiceIT extends KafkaIntegrationTestBase {

    @Autowired Admin admin;
    @Autowired TopicQueryService service;

    @BeforeEach
    void createTopic() throws Exception {
        if (!admin.listTopics().names().get().contains("orders")) {
            admin.createTopics(List.of(new NewTopic("orders", 3, (short) 1))).all().get();
        }
    }

    @Test
    void 토픽_목록에_파티션수와_복제팩터가_나온다() {
        List<TopicSummary> topics = service.listTopics();
        TopicSummary orders = topics.stream()
                .filter(t -> t.name().equals("orders")).findFirst().orElseThrow();
        assertThat(orders.partitionCount()).isEqualTo(3);
        assertThat(orders.replicationFactor()).isEqualTo(1);
    }

    @Test
    void 토픽_상세에_파티션별_리더와_isr_그리고_주요_설정이_나온다() {
        TopicDetail detail = service.describeTopic("orders");
        assertThat(detail.partitions()).hasSize(3);
        assertThat(detail.partitions().get(0).isr()).isNotEmpty();
        assertThat(detail.configs()).containsKeys("retention.ms", "min.insync.replicas", "cleanup.policy");
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew test --tests 'TopicQueryServiceIT'`
Expected: 컴파일 실패 (`TopicQueryService` 없음)

- [ ] **Step 3: 구현**

```java
package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.PartitionInfo;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicSummary;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Service;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class TopicQueryService {

    private static final Set<String> SHOWN_CONFIGS =
            Set.of("retention.ms", "min.insync.replicas", "cleanup.policy");

    private final Admin admin;

    public TopicQueryService(Admin admin) {
        this.admin = admin;
    }

    public List<TopicSummary> listTopics() {
        Set<String> names = KafkaFutures.await(
                admin.listTopics(new ListTopicsOptions().listInternal(false)).names());
        Map<String, TopicDescription> descriptions =
                KafkaFutures.await(admin.describeTopics(names).allTopicNames());
        return descriptions.values().stream()
                .map(d -> new TopicSummary(d.name(), d.partitions().size(),
                        d.partitions().get(0).replicas().size()))
                .sorted(Comparator.comparing(TopicSummary::name))
                .toList();
    }

    public TopicDetail describeTopic(String name) {
        TopicDescription desc = KafkaFutures.await(
                admin.describeTopics(List.of(name)).allTopicNames()).get(name);
        List<PartitionInfo> partitions = desc.partitions().stream()
                .map(this::toPartitionInfo)
                .sorted(Comparator.comparingInt(PartitionInfo::partition))
                .toList();
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
        Map<String, String> configs = KafkaFutures.await(
                        admin.describeConfigs(List.of(resource)).all())
                .get(resource).entries().stream()
                .filter(e -> SHOWN_CONFIGS.contains(e.name()))
                .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
        return new TopicDetail(name, partitions, configs);
    }

    private PartitionInfo toPartitionInfo(TopicPartitionInfo p) {
        return new PartitionInfo(p.partition(),
                p.leader() == null ? -1 : p.leader().id(),
                p.replicas().stream().map(Node::id).toList(),
                p.isr().stream().map(Node::id).toList());
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew test --tests 'TopicQueryServiceIT'`
Expected: 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: topic query service"
```

---

### Task 5: GroupQueryService (컨슈머 그룹 + 랙)

**Files:**
- Create: `kafka/GroupQueryService.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/kafka/GroupQueryServiceIT.java`

**Interfaces:**
- Consumes: `Admin` 빈, `KafkaFutures.await`, `Dtos.GroupSummary/PartitionLag/GroupDetail`, `KafkaIntegrationTestBase`
- Produces: `GroupQueryService.listGroups() -> List<GroupSummary>`, `describeGroup(String groupId) -> GroupDetail` (lag = latest end offset − committed offset)

- [ ] **Step 1: 실패하는 테스트 작성**

```java
package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.GroupDetail;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;

class GroupQueryServiceIT extends KafkaIntegrationTestBase {

    @Autowired Admin admin;
    @Autowired GroupQueryService service;

    @Test
    void 커밋_이후_추가된_메시지_수만큼_랙이_계산된다() throws Exception {
        admin.createTopics(List.of(new NewTopic("lag-topic", 1, (short) 1))).all().get();

        try (var producer = new KafkaProducer<String, String>(producerProps())) {
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>("lag-topic", "m" + i)).get();
            }
        }
        // 그룹 g1로 5건 전부 소비·커밋
        try (var consumer = new KafkaConsumer<String, String>(consumerProps("g1"))) {
            consumer.subscribe(List.of("lag-topic"));
            int seen = 0;
            while (seen < 5) {
                seen += consumer.poll(Duration.ofSeconds(1)).count();
            }
            consumer.commitSync();
        }
        // 2건 추가 생산 → lag 2
        try (var producer = new KafkaProducer<String, String>(producerProps())) {
            producer.send(new ProducerRecord<>("lag-topic", "m5")).get();
            producer.send(new ProducerRecord<>("lag-topic", "m6")).get();
        }

        GroupDetail detail = service.describeGroup("g1");
        assertThat(detail.totalLag()).isEqualTo(2);
        assertThat(detail.lags()).hasSize(1);
        assertThat(detail.lags().get(0).committed()).isEqualTo(5);
        assertThat(detail.lags().get(0).end()).isEqualTo(7);

        assertThat(service.listGroups())
                .anySatisfy(g -> assertThat(g.groupId()).isEqualTo("g1"));
    }

    private Map<String, Object> producerProps() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer",
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
    }

    private Map<String, Object> consumerProps(String groupId) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew test --tests 'GroupQueryServiceIT'`
Expected: 컴파일 실패 (`GroupQueryService` 없음)

- [ ] **Step 3: 구현**

```java
package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.GroupDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.GroupSummary;
import com.osstem.kafkaadmin.kafka.dto.Dtos.PartitionLag;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class GroupQueryService {

    private final Admin admin;

    public GroupQueryService(Admin admin) {
        this.admin = admin;
    }

    public List<GroupSummary> listGroups() {
        List<String> ids = KafkaFutures.await(admin.listConsumerGroups().all()).stream()
                .map(l -> l.groupId())
                .toList();
        if (ids.isEmpty()) {
            return List.of();
        }
        Map<String, ConsumerGroupDescription> descs =
                KafkaFutures.await(admin.describeConsumerGroups(ids).all());
        return descs.values().stream()
                .map(d -> new GroupSummary(d.groupId(), d.state().toString(), d.members().size()))
                .sorted(Comparator.comparing(GroupSummary::groupId))
                .toList();
    }

    public GroupDetail describeGroup(String groupId) {
        ConsumerGroupDescription desc =
                KafkaFutures.await(admin.describeConsumerGroups(List.of(groupId)).all()).get(groupId);
        Map<TopicPartition, OffsetAndMetadata> committed =
                KafkaFutures.await(admin.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata());

        Map<TopicPartition, OffsetSpec> latestSpec = committed.keySet().stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends =
                KafkaFutures.await(admin.listOffsets(latestSpec).all());

        List<PartitionLag> lags = committed.entrySet().stream()
                .map(e -> {
                    long committedOffset = e.getValue().offset();
                    long end = ends.get(e.getKey()).offset();
                    return new PartitionLag(e.getKey().topic(), e.getKey().partition(),
                            committedOffset, end, Math.max(0, end - committedOffset));
                })
                .sorted(Comparator.comparing(PartitionLag::topic)
                        .thenComparingInt(PartitionLag::partition))
                .toList();
        long totalLag = lags.stream().mapToLong(PartitionLag::lag).sum();
        return new GroupDetail(groupId, desc.state().toString(), lags, totalLag);
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew test --tests 'GroupQueryServiceIT'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: consumer group query service with lag"
```

---

### Task 6: 조회 REST API + 브로커 무응답 처리

**Files:**
- Create: `api/QueryController.java`, `api/ApiExceptionHandler.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/api/QueryControllerTest.java`

**Interfaces:**
- Consumes: Task 3~5의 서비스 3개와 DTO, `KafkaUnavailableException`
- Produces: `GET /api/cluster`, `GET /api/topics`, `GET /api/topics/{name}`, `GET /api/groups`, `GET /api/groups/{groupId}`. 브로커 무응답 시 503 + `{"error": "브로커 접속 불가: ..."}`

- [ ] **Step 1: 실패하는 테스트 작성**

```java
package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.GroupQueryService;
import com.osstem.kafkaadmin.kafka.KafkaUnavailableException;
import com.osstem.kafkaadmin.kafka.TopicQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.BrokerInfo;
import com.osstem.kafkaadmin.kafka.dto.Dtos.ClusterInfo;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicSummary;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import java.util.List;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(QueryController.class)
class QueryControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean ClusterQueryService clusterService;
    @MockitoBean TopicQueryService topicService;
    @MockitoBean GroupQueryService groupService;

    @Test
    @WithMockUser
    void 클러스터_조회() throws Exception {
        given(clusterService.getClusterInfo()).willReturn(
                new ClusterInfo("abc", 1, List.of(new BrokerInfo(1, "10.0.0.11", 9094))));
        mvc.perform(get("/api/cluster"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.controllerId").value(1))
                .andExpect(jsonPath("$.brokers[0].host").value("10.0.0.11"));
    }

    @Test
    @WithMockUser
    void 브로커_무응답이면_503과_에러_메시지를_준다() throws Exception {
        given(topicService.listTopics()).willThrow(
                new KafkaUnavailableException(new RuntimeException("timeout")));
        mvc.perform(get("/api/topics"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").exists());
    }

    @Test
    void 미인증이면_401() throws Exception {
        mvc.perform(get("/api/topics")).andExpect(status().isUnauthorized());
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew test --tests 'QueryControllerTest'`
Expected: 컴파일 실패 (`QueryController` 없음). ※ 미인증 401 케이스는 Task 7의 SecurityConfig가 생기기 전까지는 Spring Security 기본 설정(모든 요청 인증 요구 + 401/302) 동작에 따라 결과가 다를 수 있다 — Task 7 완료 후 이 테스트가 전부 통과하는 것을 최종 기준으로 한다.

- [ ] **Step 3: 구현**

`api/QueryController.java`:

```java
package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.GroupQueryService;
import com.osstem.kafkaadmin.kafka.TopicQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.*;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;

@RestController
@RequestMapping("/api")
public class QueryController {

    private final ClusterQueryService clusterService;
    private final TopicQueryService topicService;
    private final GroupQueryService groupService;

    public QueryController(ClusterQueryService clusterService,
                           TopicQueryService topicService,
                           GroupQueryService groupService) {
        this.clusterService = clusterService;
        this.topicService = topicService;
        this.groupService = groupService;
    }

    @GetMapping("/cluster")
    public ClusterInfo cluster() {
        return clusterService.getClusterInfo();
    }

    @GetMapping("/topics")
    public List<TopicSummary> topics() {
        return topicService.listTopics();
    }

    @GetMapping("/topics/{name}")
    public TopicDetail topic(@PathVariable String name) {
        return topicService.describeTopic(name);
    }

    @GetMapping("/groups")
    public List<GroupSummary> groups() {
        return groupService.listGroups();
    }

    @GetMapping("/groups/{groupId}")
    public GroupDetail group(@PathVariable String groupId) {
        return groupService.describeGroup(groupId);
    }
}
```

`api/ApiExceptionHandler.java`:

```java
package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.kafka.KafkaUnavailableException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import java.util.Map;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(KafkaUnavailableException.class)
    public ResponseEntity<Map<String, String>> kafkaUnavailable(KafkaUnavailableException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("error", e.getMessage()));
    }
}
```

- [ ] **Step 4: 테스트 확인**

Run: `./gradlew test --tests 'QueryControllerTest'`
Expected: 클러스터_조회, 503 케이스 PASS (401 케이스는 Task 7 이후 최종 확인)

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: query REST API with kafka-unavailable handling"
```

---

### Task 7: 인증 (세션 로그인, 역할, 초기 계정)

**Files:**
- Create: `auth/AppUser.java`, `auth/UserRepository.java`, `auth/AppUserDetailsService.java`, `auth/AuthController.java`, `auth/AdminUserSeeder.java`, `config/SecurityConfig.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/auth/AuthFlowTest.java`

**Interfaces:**
- Consumes: JPA/H2 (Task 1), `app.admin-initial-password` 설정값
- Produces: `POST /api/auth/login` `{username,password}` → 200 `{username, role}` / 401, `POST /api/auth/logout`, `GET /api/auth/me` → 200 `{username, role}` / 401. 역할 문자열 `ADMIN`/`DEVELOPER`. 최초 기동 시 사용자가 없으면 `ADMIN_INITIAL_PASSWORD`로 admin 계정 생성

- [ ] **Step 1: 실패하는 테스트 작성**

```java
package com.osstem.kafkaadmin.auth;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.mock.web.MockHttpSession;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = "app.admin-initial-password=test-admin-pw")
class AuthFlowTest {

    @Autowired MockMvc mvc;

    @Test
    void 로그인_성공_후_me_와_보호된_API_에_접근할_수_있다() throws Exception {
        var session = (MockHttpSession) mvc.perform(post("/api/auth/login")
                        .contentType("application/json")
                        .content("{\"username\":\"admin\",\"password\":\"test-admin-pw\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.role").value("ADMIN"))
                .andReturn().getRequest().getSession(false);

        mvc.perform(get("/api/auth/me").session(session))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.username").value("admin"));
    }

    @Test
    void 잘못된_비밀번호는_401() throws Exception {
        mvc.perform(post("/api/auth/login")
                        .contentType("application/json")
                        .content("{\"username\":\"admin\",\"password\":\"wrong\"}"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void 미로그인_상태의_me_는_401() throws Exception {
        mvc.perform(get("/api/auth/me")).andExpect(status().isUnauthorized());
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew test --tests 'AuthFlowTest'`
Expected: FAIL (로그인 엔드포인트 없음 → 401/404)

- [ ] **Step 3: 구현**

`auth/AppUser.java`:

```java
package com.osstem.kafkaadmin.auth;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "app_user")
public class AppUser {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String username;
    private String passwordHash;
    private String role; // ADMIN | DEVELOPER

    protected AppUser() {}

    public AppUser(String username, String passwordHash, String role) {
        this.username = username;
        this.passwordHash = passwordHash;
        this.role = role;
    }

    public String getUsername() { return username; }
    public String getPasswordHash() { return passwordHash; }
    public String getRole() { return role; }
}
```

`auth/UserRepository.java`:

```java
package com.osstem.kafkaadmin.auth;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;

public interface UserRepository extends JpaRepository<AppUser, Long> {
    Optional<AppUser> findByUsername(String username);
}
```

`auth/AppUserDetailsService.java`:

```java
package com.osstem.kafkaadmin.auth;

import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

@Service
public class AppUserDetailsService implements UserDetailsService {

    private final UserRepository users;

    public AppUserDetailsService(UserRepository users) {
        this.users = users;
    }

    @Override
    public UserDetails loadUserByUsername(String username) {
        AppUser u = users.findByUsername(username)
                .orElseThrow(() -> new UsernameNotFoundException(username));
        return User.withUsername(u.getUsername())
                .password(u.getPasswordHash())
                .roles(u.getRole())
                .build();
    }
}
```

`config/SecurityConfig.java`:

```java
package com.osstem.kafkaadmin.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.HttpStatusEntryPoint;
import org.springframework.security.web.context.HttpSessionSecurityContextRepository;
import org.springframework.security.web.context.SecurityContextRepository;

@Configuration
public class SecurityConfig {

    // 내부망 관리 도구 + 세션 쿠키 SameSite 기본값 전제로 1단계는 CSRF 비활성.
    // 외부 노출이 생기면 재검토한다.
    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http.csrf(AbstractHttpConfigurer::disable)
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/api/auth/login").permitAll()
                .requestMatchers("/api/**").authenticated()
                .anyRequest().permitAll())
            .exceptionHandling(e ->
                e.authenticationEntryPoint(new HttpStatusEntryPoint(HttpStatus.UNAUTHORIZED)));
        return http.build();
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public AuthenticationManager authenticationManager(AuthenticationConfiguration config)
            throws Exception {
        return config.getAuthenticationManager();
    }

    @Bean
    public SecurityContextRepository securityContextRepository() {
        return new HttpSessionSecurityContextRepository();
    }
}
```

`auth/AuthController.java`:

```java
package com.osstem.kafkaadmin.auth;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.context.SecurityContextRepository;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

    public record LoginRequest(String username, String password) {}

    private final AuthenticationManager authManager;
    private final SecurityContextRepository contextRepository;

    public AuthController(AuthenticationManager authManager,
                          SecurityContextRepository contextRepository) {
        this.authManager = authManager;
        this.contextRepository = contextRepository;
    }

    @PostMapping("/login")
    public Map<String, String> login(@RequestBody LoginRequest req,
                                     HttpServletRequest request, HttpServletResponse response) {
        Authentication auth = authManager.authenticate(
                new UsernamePasswordAuthenticationToken(req.username(), req.password()));
        SecurityContext context = SecurityContextHolder.createEmptyContext();
        context.setAuthentication(auth);
        SecurityContextHolder.setContext(context);
        contextRepository.saveContext(context, request, response);
        return Map.of("username", auth.getName(), "role", roleOf(auth));
    }

    @PostMapping("/logout")
    public void logout(HttpServletRequest request) {
        var session = request.getSession(false);
        if (session != null) {
            session.invalidate();
        }
        SecurityContextHolder.clearContext();
    }

    @GetMapping("/me")
    public Map<String, String> me(Authentication auth) {
        return Map.of("username", auth.getName(), "role", roleOf(auth));
    }

    @ExceptionHandler({AuthenticationException.class, BadCredentialsException.class})
    public ResponseEntity<Map<String, String>> badCredentials() {
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                .body(Map.of("error", "아이디 또는 비밀번호가 올바르지 않습니다"));
    }

    private String roleOf(Authentication auth) {
        return auth.getAuthorities().iterator().next().getAuthority().replace("ROLE_", "");
    }
}
```

`auth/AdminUserSeeder.java`:

```java
package com.osstem.kafkaadmin.auth;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

@Component
public class AdminUserSeeder implements CommandLineRunner {

    private final UserRepository users;
    private final PasswordEncoder encoder;
    private final String initialPassword;

    public AdminUserSeeder(UserRepository users, PasswordEncoder encoder,
                           @Value("${app.admin-initial-password}") String initialPassword) {
        this.users = users;
        this.encoder = encoder;
        this.initialPassword = initialPassword;
    }

    @Override
    public void run(String... args) {
        if (users.count() == 0 && !initialPassword.isBlank()) {
            users.save(new AppUser("admin", encoder.encode(initialPassword), "ADMIN"));
        }
    }
}
```

- [ ] **Step 4: 전체 테스트 통과 확인**

Run: `./gradlew test`
Expected: AuthFlowTest 3건 PASS + Task 6의 `미인증이면_401` 포함 기존 테스트 전부 PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: session auth with roles and admin seeding"
```

---

### Task 8: Vue 스캐폴드 + API 클라이언트 + 로그인/라우터 가드

**Files:**
- Create: `web/` (create-vue 스캐폴드), `web/src/api/client.ts`, `web/src/lib/lag.ts`, `web/src/router/index.ts`(수정), `web/src/views/LoginView.vue`, `web/vite.config.ts`(프록시 추가)
- Test: `web/src/lib/__tests__/lag.spec.ts`

**Interfaces:**
- Consumes: Task 6~7의 REST API
- Produces: `api<T>(path, init?) -> Promise<T>` (401 시 `UnauthorizedError` throw), `sumLag(lags: {lag: number}[]) -> number`, 라우트 `/login`, `/`(cluster), `/topics`, `/topics/:name`, `/groups`, `/groups/:groupId` + 로그인 가드

- [ ] **Step 1: 스캐폴드 생성**

```bash
cd ~/Documents/kafka-admin
npm create vue@latest web -- --typescript --router --vitest
cd web && npm install
```

(프롬프트가 나오면 TypeScript/Router/Vitest만 Yes, 나머지 No)

`web/vite.config.ts`의 `defineConfig` 안에 dev 프록시 추가:

```ts
server: {
  proxy: { '/api': 'http://localhost:8080' },
},
```

- [ ] **Step 2: 실패하는 테스트 작성**

`web/src/lib/__tests__/lag.spec.ts`:

```ts
import { describe, it, expect } from 'vitest'
import { sumLag } from '../lag'

describe('sumLag', () => {
  it('파티션 랙의 합을 구한다', () => {
    expect(sumLag([{ lag: 3 }, { lag: 0 }, { lag: 7 }])).toBe(10)
  })
  it('빈 배열은 0', () => {
    expect(sumLag([])).toBe(0)
  })
})
```

Run: `npm run test:unit -- --run`
Expected: FAIL (`../lag` 없음)

- [ ] **Step 3: 구현**

`web/src/lib/lag.ts`:

```ts
export function sumLag(lags: { lag: number }[]): number {
  return lags.reduce((acc, p) => acc + p.lag, 0)
}
```

`web/src/api/client.ts`:

```ts
export class UnauthorizedError extends Error {}

export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  if (res.status === 401) throw new UnauthorizedError()
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }))
    throw new Error(body.error ?? `HTTP ${res.status}`)
  }
  if (res.status === 204) return undefined as T
  return res.json()
}
```

`web/src/views/LoginView.vue`:

```vue
<script setup lang="ts">
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { api } from '@/api/client'

const router = useRouter()
const username = ref('')
const password = ref('')
const error = ref('')

async function login() {
  error.value = ''
  try {
    await api('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username: username.value, password: password.value }),
    })
    router.push('/')
  } catch {
    error.value = '아이디 또는 비밀번호가 올바르지 않습니다'
  }
}
</script>

<template>
  <main class="login">
    <h1>Kafka Admin</h1>
    <form @submit.prevent="login">
      <input v-model="username" placeholder="아이디" autocomplete="username" />
      <input v-model="password" type="password" placeholder="비밀번호"
             autocomplete="current-password" />
      <button type="submit">로그인</button>
      <p v-if="error" class="error">{{ error }}</p>
    </form>
  </main>
</template>

<style scoped>
.login { max-width: 320px; margin: 15vh auto; display: flex; flex-direction: column; gap: 1rem; }
form { display: flex; flex-direction: column; gap: 0.5rem; }
.error { color: #c00; }
</style>
```

`web/src/router/index.ts` 전체 교체 (스캐폴드가 만든 뷰 대신 우리 라우트; Task 9의 뷰 4개를 미리 등록하므로 Task 9 전까지는 빈 파일이 필요 — Step 4 참고):

```ts
import { createRouter, createWebHistory } from 'vue-router'
import { api, UnauthorizedError } from '@/api/client'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    { path: '/login', component: () => import('@/views/LoginView.vue') },
    { path: '/', component: () => import('@/views/ClusterView.vue') },
    { path: '/topics', component: () => import('@/views/TopicsView.vue') },
    { path: '/topics/:name', component: () => import('@/views/TopicDetailView.vue') },
    { path: '/groups', component: () => import('@/views/GroupsView.vue') },
    { path: '/groups/:groupId', component: () => import('@/views/GroupDetailView.vue') },
  ],
})

router.beforeEach(async (to) => {
  if (to.path === '/login') return true
  try {
    await api('/auth/me')
    return true
  } catch (e) {
    if (e instanceof UnauthorizedError) return '/login'
    return true // 브로커/서버 오류는 각 화면이 표시한다
  }
})

export default router
```

`web/src/App.vue` 전체 교체:

```vue
<script setup lang="ts">
import { useRoute } from 'vue-router'
const route = useRoute()
</script>

<template>
  <nav v-if="route.path !== '/login'">
    <RouterLink to="/">클러스터</RouterLink>
    <RouterLink to="/topics">토픽</RouterLink>
    <RouterLink to="/groups">컨슈머 그룹</RouterLink>
  </nav>
  <RouterView />
</template>

<style scoped>
nav { display: flex; gap: 1rem; padding: 1rem; border-bottom: 1px solid #ddd; }
</style>
```

- [ ] **Step 4: 뷰 자리 채우기 + 검증**

Task 9에서 구현할 뷰 4개(`ClusterView.vue`, `TopicsView.vue`, `TopicDetailView.vue`, `GroupsView.vue`, `GroupDetailView.vue`)를 최소 형태로 생성 (컴파일만 되게):

```vue
<template><main>준비 중</main></template>
```

스캐폴드가 만든 `HomeView.vue`, `AboutView.vue`와 관련 라우트/컴포넌트 잔재는 삭제.

Run: `npm run test:unit -- --run && npm run build`
Expected: 테스트 2건 PASS, 빌드 성공

수동 확인: was 기동(`ADMIN_INITIAL_PASSWORD=devpw ./gradlew bootRun`) + `npm run dev` → http://localhost:5173 접속 시 /login으로 리다이렉트, admin/devpw 로그인 성공

- [ ] **Step 5: Commit**

```bash
cd ~/Documents/kafka-admin && git add -A && git commit -m "feat: vue scaffold with login and router guard"
```

---

### Task 9: 조회 화면 (클러스터/토픽/그룹/랙)

**Files:**
- Modify: `web/src/views/ClusterView.vue`, `TopicsView.vue`, `TopicDetailView.vue`, `GroupsView.vue`, `GroupDetailView.vue`

**Interfaces:**
- Consumes: `api<T>()`, `sumLag()`, REST API 5개 (Task 6의 응답 형태 그대로)

- [ ] **Step 1: 클러스터 화면**

`ClusterView.vue`:

```vue
<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api } from '@/api/client'

interface Broker { id: number; host: string; port: number }
interface Cluster { clusterId: string; controllerId: number; brokers: Broker[] }

const cluster = ref<Cluster | null>(null)
const error = ref('')

onMounted(async () => {
  try {
    cluster.value = await api<Cluster>('/cluster')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
})
</script>

<template>
  <main>
    <h1>클러스터</h1>
    <p v-if="error" class="error">{{ error }}</p>
    <template v-else-if="cluster">
      <p>Cluster ID: {{ cluster.clusterId }}</p>
      <table>
        <thead><tr><th>브로커 ID</th><th>주소</th><th>역할</th></tr></thead>
        <tbody>
          <tr v-for="b in cluster.brokers" :key="b.id">
            <td>{{ b.id }}</td>
            <td>{{ b.host }}:{{ b.port }}</td>
            <td>{{ b.id === cluster.controllerId ? '컨트롤러' : '' }}</td>
          </tr>
        </tbody>
      </table>
    </template>
  </main>
</template>
```

- [ ] **Step 2: 토픽 목록/상세 화면**

`TopicsView.vue`:

```vue
<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api } from '@/api/client'

interface TopicSummary { name: string; partitionCount: number; replicationFactor: number }

const topics = ref<TopicSummary[]>([])
const error = ref('')

onMounted(async () => {
  try {
    topics.value = await api<TopicSummary[]>('/topics')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
})
</script>

<template>
  <main>
    <h1>토픽</h1>
    <p v-if="error" class="error">{{ error }}</p>
    <table v-else>
      <thead><tr><th>이름</th><th>파티션</th><th>복제 팩터</th></tr></thead>
      <tbody>
        <tr v-for="t in topics" :key="t.name">
          <td><RouterLink :to="`/topics/${t.name}`">{{ t.name }}</RouterLink></td>
          <td>{{ t.partitionCount }}</td>
          <td>{{ t.replicationFactor }}</td>
        </tr>
      </tbody>
    </table>
  </main>
</template>
```

`TopicDetailView.vue` (ISR 축소를 눈에 띄게 — `isr.length < replicas.length`이면 경고 표시. 스펙의 URP 가시화):

```vue
<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { api } from '@/api/client'

interface PartitionInfo { partition: number; leader: number; replicas: number[]; isr: number[] }
interface TopicDetail { name: string; partitions: PartitionInfo[]; configs: Record<string, string> }

const route = useRoute()
const detail = ref<TopicDetail | null>(null)
const error = ref('')

onMounted(async () => {
  try {
    detail.value = await api<TopicDetail>(`/topics/${route.params.name}`)
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
})
</script>

<template>
  <main>
    <h1>토픽: {{ route.params.name }}</h1>
    <p v-if="error" class="error">{{ error }}</p>
    <template v-else-if="detail">
      <h2>설정</h2>
      <ul>
        <li v-for="(v, k) in detail.configs" :key="k">{{ k }} = {{ v }}</li>
      </ul>
      <h2>파티션</h2>
      <table>
        <thead><tr><th>파티션</th><th>리더</th><th>복제본</th><th>ISR</th><th>상태</th></tr></thead>
        <tbody>
          <tr v-for="p in detail.partitions" :key="p.partition">
            <td>{{ p.partition }}</td>
            <td>{{ p.leader }}</td>
            <td>{{ p.replicas.join(', ') }}</td>
            <td>{{ p.isr.join(', ') }}</td>
            <td>
              <span v-if="p.isr.length < p.replicas.length" class="warn">복제 부족</span>
              <span v-else>정상</span>
            </td>
          </tr>
        </tbody>
      </table>
    </template>
  </main>
</template>

<style scoped>
.warn { color: #c00; font-weight: bold; }
</style>
```

- [ ] **Step 3: 그룹 목록/상세(랙) 화면**

`GroupsView.vue`:

```vue
<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api } from '@/api/client'

interface GroupSummary { groupId: string; state: string; memberCount: number }

const groups = ref<GroupSummary[]>([])
const error = ref('')

onMounted(async () => {
  try {
    groups.value = await api<GroupSummary[]>('/groups')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
})
</script>

<template>
  <main>
    <h1>컨슈머 그룹</h1>
    <p v-if="error" class="error">{{ error }}</p>
    <table v-else>
      <thead><tr><th>그룹</th><th>상태</th><th>멤버 수</th></tr></thead>
      <tbody>
        <tr v-for="g in groups" :key="g.groupId">
          <td><RouterLink :to="`/groups/${g.groupId}`">{{ g.groupId }}</RouterLink></td>
          <td>{{ g.state }}</td>
          <td>{{ g.memberCount }}</td>
        </tr>
      </tbody>
    </table>
  </main>
</template>
```

`GroupDetailView.vue`:

```vue
<script setup lang="ts">
import { computed, ref, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { api } from '@/api/client'
import { sumLag } from '@/lib/lag'

interface PartitionLag { topic: string; partition: number; committed: number; end: number; lag: number }
interface GroupDetail { groupId: string; state: string; lags: PartitionLag[]; totalLag: number }

const route = useRoute()
const detail = ref<GroupDetail | null>(null)
const error = ref('')
const clientTotal = computed(() => (detail.value ? sumLag(detail.value.lags) : 0))

onMounted(async () => {
  try {
    detail.value = await api<GroupDetail>(`/groups/${route.params.groupId}`)
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
})
</script>

<template>
  <main>
    <h1>그룹: {{ route.params.groupId }}</h1>
    <p v-if="error" class="error">{{ error }}</p>
    <template v-else-if="detail">
      <p>상태: {{ detail.state }} / 총 랙: <strong>{{ clientTotal }}</strong></p>
      <table>
        <thead>
          <tr><th>토픽</th><th>파티션</th><th>커밋 오프셋</th><th>최신 오프셋</th><th>랙</th></tr>
        </thead>
        <tbody>
          <tr v-for="p in detail.lags" :key="`${p.topic}-${p.partition}`">
            <td>{{ p.topic }}</td>
            <td>{{ p.partition }}</td>
            <td>{{ p.committed }}</td>
            <td>{{ p.end }}</td>
            <td :class="{ warn: p.lag > 0 }">{{ p.lag }}</td>
          </tr>
        </tbody>
      </table>
    </template>
  </main>
</template>

<style scoped>
.warn { color: #c00; font-weight: bold; }
</style>
```

- [ ] **Step 4: 검증**

Run: `npm run test:unit -- --run && npm run build`
Expected: PASS + 빌드 성공

수동 확인: was + `npm run dev` 상태에서 로그인 → 클러스터/토픽/그룹 화면이 실제 데이터로 렌더링. 브로커 목록의 컨트롤러 표시, 토픽 상세의 ISR/설정, 그룹 상세의 랙 표를 확인. (로컬 검증용 브로커가 없으면 devops-note의 `kafka/examples/compose-3node-kraft-plaintext`를 띄우고 `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`, `KAFKA_SECURITY_PROTOCOL=PLAINTEXT`로 was를 기동한다)

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: cluster, topic, and consumer group views"
```

---

### Task 10: 배포 (Dockerfile, Nginx, docker-compose)

**Files:**
- Create: `was/Dockerfile`, `web/Dockerfile`, `web/nginx.conf`, `deploy/docker-compose.yml`, `deploy/.env.example`

**Interfaces:**
- Consumes: Task 1~9 전체
- Produces: `cd deploy && docker compose up -d --build` 로 web(80) + was 기동. Nginx만 외부 노출, was는 컨테이너 네트워크 내부 전용

- [ ] **Step 1: was Dockerfile**

`was/Dockerfile`:

```dockerfile
FROM eclipse-temurin:21-jdk AS build
WORKDIR /app
COPY . .
RUN ./gradlew bootJar --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/build/libs/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 2: web Dockerfile + nginx.conf**

`web/nginx.conf`:

```nginx
server {
    listen 80;

    root /usr/share/nginx/html;
    index index.html;

    # API는 was로 프록시. 이 외 경로는 전부 SPA 폴백.
    location /api/ {
        proxy_pass http://was:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }

    location / {
        try_files $uri $uri/ /index.html;
    }
}
```

`web/Dockerfile`:

```dockerfile
FROM node:22-alpine AS build
WORKDIR /app
COPY . .
RUN npm ci && npm run build

FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
```

- [ ] **Step 3: docker-compose + .env.example**

`deploy/docker-compose.yml`:

```yaml
# 관리용 VM 1대에 web(Nginx) + was(Spring Boot) 컨테이너 2개를 올린다.
# was 포트는 컨테이너 네트워크 내부 전용 — 외부 진입점은 web(80) 하나다.
services:
  was:
    build: ../was
    container_name: kafka-admin-was
    restart: unless-stopped
    env_file: .env
    environment:
      APP_DATA_DIR: /data
    volumes:
      - was-data:/data
      # SASL_SSL 접속 시 truststore 마운트:
      # - ./secrets:/secrets:ro

  web:
    build: ../web
    container_name: kafka-admin-web
    restart: unless-stopped
    ports:
      - "80:80"
    depends_on:
      - was

volumes:
  was-data:
```

`deploy/.env.example`:

```bash
# 브로커 3대 전부 나열한다 (1대만 적어도 동작하지만, 그 1대 장애 시 부트스트랩이 안 된다)
KAFKA_BOOTSTRAP_SERVERS=10.0.0.11:9094,10.0.0.12:9094,10.0.0.13:9094
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_JAAS=org.apache.kafka.common.security.scram.ScramLoginModule required username="${KAFKA_ADMIN_USER}" password="${KAFKA_ADMIN_PASSWORD}";
KAFKA_TRUSTSTORE_LOCATION=/secrets/truststore.jks
KAFKA_TRUSTSTORE_PASSWORD=change-me

# 최초 기동 시 admin 계정 생성용 (생성 후 제거 가능)
ADMIN_INITIAL_PASSWORD=change-me
```

- [ ] **Step 4: 기동 검증**

```bash
cd ~/Documents/kafka-admin/deploy
cp .env.example .env
# 로컬 검증: PLAINTEXT 브로커(예: devops-note의 compose-3node-kraft-plaintext)를 띄우고
# .env를 KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092, KAFKA_SECURITY_PROTOCOL=PLAINTEXT 로 수정
docker compose up -d --build
```

Expected: http://localhost 접속 → 로그인 화면 → admin 로그인 → 클러스터/토픽/그룹 조회 동작. `docker compose logs was`에 에러 없음

- [ ] **Step 5: Commit**

```bash
cd ~/Documents/kafka-admin && git add -A && git commit -m "chore: dockerize web/was with nginx reverse proxy"
```

---

## 2단계 이후 (이 계획 범위 밖)

- 2단계: collector(@Scheduled 지표 수집) + H2 이력 + 임계치 알림 + 인증서 만료 감시
- 3단계: ops 모듈(확인 절차 + 감사 로그) + 조치 API (오프셋 리셋, 설정 변경, 파티션 증가, 리더 선출, 재할당)
- 4단계: SSH 브로커 재시작, 토픽 신청 워크플로우, 메시지 열람

각 단계는 해당 시점에 별도 계획 문서로 작성한다.
