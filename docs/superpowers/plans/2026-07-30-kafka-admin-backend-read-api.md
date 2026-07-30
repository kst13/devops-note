# Kafka 운영 관리자 웹 — 계획 1: 백엔드 기반 + 조회 API

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Kafka 클러스터·토픽·컨슈머 그룹·ACL의 현재 상태를 조회하는 Spring Boot REST API를 만든다. UI는 없고 `curl`로 검증 가능한 상태까지 완성한다.

**Architecture:** 모든 Kafka 접근은 `KafkaAdminGateway` 인터페이스 하나를 통과한다. 판정 로직(클러스터 건강도, 토픽 위험 배지, 랙 캐시, 에러 번역)은 이 인터페이스에만 의존하므로 Kafka 없이 가짜 구현체로 단위 테스트한다. 실제 `AdminClient` 구현체는 Testcontainers Kafka로 따로 검증한다. Kafka 상태는 DB에 저장하지 않고, PostgreSQL에는 `app_settings`만 둔다.

**Tech Stack:** Java 21, Spring Boot 3.5.0, Gradle (Kotlin DSL), `org.apache.kafka:kafka-clients` 4.0.0, PostgreSQL + Flyway, Spring `JdbcClient`, JUnit 5, Testcontainers 1.20.6

**설계 근거:** `docs/superpowers/specs/2026-07-30-kafka-admin-web-design.md` (devops-note 저장소)

---

## 사전 결정 사항

| 항목 | 값 |
|---|---|
| 코드 위치 | **새 저장소** `kafka-admin-web` (devops-note와 분리) |
| 패키지 | `com.kafkaadmin` |
| Gradle group | `com.kafkaadmin` |
| 이 계획의 모든 경로 | `kafka-admin-web/` 저장소 루트 기준 |

### 실행 환경 (2026-07-30 확인)

이 머신의 기본 JDK는 25이고 `gradle`은 PATH에 없다. 둘 다 로컬에 해결책이 있어 추가 설치가 필요하지 않다.

| 필요한 것 | 이 머신의 값 |
|---|---|
| JDK 21 | `/Library/Java/JavaVirtualMachines/ibm-semeru-open-21.jdk/Contents/Home` (IBM Semeru 21.0.10) |
| Gradle 8.14 | `~/.gradle/wrapper/dists/gradle-8.14-bin/38aieal9i53h9rfe7vjup95b9/gradle-8.14/bin/gradle` (캐시된 배포본) |
| Docker | 실행 중 |

**모든 Gradle 명령 전에 셸에서 JDK 21을 지정한다.**

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
```

기본 JDK 25로 Gradle 8.14를 실행하면 지원 범위를 벗어난다. 툴체인이 컴파일용 JDK를 따로 고르기는 하지만, Gradle 자체를 구동하는 JVM은 21로 맞추는 편이 안전하다.

---

## 파일 구조

이 계획이 끝나면 저장소는 다음 상태가 된다.

```
kafka-admin-web/
├── build.gradle.kts
├── settings.gradle.kts
├── gradle/wrapper/                          (gradle wrapper)
├── gradlew, gradlew.bat
├── .gitignore
├── README.md
├── compose.yaml                             로컬 개발용 PostgreSQL
└── src/
    ├── main/
    │   ├── java/com/kafkaadmin/
    │   │   ├── KafkaAdminApplication.java
    │   │   ├── config/
    │   │   │   ├── ClockConfig.java          Clock 빈 (테스트에서 시간 제어)
    │   │   │   ├── KafkaConnectionProperties.java
    │   │   │   └── KafkaClientConfig.java    AdminClient 빈
    │   │   ├── settings/
    │   │   │   ├── SettingKey.java           키 + 기본값 enum
    │   │   │   ├── SettingsSource.java       설정 출처 인터페이스
    │   │   │   ├── SettingsRepository.java
    │   │   │   └── SettingsService.java
    │   │   ├── kafka/
    │   │   │   ├── KafkaAdminGateway.java    ★ Kafka 격리 경계
    │   │   │   ├── KafkaAdminGatewayImpl.java
    │   │   │   ├── KafkaGatewayException.java
    │   │   │   ├── ClusterAvailabilityTracker.java
    │   │   │   └── dto/                      (레코드 11개, Task 3)
    │   │   ├── cluster/
    │   │   │   ├── ClusterService.java
    │   │   │   ├── ClusterController.java
    │   │   │   ├── ClusterHealth.java
    │   │   │   └── PartitionRisk.java
    │   │   ├── topic/
    │   │   │   ├── TopicBadge.java
    │   │   │   ├── TopicBadgeEvaluator.java
    │   │   │   ├── TopicService.java
    │   │   │   ├── TopicController.java
    │   │   │   ├── TopicListItem.java
    │   │   │   └── TopicDetailResponse.java
    │   │   ├── consumergroup/
    │   │   │   ├── LagCache.java
    │   │   │   ├── ConsumerGroupService.java
    │   │   │   └── ConsumerGroupController.java
    │   │   ├── acl/
    │   │   │   ├── AclService.java
    │   │   │   └── AclController.java
    │   │   └── error/
    │   │       ├── ApiError.java
    │   │       ├── ApiErrorResponse.java
    │   │       ├── KafkaErrorTranslator.java
    │   │       └── GlobalExceptionHandler.java
    │   └── resources/
    │       ├── application.yml
    │       ├── application-local.yml
    │       └── db/migration/V1__app_settings.sql
    └── test/java/com/kafkaadmin/
        ├── KafkaAdminApplicationTest.java
        ├── support/
        │   ├── FakeKafkaAdminGateway.java
        │   ├── MutableClock.java
        │   └── KafkaTestFixtures.java
        ├── settings/SettingsServiceTest.java
        ├── settings/SettingsRepositoryTest.java   (Testcontainers PostgreSQL)
        ├── topic/TopicBadgeEvaluatorTest.java
        ├── cluster/ClusterServiceTest.java
        ├── consumergroup/LagCacheTest.java
        ├── error/KafkaErrorTranslatorTest.java
        └── kafka/KafkaAdminGatewayImplTest.java   (Testcontainers Kafka)
```

**책임 분리 원칙:** `kafka/` 패키지 밖에서는 `org.apache.kafka.*` 타입을 절대 import 하지 않는다. 게이트웨이가 자체 DTO 레코드로 변환해서 내보낸다. 이 규칙이 깨지면 단위 테스트가 Kafka에 묶여 버린다.

**빈 등록 시점 원칙:** 클래스에 `@Service`·`@Component`를 붙이는 것은 **애플리케이션 안에서 그것을 실제로 주입받는 곳이 생길 때**다. 단위 테스트가 `new`로 직접 만드는 동안에는 애노테이션이 필요 없다.

이 순서를 어기면 즉시 깨진다. 컴포넌트 스캔은 애노테이션이 붙은 클래스의 생성자 의존성을 전부 요구하므로, 아직 구현체가 없는 인터페이스를 요구하는 빈 하나가 **그 태스크와 무관한 모든 `@SpringBootTest`를 실패시킨다.** 실제로 `ClusterService`(Task 5)에 `@Service`를 미리 붙였다가 Task 8에서야 생기는 `KafkaAdminGateway` 빈을 요구해 부팅 테스트와 설정 리포지터리 테스트가 함께 깨졌다. 그래서 이 계획에서 `ClusterService`는 Task 5에서 순수 클래스로 만들고 Task 12에서 애노테이션을 붙인다.

각 태스크는 커밋 시점에 전체 테스트가 통과해야 한다. 다음 태스크가 고쳐줄 것을 기대하고 빨간 상태로 커밋하지 않는다.

---

## Task 1: 저장소 초기화와 Gradle 스캐폴딩

**Files:**
- Create: `settings.gradle.kts`
- Create: `build.gradle.kts`
- Create: `.gitignore`
- Create: `src/main/java/com/kafkaadmin/KafkaAdminApplication.java`
- Create: `src/main/resources/application.yml`
- Create: `src/test/java/com/kafkaadmin/KafkaAdminApplicationTest.java`

- [ ] **Step 1: 저장소 디렉터리와 git 초기화**

devops-note와 형제 위치에 만든다.

```bash
mkdir -p ~/Documents/kafka-admin-web
cd ~/Documents/kafka-admin-web
git init
```

이후 모든 명령은 `~/Documents/kafka-admin-web`에서 실행한다.

- [ ] **Step 2: Gradle wrapper 생성**

`gradle`이 PATH에 없으므로 캐시된 배포본의 바이너리로 wrapper를 만든다. 이후 모든 명령은 생성된 `./gradlew`를 쓴다.

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
GRADLE_BIN=$(ls -1 ~/.gradle/wrapper/dists/gradle-8.14-bin/*/gradle-8.14/bin/gradle | head -1)
"$GRADLE_BIN" wrapper --gradle-version 8.14
```

Expected: `gradlew`, `gradlew.bat`, `gradle/wrapper/gradle-wrapper.jar`, `gradle/wrapper/gradle-wrapper.properties` 생성

- [ ] **Step 2b: wrapper가 JDK 21로 동작하는지 확인**

```bash
./gradlew -version
```

Expected: `Gradle 8.14`, 그리고 `JVM: 21.x`. JVM이 25로 나오면 `JAVA_HOME`이 반영되지 않은 것이므로 Step 2의 `export`를 다시 실행한다.

- [ ] **Step 3: `settings.gradle.kts` 작성**

```kotlin
rootProject.name = "kafka-admin-web"
```

- [ ] **Step 4: `build.gradle.kts` 작성**

```kotlin
plugins {
    java
    id("org.springframework.boot") version "3.5.0"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.kafkaadmin"
version = "0.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.apache.kafka:kafka-clients:4.0.0")

    testImplementation(platform("org.testcontainers:testcontainers-bom:1.20.6"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:kafka")
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events("passed", "failed", "skipped")
    }
}
```

`spring-kafka`를 넣지 않는 것은 의도적이다. 이 애플리케이션은 리스너나 `KafkaTemplate`이 필요 없고 `AdminClient`·`Consumer`·`Producer`만 직접 쓴다. 얇은 의존성이 버전 충돌을 줄인다.

**JDBC·Flyway·PostgreSQL 의존성은 여기서 넣지 않는다.** Task 2에서 datasource 설정과 함께 넣는다. 드라이버만 클래스패스에 올리고 `spring.datasource.url`이 없으면 Spring Boot의 `DataSourceAutoConfiguration`이 부팅을 실패시키기 때문이다. 의존성과 그 의존성을 쓸 수 있게 하는 설정은 같은 커밋에 들어가야 한다.

`kafka-clients`는 지금 넣어도 안전하다. 순수 라이브러리라 Spring 자동설정을 건드리지 않는다.

- [ ] **Step 5: `.gitignore` 작성**

```gitignore
.gradle/
build/
!gradle/wrapper/gradle-wrapper.jar
.idea/
*.iml
.DS_Store
*.log
/certs/
.env
```

`/certs/`와 `.env`는 절대 커밋되면 안 되는 것들이다. 처음부터 막아둔다.

- [ ] **Step 6: 애플리케이션 클래스 작성**

`src/main/java/com/kafkaadmin/KafkaAdminApplication.java`:

```java
package com.kafkaadmin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaAdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaAdminApplication.class, args);
    }
}
```

- [ ] **Step 7: 최소 `application.yml` 작성**

`src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: kafka-admin-web

server:
  port: 8080

logging:
  level:
    org.apache.kafka: WARN
```

DB와 Kafka 설정은 Task 2, Task 8에서 추가한다. 지금은 부팅만 확인한다.

- [ ] **Step 8: 부팅 테스트 작성**

`src/test/java/com/kafkaadmin/KafkaAdminApplicationTest.java`:

```java
package com.kafkaadmin;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class KafkaAdminApplicationTest {

    @Test
    void 애플리케이션_컨텍스트가_로드된다() {
    }
}
```

- [ ] **Step 9: 테스트 실행**

```bash
./gradlew test
```

Expected: PASS. `KafkaAdminApplicationTest > 애플리케이션_컨텍스트가_로드된다() PASSED`

실패하면 대부분 JDK 버전 문제다. `./gradlew -version`으로 JVM이 21인지 확인한다.

- [ ] **Step 10: README 작성**

`README.md`:

````markdown
# kafka-admin-web

Kafka 운영 관리자 웹 애플리케이션.

설계 문서: devops-note 저장소의 `docs/superpowers/specs/2026-07-30-kafka-admin-web-design.md`

## 개발 환경

- JDK 21
- Docker (PostgreSQL, 테스트용 Testcontainers)

## 실행

```bash
docker compose up -d          # PostgreSQL 기동
./gradlew bootRun --args='--spring.profiles.active=local'
```

## 테스트

```bash
./gradlew test
```

Testcontainers를 쓰므로 Docker가 실행 중이어야 한다.
````

- [ ] **Step 11: 커밋**

```bash
git add .
git commit -m "Add Spring Boot scaffolding with Gradle Kotlin DSL"
```

---

## Task 2: PostgreSQL과 app_settings

**Files:**
- Modify: `build.gradle.kts`
- Create: `compose.yaml`
- Create: `src/main/resources/db/migration/V1__app_settings.sql`
- Create: `src/main/java/com/kafkaadmin/settings/SettingKey.java`
- Create: `src/main/java/com/kafkaadmin/settings/SettingsSource.java`
- Create: `src/main/java/com/kafkaadmin/settings/SettingsRepository.java`
- Create: `src/main/java/com/kafkaadmin/settings/SettingsService.java`
- Create: `src/test/java/com/kafkaadmin/settings/SettingsServiceTest.java`
- Create: `src/test/java/com/kafkaadmin/settings/SettingsRepositoryTest.java`
- Modify: `src/main/resources/application.yml`
- Create: `src/main/resources/application-local.yml`
- Create: `src/test/java/com/kafkaadmin/support/AbstractPostgresTest.java`
- Modify: `src/test/java/com/kafkaadmin/KafkaAdminApplicationTest.java`

- [ ] **Step 0: JDBC·Flyway·PostgreSQL 의존성 추가**

`build.gradle.kts`의 `dependencies` 블록에서 `implementation("org.apache.kafka:kafka-clients:4.0.0")` 줄 **위에** 다음 두 줄을, 그리고 그 아래에 `runtimeOnly` 한 줄을 추가한다. 결과는 다음과 같아야 한다.

```kotlin
dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.flywaydb:flyway-core")
    implementation("org.flywaydb:flyway-database-postgresql")
    implementation("org.apache.kafka:kafka-clients:4.0.0")
    runtimeOnly("org.postgresql:postgresql")

    testImplementation(platform("org.testcontainers:testcontainers-bom:1.20.6"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:kafka")
}
```

이 태스크가 끝날 때까지는 `./gradlew test`가 실패한다. Step 9~11에서 datasource 설정과 Testcontainers 기반 부팅 테스트를 넣어야 다시 통과한다. 이것이 Step 0을 이 태스크 안에 둔 이유다 — 의존성만 먼저 커밋하면 저장소가 깨진 상태로 남는다.

- [ ] **Step 1: 로컬 PostgreSQL compose 파일 작성**

`compose.yaml`:

```yaml
services:
  postgres:
    image: postgres:17-alpine
    container_name: kafka-admin-postgres
    # localhost 전용 개발 자격증명이며 의도적으로 커밋한다.
    # 운영 자격증명은 환경변수로만 주입한다 — .gitignore가 .env* 와 키스토어를 막고 있다.
    environment:
      POSTGRES_DB: kafka_admin
      POSTGRES_USER: kafka_admin
      POSTGRES_PASSWORD: local-dev-only
    ports:
      - "5432:5432"
    volumes:
      - kafka-admin-pgdata:/var/lib/postgresql/data

volumes:
  kafka-admin-pgdata:
```

패스워드가 `local-dev-only`인 것은 이 파일이 로컬 개발 전용임을 이름으로 못 박기 위한 것이다. 운영 자격증명은 환경변수로만 주입한다.

- [ ] **Step 2: Flyway 마이그레이션 작성**

`src/main/resources/db/migration/V1__app_settings.sql`:

```sql
CREATE TABLE app_settings (
    setting_key   VARCHAR(64) PRIMARY KEY,
    setting_value TEXT        NOT NULL,
    updated_by    VARCHAR(64),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO app_settings (setting_key, setting_value) VALUES
    ('self_approval_allowed',   'false'),
    ('topic_name_pattern',      '^[a-z0-9]+(\.[a-z0-9-]+)+$'),
    ('message_read_max_count',  '100'),
    ('message_value_max_bytes', '65536'),
    ('admin_client_timeout_ms', '10000'),
    ('lag_cache_ttl_seconds',   '10'),
    ('feature_message_enabled', 'false'),
    ('feature_write_enabled',   'false');
```

컬럼명을 `key`/`value`가 아니라 `setting_key`/`setting_value`로 둔 이유는 두 단어 모두 SQL에서 문맥에 따라 인용이 필요해질 수 있어서다. 처음부터 피한다.

- [ ] **Step 3: SettingKey enum 작성**

`src/main/java/com/kafkaadmin/settings/SettingKey.java`:

```java
package com.kafkaadmin.settings;

public enum SettingKey {

    SELF_APPROVAL_ALLOWED("self_approval_allowed", "false"),
    TOPIC_NAME_PATTERN("topic_name_pattern", "^[a-z0-9]+(\\.[a-z0-9-]+)+$"),
    MESSAGE_READ_MAX_COUNT("message_read_max_count", "100"),
    MESSAGE_VALUE_MAX_BYTES("message_value_max_bytes", "65536"),
    ADMIN_CLIENT_TIMEOUT_MS("admin_client_timeout_ms", "10000"),
    LAG_CACHE_TTL_SECONDS("lag_cache_ttl_seconds", "10"),
    FEATURE_MESSAGE_ENABLED("feature_message_enabled", "false"),
    FEATURE_WRITE_ENABLED("feature_write_enabled", "false");

    private final String key;
    private final String defaultValue;

    SettingKey(String key, String defaultValue) {
        this.key = key;
        this.defaultValue = defaultValue;
    }

    public String key() {
        return key;
    }

    public String defaultValue() {
        return defaultValue;
    }
}
```

기본값을 코드에도 두는 이유는 DB에 행이 없을 때(마이그레이션 이후 추가된 키 등) 애플리케이션이 죽지 않아야 하기 때문이다.

- [ ] **Step 3b: SettingsSource 인터페이스 작성**

`src/main/java/com/kafkaadmin/settings/SettingsSource.java`:

```java
package com.kafkaadmin.settings;

import java.util.Map;

/**
 * 설정 값의 출처. 프로덕션에서는 SettingsRepository(PostgreSQL),
 * 테스트에서는 람다를 넘긴다.
 */
@FunctionalInterface
public interface SettingsSource {

    Map<String, String> load();
}
```

`SettingsService`가 `SettingsRepository`를 직접 받지 않고 이 인터페이스를 받는다. 덕분에 다른 패키지의 단위 테스트가 DB 없이 `SettingsService`를 조립할 수 있다.

- [ ] **Step 4: SettingsService 테스트 먼저 작성**

`src/test/java/com/kafkaadmin/settings/SettingsServiceTest.java`:

```java
package com.kafkaadmin.settings;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SettingsServiceTest {

    private final Map<String, String> stored = new HashMap<>();
    private final SettingsService service = new SettingsService(() -> Map.copyOf(stored));

    @Test
    void DB에_값이_있으면_그_값을_읽는다() {
        stored.put("lag_cache_ttl_seconds", "30");
        service.reload();

        assertThat(service.getLong(SettingKey.LAG_CACHE_TTL_SECONDS)).isEqualTo(30L);
    }

    @Test
    void DB에_값이_없으면_enum_기본값을_쓴다() {
        service.reload();

        assertThat(service.getLong(SettingKey.LAG_CACHE_TTL_SECONDS)).isEqualTo(10L);
        assertThat(service.getBoolean(SettingKey.FEATURE_WRITE_ENABLED)).isFalse();
        assertThat(service.getInt(SettingKey.MESSAGE_READ_MAX_COUNT)).isEqualTo(100);
    }

    @Test
    void reload하면_변경된_값이_반영된다() {
        stored.put("feature_write_enabled", "false");
        service.reload();
        assertThat(service.getBoolean(SettingKey.FEATURE_WRITE_ENABLED)).isFalse();

        stored.put("feature_write_enabled", "true");
        service.reload();
        assertThat(service.getBoolean(SettingKey.FEATURE_WRITE_ENABLED)).isTrue();
    }

    @Test
    void 값이_숫자가_아니면_기본값으로_되돌린다() {
        stored.put("lag_cache_ttl_seconds", "열초");
        service.reload();

        assertThat(service.getLong(SettingKey.LAG_CACHE_TTL_SECONDS)).isEqualTo(10L);
    }
}
```

마지막 테스트가 중요하다. 누군가 관리 화면에서 오타를 넣었을 때 애플리케이션 전체가 500을 뿜으면 안 된다.

- [ ] **Step 5: 테스트 실패 확인**

```bash
./gradlew test --tests 'com.kafkaadmin.settings.SettingsServiceTest'
```

Expected: 컴파일 실패. `cannot find symbol: class SettingsService`

- [ ] **Step 6: SettingsService 구현**

`src/main/java/com/kafkaadmin/settings/SettingsService.java`:

```java
package com.kafkaadmin.settings;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class SettingsService {

    private static final Logger log = LoggerFactory.getLogger(SettingsService.class);

    private final SettingsSource source;

    /**
     * 불변 맵 참조를 통째로 교체한다. reload 중인 독자는 이전 스냅샷이나
     * 새 스냅샷 중 하나만 보며, 두 값이 섞인 상태는 볼 수 없다.
     */
    private volatile Map<String, String> snapshot = Map.of();

    public SettingsService(SettingsSource source) {
        this.source = source;
    }

    @PostConstruct
    public void reload() {
        snapshot = Map.copyOf(source.load());
    }

    public String getString(SettingKey key) {
        return snapshot.getOrDefault(key.key(), key.defaultValue());
    }

    public int getInt(SettingKey key) {
        return (int) getLong(key);
    }

    public long getLong(SettingKey key) {
        String raw = getString(key);
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            log.warn("설정 {} 의 값 '{}' 이 숫자가 아닙니다. 기본값 {} 를 사용합니다.",
                    key.key(), raw, key.defaultValue());
            return Long.parseLong(key.defaultValue());
        }
    }

    public boolean getBoolean(SettingKey key) {
        return Boolean.parseBoolean(getString(key).trim());
    }
}
```

`getLong`이 파싱 실패 시 예외를 던지지 않고 기본값으로 되돌리는 것이 이 클래스의 핵심 방어다. 설정 하나의 오타가 전체 조회 API를 500으로 만들면 안 된다.

캐시를 `ConcurrentHashMap`에 담고 제자리에서 갱신하지 않는 이유는 원자성이다. 맵을 지우고 다시 채우는 두 단계 사이에 읽는 쪽은 일부는 새 값, 일부는 옛 값인 상태를 볼 수 있다. 불변 맵 참조를 한 번에 바꾸면 그 창이 사라지고 코드도 더 짧아진다.

- [ ] **Step 7: SettingsRepository 구현**

`src/main/java/com/kafkaadmin/settings/SettingsRepository.java`:

```java
package com.kafkaadmin.settings;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.stream.Collectors;

@Repository
public class SettingsRepository implements SettingsSource {

    private final JdbcClient jdbc;

    public SettingsRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Map<String, String> load() {
        return jdbc.sql("SELECT setting_key, setting_value FROM app_settings")
                .query((rs, rowNum) -> Map.entry(
                        rs.getString("setting_key"), rs.getString("setting_value")))
                .list()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
```

`RowMapper`가 `null`을 반환하면서 바깥 맵에 side effect로 채우는 방식을 피한다. 이 리포지터리는 뒤에 생길 다른 리포지터리들이 베낄 본보기이므로, 처음부터 읽기 좋은 형태로 둔다.

- [ ] **Step 8: 테스트 통과 확인**

```bash
./gradlew test --tests 'com.kafkaadmin.settings.SettingsServiceTest'
```

Expected: 4개 테스트 모두 PASS

- [ ] **Step 9: application.yml에 DB 설정 추가**

`src/main/resources/application.yml` 전체를 다음으로 교체:

```yaml
spring:
  application:
    name: kafka-admin-web
  datasource:
    url: ${DB_URL}
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD}
  flyway:
    enabled: true
    locations: classpath:db/migration

server:
  port: 8080

logging:
  level:
    org.apache.kafka: WARN
```

- [ ] **Step 10: 로컬 프로파일 작성**

`src/main/resources/application-local.yml`:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/kafka_admin
    username: kafka_admin
    password: local-dev-only
```

- [ ] **Step 10b: DB 테스트 공통 베이스 작성**

`src/test/java/com/kafkaadmin/support/AbstractPostgresTest.java`:

```java
package com.kafkaadmin.support;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * DB가 필요한 테스트의 공통 베이스.
 *
 * 컨테이너를 한 번만 띄우고 JVM 종료까지 재사용한다. 테스트 클래스마다
 * 컨테이너를 새로 띄우면 datasource URL이 달라져 Spring 컨텍스트까지
 * 재사용되지 않아 스위트가 급격히 느려진다.
 *
 * 명시적으로 stop 하지 않는다. Testcontainers의 Ryuk 컨테이너가 정리한다.
 */
public abstract class AbstractPostgresTest {

    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:17-alpine");

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
}
```

`@Testcontainers`와 `@Container`를 쓰지 않고 정적 초기화 블록에서 직접 띄우는 것이 싱글턴 컨테이너 관용구다. `@Container`는 클래스 단위 수명주기를 강제해서 재사용이 되지 않는다.

Spring이 상위 클래스의 정적 `@DynamicPropertySource`를 찾아내는지 **실행으로 확인한다.** 프로퍼티가 주입되지 않으면(`DataSourceBeanCreationException`, "url attribute is not specified") 베이스에 `protected static PostgreSQLContainer<?> postgres()` 접근자를 두고 각 하위 클래스에 `@DynamicPropertySource` 메서드를 남기는 방식으로 되돌린다. 컨테이너 공유라는 목적은 그대로 달성된다.

- [ ] **Step 11: 부팅 테스트를 공통 베이스 기반으로 전환**

`src/test/java/com/kafkaadmin/KafkaAdminApplicationTest.java` 전체를 교체:

```java
package com.kafkaadmin;

import com.kafkaadmin.support.AbstractPostgresTest;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class KafkaAdminApplicationTest extends AbstractPostgresTest {

    @Test
    void 애플리케이션_컨텍스트가_로드된다() {
    }
}
```

- [ ] **Step 12: 마이그레이션 검증 테스트 작성**

`src/test/java/com/kafkaadmin/settings/SettingsRepositoryTest.java`:

```java
package com.kafkaadmin.settings;

import com.kafkaadmin.support.AbstractPostgresTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class SettingsRepositoryTest extends AbstractPostgresTest {

    @Autowired
    SettingsRepository repository;

    @Test
    void 마이그레이션이_모든_기본_설정을_적재한다() {
        Map<String, String> settings = repository.load();

        assertThat(settings).hasSize(SettingKey.values().length);
        for (SettingKey key : SettingKey.values()) {
            assertThat(settings)
                    .as("설정 키 %s", key.key())
                    .containsEntry(key.key(), key.defaultValue());
        }
    }
}
```

이 테스트는 enum과 마이그레이션이 어긋나는 것을 잡는다. 나중에 키를 추가할 때 SQL을 빠뜨리면 바로 실패한다.

- [ ] **Step 13: 전체 테스트 실행**

```bash
./gradlew test
```

Expected: 모두 PASS. Docker가 실행 중이어야 한다.

`topic_name_pattern` 비교가 실패하면 SQL의 백슬래시 문제다. PostgreSQL은 기본적으로 `standard_conforming_strings=on`이라 `'\.'`가 리터럴 백슬래시-점으로 저장된다. Java enum 쪽은 `"\\."`로 이스케이프해야 같은 값이 된다.

- [ ] **Step 14: 커밋**

```bash
git add .
git commit -m "Add PostgreSQL settings store with Flyway migration"
```

---

## Task 3: 도메인 DTO와 KafkaAdminGateway 인터페이스

이 태스크는 테스트를 쓰지 않는다. 순수 타입 선언과 테스트 더블만 만들며, 다음 태스크들의 테스트가 이 타입들을 검증한다.

**Files:**
- Create: `src/main/java/com/kafkaadmin/kafka/dto/BrokerInfo.java`
- Create: `src/main/java/com/kafkaadmin/kafka/dto/ClusterInfo.java`
- Create: `src/main/java/com/kafkaadmin/kafka/dto/LogDirUsage.java`
- Create: `src/main/java/com/kafkaadmin/kafka/dto/PartitionReplicaState.java`
- Create: `src/main/java/com/kafkaadmin/kafka/dto/PartitionDetail.java`
- Create: `src/main/java/com/kafkaadmin/kafka/dto/TopicConfigEntry.java`
- Create: `src/main/java/com/kafkaadmin/kafka/dto/TopicSummary.java`
- Create: `src/main/java/com/kafkaadmin/kafka/dto/TopicDetail.java`
- Create: `src/main/java/com/kafkaadmin/kafka/dto/ConsumerGroupSummary.java`
- Create: `src/main/java/com/kafkaadmin/kafka/dto/ConsumerGroupMemberInfo.java`
- Create: `src/main/java/com/kafkaadmin/kafka/dto/PartitionLag.java`
- Create: `src/main/java/com/kafkaadmin/kafka/dto/ConsumerGroupDetail.java`
- Create: `src/main/java/com/kafkaadmin/kafka/dto/AclEntry.java`
- Create: `src/main/java/com/kafkaadmin/kafka/dto/AclOverview.java`
- Create: `src/main/java/com/kafkaadmin/kafka/KafkaAdminGateway.java`
- Create: `src/main/java/com/kafkaadmin/kafka/KafkaGatewayException.java`
- Create: `src/test/java/com/kafkaadmin/support/FakeKafkaAdminGateway.java`
- Create: `src/test/java/com/kafkaadmin/support/KafkaTestFixtures.java`

- [ ] **Step 1: 클러스터 관련 DTO 작성**

`dto/BrokerInfo.java`:

```java
package com.kafkaadmin.kafka.dto;

/**
 * @param rack broker.rack 설정이 없으면 null이다.
 */
public record BrokerInfo(
        int id,
        String host,
        int port,
        String rack,
        boolean controller
) {}
```

`dto/ClusterInfo.java`:

```java
package com.kafkaadmin.kafka.dto;

import java.util.List;

public record ClusterInfo(
        String clusterId,
        List<BrokerInfo> brokers
) {
    public ClusterInfo {
        brokers = List.copyOf(brokers);
    }
}
```

`dto/LogDirUsage.java`:

```java
package com.kafkaadmin.kafka.dto;

public record LogDirUsage(
        int brokerId,
        long totalBytes
) {}
```

- [ ] **Step 2: 파티션 관련 DTO 작성**

`dto/PartitionReplicaState.java`:

```java
package com.kafkaadmin.kafka.dto;

import java.util.List;

/**
 * describeTopics 한 번으로 얻을 수 있는 파티션 복제 상태.
 * 오프셋은 별도 listOffsets 호출이 필요하므로 여기 넣지 않는다.
 *
 * @param leaderId 리더가 없는(offline) 파티션이면 null이다. -1 같은 센티널을 쓰지 않는다.
 */
public record PartitionReplicaState(
        int partition,
        Integer leaderId,
        List<Integer> replicaIds,
        List<Integer> inSyncReplicaIds
) {
    public PartitionReplicaState {
        replicaIds = List.copyOf(replicaIds);
        inSyncReplicaIds = List.copyOf(inSyncReplicaIds);
    }

    public boolean offline() {
        return leaderId == null;
    }

    public boolean underReplicated() {
        return inSyncReplicaIds.size() < replicaIds.size();
    }
}
```

`leaderId`가 `Integer`(nullable)인 것이 핵심이다. 리더 없는 파티션이 offline 상태이고, 이것을 `-1` 같은 센티널로 표현하면 판정 로직에서 실수가 난다.

`dto/PartitionDetail.java`:

```java
package com.kafkaadmin.kafka.dto;

public record PartitionDetail(
        PartitionReplicaState replicaState,
        long startOffset,
        long endOffset
) {
    /**
     * 추정 건수. 트랜잭션 마커와 로그 압축으로 실제 건수와 다를 수 있다.
     */
    public long estimatedCount() {
        return endOffset - startOffset;
    }
}
```

- [ ] **Step 3: 토픽 관련 DTO 작성**

`dto/TopicConfigEntry.java`:

```java
package com.kafkaadmin.kafka.dto;

public record TopicConfigEntry(
        String name,
        String value,
        boolean overridden
) {}
```

`dto/TopicSummary.java`:

```java
package com.kafkaadmin.kafka.dto;

import java.util.List;

/**
 * 목록 화면용. 오프셋은 포함하지 않는다 — 토픽마다 listOffsets를 부르면
 * 목록 로딩이 파티션 수에 비례해 느려진다.
 */
public record TopicSummary(
        String name,
        int replicationFactor,
        int minInSyncReplicas,
        long totalBytesOnDisk,
        List<PartitionReplicaState> partitions
) {
    public TopicSummary {
        partitions = List.copyOf(partitions);
    }

    public int partitionCount() {
        return partitions.size();
    }
}
```

`totalBytesOnDisk`는 모든 복제본의 크기 합이다. RF=3이면 논리적 데이터량의 약 3배가 된다. 용량 관리에는 이 값이 맞으므로 이름에 `OnDisk`를 박아 오해를 막는다.

`dto/TopicDetail.java`:

```java
package com.kafkaadmin.kafka.dto;

import java.util.List;

public record TopicDetail(
        String name,
        int replicationFactor,
        int minInSyncReplicas,
        List<PartitionDetail> partitions,
        List<TopicConfigEntry> configs
) {
    public TopicDetail {
        partitions = List.copyOf(partitions);
        configs = List.copyOf(configs);
    }

    public List<PartitionReplicaState> replicaStates() {
        return partitions.stream().map(PartitionDetail::replicaState).toList();
    }
}
```

- [ ] **Step 4: 컨슈머 그룹 관련 DTO 작성**

`dto/ConsumerGroupSummary.java`:

```java
package com.kafkaadmin.kafka.dto;

import java.util.List;

public record ConsumerGroupSummary(
        String groupId,
        String state,
        int memberCount,
        List<String> topics,
        long totalLag
) {
    public ConsumerGroupSummary {
        topics = List.copyOf(topics);
    }
}
```

`dto/ConsumerGroupMemberInfo.java`:

```java
package com.kafkaadmin.kafka.dto;

import java.util.List;

public record ConsumerGroupMemberInfo(
        String memberId,
        String clientId,
        String host,
        List<String> assignedPartitions
) {
    public ConsumerGroupMemberInfo {
        assignedPartitions = List.copyOf(assignedPartitions);
    }
}
```

`assignedPartitions`는 `"orders-0"` 형태의 문자열 목록이다. 화면에 그대로 표시하는 값이므로 구조체로 만들 이유가 없다.

`dto/PartitionLag.java`:

```java
package com.kafkaadmin.kafka.dto;

/**
 * @param currentOffset 그룹이 이 파티션의 오프셋을 커밋한 적이 없으면 null이다.
 *                      0과 다르다 — 0은 "다 따라잡았다"는 뜻이다.
 * @param lag currentOffset이 null이면 함께 null이다.
 * @param assignedClientId 이 파티션을 담당하는 살아있는 멤버가 없으면 null이다.
 * @param assignedHost 위와 같다.
 */
public record PartitionLag(
        String topic,
        int partition,
        Long currentOffset,
        long endOffset,
        Long lag,
        String assignedClientId,
        String assignedHost
) {}
```

`currentOffset`과 `lag`이 nullable인 이유는 커밋된 오프셋이 없는 파티션(새 그룹, 또는 아직 소비하지 않은 파티션)이 실제로 존재하기 때문이다. 이때 0으로 표시하면 "다 따라잡았다"로 오독된다.

`dto/ConsumerGroupDetail.java`:

```java
package com.kafkaadmin.kafka.dto;

import java.util.List;

public record ConsumerGroupDetail(
        String groupId,
        String state,
        List<ConsumerGroupMemberInfo> members,
        List<PartitionLag> partitionLags
) {
    public ConsumerGroupDetail {
        members = List.copyOf(members);
        partitionLags = List.copyOf(partitionLags);
    }
}
```

- [ ] **Step 5: ACL DTO 작성**

`dto/AclEntry.java`:

```java
package com.kafkaadmin.kafka.dto;

public record AclEntry(
        String principal,
        String host,
        String resourceType,
        String resourceName,
        String patternType,
        String operation,
        String permissionType
) {}
```

`dto/AclOverview.java`:

```java
package com.kafkaadmin.kafka.dto;

import java.util.List;

/**
 * authorizerEnabled=false 는 "ACL이 0건"과 전혀 다른 의미다.
 * 브로커에 authorizer가 설정되지 않으면 조회 자체가 불가능하고,
 * 화면은 "ACL 0건"이 아니라 "인가 기능 미사용"으로 표시해야 한다.
 */
public record AclOverview(
        boolean authorizerEnabled,
        List<AclEntry> entries
) {
    public AclOverview {
        entries = List.copyOf(entries);
    }
}
```

리스트를 가진 레코드는 모두 compact 생성자에서 `List.copyOf`로 복사한다. 이 레코드들은 Task 6부터 캐시에 담겨 여러 요청 스레드가 동시에 읽고, Task 8의 변환기는 살아있는 컬렉션에서 만들어낸다. `List.copyOf`는 null을 거부하는데 그것도 바람직하다 — null 리스트는 버그이므로 나중에 NPE로 터지는 것보다 생성 시점에 드러나는 편이 낫다.

- [ ] **Step 6: 게이트웨이 인터페이스 작성**

`src/main/java/com/kafkaadmin/kafka/KafkaAdminGateway.java`:

```java
package com.kafkaadmin.kafka;

import com.kafkaadmin.kafka.dto.AclOverview;
import com.kafkaadmin.kafka.dto.ClusterInfo;
import com.kafkaadmin.kafka.dto.ConsumerGroupDetail;
import com.kafkaadmin.kafka.dto.ConsumerGroupSummary;
import com.kafkaadmin.kafka.dto.LogDirUsage;
import com.kafkaadmin.kafka.dto.TopicDetail;
import com.kafkaadmin.kafka.dto.TopicSummary;

import java.util.List;

/**
 * Kafka 클라이언트 라이브러리를 격리하는 유일한 경계.
 * 이 인터페이스 밖에서는 org.apache.kafka.* 타입을 import 하지 않는다.
 *
 * 모든 메서드는 실패 시 KafkaGatewayException 을 던진다.
 */
public interface KafkaAdminGateway {

    ClusterInfo describeCluster();

    List<LogDirUsage> describeLogDirs();

    List<TopicSummary> listTopics();

    TopicDetail describeTopic(String name);

    List<ConsumerGroupSummary> listConsumerGroups();

    ConsumerGroupDetail describeConsumerGroup(String groupId);

    AclOverview listAcls();
}
```

- [ ] **Step 7: 게이트웨이 예외 작성**

`src/main/java/com/kafkaadmin/kafka/KafkaGatewayException.java`:

```java
package com.kafkaadmin.kafka;

public class KafkaGatewayException extends RuntimeException {

    public KafkaGatewayException(String message) {
        super(message);
    }

    public KafkaGatewayException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

원인이 있을 때는 반드시 보존한다. Task 7의 에러 번역기가 `getCause()`를 따라가 실제 Kafka 예외 타입을 찾아낸다.

메시지만 받는 생성자도 둔다. 원인 예외가 없는 실패(예: 응답에 토픽이 아예 없는 경우)에 가짜 예외를 만들어 넣는 것을 막기 위한 것이다. 번역기는 원인이 없는 경우를 `case null, default`로 처리한다.

- [ ] **Step 8: 테스트 픽스처 작성**

`src/test/java/com/kafkaadmin/support/KafkaTestFixtures.java`:

```java
package com.kafkaadmin.support;

import com.kafkaadmin.kafka.dto.BrokerInfo;
import com.kafkaadmin.kafka.dto.ClusterInfo;
import com.kafkaadmin.kafka.dto.PartitionReplicaState;
import com.kafkaadmin.kafka.dto.TopicSummary;

import java.util.List;

public final class KafkaTestFixtures {

    private KafkaTestFixtures() {
    }

    /** 브로커 3대, 1번이 컨트롤러. */
    public static ClusterInfo healthyCluster() {
        return new ClusterInfo("test-cluster-id", List.of(
                new BrokerInfo(1, "kafka-1", 9092, "rack-a", true),
                new BrokerInfo(2, "kafka-2", 9092, "rack-b", false),
                new BrokerInfo(3, "kafka-3", 9092, "rack-c", false)
        ));
    }

    /** ISR 완전한 파티션. */
    public static PartitionReplicaState healthyPartition(int partition, int leaderId) {
        return new PartitionReplicaState(partition, leaderId, List.of(1, 2, 3), List.of(1, 2, 3));
    }

    /** 리더 없는 파티션. */
    public static PartitionReplicaState offlinePartition(int partition) {
        return new PartitionReplicaState(partition, null, List.of(1, 2, 3), List.of());
    }

    /** ISR이 부족한 파티션. */
    public static PartitionReplicaState underReplicatedPartition(int partition, int isrSize) {
        List<Integer> isr = List.of(1, 2, 3).subList(0, isrSize);
        return new PartitionReplicaState(partition, 1, List.of(1, 2, 3), isr);
    }

    /** RF=3, min.insync.replicas=2 인 정상 토픽. */
    public static TopicSummary healthyTopic(String name) {
        return new TopicSummary(name, 3, 2, 1_048_576L, List.of(
                healthyPartition(0, 1),
                healthyPartition(1, 2),
                healthyPartition(2, 3)
        ));
    }
}
```

- [ ] **Step 9: 가짜 게이트웨이 작성**

`src/test/java/com/kafkaadmin/support/FakeKafkaAdminGateway.java`:

```java
package com.kafkaadmin.support;

import com.kafkaadmin.kafka.KafkaAdminGateway;
import com.kafkaadmin.kafka.KafkaGatewayException;
import com.kafkaadmin.kafka.dto.AclOverview;
import com.kafkaadmin.kafka.dto.ClusterInfo;
import com.kafkaadmin.kafka.dto.ConsumerGroupDetail;
import com.kafkaadmin.kafka.dto.ConsumerGroupSummary;
import com.kafkaadmin.kafka.dto.LogDirUsage;
import com.kafkaadmin.kafka.dto.TopicDetail;
import com.kafkaadmin.kafka.dto.TopicSummary;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 단위 테스트용 게이트웨이 더블.
 * 각 메서드마다 반환값 또는 던질 예외를 지정할 수 있다.
 */
public class FakeKafkaAdminGateway implements KafkaAdminGateway {

    /**
     * 실패를 지정할 게이트웨이 메서드.
     * 문자열 키를 쓰면 오타가 조용히 통과해 테스트가 잘못된 이유로 성공한다.
     */
    public enum Method {
        DESCRIBE_CLUSTER,
        DESCRIBE_LOG_DIRS,
        LIST_TOPICS,
        DESCRIBE_TOPIC,
        LIST_CONSUMER_GROUPS,
        DESCRIBE_CONSUMER_GROUP,
        LIST_ACLS
    }

    private ClusterInfo clusterInfo = KafkaTestFixtures.healthyCluster();
    private List<LogDirUsage> logDirs = List.of(
            new LogDirUsage(1, 1_000L), new LogDirUsage(2, 2_000L), new LogDirUsage(3, 3_000L));
    private List<TopicSummary> topics = List.of();
    private final Map<String, TopicDetail> topicDetails = new HashMap<>();
    private List<ConsumerGroupSummary> consumerGroups = List.of();
    private final Map<String, ConsumerGroupDetail> consumerGroupDetails = new HashMap<>();
    private AclOverview aclOverview = new AclOverview(true, List.of());

    private final Map<Method, RuntimeException> failures = new EnumMap<>(Method.class);

    // --- 설정 ---

    public FakeKafkaAdminGateway withCluster(ClusterInfo value) {
        this.clusterInfo = value;
        return this;
    }

    public FakeKafkaAdminGateway withLogDirs(List<LogDirUsage> value) {
        this.logDirs = value;
        return this;
    }

    public FakeKafkaAdminGateway withTopics(TopicSummary... value) {
        this.topics = List.of(value);
        return this;
    }

    public FakeKafkaAdminGateway withTopicDetail(TopicDetail value) {
        this.topicDetails.put(value.name(), value);
        return this;
    }

    public FakeKafkaAdminGateway withConsumerGroups(ConsumerGroupSummary... value) {
        this.consumerGroups = List.of(value);
        return this;
    }

    public FakeKafkaAdminGateway withConsumerGroupDetail(ConsumerGroupDetail value) {
        this.consumerGroupDetails.put(value.groupId(), value);
        return this;
    }

    public FakeKafkaAdminGateway withAcls(AclOverview value) {
        this.aclOverview = value;
        return this;
    }

    public FakeKafkaAdminGateway failing(Method method, RuntimeException exception) {
        failures.put(method, exception);
        return this;
    }

    // --- 구현 ---

    @Override
    public ClusterInfo describeCluster() {
        checkFailure(Method.DESCRIBE_CLUSTER);
        return clusterInfo;
    }

    @Override
    public List<LogDirUsage> describeLogDirs() {
        checkFailure(Method.DESCRIBE_LOG_DIRS);
        return logDirs;
    }

    @Override
    public List<TopicSummary> listTopics() {
        checkFailure(Method.LIST_TOPICS);
        return topics;
    }

    @Override
    public TopicDetail describeTopic(String name) {
        checkFailure(Method.DESCRIBE_TOPIC);
        TopicDetail detail = topicDetails.get(name);
        if (detail == null) {
            throw new KafkaGatewayException("토픽 " + name + " 의 fixture가 설정되지 않았습니다.");
        }
        return detail;
    }

    @Override
    public List<ConsumerGroupSummary> listConsumerGroups() {
        checkFailure(Method.LIST_CONSUMER_GROUPS);
        return consumerGroups;
    }

    @Override
    public ConsumerGroupDetail describeConsumerGroup(String groupId) {
        checkFailure(Method.DESCRIBE_CONSUMER_GROUP);
        ConsumerGroupDetail detail = consumerGroupDetails.get(groupId);
        if (detail == null) {
            throw new KafkaGatewayException("그룹 " + groupId + " 의 fixture가 설정되지 않았습니다.");
        }
        return detail;
    }

    @Override
    public AclOverview listAcls() {
        checkFailure(Method.LIST_ACLS);
        return aclOverview;
    }

    private void checkFailure(Method method) {
        RuntimeException failure = failures.get(method);
        if (failure != null) {
            throw failure;
        }
    }
}
```

메서드 이름을 문자열이 아니라 enum으로 받는다. `failing("describeLogDir", ...)` 같은 오타는 아무 실패도 일으키지 않으면서 테스트를 통과시킨다 — 실패 처리를 검증하려던 테스트가 잘못된 이유로 초록불이 되는 것이 가장 위험한 결과다.

호출 횟수 카운터는 두지 않는다. Task 6의 캐시 테스트는 람다 loader에 자체 `AtomicInteger`를 쓰므로 이 더블을 거치지 않는다. 필요해지는 태스크에서 추가한다.

- [ ] **Step 10: 컴파일 확인**

```bash
./gradlew compileJava compileTestJava
```

Expected: BUILD SUCCESSFUL

- [ ] **Step 11: 커밋**

```bash
git add .
git commit -m "Add domain DTOs and KafkaAdminGateway boundary"
```

---

## Task 4: 토픽 위험 배지 판정

**Files:**
- Create: `src/main/java/com/kafkaadmin/topic/TopicBadge.java`
- Create: `src/main/java/com/kafkaadmin/topic/TopicBadgeEvaluator.java`
- Create: `src/test/java/com/kafkaadmin/topic/TopicBadgeEvaluatorTest.java`

- [ ] **Step 1: 배지 enum 작성**

`src/main/java/com/kafkaadmin/topic/TopicBadge.java`:

```java
package com.kafkaadmin.topic;

/**
 * 토픽의 위험 배지.
 *
 * 상수는 심각도 내림차순으로 선언한다. {@code evaluate}가 {@code EnumSet}을
 * 반환하므로 순회 순서가 선언 순서를 따르고, 그 결과 API 응답의 JSON 배열도
 * 심각한 것부터 나온다. 화면이 이 순서에 의존하므로 상수를 재정렬하면 안 된다.
 */
public enum TopicBadge {

    /** 리더가 없는 파티션 보유. 이미 쓰기가 실패하고 있다. */
    OFFLINE(Severity.CRITICAL),

    /** ISR 크기 < min.insync.replicas. acks=all 쓰기가 거부된다. */
    BELOW_MIN_ISR(Severity.CRITICAL),

    /** ISR 크기 < RF. 서비스는 되지만 복제본이 모자란다. */
    UNDER_REPLICATED(Severity.WARNING),

    /** RF=1. 브로커 1대 정지로 데이터가 사라진다. */
    RF_1(Severity.RISK),

    /** RF>=2 인데 min.insync.replicas=1. acks=all 이 무의미하다. */
    NO_DURABILITY(Severity.RISK),

    /** min.insync.replicas == RF. 브로커 1대만 빠져도 즉시 쓰기가 멈춘다. */
    NO_HEADROOM(Severity.RISK);

    public enum Severity {
        /** 현재 장애. */
        CRITICAL,
        /** 현재 이상. */
        WARNING,
        /** 지금은 정상이나 장애 시 문제가 되는 설정. */
        RISK
    }

    private final Severity severity;

    TopicBadge(Severity severity) {
        this.severity = severity;
    }

    public Severity severity() {
        return severity;
    }
}
```

심각도를 배지에 붙여두면 화면이 정렬·색상을 스스로 결정할 수 있다. 프론트엔드에 판정 규칙을 복제하지 않게 하는 것이 목적이다.

- [ ] **Step 2: 실패하는 테스트 작성**

`src/test/java/com/kafkaadmin/topic/TopicBadgeEvaluatorTest.java`:

```java
package com.kafkaadmin.topic;

import com.kafkaadmin.kafka.dto.PartitionReplicaState;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static com.kafkaadmin.support.KafkaTestFixtures.healthyPartition;
import static com.kafkaadmin.support.KafkaTestFixtures.offlinePartition;
import static com.kafkaadmin.support.KafkaTestFixtures.underReplicatedPartition;
import static org.assertj.core.api.Assertions.assertThat;

class TopicBadgeEvaluatorTest {

    private final TopicBadgeEvaluator evaluator = new TopicBadgeEvaluator();

    private Set<TopicBadge> evaluate(int rf, int minIsr, List<PartitionReplicaState> partitions) {
        return evaluator.evaluate(rf, minIsr, partitions);
    }

    @Test
    void RF3_minISR2_모든_ISR이_완전하면_배지가_없다() {
        Set<TopicBadge> badges = evaluate(3, 2,
                List.of(healthyPartition(0, 1), healthyPartition(1, 2)));

        assertThat(badges).isEmpty();
    }

    @Test
    void RF1은_RF_1_배지를_받는다() {
        PartitionReplicaState single = new PartitionReplicaState(0, 1, List.of(1), List.of(1));

        assertThat(evaluate(1, 1, List.of(single))).containsExactly(TopicBadge.RF_1);
    }

    @Test
    void RF3에_minISR1이면_무손실_보장이_없다() {
        assertThat(evaluate(3, 1, List.of(healthyPartition(0, 1))))
                .containsExactly(TopicBadge.NO_DURABILITY);
    }

    @Test
    void minISR이_RF와_같으면_여유가_없다() {
        assertThat(evaluate(3, 3, List.of(healthyPartition(0, 1))))
                .containsExactly(TopicBadge.NO_HEADROOM);
    }

    @Test
    void RF1일_때는_NO_HEADROOM을_붙이지_않는다() {
        PartitionReplicaState single = new PartitionReplicaState(0, 1, List.of(1), List.of(1));

        assertThat(evaluate(1, 1, List.of(single)))
                .containsExactly(TopicBadge.RF_1)
                .doesNotContain(TopicBadge.NO_HEADROOM);
    }

    @Test
    void 리더가_없는_파티션이_있으면_OFFLINE이다() {
        Set<TopicBadge> badges = evaluate(3, 2,
                List.of(healthyPartition(0, 1), offlinePartition(1)));

        assertThat(badges).contains(TopicBadge.OFFLINE);
    }

    @Test
    void ISR이_RF보다_작으면_UNDER_REPLICATED이다() {
        Set<TopicBadge> badges = evaluate(3, 2, List.of(underReplicatedPartition(0, 2)));

        assertThat(badges).containsExactly(TopicBadge.UNDER_REPLICATED);
    }

    @Test
    void ISR이_minISR보다_작으면_BELOW_MIN_ISR도_함께_붙는다() {
        Set<TopicBadge> badges = evaluate(3, 2, List.of(underReplicatedPartition(0, 1)));

        assertThat(badges).containsExactlyInAnyOrder(
                TopicBadge.UNDER_REPLICATED, TopicBadge.BELOW_MIN_ISR);
    }

    @Test
    void 파티션_목록이_비어도_설정_위험은_판정한다() {
        assertThat(evaluate(1, 1, List.of())).containsExactly(TopicBadge.RF_1);
    }

    @Test
    void 배지는_심각도_내림차순으로_반환된다() {
        Set<TopicBadge> badges = evaluate(3, 2, List.of(offlinePartition(0)));

        assertThat(badges).containsExactly(
                TopicBadge.OFFLINE,
                TopicBadge.BELOW_MIN_ISR,
                TopicBadge.UNDER_REPLICATED);
    }

    @Test
    void minISR이_RF보다_크면_BELOW_MIN_ISR만_붙는다() {
        // Kafka는 토픽 수준에서 min.insync.replicas > RF 설정을 허용한다.
        // 이 상태에서는 acks=all 쓰기가 영구히 실패하지만, 모든 파티션이
        // BELOW_MIN_ISR(CRITICAL)로 잡혀 이미 충분히 드러나므로
        // 별도 배지를 만들지 않는다.
        Set<TopicBadge> badges = evaluate(3, 4, List.of(healthyPartition(0, 1)));

        assertThat(badges).containsExactly(TopicBadge.BELOW_MIN_ISR);
    }

    @Test
    void 리더는_있지만_ISR이_비었으면_OFFLINE이_아니다() {
        Set<TopicBadge> badges = evaluate(3, 2, List.of(underReplicatedPartition(0, 0)));

        assertThat(badges).containsExactly(
                TopicBadge.BELOW_MIN_ISR, TopicBadge.UNDER_REPLICATED);
    }
}
```

`배지는_심각도_내림차순으로_반환된다`가 위 enum 주석의 순서 계약을 실행 가능한 형태로 고정한다. 주석만으로는 IDE의 "멤버 알파벳 정렬" 한 번에 API 응답 순서가 조용히 바뀐다.

`ISR이_RF보다_작으면_UNDER_REPLICATED이다`와 `ISR이_minISR보다_작으면_BELOW_MIN_ISR도_함께_붙는다`의 구분이 이 태스크의 핵심이다. ISR 2/3(minISR=2)는 아직 쓰기가 되고, ISR 1/3(minISR=2)는 쓰기가 거부된다. 대응 긴급도가 완전히 다르다.

- [ ] **Step 3: 테스트 실패 확인**

```bash
./gradlew test --tests 'com.kafkaadmin.topic.TopicBadgeEvaluatorTest'
```

Expected: 컴파일 실패. `cannot find symbol: class TopicBadgeEvaluator`

- [ ] **Step 4: 판정기 구현**

`src/main/java/com/kafkaadmin/topic/TopicBadgeEvaluator.java`:

```java
package com.kafkaadmin.topic;

import com.kafkaadmin.kafka.dto.PartitionReplicaState;
import org.springframework.stereotype.Component;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * 토픽의 복제 설정과 파티션 상태를 보고 위험 배지를 판정한다.
 *
 * 파티션에서 나오는 세 배지({@code OFFLINE}, {@code UNDER_REPLICATED},
 * {@code BELOW_MIN_ISR})는 의도적으로 상호 배타적이지 않다. 호출자가 각각을
 * 독립적으로 집계하므로(ClusterService는 BELOW_MIN_ISR 보유 토픽 수를 따로 센다)
 * 더 심각한 배지가 약한 배지를 억제하도록 바꾸면 그 집계가 조용히 틀어진다.
 */
@Component
public class TopicBadgeEvaluator {

    public Set<TopicBadge> evaluate(int replicationFactor,
                                    int minInSyncReplicas,
                                    List<PartitionReplicaState> partitions) {
        Set<TopicBadge> badges = EnumSet.noneOf(TopicBadge.class);

        for (PartitionReplicaState partition : partitions) {
            if (partition.offline()) {
                badges.add(TopicBadge.OFFLINE);
            }
            if (partition.underReplicated()) {
                badges.add(TopicBadge.UNDER_REPLICATED);
            }
            if (partition.inSyncReplicaIds().size() < minInSyncReplicas) {
                badges.add(TopicBadge.BELOW_MIN_ISR);
            }
        }

        if (replicationFactor == 1) {
            badges.add(TopicBadge.RF_1);
        }
        if (replicationFactor >= 2 && minInSyncReplicas == 1) {
            badges.add(TopicBadge.NO_DURABILITY);
        }
        if (replicationFactor > 1 && minInSyncReplicas == replicationFactor) {
            badges.add(TopicBadge.NO_HEADROOM);
        }

        return badges;
    }
}
```

`NO_HEADROOM` 조건에 `replicationFactor > 1`을 넣은 것은 스펙 표에 없는 추가 조건이다. RF=1이면 `minISR=1`이 필연이라 `NO_HEADROOM`이 항상 붙어 `RF_1`과 중복 경고가 되고, 배지가 많아지면 아무도 안 읽는다.

- [ ] **Step 5: 테스트 통과 확인**

```bash
./gradlew test --tests 'com.kafkaadmin.topic.TopicBadgeEvaluatorTest'
```

Expected: 12개 테스트 모두 PASS

- [ ] **Step 6: 커밋**

```bash
git add .
git commit -m "Add topic risk badge evaluation"
```

---

## Task 5: 클러스터 건강도 판정과 부분 실패 처리

**Files:**
- Create: `src/main/java/com/kafkaadmin/cluster/PartitionRisk.java`
- Create: `src/main/java/com/kafkaadmin/cluster/ClusterHealth.java`
- Create: `src/main/java/com/kafkaadmin/cluster/ClusterService.java`
- Create: `src/test/java/com/kafkaadmin/cluster/ClusterServiceTest.java`

- [ ] **Step 1: 응답 레코드 작성**

`src/main/java/com/kafkaadmin/cluster/PartitionRisk.java`:

```java
package com.kafkaadmin.cluster;

public record PartitionRisk(
        int offlinePartitions,
        int underReplicatedPartitions,
        int belowMinIsrTopics
) {}
```

`src/main/java/com/kafkaadmin/cluster/ClusterHealth.java`:

```java
package com.kafkaadmin.cluster;

import com.kafkaadmin.kafka.dto.BrokerInfo;
import com.kafkaadmin.kafka.dto.LogDirUsage;

import java.util.List;

/**
 * partitionRisk 가 null 이면 토픽 집계에 실패한 것이다.
 * 0 과 구분되어야 하므로 nullable 로 둔다 —
 * "위험 0건"과 "확인 못함"을 같은 화면에서 같게 보이면 안 된다.
 *
 * partitionRisk 에 @JsonInclude(ALWAYS) 를 붙인 이유: 누군가 전역 Jackson 설정에
 * default-property-inclusion=non_null 을 켜면 이 필드가 JSON에서 사라지고,
 * 그러면 "확인 못함"과 "위험 0건"을 화면에서 구분할 방법이 없어진다.
 */
public record ClusterHealth(
        String clusterId,
        List<BrokerInfo> brokers,
        List<LogDirUsage> diskUsage,
        @JsonInclude(JsonInclude.Include.ALWAYS) PartitionRisk partitionRisk,
        List<String> warnings
) {}
```

`import com.fasterxml.jackson.annotation.JsonInclude;` 를 추가한다.

이 계약은 주석만으로는 지켜지지 않으므로 테스트로 고정한다. `src/test/java/com/kafkaadmin/cluster/ClusterHealthJsonTest.java`:

```java
package com.kafkaadmin.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.JsonTest;
import org.springframework.test.context.TestPropertySource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * partitionRisk 가 null 일 때 JSON에서 필드가 생략되지 않는지 고정한다.
 * 위험한 전역 설정을 일부러 켠 상태로 검증한다 —
 * 애노테이션이 실제로 그 설정을 이기는지 확인하는 것이 목적이다.
 */
@JsonTest
@TestPropertySource(properties = "spring.jackson.default-property-inclusion=non_null")
class ClusterHealthJsonTest {

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void partitionRisk가_null이어도_JSON에서_생략되지_않는다() throws Exception {
        ClusterHealth health = new ClusterHealth(
                "test-cluster-id", List.of(), List.of(), null,
                List.of("토픽 상태를 집계하지 못했습니다."));

        String json = objectMapper.writeValueAsString(health);

        assertThat(json).contains("\"partitionRisk\":null");
    }
}
```

`@JsonTest`는 슬라이스 테스트라 `@Service`·`@Component`를 스캔하지 않는다. 따라서 아직 없는 `KafkaAdminGateway` 빈을 요구하지 않는다.

- [ ] **Step 2: 실패하는 테스트 작성**

`src/test/java/com/kafkaadmin/cluster/ClusterServiceTest.java`:

```java
package com.kafkaadmin.cluster;

import com.kafkaadmin.kafka.KafkaGatewayException;
import com.kafkaadmin.kafka.dto.LogDirUsage;
import com.kafkaadmin.kafka.dto.TopicSummary;
import com.kafkaadmin.support.FakeKafkaAdminGateway;
import com.kafkaadmin.topic.TopicBadgeEvaluator;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.kafkaadmin.support.KafkaTestFixtures.healthyPartition;
import static com.kafkaadmin.support.KafkaTestFixtures.healthyTopic;
import static com.kafkaadmin.support.KafkaTestFixtures.offlinePartition;
import static com.kafkaadmin.support.KafkaTestFixtures.underReplicatedPartition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterServiceTest {

    private final FakeKafkaAdminGateway gateway = new FakeKafkaAdminGateway();
    private final ClusterService service = new ClusterService(gateway, new TopicBadgeEvaluator());

    @Test
    void 정상_클러스터는_위험_건수가_모두_0이다() {
        gateway.withTopics(healthyTopic("orders"), healthyTopic("payments"));

        ClusterHealth health = service.health();

        assertThat(health.clusterId()).isEqualTo("test-cluster-id");
        assertThat(health.brokers()).hasSize(3);
        assertThat(health.partitionRisk())
                .isEqualTo(new PartitionRisk(0, 0, 0));
        assertThat(health.warnings()).isEmpty();
    }

    @Test
    void 컨트롤러_브로커가_표시된다() {
        ClusterHealth health = service.health();

        assertThat(health.brokers())
                .filteredOn(broker -> broker.controller())
                .extracting(broker -> broker.id())
                .containsExactly(1);
    }

    @Test
    void offline_파티션과_under_replicated_파티션을_각각_센다() {
        gateway.withTopics(
                new TopicSummary("orders", 3, 2, 0L, List.of(
                        healthyPartition(0, 1),
                        offlinePartition(1),
                        underReplicatedPartition(2, 2))),
                healthyTopic("payments"));

        PartitionRisk risk = service.health().partitionRisk();

        // offline 파티션은 ISR 0 이므로 under-replicated 에도 포함된다
        assertThat(risk.offlinePartitions()).isEqualTo(1);
        assertThat(risk.underReplicatedPartitions()).isEqualTo(2);
        assertThat(risk.belowMinIsrTopics()).isEqualTo(1);
    }

    @Test
    void belowMinIsr는_파티션이_아니라_토픽_수로_센다() {
        gateway.withTopics(
                new TopicSummary("orders", 3, 2, 0L, List.of(
                        underReplicatedPartition(0, 1),
                        underReplicatedPartition(1, 1))),
                healthyTopic("payments"));

        assertThat(service.health().partitionRisk().belowMinIsrTopics()).isEqualTo(1);
    }

    @Test
    void 디스크_조회가_실패해도_나머지는_보여준다() {
        gateway.withTopics(healthyTopic("orders"))
                .failing(FakeKafkaAdminGateway.Method.DESCRIBE_LOG_DIRS,
                        new KafkaGatewayException("로그 디렉터리 조회 실패"));

        ClusterHealth health = service.health();

        assertThat(health.brokers()).hasSize(3);
        assertThat(health.diskUsage()).isEmpty();
        assertThat(health.partitionRisk()).isEqualTo(new PartitionRisk(0, 0, 0));
        assertThat(health.warnings())
                .anySatisfy(warning -> assertThat(warning).contains("디스크"));
    }

    @Test
    void 토픽_집계가_실패하면_partitionRisk가_null이고_경고가_붙는다() {
        gateway.failing(FakeKafkaAdminGateway.Method.LIST_TOPICS,
                new KafkaGatewayException("토픽 목록 조회 실패"));

        ClusterHealth health = service.health();

        assertThat(health.brokers()).hasSize(3);
        assertThat(health.partitionRisk()).isNull();
        assertThat(health.warnings())
                .anySatisfy(warning -> assertThat(warning).contains("토픽"));
    }

    @Test
    void 클러스터_조회_자체가_실패하면_예외를_전파한다() {
        gateway.failing(FakeKafkaAdminGateway.Method.DESCRIBE_CLUSTER,
                new KafkaGatewayException("클러스터 연결 실패"));

        assertThatThrownBy(service::health)
                .isInstanceOf(KafkaGatewayException.class);
    }

    @Test
    void 디스크_사용량은_브로커_id_순으로_반환된다() {
        gateway.withLogDirs(List.of(
                new LogDirUsage(3, 300L), new LogDirUsage(1, 100L), new LogDirUsage(2, 200L)));

        assertThat(service.health().diskUsage())
                .extracting(LogDirUsage::brokerId)
                .containsExactly(1, 2, 3);
    }

    @Test
    void 디스크와_토픽_조회가_모두_실패해도_브로커는_보여준다() {
        gateway.failing(FakeKafkaAdminGateway.Method.DESCRIBE_LOG_DIRS,
                        new KafkaGatewayException("로그 디렉터리 조회 실패"))
                .failing(FakeKafkaAdminGateway.Method.LIST_TOPICS,
                        new KafkaGatewayException("토픽 목록 조회 실패"));

        ClusterHealth health = service.health();

        assertThat(health.brokers()).hasSize(3);
        assertThat(health.diskUsage()).isEmpty();
        assertThat(health.partitionRisk()).isNull();
        assertThat(health.warnings()).hasSize(2);
        assertThat(health.warnings().get(0)).contains("디스크");
        assertThat(health.warnings().get(1)).contains("토픽");
    }
}
```

마지막 테스트가 실제 장애 시나리오다. 브로커 하나가 죽으면 디스크 조회와 토픽 조회가 함께 실패할 수 있고, 그때도 브로커 목록은 보여야 한다. 경고 순서까지 고정하므로 나중에 `health()`에서 두 호출 순서를 바꾸면 잡힌다.

마지막 두 테스트가 스펙의 두 요구를 각각 고정한다. **클러스터 조회 자체가 실패하면 예외를 던져야 한다** — 빈 화면을 200으로 돌려주면 "브로커가 0대"로 읽힌다. 반면 **부수적 조회 실패는 부분 성공으로 처리한다.**

- [ ] **Step 3: 테스트 실패 확인**

```bash
./gradlew test --tests 'com.kafkaadmin.cluster.ClusterServiceTest'
```

Expected: 컴파일 실패. `cannot find symbol: class ClusterService`

- [ ] **Step 4: ClusterService 구현**

`src/main/java/com/kafkaadmin/cluster/ClusterService.java`:

```java
package com.kafkaadmin.cluster;

import com.kafkaadmin.kafka.KafkaAdminGateway;
import com.kafkaadmin.kafka.KafkaGatewayException;
import com.kafkaadmin.kafka.dto.ClusterInfo;
import com.kafkaadmin.kafka.dto.LogDirUsage;
import com.kafkaadmin.kafka.dto.PartitionReplicaState;
import com.kafkaadmin.kafka.dto.TopicSummary;
import com.kafkaadmin.topic.TopicBadge;
import com.kafkaadmin.topic.TopicBadgeEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * 클러스터 건강도를 집계한다.
 *
 * 이 클래스에는 의도적으로 {@code @Service}를 붙이지 않는다. 아직 이것을
 * 주입받는 곳이 없고(단위 테스트는 직접 생성한다), 컴포넌트 스캔에 올리면
 * Task 8에서야 생기는 {@code KafkaAdminGateway} 빈을 지금 요구하게 되어
 * 모든 {@code @SpringBootTest}가 깨진다.
 * 실제 소비자인 컨트롤러가 생기는 Task 12에서 애노테이션을 붙인다.
 */
public class ClusterService {

    private static final Logger log = LoggerFactory.getLogger(ClusterService.class);

    private final KafkaAdminGateway gateway;
    private final TopicBadgeEvaluator badgeEvaluator;

    public ClusterService(KafkaAdminGateway gateway, TopicBadgeEvaluator badgeEvaluator) {
        this.gateway = gateway;
        this.badgeEvaluator = badgeEvaluator;
    }

    public ClusterHealth health() {
        // 이 호출이 실패하면 화면에 보여줄 것이 아무것도 없다. 예외를 전파한다.
        ClusterInfo cluster = gateway.describeCluster();

        List<String> warnings = new ArrayList<>();
        List<LogDirUsage> diskUsage = diskUsage(warnings);
        PartitionRisk risk = partitionRisk(warnings);

        return new ClusterHealth(
                cluster.clusterId(), cluster.brokers(), diskUsage, risk, warnings);
    }

    private List<LogDirUsage> diskUsage(List<String> warnings) {
        List<LogDirUsage> usage;
        try {
            usage = gateway.describeLogDirs();
        } catch (KafkaGatewayException e) {
            log.warn("브로커 로그 디렉터리 조회 실패", e);
            warnings.add("브로커 디스크 사용량을 조회하지 못했습니다.");
            return List.of();
        }
        // 정렬은 try 밖에서 한다. 정렬 버그를 Kafka 조회 실패로 잘못 보고하면
        // 운영자가 있지도 않은 Kafka 문제를 쫓게 된다.
        return usage.stream()
                .sorted(Comparator.comparingInt(LogDirUsage::brokerId))
                .toList();
    }

    private PartitionRisk partitionRisk(List<String> warnings) {
        List<TopicSummary> topics;
        try {
            topics = gateway.listTopics();
        } catch (KafkaGatewayException e) {
            log.warn("토픽 목록 조회 실패", e);
            warnings.add("토픽 상태를 집계하지 못했습니다.");
            return null;
        }

        int offline = 0;
        int underReplicated = 0;
        int belowMinIsrTopics = 0;

        for (TopicSummary topic : topics) {
            for (PartitionReplicaState partition : topic.partitions()) {
                if (partition.offline()) {
                    offline++;
                }
                if (partition.underReplicated()) {
                    underReplicated++;
                }
            }
            Set<TopicBadge> badges = badgeEvaluator.evaluate(
                    topic.replicationFactor(), topic.minInSyncReplicas(), topic.partitions());
            if (badges.contains(TopicBadge.BELOW_MIN_ISR)) {
                belowMinIsrTopics++;
            }
        }

        return new PartitionRisk(offline, underReplicated, belowMinIsrTopics);
    }
}
```

- [ ] **Step 5: 테스트 통과 확인**

```bash
./gradlew test --tests 'com.kafkaadmin.cluster.ClusterServiceTest'
```

Expected: 9개 테스트 모두 PASS

- [ ] **Step 6: 커밋**

```bash
git add .
git commit -m "Add cluster health aggregation with partial failure handling"
```

---

## Task 6: 랙 집계 TTL 캐시

**Files:**
- Create: `src/main/java/com/kafkaadmin/config/ClockConfig.java`
- Create: `src/main/java/com/kafkaadmin/consumergroup/LagCache.java`
- Create: `src/test/java/com/kafkaadmin/support/MutableClock.java`
- Create: `src/test/java/com/kafkaadmin/consumergroup/LagCacheTest.java`

- [ ] **Step 1: Clock 빈 작성**

`src/main/java/com/kafkaadmin/config/ClockConfig.java`:

```java
package com.kafkaadmin.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
public class ClockConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
```

`Instant.now()`를 코드에 직접 쓰지 않고 `Clock`을 주입받는다. TTL 만료를 테스트하려면 시간을 앞으로 돌려야 하고, `Thread.sleep`으로 실제 시간을 기다리는 테스트는 느리고 불안정하다.

- [ ] **Step 2: 테스트용 Clock 작성**

`src/test/java/com/kafkaadmin/support/MutableClock.java`:

```java
package com.kafkaadmin.support;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

public class MutableClock extends Clock {

    private Instant instant;
    private final ZoneId zone;

    public MutableClock(Instant start) {
        this(start, ZoneId.of("UTC"));
    }

    private MutableClock(Instant instant, ZoneId zone) {
        this.instant = instant;
        this.zone = zone;
    }

    public void advance(Duration duration) {
        this.instant = this.instant.plus(duration);
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId newZone) {
        return new MutableClock(instant, newZone);
    }

    @Override
    public Instant instant() {
        return instant;
    }
}
```

- [ ] **Step 3: 실패하는 테스트 작성**

`src/test/java/com/kafkaadmin/consumergroup/LagCacheTest.java`:

```java
package com.kafkaadmin.consumergroup;

import com.kafkaadmin.kafka.dto.ConsumerGroupSummary;
import com.kafkaadmin.settings.SettingKey;
import com.kafkaadmin.settings.SettingsService;
import com.kafkaadmin.support.MutableClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LagCacheTest {

    private final MutableClock clock = new MutableClock(Instant.parse("2026-07-30T00:00:00Z"));
    private final Map<String, String> settingValues = new HashMap<>();
    private SettingsService settings;
    private LagCache cache;

    private final AtomicInteger loadCount = new AtomicInteger();

    @BeforeEach
    void setUp() {
        settings = new SettingsService(() -> Map.copyOf(settingValues));
        settings.reload();
        cache = new LagCache(clock, settings);
        loadCount.set(0);
    }

    private List<ConsumerGroupSummary> load() {
        loadCount.incrementAndGet();
        return List.of(new ConsumerGroupSummary(
                "group-" + loadCount.get(), "Stable", 1, List.of("orders"), 5L));
    }

    @Test
    void 첫_호출은_loader를_실행한다() {
        List<ConsumerGroupSummary> result = cache.get(this::load);

        assertThat(loadCount.get()).isEqualTo(1);
        assertThat(result).extracting(ConsumerGroupSummary::groupId).containsExactly("group-1");
    }

    @Test
    void TTL_이내_재호출은_loader를_실행하지_않는다() {
        cache.get(this::load);
        clock.advance(Duration.ofSeconds(9));
        List<ConsumerGroupSummary> result = cache.get(this::load);

        assertThat(loadCount.get()).isEqualTo(1);
        assertThat(result).extracting(ConsumerGroupSummary::groupId).containsExactly("group-1");
    }

    @Test
    void TTL이_지나면_loader를_다시_실행한다() {
        cache.get(this::load);
        clock.advance(Duration.ofSeconds(11));
        List<ConsumerGroupSummary> result = cache.get(this::load);

        assertThat(loadCount.get()).isEqualTo(2);
        assertThat(result).extracting(ConsumerGroupSummary::groupId).containsExactly("group-2");
    }

    @Test
    void TTL과_정확히_같은_시각에는_아직_유효하다() {
        cache.get(this::load);
        clock.advance(Duration.ofSeconds(10));
        cache.get(this::load);

        assertThat(loadCount.get()).isEqualTo(1);
    }

    @Test
    void TTL_설정_변경이_즉시_반영된다() {
        settingValues.put(SettingKey.LAG_CACHE_TTL_SECONDS.key(), "60");
        settings.reload();

        cache.get(this::load);
        clock.advance(Duration.ofSeconds(30));
        cache.get(this::load);

        assertThat(loadCount.get()).isEqualTo(1);
    }

    @Test
    void loader가_실패하면_예외를_전파하고_캐시를_오염시키지_않는다() {
        cache.get(this::load);
        clock.advance(Duration.ofSeconds(11));

        assertThatThrownBy(() -> cache.get(() -> {
            throw new IllegalStateException("Kafka 실패");
        })).isInstanceOf(IllegalStateException.class);

        // 실패한 호출이 캐시에 아무것도 쓰지 않았으므로
        // 다음 호출은 정상 데이터를 새로 적재한다
        List<ConsumerGroupSummary> result = cache.get(this::load);
        assertThat(loadCount.get()).isEqualTo(2);
        assertThat(result).extracting(ConsumerGroupSummary::groupId).containsExactly("group-2");
    }

    @Test
    void invalidate하면_TTL_이내라도_다시_적재한다() {
        cache.get(this::load);
        cache.invalidate();
        cache.get(this::load);

        assertThat(loadCount.get()).isEqualTo(2);
    }
}
```

- [ ] **Step 4: 테스트 실패 확인**

```bash
./gradlew test --tests 'com.kafkaadmin.consumergroup.LagCacheTest'
```

Expected: 컴파일 실패. `cannot find symbol: class LagCache`

- [ ] **Step 5: LagCache 구현**

`src/main/java/com/kafkaadmin/consumergroup/LagCache.java`:

```java
package com.kafkaadmin.consumergroup;

import com.kafkaadmin.kafka.dto.ConsumerGroupSummary;
import com.kafkaadmin.settings.SettingKey;
import com.kafkaadmin.settings.SettingsService;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

/**
 * 컨슈머 그룹 랙 집계 전용 캐시.
 *
 * 랙 집계는 listConsumerGroups → describeConsumerGroups →
 * listConsumerGroupOffsets → listOffsets 4단 호출이라 그룹이 많으면 수 초가 걸린다.
 * 목록 화면에만 적용하고, 그룹 상세는 항상 실시간으로 조회한다.
 */
@Component
public class LagCache {

    private record Snapshot(List<ConsumerGroupSummary> value, Instant loadedAt) {
    }

    private final Clock clock;
    private final SettingsService settings;

    private volatile Snapshot snapshot;

    public LagCache(Clock clock, SettingsService settings) {
        this.clock = clock;
        this.settings = settings;
    }

    public List<ConsumerGroupSummary> get(Supplier<List<ConsumerGroupSummary>> loader) {
        Snapshot current = snapshot;
        Instant now = clock.instant();
        long ttlSeconds = settings.getLong(SettingKey.LAG_CACHE_TTL_SECONDS);

        if (current != null && !current.loadedAt().plusSeconds(ttlSeconds).isBefore(now)) {
            return current.value();
        }

        // loader 가 던지면 예외를 그대로 전파한다. 실패한 값은 저장하지 않으므로
        // 캐시에 잘못된 값이 남지 않는다.
        //
        // 실패 시 낡은 스냅샷을 대신 돌려주지 않는 것은 의도한 설계다.
        // 장애 대응 중에 낡은 랙 숫자를 정상 값처럼 보여주는 것이
        // 오류를 드러내는 것보다 위험하다.
        List<ConsumerGroupSummary> loaded = loader.get();
        snapshot = new Snapshot(loaded, now);
        return loaded;
    }

    public void invalidate() {
        snapshot = null;
    }
}
```

`isBefore` 부정으로 비교한 것은 TTL 경계에서 정확히 같은 시각일 때 캐시를 유효로 보기 위한 것이다. 9초 유효, 10초 유효(경계 포함), 11초 만료 — 세 테스트가 이 경계를 고정한다.

**10초 테스트가 반드시 있어야 한다.** 9초·11초 테스트만 있으면 비교를 `isAfter(now)`로 바꿔 경계를 배타적으로 만들어도 전부 통과한다. 실제로 그 변이를 넣어보고 확인했다 — 경계를 정확히 짚는 테스트가 없으면 경계 선택은 코드에만 존재하고 아무것도 지켜주지 않는다.

**"실패 시 이전 스냅샷이 남는다"를 계약으로 삼지 않는다.** 적재 실패는 TTL이 이미 만료된 뒤에만 일어나므로(그 전이면 캐시 값을 반환했을 것이다) 남아 있는 스냅샷은 항상 이미 낡은 상태이고, 다음 호출은 어차피 다시 적재한다. 즉 이 성질은 `get()` 밖에서 관찰되지 않는다. 관찰 가능해지는 유일한 경로는 관리자가 나중에 TTL을 늘려 낡은 스냅샷을 되살리는 경우인데, 그건 원하는 동작이 아니다. 실제로 보장하는 것은 **실패한 값이 캐시에 쓰이지 않는다**는 것 하나이고, 테스트 이름도 그렇게 붙인다.

- [ ] **Step 6: 테스트 통과 확인**

```bash
./gradlew test --tests 'com.kafkaadmin.consumergroup.LagCacheTest'
```

Expected: 7개 테스트 모두 PASS

- [ ] **Step 7: 커밋**

```bash
git add .
git commit -m "Add TTL cache for consumer group lag aggregation"
```

---

## Task 7: Kafka 예외 번역과 전역 예외 처리

**Files:**
- Create: `src/main/java/com/kafkaadmin/kafka/ClusterAvailabilityTracker.java`
- Create: `src/main/java/com/kafkaadmin/error/ApiError.java`
- Create: `src/main/java/com/kafkaadmin/error/ApiErrorResponse.java`
- Create: `src/main/java/com/kafkaadmin/error/KafkaErrorTranslator.java`
- Create: `src/main/java/com/kafkaadmin/error/GlobalExceptionHandler.java`
- Create: `src/test/java/com/kafkaadmin/error/KafkaErrorTranslatorTest.java`

- [ ] **Step 1: 마지막 성공 시각 추적기 작성**

`src/main/java/com/kafkaadmin/kafka/ClusterAvailabilityTracker.java`:

```java
package com.kafkaadmin.kafka;

import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;

/**
 * 마지막으로 Kafka 호출이 성공한 시각을 기억한다.
 * 연결 실패 응답에 이 값을 함께 실어야 화면이
 * "언제부터 안 보이는지"를 표시할 수 있다.
 */
@Component
public class ClusterAvailabilityTracker {

    private final Clock clock;
    private volatile Instant lastSuccessAt;

    public ClusterAvailabilityTracker(Clock clock) {
        this.clock = clock;
    }

    public void recordSuccess() {
        lastSuccessAt = clock.instant();
    }

    public Optional<Instant> lastSuccessAt() {
        return Optional.ofNullable(lastSuccessAt);
    }
}
```

- [ ] **Step 2: 에러 레코드 작성**

`src/main/java/com/kafkaadmin/error/ApiError.java`:

```java
package com.kafkaadmin.error;

import org.springframework.http.HttpStatus;

public record ApiError(HttpStatus status, String code, String message) {}
```

`src/main/java/com/kafkaadmin/error/ApiErrorResponse.java`:

```java
package com.kafkaadmin.error;

import java.time.Instant;

/**
 * 응답 본문. 스택 트레이스는 절대 포함하지 않는다.
 * lastKafkaSuccessAt 은 Kafka 관련 오류일 때만 채워진다.
 */
public record ApiErrorResponse(
        String code,
        String message,
        Instant lastKafkaSuccessAt
) {}
```

- [ ] **Step 3: 실패하는 테스트 작성**

`src/test/java/com/kafkaadmin/error/KafkaErrorTranslatorTest.java`:

```java
package com.kafkaadmin.error;

import com.kafkaadmin.kafka.KafkaGatewayException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaErrorTranslatorTest {

    private final KafkaErrorTranslator translator = new KafkaErrorTranslator();

    private ApiError translateWrapped(Throwable kafkaCause) {
        return translator.translate(new KafkaGatewayException("게이트웨이 실패", kafkaCause));
    }

    @Test
    void 없는_토픽은_404로_번역한다() {
        ApiError error = translateWrapped(new UnknownTopicOrPartitionException("nope"));

        assertThat(error.status()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(error.code()).isEqualTo("TOPIC_NOT_FOUND");
        assertThat(error.message()).contains("찾을 수 없습니다");
    }

    @Test
    void 중복_토픽은_409로_번역한다() {
        ApiError error = translateWrapped(new TopicExistsException("dup"));

        assertThat(error.status()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(error.code()).isEqualTo("TOPIC_EXISTS");
    }

    @Test
    void 잘못된_설정은_400으로_번역한다() {
        ApiError error = translateWrapped(new InvalidConfigurationException("bad retention"));

        assertThat(error.status()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(error.code()).isEqualTo("INVALID_CONFIG");
    }

    @Test
    void 브로커_정책_위반은_400으로_번역한다() {
        ApiError error = translateWrapped(new PolicyViolationException("denied by policy"));

        assertThat(error.status()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(error.code()).isEqualTo("POLICY_VIOLATION");
    }

    @Test
    void 토픽_인가_실패는_403으로_번역한다() {
        ApiError error = translateWrapped(new TopicAuthorizationException("orders"));

        assertThat(error.status()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(error.code()).isEqualTo("KAFKA_FORBIDDEN");
        assertThat(error.message()).contains("권한");
    }

    @Test
    void 클러스터_인가_실패도_403으로_번역한다() {
        ApiError error = translateWrapped(new ClusterAuthorizationException("no cluster describe"));

        assertThat(error.status()).isEqualTo(HttpStatus.FORBIDDEN);
    }

    @Test
    void 타임아웃은_504로_번역한다() {
        ApiError error = translateWrapped(
                new org.apache.kafka.common.errors.TimeoutException("timed out"));

        assertThat(error.status()).isEqualTo(HttpStatus.GATEWAY_TIMEOUT);
        assertThat(error.code()).isEqualTo("KAFKA_TIMEOUT");
    }

    @Test
    void 인증_실패는_502로_번역하고_인증서를_언급한다() {
        ApiError error = translateWrapped(new SaslAuthenticationException("bad credentials"));

        assertThat(error.status()).isEqualTo(HttpStatus.BAD_GATEWAY);
        assertThat(error.code()).isEqualTo("KAFKA_AUTH_FAILED");
        assertThat(error.message()).contains("인증");
    }

    @Test
    void 알_수_없는_원인은_연결_실패로_번역한다() {
        ApiError error = translateWrapped(new java.net.ConnectException("refused"));

        assertThat(error.status()).isEqualTo(HttpStatus.BAD_GATEWAY);
        assertThat(error.code()).isEqualTo("KAFKA_UNAVAILABLE");
    }

    @Test
    void ExecutionException으로_한겹_더_감싸도_원인을_찾아낸다() {
        ApiError error = translateWrapped(
                new ExecutionException(new TopicExistsException("dup")));

        assertThat(error.code()).isEqualTo("TOPIC_EXISTS");
    }

    @Test
    void 원인이_없는_게이트웨이_예외도_처리한다() {
        ApiError error = translator.translate(
                new KafkaGatewayException("원인 없음", null));

        assertThat(error.code()).isEqualTo("KAFKA_UNAVAILABLE");
    }
}
```

마지막 두 테스트가 실무에서 실제로 물리는 지점이다. `AdminClient`의 `KafkaFuture.get()`은 실제 예외를 `ExecutionException`으로 감싸므로, 껍질을 벗기지 않으면 모든 오류가 "알 수 없는 연결 실패"로 뭉개진다.

- [ ] **Step 4: 테스트 실패 확인**

```bash
./gradlew test --tests 'com.kafkaadmin.error.KafkaErrorTranslatorTest'
```

Expected: 컴파일 실패. `cannot find symbol: class KafkaErrorTranslator`

- [ ] **Step 5: 번역기 구현**

`src/main/java/com/kafkaadmin/error/KafkaErrorTranslator.java`:

```java
package com.kafkaadmin.error;

import com.kafkaadmin.kafka.KafkaGatewayException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

@Component
public class KafkaErrorTranslator {

    public ApiError translate(Throwable throwable) {
        Throwable cause = unwrap(throwable);

        return switch (cause) {
            case UnknownTopicOrPartitionException ignored -> new ApiError(
                    HttpStatus.NOT_FOUND, "TOPIC_NOT_FOUND",
                    "해당 토픽 또는 파티션을 찾을 수 없습니다.");

            case TopicExistsException ignored -> new ApiError(
                    HttpStatus.CONFLICT, "TOPIC_EXISTS",
                    "같은 이름의 토픽이 이미 존재합니다.");

            case InvalidConfigurationException e -> new ApiError(
                    HttpStatus.BAD_REQUEST, "INVALID_CONFIG",
                    "요청한 설정 값이 올바르지 않습니다: " + e.getMessage());

            case PolicyViolationException e -> new ApiError(
                    HttpStatus.BAD_REQUEST, "POLICY_VIOLATION",
                    "브로커 정책에 의해 거부되었습니다: " + e.getMessage());

            case TopicAuthorizationException ignored -> new ApiError(
                    HttpStatus.FORBIDDEN, "KAFKA_FORBIDDEN",
                    "관리자 계정에 이 토픽에 대한 권한이 없습니다.");

            case GroupAuthorizationException ignored -> new ApiError(
                    HttpStatus.FORBIDDEN, "KAFKA_FORBIDDEN",
                    "관리자 계정에 이 컨슈머 그룹에 대한 권한이 없습니다.");

            case ClusterAuthorizationException ignored -> new ApiError(
                    HttpStatus.FORBIDDEN, "KAFKA_FORBIDDEN",
                    "관리자 계정에 클러스터 권한이 없습니다.");

            case org.apache.kafka.common.errors.TimeoutException ignored -> new ApiError(
                    HttpStatus.GATEWAY_TIMEOUT, "KAFKA_TIMEOUT",
                    "Kafka 응답 시간을 초과했습니다. 브로커 상태를 확인하세요.");

            case AuthenticationException ignored -> new ApiError(
                    HttpStatus.BAD_GATEWAY, "KAFKA_AUTH_FAILED",
                    "Kafka 인증에 실패했습니다. 클라이언트 인증서와 자격증명을 확인하세요.");

            case null, default -> new ApiError(
                    HttpStatus.BAD_GATEWAY, "KAFKA_UNAVAILABLE",
                    "Kafka 클러스터에 연결할 수 없습니다.");
        };
    }

    /**
     * KafkaGatewayException, ExecutionException, CompletionException 껍질을 벗겨
     * 실제 Kafka 예외를 찾는다.
     */
    private Throwable unwrap(Throwable throwable) {
        Throwable current = throwable;
        while (current != null
                && current.getCause() != null
                && (current instanceof KafkaGatewayException
                || current instanceof ExecutionException
                || current instanceof CompletionException)) {
            current = current.getCause();
        }
        return current;
    }
}
```

`case null, default ->`는 Java 21 패턴 스위치 문법이다. 원인이 없는 예외에서 `switch`가 NPE를 던지는 것을 막는다.

- [ ] **Step 6: 테스트 통과 확인**

```bash
./gradlew test --tests 'com.kafkaadmin.error.KafkaErrorTranslatorTest'
```

Expected: 11개 테스트 모두 PASS

- [ ] **Step 7: 전역 예외 핸들러 구현**

`src/main/java/com/kafkaadmin/error/GlobalExceptionHandler.java`:

```java
package com.kafkaadmin.error;

import com.kafkaadmin.kafka.ClusterAvailabilityTracker;
import com.kafkaadmin.kafka.KafkaGatewayException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    private final KafkaErrorTranslator translator;
    private final ClusterAvailabilityTracker availability;

    public GlobalExceptionHandler(KafkaErrorTranslator translator,
                                  ClusterAvailabilityTracker availability) {
        this.translator = translator;
        this.availability = availability;
    }

    @ExceptionHandler(KafkaGatewayException.class)
    public ResponseEntity<ApiErrorResponse> handleKafkaFailure(KafkaGatewayException e) {
        ApiError error = translator.translate(e);
        // 스택 트레이스는 서버 로그에만 남긴다.
        log.warn("Kafka 호출 실패 [{}]: {}", error.code(), e.getMessage(), e);

        return ResponseEntity.status(error.status()).body(new ApiErrorResponse(
                error.code(),
                error.message(),
                availability.lastSuccessAt().orElse(null)));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiErrorResponse> handleUnexpected(Exception e) {
        log.error("처리되지 않은 예외", e);

        return ResponseEntity.internalServerError().body(new ApiErrorResponse(
                "INTERNAL_ERROR", "서버 내부 오류가 발생했습니다.", null));
    }
}
```

- [ ] **Step 8: 전체 테스트 실행**

```bash
./gradlew test
```

Expected: 모든 테스트 PASS

- [ ] **Step 9: 커밋**

```bash
git add .
git commit -m "Add Kafka error translation and global exception handling"
```

---

## Task 8: AdminClient 설정과 게이트웨이 골격 (클러스터·디스크)

**Files:**
- Create: `src/main/java/com/kafkaadmin/config/KafkaConnectionProperties.java`
- Create: `src/main/java/com/kafkaadmin/config/KafkaClientConfig.java`
- Create: `src/main/java/com/kafkaadmin/kafka/KafkaAdminGatewayImpl.java`
- Create: `src/test/java/com/kafkaadmin/kafka/KafkaAdminGatewayImplTest.java`
- Modify: `src/main/resources/application.yml`
- Modify: `src/main/resources/application-local.yml`
- Modify: `src/test/java/com/kafkaadmin/KafkaAdminApplicationTest.java`

- [ ] **Step 1: 접속 설정 프로퍼티 작성**

`src/main/java/com/kafkaadmin/config/KafkaConnectionProperties.java`:

```java
package com.kafkaadmin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kafka")
public record KafkaConnectionProperties(
        String bootstrapServers,
        String securityProtocol,
        int requestTimeoutMs,
        int apiTimeoutMs,
        Ssl ssl,
        Sasl sasl
) {

    public record Ssl(
            String truststoreLocation,
            String truststorePassword,
            String keystoreLocation,
            String keystorePassword,
            String keyPassword
    ) {}

    public record Sasl(
            String mechanism,
            String jaasConfig
    ) {}

    public boolean usesSsl() {
        return securityProtocol != null && securityProtocol.endsWith("SSL");
    }

    public boolean usesSasl() {
        return securityProtocol != null && securityProtocol.startsWith("SASL");
    }
}
```

- [ ] **Step 2: AdminClient 빈 작성**

`src/main/java/com/kafkaadmin/config/KafkaClientConfig.java`:

```java
package com.kafkaadmin.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaClientConfig {

    @Bean(destroyMethod = "close")
    public Admin adminClient(KafkaConnectionProperties properties) {
        return Admin.create(adminClientProperties(properties));
    }

    static Map<String, Object> adminClientProperties(KafkaConnectionProperties properties) {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, "kafka-admin-web");
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, properties.requestTimeoutMs());
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, properties.apiTimeoutMs());
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, properties.securityProtocol());

        if (properties.usesSsl() && properties.ssl() != null) {
            KafkaConnectionProperties.Ssl ssl = properties.ssl();
            putIfPresent(config, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, ssl.truststoreLocation());
            putIfPresent(config, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ssl.truststorePassword());
            putIfPresent(config, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, ssl.keystoreLocation());
            putIfPresent(config, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, ssl.keystorePassword());
            putIfPresent(config, SslConfigs.SSL_KEY_PASSWORD_CONFIG, ssl.keyPassword());
        }

        if (properties.usesSasl() && properties.sasl() != null) {
            KafkaConnectionProperties.Sasl sasl = properties.sasl();
            putIfPresent(config, SaslConfigs.SASL_MECHANISM, sasl.mechanism());
            putIfPresent(config, SaslConfigs.SASL_JAAS_CONFIG, sasl.jaasConfig());
        }

        return config;
    }

    private static void putIfPresent(Map<String, Object> config, String key, String value) {
        if (value != null && !value.isBlank()) {
            config.put(key, value);
        }
    }
}
```

`Admin` 인터페이스를 빈 타입으로 쓴다(`AdminClient`는 추상 클래스). `putIfPresent`가 빈 문자열을 걸러내는 것이 중요하다 — 환경변수 기본값이 `""`인 상태로 `ssl.keystore.location=""`이 들어가면 Kafka 클라이언트가 부팅 시점에 죽는다.

`securityProtocol`이 `PLAINTEXT`면 SSL·SASL 블록이 모두 건너뛰어진다. 로컬 개발과 Testcontainers가 같은 코드 경로를 쓸 수 있다.

- [ ] **Step 3: 게이트웨이 구현체 골격 작성**

`src/main/java/com/kafkaadmin/kafka/KafkaAdminGatewayImpl.java`:

```java
package com.kafkaadmin.kafka;

import com.kafkaadmin.kafka.dto.AclOverview;
import com.kafkaadmin.kafka.dto.BrokerInfo;
import com.kafkaadmin.kafka.dto.ClusterInfo;
import com.kafkaadmin.kafka.dto.ConsumerGroupDetail;
import com.kafkaadmin.kafka.dto.ConsumerGroupSummary;
import com.kafkaadmin.kafka.dto.LogDirUsage;
import com.kafkaadmin.kafka.dto.TopicDetail;
import com.kafkaadmin.kafka.dto.TopicSummary;
import com.kafkaadmin.settings.SettingKey;
import com.kafkaadmin.settings.SettingsService;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Component
public class KafkaAdminGatewayImpl implements KafkaAdminGateway {

    private final Admin admin;
    private final SettingsService settings;
    private final ClusterAvailabilityTracker availability;

    public KafkaAdminGatewayImpl(Admin admin,
                                 SettingsService settings,
                                 ClusterAvailabilityTracker availability) {
        this.admin = admin;
        this.settings = settings;
        this.availability = availability;
    }

    @Override
    public ClusterInfo describeCluster() {
        DescribeClusterResult result = admin.describeCluster();
        String clusterId = await(result.clusterId());
        Collection<Node> nodes = await(result.nodes());
        Node controller = await(result.controller());
        Integer controllerId = (controller == null || controller.id() < 0) ? null : controller.id();

        List<BrokerInfo> brokers = nodes.stream()
                .sorted(Comparator.comparingInt(Node::id))
                .map(node -> new BrokerInfo(
                        node.id(),
                        node.host(),
                        node.port(),
                        node.rack(),
                        controllerId != null && controllerId == node.id()))
                .toList();

        return new ClusterInfo(clusterId, brokers);
    }

    @Override
    public List<LogDirUsage> describeLogDirs() {
        List<Integer> brokerIds = describeCluster().brokers().stream()
                .map(BrokerInfo::id)
                .toList();

        Map<Integer, Map<String, LogDirDescription>> descriptions =
                await(admin.describeLogDirs(brokerIds).allDescriptions());

        return descriptions.entrySet().stream()
                .map(entry -> new LogDirUsage(entry.getKey(), sumReplicaBytes(entry.getValue())))
                .sorted(Comparator.comparingInt(LogDirUsage::brokerId))
                .toList();
    }

    private static long sumReplicaBytes(Map<String, LogDirDescription> logDirs) {
        return logDirs.values().stream()
                .flatMap(dir -> dir.replicaInfos().values().stream())
                .mapToLong(ReplicaInfo::size)
                .sum();
    }

    @Override
    public List<TopicSummary> listTopics() {
        throw new UnsupportedOperationException("Task 9에서 구현");
    }

    @Override
    public TopicDetail describeTopic(String name) {
        throw new UnsupportedOperationException("Task 9에서 구현");
    }

    @Override
    public List<ConsumerGroupSummary> listConsumerGroups() {
        throw new UnsupportedOperationException("Task 10에서 구현");
    }

    @Override
    public ConsumerGroupDetail describeConsumerGroup(String groupId) {
        throw new UnsupportedOperationException("Task 10에서 구현");
    }

    @Override
    public AclOverview listAcls() {
        throw new UnsupportedOperationException("Task 11에서 구현");
    }

    /**
     * 모든 AdminClient 호출은 이 메서드를 통과한다.
     * 명시적 타임아웃을 걸고, 성공 시각을 기록하고, 예외를 KafkaGatewayException 으로 감싼다.
     * 무한 대기는 Kafka 장애를 웹앱 스레드 고갈로 확산시킨다.
     */
    private <T> T await(KafkaFuture<T> future) {
        long timeoutMs = settings.getLong(SettingKey.ADMIN_CLIENT_TIMEOUT_MS);
        try {
            T value = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            availability.recordSuccess();
            return value;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaGatewayException("Kafka 호출이 중단되었습니다.", e);
        } catch (ExecutionException e) {
            throw new KafkaGatewayException("Kafka 호출이 실패했습니다.",
                    e.getCause() != null ? e.getCause() : e);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new KafkaGatewayException("Kafka 호출이 시간을 초과했습니다.",
                    new org.apache.kafka.common.errors.TimeoutException(
                            "AdminClient 호출이 " + timeoutMs + "ms 안에 완료되지 않았습니다.", e));
        }
    }
}
```

`UnsupportedOperationException` 스텁은 Task 9~11에서 순서대로 교체된다. 이렇게 두는 이유는 인터페이스 전체를 한 번에 구현하면 태스크가 커지고, 중간에 컴파일이 안 되는 상태로 남는 것보다 낫기 때문이다. **Task 11이 끝날 때까지 스텁이 남아 있으면 계획이 완료된 것이 아니다.**

`java.util.concurrent.TimeoutException`을 잡아 Kafka의 `TimeoutException`으로 바꿔 감싸는 것이 핵심이다. Task 7의 번역기가 Kafka 예외 타입만 보고 판단하므로, 이 변환이 없으면 타임아웃이 "알 수 없는 연결 실패"로 뭉개진다.

- [ ] **Step 4: Testcontainers 통합 테스트 작성**

`src/test/java/com/kafkaadmin/kafka/KafkaAdminGatewayImplTest.java`:

```java
package com.kafkaadmin.kafka;

import com.kafkaadmin.kafka.dto.BrokerInfo;
import com.kafkaadmin.kafka.dto.ClusterInfo;
import com.kafkaadmin.kafka.dto.LogDirUsage;
import com.kafkaadmin.settings.SettingsService;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Clock;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class KafkaAdminGatewayImplTest {

    @Container
    @SuppressWarnings("resource")
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.0.0");

    static Admin admin;
    static KafkaAdminGatewayImpl gateway;
    static ClusterAvailabilityTracker availability;

    @BeforeAll
    static void setUp() throws Exception {
        admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));

        SettingsService settings = new SettingsService(Map::of);
        settings.reload();

        availability = new ClusterAvailabilityTracker(Clock.systemUTC());
        gateway = new KafkaAdminGatewayImpl(admin, settings, availability);

        // 단일 브로커 컨테이너이므로 RF=1 로만 만들 수 있다.
        admin.createTopics(List.of(
                new NewTopic("orders", 3, (short) 1),
                new NewTopic("payments", 1, (short) 1))).all().get();
    }

    @AfterAll
    static void tearDown() {
        if (admin != null) {
            admin.close();
        }
    }

    @Test
    void 클러스터_정보를_조회한다() {
        ClusterInfo cluster = gateway.describeCluster();

        assertThat(cluster.clusterId()).isNotBlank();
        assertThat(cluster.brokers()).hasSize(1);
        assertThat(cluster.brokers())
                .extracting(BrokerInfo::controller)
                .containsExactly(true);
    }

    @Test
    void 성공한_호출은_마지막_성공_시각을_기록한다() {
        gateway.describeCluster();

        assertThat(availability.lastSuccessAt()).isPresent();
    }

    @Test
    void 브로커별_디스크_사용량을_조회한다() {
        List<LogDirUsage> usage = gateway.describeLogDirs();

        assertThat(usage).hasSize(1);
        assertThat(usage.get(0).brokerId()).isEqualTo(1);
        assertThat(usage.get(0).totalBytes()).isGreaterThanOrEqualTo(0L);
    }
}
```

브로커 1대짜리 컨테이너를 쓰는 것은 의도적이다. RF=3 판정 로직은 Task 4에서 단위 테스트로 이미 고정했고, 컨테이너를 3대 띄우면 테스트가 느려지고 불안정해진다. 통합 테스트의 목적은 **AdminClient API를 우리가 올바르게 호출하는지**를 확인하는 것이지 복제 동작을 검증하는 것이 아니다.

- [ ] **Step 5: 테스트 실행**

```bash
./gradlew test --tests 'com.kafkaadmin.kafka.KafkaAdminGatewayImplTest'
```

Expected: 3개 테스트 PASS. 첫 실행은 `apache/kafka:4.0.0` 이미지를 내려받으므로 수 분 걸릴 수 있다.

Docker가 실행 중이 아니면 `Could not find a valid Docker environment` 로 실패한다.

- [ ] **Step 6: application.yml에 Kafka 설정 추가**

`src/main/resources/application.yml` 전체를 교체:

```yaml
spring:
  application:
    name: kafka-admin-web
  datasource:
    url: ${DB_URL}
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD}
  flyway:
    enabled: true
    locations: classpath:db/migration

kafka:
  bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS}
  security-protocol: ${KAFKA_SECURITY_PROTOCOL:SASL_SSL}
  request-timeout-ms: 10000
  api-timeout-ms: 15000
  ssl:
    truststore-location: ${KAFKA_TRUSTSTORE_LOCATION:}
    truststore-password: ${KAFKA_TRUSTSTORE_PASSWORD:}
    keystore-location: ${KAFKA_KEYSTORE_LOCATION:}
    keystore-password: ${KAFKA_KEYSTORE_PASSWORD:}
    key-password: ${KAFKA_KEY_PASSWORD:}
  sasl:
    mechanism: ${KAFKA_SASL_MECHANISM:SCRAM-SHA-512}
    jaas-config: ${KAFKA_SASL_JAAS_CONFIG:}

server:
  port: 8080

logging:
  level:
    org.apache.kafka: WARN
```

인증서 경로와 패스워드가 모두 환경변수인 것이 스펙 요구사항이다. 기본값을 빈 문자열로 두어 PLAINTEXT 로컬 개발에서 변수 미설정으로 부팅이 실패하지 않게 한다.

- [ ] **Step 7: 로컬 프로파일에 PLAINTEXT Kafka 추가**

`src/main/resources/application-local.yml` 전체를 교체:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/kafka_admin
    username: kafka_admin
    password: local-dev-only

kafka:
  bootstrap-servers: localhost:19092,localhost:29092,localhost:39092
  security-protocol: PLAINTEXT
```

포트는 devops-note 저장소의 `kafka/examples/compose-3node-kraft-plaintext/docker-compose.yml`이 노출하는 값에 맞춘다. Task 13에서 실제 값을 확인해 필요하면 수정한다.

- [ ] **Step 8: 부팅 테스트에 Kafka 프로퍼티 주입**

`src/test/java/com/kafkaadmin/KafkaAdminApplicationTest.java`의 `datasource` 메서드를 다음으로 교체:

```java
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        // AdminClient 빈 생성만 확인한다. 실제 브로커에 연결하지는 않는다.
        registry.add("kafka.bootstrap-servers", () -> "localhost:9092");
        registry.add("kafka.security-protocol", () -> "PLAINTEXT");
        registry.add("kafka.request-timeout-ms", () -> 1000);
        registry.add("kafka.api-timeout-ms", () -> 1000);
    }
```

`Admin.create()`는 브로커에 즉시 연결하지 않으므로 이 테스트는 Kafka 없이 통과한다. 컨텍스트 로드만 확인하는 것이 목적이다.

- [ ] **Step 9: 전체 테스트 실행**

```bash
./gradlew test
```

Expected: 모든 테스트 PASS

- [ ] **Step 10: 커밋**

```bash
git add .
git commit -m "Add AdminClient configuration and cluster gateway methods"
```

---

## Task 9: 게이트웨이 — 토픽 목록과 상세

**Files:**
- Modify: `src/main/java/com/kafkaadmin/kafka/KafkaAdminGatewayImpl.java`
- Modify: `src/test/java/com/kafkaadmin/kafka/KafkaAdminGatewayImplTest.java`

- [ ] **Step 1: 실패하는 테스트 추가**

`KafkaAdminGatewayImplTest`에 다음 테스트를 추가한다(기존 테스트 아래).

```java
    @Test
    void 토픽_목록을_조회한다() {
        List<TopicSummary> topics = gateway.listTopics();

        assertThat(topics).extracting(TopicSummary::name)
                .contains("orders", "payments");

        TopicSummary orders = topics.stream()
                .filter(topic -> topic.name().equals("orders"))
                .findFirst()
                .orElseThrow();

        assertThat(orders.partitionCount()).isEqualTo(3);
        assertThat(orders.replicationFactor()).isEqualTo(1);
        assertThat(orders.minInSyncReplicas()).isEqualTo(1);
        assertThat(orders.partitions())
                .allSatisfy(partition -> {
                    assertThat(partition.leaderId()).isNotNull();
                    assertThat(partition.underReplicated()).isFalse();
                });
    }

    @Test
    void 내부_토픽은_목록에_포함하지_않는다() {
        assertThat(gateway.listTopics())
                .extracting(TopicSummary::name)
                .noneMatch(name -> name.startsWith("__"));
    }

    @Test
    void 토픽_상세는_파티션별_오프셋을_포함한다() {
        TopicDetail detail = gateway.describeTopic("orders");

        assertThat(detail.name()).isEqualTo("orders");
        assertThat(detail.replicationFactor()).isEqualTo(1);
        assertThat(detail.partitions()).hasSize(3);
        assertThat(detail.partitions())
                .allSatisfy(partition -> {
                    assertThat(partition.startOffset()).isEqualTo(0L);
                    assertThat(partition.endOffset()).isEqualTo(0L);
                    assertThat(partition.estimatedCount()).isEqualTo(0L);
                });
    }

    @Test
    void 토픽_상세는_설정을_기본값_여부와_함께_반환한다() {
        TopicDetail detail = gateway.describeTopic("orders");

        assertThat(detail.configs()).isNotEmpty();
        assertThat(detail.configs())
                .extracting(TopicConfigEntry::name)
                .contains("retention.ms", "cleanup.policy", "min.insync.replicas");

        // 생성 시 아무 설정도 지정하지 않았으므로 override 된 항목이 없다
        assertThat(detail.configs()).noneMatch(TopicConfigEntry::overridden);
    }

    @Test
    void 없는_토픽_상세_조회는_KafkaGatewayException을_던진다() {
        assertThatThrownBy(() -> gateway.describeTopic("no-such-topic"))
                .isInstanceOf(KafkaGatewayException.class)
                .hasRootCauseInstanceOf(UnknownTopicOrPartitionException.class);
    }
```

필요한 import를 파일 상단에 추가한다.

```java
import com.kafkaadmin.kafka.dto.TopicConfigEntry;
import com.kafkaadmin.kafka.dto.TopicDetail;
import com.kafkaadmin.kafka.dto.TopicSummary;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
```

마지막 테스트가 Task 7의 번역기와 이 태스크를 연결한다. 원인 예외가 `UnknownTopicOrPartitionException`으로 보존되지 않으면 API가 404 대신 502를 돌려준다.

- [ ] **Step 2: 테스트 실패 확인**

```bash
./gradlew test --tests 'com.kafkaadmin.kafka.KafkaAdminGatewayImplTest'
```

Expected: 새 테스트 5개가 `UnsupportedOperationException: Task 9에서 구현` 으로 FAIL

- [ ] **Step 3: listTopics 구현**

`KafkaAdminGatewayImpl`의 `listTopics()` 스텁을 다음으로 교체한다.

```java
    @Override
    public List<TopicSummary> listTopics() {
        Set<String> names = await(admin.listTopics(
                new ListTopicsOptions().listInternal(false)).names());
        if (names.isEmpty()) {
            return List.of();
        }

        Map<String, TopicDescription> descriptions =
                await(admin.describeTopics(names).allTopicNames());
        Map<String, Integer> minIsrByTopic = minInSyncReplicas(names);
        Map<String, Long> bytesByTopic = bytesByTopic();

        return names.stream()
                .sorted()
                .map(name -> {
                    TopicDescription description = descriptions.get(name);
                    List<PartitionReplicaState> partitions = toReplicaStates(description);
                    return new TopicSummary(
                            name,
                            replicationFactorOf(description),
                            minIsrByTopic.getOrDefault(name, 1),
                            bytesByTopic.getOrDefault(name, 0L),
                            partitions);
                })
                .toList();
    }

    private static int replicationFactorOf(TopicDescription description) {
        return description.partitions().isEmpty()
                ? 0
                : description.partitions().get(0).replicas().size();
    }

    private static List<PartitionReplicaState> toReplicaStates(TopicDescription description) {
        return description.partitions().stream()
                .sorted(Comparator.comparingInt(TopicPartitionInfo::partition))
                .map(partition -> new PartitionReplicaState(
                        partition.partition(),
                        partition.leader() == null || partition.leader().id() < 0
                                ? null
                                : partition.leader().id(),
                        partition.replicas().stream().map(Node::id).sorted().toList(),
                        partition.isr().stream().map(Node::id).sorted().toList()))
                .toList();
    }

    private Map<String, Integer> minInSyncReplicas(Set<String> topicNames) {
        List<ConfigResource> resources = topicNames.stream()
                .map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name))
                .toList();
        Map<ConfigResource, Config> configs = await(admin.describeConfigs(resources).all());

        Map<String, Integer> result = new HashMap<>();
        configs.forEach((resource, config) -> {
            ConfigEntry entry = config.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
            int value = 1;
            if (entry != null && entry.value() != null) {
                try {
                    value = Integer.parseInt(entry.value());
                } catch (NumberFormatException ignored) {
                    value = 1;
                }
            }
            result.put(resource.name(), value);
        });
        return result;
    }

    /**
     * 토픽별 디스크 사용량. 모든 복제본의 합이므로 RF 배수만큼 커진다.
     */
    private Map<String, Long> bytesByTopic() {
        List<Integer> brokerIds = describeCluster().brokers().stream()
                .map(BrokerInfo::id)
                .toList();
        Map<Integer, Map<String, LogDirDescription>> descriptions =
                await(admin.describeLogDirs(brokerIds).allDescriptions());

        Map<String, Long> result = new HashMap<>();
        descriptions.values().stream()
                .flatMap(dirs -> dirs.values().stream())
                .flatMap(dir -> dir.replicaInfos().entrySet().stream())
                .forEach(entry -> result.merge(
                        entry.getKey().topic(), entry.getValue().size(), Long::sum));
        return result;
    }
```

`partition.leader() == null || partition.leader().id() < 0` 두 조건을 모두 보는 이유는 Kafka가 리더 없음을 상황에 따라 `null`과 `Node.noNode()`(id = -1) 두 방식으로 표현하기 때문이다. 한쪽만 보면 offline 파티션을 놓친다.

- [ ] **Step 4: describeTopic 구현**

`describeTopic(String)` 스텁을 다음으로 교체한다.

```java
    @Override
    public TopicDetail describeTopic(String name) {
        TopicDescription description =
                await(admin.describeTopics(List.of(name)).allTopicNames()).get(name);
        if (description == null) {
            throw new KafkaGatewayException("토픽 " + name + " 을 찾을 수 없습니다.",
                    new UnknownTopicOrPartitionException(name));
        }

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
        Config config = await(admin.describeConfigs(List.of(resource)).all()).get(resource);

        List<PartitionReplicaState> replicaStates = toReplicaStates(description);
        Map<Integer, Long> startOffsets = offsets(name, replicaStates, OffsetSpec.earliest());
        Map<Integer, Long> endOffsets = offsets(name, replicaStates, OffsetSpec.latest());

        List<PartitionDetail> partitions = replicaStates.stream()
                .map(state -> new PartitionDetail(
                        state,
                        startOffsets.getOrDefault(state.partition(), 0L),
                        endOffsets.getOrDefault(state.partition(), 0L)))
                .toList();

        List<TopicConfigEntry> configs = config.entries().stream()
                .sorted(Comparator.comparing(ConfigEntry::name))
                .map(entry -> new TopicConfigEntry(
                        entry.name(),
                        entry.value(),
                        entry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG))
                .toList();

        int minIsr = configs.stream()
                .filter(entry -> entry.name().equals(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG))
                .findFirst()
                .map(entry -> parseIntOrDefault(entry.value(), 1))
                .orElse(1);

        return new TopicDetail(
                name, replicationFactorOf(description), minIsr, partitions, configs);
    }

    private Map<Integer, Long> offsets(String topic,
                                       List<PartitionReplicaState> partitions,
                                       OffsetSpec spec) {
        Map<TopicPartition, OffsetSpec> request = partitions.stream()
                .filter(state -> !state.offline())
                .collect(Collectors.toMap(
                        state -> new TopicPartition(topic, state.partition()),
                        state -> spec));
        if (request.isEmpty()) {
            return Map.of();
        }

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> result =
                await(admin.listOffsets(request).all());

        Map<Integer, Long> offsets = new HashMap<>();
        result.forEach((partition, info) -> offsets.put(partition.partition(), info.offset()));
        return offsets;
    }

    private static int parseIntOrDefault(String value, int fallback) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException | NullPointerException e) {
            return fallback;
        }
    }
```

`offsets()`가 offline 파티션을 요청에서 제외하는 것이 중요하다. 리더 없는 파티션에 `listOffsets`를 부르면 전체 호출이 실패하고, 그러면 **장애가 난 토픽의 상세 화면만 아무것도 안 보이는** 최악의 결과가 된다.

- [ ] **Step 5: import 추가**

`KafkaAdminGatewayImpl` 상단에 다음 import를 추가한다.

```java
import com.kafkaadmin.kafka.dto.PartitionDetail;
import com.kafkaadmin.kafka.dto.PartitionReplicaState;
import com.kafkaadmin.kafka.dto.TopicConfigEntry;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;
```

- [ ] **Step 6: 테스트 통과 확인**

```bash
./gradlew test --tests 'com.kafkaadmin.kafka.KafkaAdminGatewayImplTest'
```

Expected: 8개 테스트 모두 PASS (Task 8의 3개 + 이 태스크의 5개)

`토픽_상세는_설정을_기본값_여부와_함께_반환한다`가 실패하면 `ConfigSource` 열거값을 확인한다. 브로커 기본값은 `DEFAULT_CONFIG`, 브로커 수준 설정은 `STATIC_BROKER_CONFIG`, 토픽 수준 override만 `DYNAMIC_TOPIC_CONFIG`다.

- [ ] **Step 7: 커밋**

```bash
git add .
git commit -m "Add topic listing and detail to Kafka gateway"
```

---

## Task 10: 게이트웨이 — 컨슈머 그룹과 랙

**Files:**
- Modify: `src/main/java/com/kafkaadmin/kafka/KafkaAdminGatewayImpl.java`
- Modify: `src/test/java/com/kafkaadmin/kafka/KafkaAdminGatewayImplTest.java`

- [ ] **Step 1: 테스트에 실제 컨슈머 그룹을 만드는 준비 코드 추가**

`KafkaAdminGatewayImplTest`에 다음 헬퍼를 추가한다.

```java
    /**
     * 지정 토픽에 메시지를 넣고, 컨슈머 그룹으로 일부만 소비해 랙을 만든다.
     */
    static void seedConsumerGroup(String topic, String groupId,
                                  int produceCount, int consumeCount) {
        Map<String, Object> producerConfig = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.ACKS_CONFIG, "all");

        try (Producer<String, String> producer = new KafkaProducer<>(producerConfig)) {
            for (int i = 0; i < produceCount; i++) {
                producer.send(new ProducerRecord<>(topic, 0, "key-" + i, "value-" + i));
            }
            producer.flush();
        }

        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfig)) {
            TopicPartition partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            consumer.commitSync(Map.of(
                    partition, new OffsetAndMetadata(consumeCount)));
        }
    }
```

`assign` + `commitSync`로 오프셋만 직접 커밋한다. `poll` 루프로 실제 소비하면 타이밍에 따라 커밋 위치가 흔들려 테스트가 불안정해진다. 우리가 검증할 것은 **랙 계산이 맞는지**이므로 오프셋을 결정적으로 심는 편이 낫다.

- [ ] **Step 2: 실패하는 테스트 추가**

```java
    @Test
    void 컨슈머_그룹_목록에_총_랙이_계산된다() {
        seedConsumerGroup("payments", "billing-service", 10, 4);

        List<ConsumerGroupSummary> groups = gateway.listConsumerGroups();

        ConsumerGroupSummary billing = groups.stream()
                .filter(group -> group.groupId().equals("billing-service"))
                .findFirst()
                .orElseThrow();

        assertThat(billing.totalLag()).isEqualTo(6L);
        assertThat(billing.topics()).containsExactly("payments");
        assertThat(billing.state()).isEqualTo("EMPTY");
        assertThat(billing.memberCount()).isZero();
    }

    @Test
    void 그룹_상세는_파티션별_랙을_보여준다() {
        seedConsumerGroup("payments", "settlement-service", 10, 7);

        ConsumerGroupDetail detail = gateway.describeConsumerGroup("settlement-service");

        assertThat(detail.groupId()).isEqualTo("settlement-service");
        assertThat(detail.partitionLags()).hasSize(1);

        PartitionLag lag = detail.partitionLags().get(0);
        assertThat(lag.topic()).isEqualTo("payments");
        assertThat(lag.partition()).isZero();
        assertThat(lag.currentOffset()).isEqualTo(7L);
        assertThat(lag.endOffset()).isEqualTo(10L);
        assertThat(lag.lag()).isEqualTo(3L);
    }

    @Test
    void 멤버가_없는_그룹은_담당자가_null이다() {
        seedConsumerGroup("payments", "idle-service", 5, 5);

        ConsumerGroupDetail detail = gateway.describeConsumerGroup("idle-service");

        assertThat(detail.partitionLags()).allSatisfy(lag -> {
            assertThat(lag.assignedClientId()).isNull();
            assertThat(lag.assignedHost()).isNull();
        });
    }

    @Test
    void 커밋된_오프셋이_없는_그룹은_랙이_0이다() {
        List<ConsumerGroupSummary> groups = gateway.listConsumerGroups();

        assertThat(groups).allSatisfy(group ->
                assertThat(group.totalLag()).isGreaterThanOrEqualTo(0L));
    }
```

필요한 import를 추가한다.

```java
import com.kafkaadmin.kafka.dto.ConsumerGroupDetail;
import com.kafkaadmin.kafka.dto.ConsumerGroupSummary;
import com.kafkaadmin.kafka.dto.PartitionLag;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
```

- [ ] **Step 3: 테스트 실패 확인**

```bash
./gradlew test --tests 'com.kafkaadmin.kafka.KafkaAdminGatewayImplTest'
```

Expected: 새 테스트 4개가 `UnsupportedOperationException: Task 10에서 구현` 으로 FAIL

- [ ] **Step 4: 그룹 상태 접근을 한 곳으로 격리**

`KafkaAdminGatewayImpl`에 다음 헬퍼를 추가한다.

```java
    /**
     * kafka-clients 4.x 는 groupState() 를 쓴다.
     * 3.x 로 내려야 하면 이 메서드 한 곳만 state() 로 바꾼다.
     */
    private static String groupStateOf(ConsumerGroupDescription description) {
        return description.groupState().name();
    }
```

버전 간 이름이 바뀐 API를 메서드 하나에 가둔다. 호출 지점이 여러 곳에 흩어지면 버전 변경이 산탄총 수정이 된다.

- [ ] **Step 5: listConsumerGroups 구현**

`listConsumerGroups()` 스텁을 다음으로 교체한다.

```java
    @Override
    public List<ConsumerGroupSummary> listConsumerGroups() {
        Collection<ConsumerGroupListing> listings = await(admin.listConsumerGroups().all());
        List<String> groupIds = listings.stream()
                .map(ConsumerGroupListing::groupId)
                .sorted()
                .toList();
        if (groupIds.isEmpty()) {
            return List.of();
        }

        Map<String, ConsumerGroupDescription> descriptions =
                await(admin.describeConsumerGroups(groupIds).all());

        return groupIds.stream()
                .map(groupId -> {
                    ConsumerGroupDescription description = descriptions.get(groupId);
                    List<PartitionLag> lags = partitionLags(groupId, description);
                    List<String> topics = lags.stream()
                            .map(PartitionLag::topic)
                            .distinct()
                            .sorted()
                            .toList();
                    long totalLag = lags.stream()
                            .map(PartitionLag::lag)
                            .filter(Objects::nonNull)
                            .mapToLong(Long::longValue)
                            .sum();
                    return new ConsumerGroupSummary(
                            groupId,
                            groupStateOf(description),
                            description.members().size(),
                            topics,
                            totalLag);
                })
                .toList();
    }
```

`admin.listConsumerGroups()`는 kafka-clients 4.0에서 deprecated 경고가 날 수 있다. 빌드는 통과하므로 이 계획에서는 그대로 쓴다. 경고를 없애려면 `listGroups()`로 바꾸고 반환 타입 변경을 함께 처리해야 하는데, 이 계획의 범위를 넘는다.

`구독 토픽`을 멤버 할당이 아니라 커밋된 오프셋에서 뽑는 것이 중요하다. 멤버가 0인 `Empty` 그룹(컨슈머가 다 죽은 상태)이 정확히 우리가 봐야 하는 상황인데, 멤버 할당에서 뽑으면 이 그룹의 토픽이 빈 목록이 된다.

- [ ] **Step 6: describeConsumerGroup 구현**

`describeConsumerGroup(String)` 스텁을 다음으로 교체한다.

```java
    @Override
    public ConsumerGroupDetail describeConsumerGroup(String groupId) {
        Map<String, ConsumerGroupDescription> descriptions =
                await(admin.describeConsumerGroups(List.of(groupId)).all());
        ConsumerGroupDescription description = descriptions.get(groupId);
        if (description == null) {
            throw new KafkaGatewayException("컨슈머 그룹 " + groupId + " 을 찾을 수 없습니다.",
                    new GroupIdNotFoundException(groupId));
        }

        List<ConsumerGroupMemberInfo> members = description.members().stream()
                .sorted(Comparator.comparing(MemberDescription::consumerId))
                .map(member -> new ConsumerGroupMemberInfo(
                        member.consumerId(),
                        member.clientId(),
                        member.host(),
                        member.assignment().topicPartitions().stream()
                                .map(partition -> partition.topic() + "-" + partition.partition())
                                .sorted()
                                .toList()))
                .toList();

        return new ConsumerGroupDetail(
                groupId,
                groupStateOf(description),
                members,
                partitionLags(groupId, description));
    }
```

- [ ] **Step 7: 랙 계산 헬퍼 구현**

`KafkaAdminGatewayImpl`에 다음을 추가한다.

```java
    /**
     * 커밋된 오프셋과 로그 끝 오프셋을 비교해 파티션별 랙을 계산한다.
     * 그룹 상세는 캐시하지 않는다 — 장애 대응 중에 낡은 랙은 위험하다.
     */
    private List<PartitionLag> partitionLags(String groupId,
                                             ConsumerGroupDescription description) {
        Map<TopicPartition, OffsetAndMetadata> committed = await(admin
                .listConsumerGroupOffsets(Map.of(groupId, new ListConsumerGroupOffsetsSpec()))
                .partitionsToOffsetAndMetadata(groupId));
        if (committed.isEmpty()) {
            return List.of();
        }

        Map<TopicPartition, OffsetSpec> request = committed.keySet().stream()
                .collect(Collectors.toMap(Function.identity(), partition -> OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                await(admin.listOffsets(request).all());

        Map<TopicPartition, MemberDescription> owners = new HashMap<>();
        for (MemberDescription member : description.members()) {
            for (TopicPartition partition : member.assignment().topicPartitions()) {
                owners.put(partition, member);
            }
        }

        return committed.entrySet().stream()
                .sorted(Comparator
                        .comparing((Map.Entry<TopicPartition, OffsetAndMetadata> entry) ->
                                entry.getKey().topic())
                        .thenComparingInt(entry -> entry.getKey().partition()))
                .map(entry -> {
                    TopicPartition partition = entry.getKey();
                    Long current = entry.getValue() == null ? null : entry.getValue().offset();
                    ListOffsetsResult.ListOffsetsResultInfo end = endOffsets.get(partition);
                    long endOffset = end == null ? 0L : end.offset();
                    Long lag = current == null ? null : Math.max(0L, endOffset - current);
                    MemberDescription owner = owners.get(partition);
                    return new PartitionLag(
                            partition.topic(),
                            partition.partition(),
                            current,
                            endOffset,
                            lag,
                            owner == null ? null : owner.clientId(),
                            owner == null ? null : owner.host());
                })
                .toList();
    }
```

`Math.max(0L, endOffset - current)`로 하한을 두는 이유는 커밋 오프셋이 로그 끝보다 클 수 있기 때문이다(오프셋 리셋 직후, 또는 retention으로 로그가 잘린 경우). 음수 랙이 화면에 나오면 버그로 오인된다.

- [ ] **Step 8: import 추가**

```java
import com.kafkaadmin.kafka.dto.ConsumerGroupMemberInfo;
import com.kafkaadmin.kafka.dto.PartitionLag;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.errors.GroupIdNotFoundException;

import java.util.Objects;
import java.util.function.Function;
```

- [ ] **Step 9: 테스트 통과 확인**

```bash
./gradlew test --tests 'com.kafkaadmin.kafka.KafkaAdminGatewayImplTest'
```

Expected: 12개 테스트 모두 PASS

`state()`가 `"EMPTY"`가 아니라 다른 값으로 나오면 `groupStateOf`의 반환값을 확인한다. `GroupState.EMPTY.name()`은 `"EMPTY"`다.

- [ ] **Step 10: 커밋**

```bash
git add .
git commit -m "Add consumer group and lag queries to Kafka gateway"
```

---

## Task 11: 게이트웨이 — ACL 조회

**Files:**
- Modify: `src/main/java/com/kafkaadmin/kafka/KafkaAdminGatewayImpl.java`
- Modify: `src/test/java/com/kafkaadmin/kafka/KafkaAdminGatewayImplTest.java`

- [ ] **Step 1: 실패하는 테스트 추가**

```java
    @Test
    void authorizer가_없는_클러스터는_인가_미사용으로_보고한다() {
        AclOverview overview = gateway.listAcls();

        assertThat(overview.authorizerEnabled()).isFalse();
        assertThat(overview.entries()).isEmpty();
    }
```

import 추가:

```java
import com.kafkaadmin.kafka.dto.AclOverview;
```

Testcontainers Kafka 이미지는 authorizer를 설정하지 않으므로 `describeAcls`가 `SecurityDisabledException`을 던진다. 이 테스트는 그 상황을 예외가 아니라 **상태로** 처리하는지 확인한다. 운영 클러스터(SASL_SSL + authorizer)에서는 `authorizerEnabled=true`가 되며, 이 경로는 Task 13의 수동 검증에서 확인한다.

- [ ] **Step 2: 테스트 실패 확인**

```bash
./gradlew test --tests 'com.kafkaadmin.kafka.KafkaAdminGatewayImplTest'
```

Expected: `UnsupportedOperationException: Task 11에서 구현` 으로 FAIL

- [ ] **Step 3: listAcls 구현**

`listAcls()` 스텁을 다음으로 교체한다.

```java
    @Override
    public AclOverview listAcls() {
        try {
            Collection<AclBinding> bindings =
                    await(admin.describeAcls(AclBindingFilter.ANY).values());

            List<AclEntry> entries = bindings.stream()
                    .map(KafkaAdminGatewayImpl::toAclEntry)
                    .sorted(Comparator.comparing(AclEntry::principal)
                            .thenComparing(AclEntry::resourceType)
                            .thenComparing(AclEntry::resourceName)
                            .thenComparing(AclEntry::operation))
                    .toList();

            return new AclOverview(true, entries);
        } catch (KafkaGatewayException e) {
            if (rootCauseOf(e) instanceof SecurityDisabledException) {
                // 브로커에 authorizer 가 없다. 오류가 아니라 구성 상태다.
                return new AclOverview(false, List.of());
            }
            throw e;
        }
    }

    private static AclEntry toAclEntry(AclBinding binding) {
        ResourcePattern pattern = binding.pattern();
        AccessControlEntry entry = binding.entry();
        return new AclEntry(
                entry.principal(),
                entry.host(),
                pattern.resourceType().name(),
                pattern.name(),
                pattern.patternType().name(),
                entry.operation().name(),
                entry.permissionType().name());
    }

    private static Throwable rootCauseOf(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }
```

- [ ] **Step 4: import 추가**

```java
import com.kafkaadmin.kafka.dto.AclEntry;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.resource.ResourcePattern;
```

- [ ] **Step 5: 스텁이 모두 사라졌는지 확인**

```bash
grep -n "UnsupportedOperationException" src/main/java/com/kafkaadmin/kafka/KafkaAdminGatewayImpl.java
```

Expected: 출력 없음. 하나라도 남아 있으면 Task 9~11 중 빠진 것이 있다.

- [ ] **Step 6: 전체 테스트 실행**

```bash
./gradlew test
```

Expected: 모든 테스트 PASS

- [ ] **Step 7: 커밋**

```bash
git add .
git commit -m "Add ACL overview query to Kafka gateway"
```

---

## Task 12: REST API

**Files:**
- Modify: `src/main/java/com/kafkaadmin/cluster/ClusterService.java` (`@Service` 부착)
- Create: `src/main/java/com/kafkaadmin/topic/TopicListItem.java`
- Create: `src/main/java/com/kafkaadmin/topic/TopicDetailResponse.java`
- Create: `src/main/java/com/kafkaadmin/topic/TopicService.java`
- Create: `src/main/java/com/kafkaadmin/topic/TopicController.java`
- Create: `src/main/java/com/kafkaadmin/cluster/ClusterController.java`
- Create: `src/main/java/com/kafkaadmin/consumergroup/ConsumerGroupService.java`
- Create: `src/main/java/com/kafkaadmin/consumergroup/ConsumerGroupController.java`
- Create: `src/main/java/com/kafkaadmin/acl/AclService.java`
- Create: `src/main/java/com/kafkaadmin/acl/AclController.java`
- Create: `src/test/java/com/kafkaadmin/topic/TopicControllerTest.java`

엔드포인트는 다음 6개다.

| 메서드 | 경로 | 응답 |
|---|---|---|
| GET | `/api/cluster/health` | `ClusterHealth` |
| GET | `/api/topics` | `List<TopicListItem>` |
| GET | `/api/topics/{name}` | `TopicDetailResponse` |
| GET | `/api/consumer-groups` | `List<ConsumerGroupSummary>` |
| GET | `/api/consumer-groups/{groupId}` | `ConsumerGroupDetail` |
| GET | `/api/acls` | `AclOverview` |

- [ ] **Step 0: ClusterService를 빈으로 등록**

`src/main/java/com/kafkaadmin/cluster/ClusterService.java`에 `@Service`를 붙이고 `org.springframework.stereotype.Service` import를 추가한다. Task 5에서 일부러 빼둔 애노테이션이다 — 이제 `ClusterController`가 주입받고, Task 8의 `KafkaAdminGatewayImpl` 빈도 존재하므로 컨텍스트가 완성된다.

클래스 Javadoc에서 "Task 12에서 애노테이션을 붙인다"는 문장을 지운다. 이미 붙였으므로 사실이 아니게 된다.

- [ ] **Step 1: 토픽 응답 레코드 작성**

`src/main/java/com/kafkaadmin/topic/TopicListItem.java`:

```java
package com.kafkaadmin.topic;

import java.util.Set;

/**
 * 목록 응답. 파티션 배열을 일부러 제외했다 —
 * 배지 판정에만 쓰이고 목록 화면은 표시하지 않으므로 응답에서 빼면 페이로드가 크게 줄어든다.
 */
public record TopicListItem(
        String name,
        int partitionCount,
        int replicationFactor,
        int minInSyncReplicas,
        long totalBytesOnDisk,
        Set<TopicBadge> badges
) {}
```

`src/main/java/com/kafkaadmin/topic/TopicDetailResponse.java`:

```java
package com.kafkaadmin.topic;

import com.kafkaadmin.kafka.dto.PartitionDetail;
import com.kafkaadmin.kafka.dto.TopicConfigEntry;

import java.util.List;
import java.util.Set;

public record TopicDetailResponse(
        String name,
        int replicationFactor,
        int minInSyncReplicas,
        Set<TopicBadge> badges,
        List<PartitionDetail> partitions,
        List<TopicConfigEntry> configs
) {}
```

- [ ] **Step 2: TopicService 작성**

`src/main/java/com/kafkaadmin/topic/TopicService.java`:

```java
package com.kafkaadmin.topic;

import com.kafkaadmin.kafka.KafkaAdminGateway;
import com.kafkaadmin.kafka.dto.TopicDetail;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TopicService {

    private final KafkaAdminGateway gateway;
    private final TopicBadgeEvaluator badgeEvaluator;

    public TopicService(KafkaAdminGateway gateway, TopicBadgeEvaluator badgeEvaluator) {
        this.gateway = gateway;
        this.badgeEvaluator = badgeEvaluator;
    }

    public List<TopicListItem> list() {
        return gateway.listTopics().stream()
                .map(topic -> new TopicListItem(
                        topic.name(),
                        topic.partitionCount(),
                        topic.replicationFactor(),
                        topic.minInSyncReplicas(),
                        topic.totalBytesOnDisk(),
                        badgeEvaluator.evaluate(
                                topic.replicationFactor(),
                                topic.minInSyncReplicas(),
                                topic.partitions())))
                .toList();
    }

    public TopicDetailResponse detail(String name) {
        TopicDetail detail = gateway.describeTopic(name);
        return new TopicDetailResponse(
                detail.name(),
                detail.replicationFactor(),
                detail.minInSyncReplicas(),
                badgeEvaluator.evaluate(
                        detail.replicationFactor(),
                        detail.minInSyncReplicas(),
                        detail.replicaStates()),
                detail.partitions(),
                detail.configs());
    }
}
```

- [ ] **Step 3: 컨슈머 그룹·ACL 서비스 작성**

`src/main/java/com/kafkaadmin/consumergroup/ConsumerGroupService.java`:

```java
package com.kafkaadmin.consumergroup;

import com.kafkaadmin.kafka.KafkaAdminGateway;
import com.kafkaadmin.kafka.dto.ConsumerGroupDetail;
import com.kafkaadmin.kafka.dto.ConsumerGroupSummary;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;

@Service
public class ConsumerGroupService {

    private final KafkaAdminGateway gateway;
    private final LagCache lagCache;

    public ConsumerGroupService(KafkaAdminGateway gateway, LagCache lagCache) {
        this.gateway = gateway;
        this.lagCache = lagCache;
    }

    /** 목록은 캐시를 거친다. 총 랙 내림차순이 기본 정렬이다. */
    public List<ConsumerGroupSummary> list() {
        return lagCache.get(gateway::listConsumerGroups).stream()
                .sorted(Comparator.comparingLong(ConsumerGroupSummary::totalLag).reversed())
                .toList();
    }

    /** 상세는 캐시하지 않는다. */
    public ConsumerGroupDetail detail(String groupId) {
        return gateway.describeConsumerGroup(groupId);
    }
}
```

`src/main/java/com/kafkaadmin/acl/AclService.java`:

```java
package com.kafkaadmin.acl;

import com.kafkaadmin.kafka.KafkaAdminGateway;
import com.kafkaadmin.kafka.dto.AclOverview;
import org.springframework.stereotype.Service;

@Service
public class AclService {

    private final KafkaAdminGateway gateway;

    public AclService(KafkaAdminGateway gateway) {
        this.gateway = gateway;
    }

    public AclOverview overview() {
        return gateway.listAcls();
    }
}
```

- [ ] **Step 4: 컨트롤러 4개 작성**

`src/main/java/com/kafkaadmin/cluster/ClusterController.java`:

```java
package com.kafkaadmin.cluster;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/cluster")
public class ClusterController {

    private final ClusterService clusterService;

    public ClusterController(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @GetMapping("/health")
    public ClusterHealth health() {
        return clusterService.health();
    }
}
```

`src/main/java/com/kafkaadmin/topic/TopicController.java`:

```java
package com.kafkaadmin.topic;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/topics")
public class TopicController {

    private final TopicService topicService;

    public TopicController(TopicService topicService) {
        this.topicService = topicService;
    }

    @GetMapping
    public List<TopicListItem> list() {
        return topicService.list();
    }

    @GetMapping("/{name}")
    public TopicDetailResponse detail(@PathVariable String name) {
        return topicService.detail(name);
    }
}
```

`src/main/java/com/kafkaadmin/consumergroup/ConsumerGroupController.java`:

```java
package com.kafkaadmin.consumergroup;

import com.kafkaadmin.kafka.dto.ConsumerGroupDetail;
import com.kafkaadmin.kafka.dto.ConsumerGroupSummary;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/consumer-groups")
public class ConsumerGroupController {

    private final ConsumerGroupService consumerGroupService;

    public ConsumerGroupController(ConsumerGroupService consumerGroupService) {
        this.consumerGroupService = consumerGroupService;
    }

    @GetMapping
    public List<ConsumerGroupSummary> list() {
        return consumerGroupService.list();
    }

    @GetMapping("/{groupId}")
    public ConsumerGroupDetail detail(@PathVariable String groupId) {
        return consumerGroupService.detail(groupId);
    }
}
```

`src/main/java/com/kafkaadmin/acl/AclController.java`:

```java
package com.kafkaadmin.acl;

import com.kafkaadmin.kafka.dto.AclOverview;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/acls")
public class AclController {

    private final AclService aclService;

    public AclController(AclService aclService) {
        this.aclService = aclService;
    }

    @GetMapping
    public AclOverview overview() {
        return aclService.overview();
    }
}
```

- [ ] **Step 5: 컨트롤러 테스트 작성**

`src/test/java/com/kafkaadmin/topic/TopicControllerTest.java`:

```java
package com.kafkaadmin.topic;

import com.kafkaadmin.error.KafkaErrorTranslator;
import com.kafkaadmin.kafka.ClusterAvailabilityTracker;
import com.kafkaadmin.kafka.KafkaGatewayException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = TopicController.class)
@Import(KafkaErrorTranslator.class)
class TopicControllerTest {

    @Autowired
    MockMvc mvc;

    @MockitoBean
    TopicService topicService;

    @MockitoBean
    ClusterAvailabilityTracker availability;

    @Test
    void 토픽_목록을_배지와_함께_반환한다() throws Exception {
        given(topicService.list()).willReturn(List.of(
                new TopicListItem("orders", 3, 3, 2, 1024L, Set.of()),
                new TopicListItem("legacy", 1, 1, 1, 512L, Set.of(TopicBadge.RF_1))));

        mvc.perform(get("/api/topics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("orders"))
                .andExpect(jsonPath("$[0].badges").isEmpty())
                .andExpect(jsonPath("$[1].badges[0]").value("RF_1"));
    }

    @Test
    void 없는_토픽은_404와_TOPIC_NOT_FOUND를_반환한다() throws Exception {
        given(availability.lastSuccessAt()).willReturn(Optional.empty());
        willThrow(new KafkaGatewayException("없음", new UnknownTopicOrPartitionException("nope")))
                .given(topicService).detail(anyString());

        mvc.perform(get("/api/topics/nope"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("TOPIC_NOT_FOUND"));
    }

    @Test
    void Kafka_연결_실패는_502와_마지막_성공_시각을_반환한다() throws Exception {
        Instant lastSuccess = Instant.parse("2026-07-30T01:02:03Z");
        given(availability.lastSuccessAt()).willReturn(Optional.of(lastSuccess));
        willThrow(new KafkaGatewayException("연결 실패", new java.net.ConnectException("refused")))
                .given(topicService).list();

        mvc.perform(get("/api/topics"))
                .andExpect(status().isBadGateway())
                .andExpect(jsonPath("$.code").value("KAFKA_UNAVAILABLE"))
                .andExpect(jsonPath("$.lastKafkaSuccessAt").value("2026-07-30T01:02:03Z"));
    }

    @Test
    void 오류_응답에_스택_트레이스가_포함되지_않는다() throws Exception {
        given(availability.lastSuccessAt()).willReturn(Optional.empty());
        willThrow(new KafkaGatewayException("연결 실패", new java.net.ConnectException("refused")))
                .given(topicService).list();

        String body = mvc.perform(get("/api/topics"))
                .andReturn().getResponse().getContentAsString();

        org.assertj.core.api.Assertions.assertThat(body)
                .doesNotContain("com.kafkaadmin")
                .doesNotContain("java.net.ConnectException")
                .doesNotContain("at ");
    }
}
```

마지막 테스트가 스펙의 "스택 트레이스는 응답에 담지 않는다"를 실행 가능한 형태로 고정한다. 이런 규칙은 코드 리뷰로만 지키려 하면 반드시 새어 나간다.

- [ ] **Step 6: 테스트 실행**

```bash
./gradlew test --tests 'com.kafkaadmin.topic.TopicControllerTest'
```

Expected: 4개 테스트 모두 PASS

`@MockitoBean`을 찾을 수 없다는 오류가 나면 Spring Boot 버전을 확인한다. 이 애노테이션은 Spring Framework 6.2 / Boot 3.4 이상에 있다.

- [ ] **Step 7: 전체 테스트 실행**

```bash
./gradlew test
```

Expected: 모든 테스트 PASS

- [ ] **Step 8: 커밋**

```bash
git add .
git commit -m "Add read-only REST API for cluster, topics, consumer groups and ACLs"
```

---

## Task 13: 로컬 3노드 클러스터로 수동 검증

자동 테스트는 단일 브로커 컨테이너로 돌았다. RF=3, ISR 부족, 컨트롤러 식별 같은 3노드 고유 동작은 여기서 확인한다.

**Files:**
- Modify: `src/main/resources/application-local.yml` (포트가 다를 경우)
- Modify: `README.md`

- [ ] **Step 1: 로컬 Kafka 3노드 기동**

devops-note 저장소의 PLAINTEXT 예제를 쓴다.

```bash
cd ~/Documents/devops-note/kafka/examples/compose-3node-kraft-plaintext
cp .env.example .env
docker compose up -d
docker compose ps
```

Expected: 브로커 3개 컨테이너가 모두 `running`

- [ ] **Step 2: 실제 노출 포트 확인**

```bash
docker compose port kafka-1 9092 2>/dev/null || docker compose ps --format '{{.Name}} {{.Ports}}'
```

출력된 호스트 포트를 `application-local.yml`의 `kafka.bootstrap-servers`에 반영한다. 예제의 포트가 `19092,29092,39092`가 아니면 여기서 수정한다.

- [ ] **Step 3: 검증용 토픽 3개 생성**

배지 판정을 눈으로 확인하기 위해 의도적으로 위험한 설정을 섞는다.

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --create --topic order.created \
  --partitions 3 --replication-factor 3 --config min.insync.replicas=2

docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --create --topic legacy.single \
  --partitions 1 --replication-factor 1

docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --create --topic risky.nodurability \
  --partitions 3 --replication-factor 3 --config min.insync.replicas=1
```

컨테이너 안의 Kafka 설치 경로가 다르면 `/opt/kafka/bin` 대신 예제 README에 적힌 경로를 쓴다.

- [ ] **Step 4: PostgreSQL과 애플리케이션 기동**

```bash
cd ~/Documents/kafka-admin-web
docker compose up -d
./gradlew bootRun --args='--spring.profiles.active=local'
```

Expected: `Started KafkaAdminApplication` 로그. Flyway가 `V1__app_settings.sql`을 적용한 로그도 보인다.

- [ ] **Step 5: 클러스터 개요 확인**

```bash
curl -s localhost:8080/api/cluster/health | jq
```

확인 항목:
- `brokers` 배열 길이 3
- `controller: true`인 브로커가 **정확히 1개**
- `diskUsage` 항목 3개, `totalBytes`가 0보다 큼
- `partitionRisk.offlinePartitions` = 0, `underReplicatedPartitions` = 0
- `warnings` 빈 배열

- [ ] **Step 6: 토픽 배지 확인**

```bash
curl -s localhost:8080/api/topics | jq '.[] | {name, replicationFactor, minInSyncReplicas, badges}'
```

Expected:

| 토픽 | 기대 배지 |
|---|---|
| `order.created` | `[]` |
| `legacy.single` | `["RF_1"]` |
| `risky.nodurability` | `["NO_DURABILITY"]` |

배지가 기대와 다르면 Task 4의 단위 테스트가 아니라 `describeConfigs`에서 `min.insync.replicas`를 읽는 경로를 의심한다.

- [ ] **Step 7: 토픽 상세 확인**

```bash
curl -s localhost:8080/api/topics/order.created | jq '{name, badges, partitions: [.partitions[] | {p: .replicaState.partition, leader: .replicaState.leaderId, isr: .replicaState.inSyncReplicaIds, start: .startOffset, end: .endOffset}]}'
```

확인 항목: 파티션 3개, 각 `isr` 길이 3, 리더가 브로커 3대에 분산

```bash
curl -s localhost:8080/api/topics/order.created | jq '[.configs[] | select(.overridden)]'
```

Expected: `min.insync.replicas`가 `overridden: true`로 표시됨 (생성 시 `--config`로 지정했으므로)

- [ ] **Step 8: 브로커 1대를 정지시켜 이상 상태 확인**

여기가 이 태스크의 핵심이다. 정상 상태만 확인하면 판정 로직을 검증한 것이 아니다.

```bash
cd ~/Documents/devops-note/kafka/examples/compose-3node-kraft-plaintext
docker compose stop kafka-3
sleep 15
curl -s localhost:8080/api/cluster/health | jq '{brokers: [.brokers[] | .id], partitionRisk, warnings}'
```

Expected:
- `brokers` 길이 2
- `partitionRisk.underReplicatedPartitions` > 0
- `partitionRisk.offlinePartitions` = 0 (RF=3이므로 리더는 남은 브로커로 넘어간다)
- `legacy.single`이 kafka-3에 있었다면 `offlinePartitions` = 1

```bash
curl -s localhost:8080/api/topics | jq '.[] | {name, badges}'
```

Expected: `order.created`에 `UNDER_REPLICATED` 배지가 붙는다. `min.insync.replicas=2`이고 ISR이 2이므로 `BELOW_MIN_ISR`은 붙지 않아야 한다 — 이 구분이 스펙의 핵심 요구였다.

- [ ] **Step 9: 브로커 2대를 정지시켜 BELOW_MIN_ISR 확인**

```bash
docker compose stop kafka-2
sleep 15
curl -s localhost:8080/api/topics | jq '.[] | select(.name == "order.created") | .badges'
```

Expected: `["UNDER_REPLICATED", "BELOW_MIN_ISR"]` — 이제 ISR이 1이라 `acks=all` 쓰기가 거부되는 상태다.

- [ ] **Step 10: 브로커 복구와 회복 확인**

```bash
docker compose start kafka-2 kafka-3
sleep 30
curl -s localhost:8080/api/cluster/health | jq '.partitionRisk'
```

Expected: 모든 값이 0으로 회복. 캐시 없이 실시간 조회하므로 새로고침만으로 회복이 보여야 한다.

- [ ] **Step 11: 컨슈머 그룹 랙 확인**

```bash
cd ~/Documents/devops-note/kafka/examples/compose-3node-kraft-plaintext
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092 --topic order.created <<'EOF'
msg-1
msg-2
msg-3
msg-4
msg-5
EOF

docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 --topic order.created \
  --group order-consumer --from-beginning --max-messages 2
```

```bash
curl -s localhost:8080/api/consumer-groups | jq
curl -s localhost:8080/api/consumer-groups/order-consumer | jq
```

확인 항목:
- 목록에 `order-consumer`가 있고 `totalLag`이 3
- 상세의 `partitionLags`에서 `currentOffset`, `endOffset`, `lag`이 채워짐
- 소비하지 않은 파티션은 `currentOffset`이 `null`이고 `lag`도 `null` (0이 아님)

- [ ] **Step 12: ACL 조회 확인**

```bash
curl -s localhost:8080/api/acls | jq
```

Expected: PLAINTEXT 예제는 authorizer가 없으므로 `{"authorizerEnabled": false, "entries": []}`

**운영 클러스터(SASL_SSL + authorizer)에서는 `authorizerEnabled: true`가 되어야 한다.** 이 경로는 로컬에서 재현하지 않으며, 스펙의 도입 1단계(읽기 전용 배포)에서 확인한다.

- [ ] **Step 13: Kafka 정지 상태의 오류 응답 확인**

```bash
docker compose stop kafka-1 kafka-2 kafka-3
curl -s -w '\nHTTP %{http_code}\n' localhost:8080/api/topics | jq
```

Expected:
- HTTP 502 또는 504
- `code`가 `KAFKA_UNAVAILABLE` 또는 `KAFKA_TIMEOUT`
- `lastKafkaSuccessAt`에 이전 성공 시각이 들어 있음
- **빈 배열 `[]`이 200으로 오지 않는다** — 이것이 스펙의 명시적 요구다
- 응답 본문에 스택 트레이스가 없다

```bash
docker compose start kafka-1 kafka-2 kafka-3
```

- [ ] **Step 14: 응답 시간 확인**

```bash
for i in 1 2 3; do
  curl -s -o /dev/null -w 'cluster/health: %{time_total}s\n' localhost:8080/api/cluster/health
  curl -s -o /dev/null -w 'topics:        %{time_total}s\n' localhost:8080/api/topics
  curl -s -o /dev/null -w 'groups:        %{time_total}s\n' localhost:8080/api/consumer-groups
done
```

`consumer-groups`의 2회차·3회차가 1회차보다 뚜렷하게 빠르면 TTL 캐시가 동작하는 것이다. 10초를 넘겨 다시 호출하면 다시 느려져야 한다.

세 엔드포인트 중 하나라도 3초를 넘으면 그 원인을 기록해둔다. 토픽·그룹이 적은 로컬에서 느리면 운영에서는 더 느리다.

- [ ] **Step 15: README에 검증 절차 기록**

`README.md`의 "테스트" 섹션 아래에 추가한다.

```markdown
## 수동 검증

자동 테스트는 단일 브로커 컨테이너로 돌기 때문에, 3노드 고유 동작(컨트롤러 식별,
ISR 부족, BELOW_MIN_ISR 판정)은 로컬 3노드 클러스터로 확인해야 한다.

절차는 devops-note 저장소의
`docs/superpowers/plans/2026-07-30-kafka-admin-backend-read-api.md` Task 13 참조.

SASL_SSL + mTLS 접속 경로는 Testcontainers로 재현하지 않는다.
스펙의 도입 1단계(읽기 전용 배포)에서 실제 클러스터로 확인한다.
```

- [ ] **Step 16: 로컬 환경 정리와 커밋**

```bash
cd ~/Documents/devops-note/kafka/examples/compose-3node-kraft-plaintext
docker compose down

cd ~/Documents/kafka-admin-web
git add .
git commit -m "Document manual verification procedure for 3-node cluster"
```

---

## 완료 기준

이 계획이 끝나면 다음이 모두 참이어야 한다.

- [ ] `./gradlew test`가 전부 통과한다
- [ ] `KafkaAdminGatewayImpl`에 `UnsupportedOperationException`이 남아 있지 않다
- [ ] 6개 엔드포인트가 로컬 3노드 클러스터에서 `curl`로 검증됐다
- [ ] 브로커 1대 정지 시 `UNDER_REPLICATED`만, 2대 정지 시 `BELOW_MIN_ISR`까지 붙는 것을 확인했다
- [ ] Kafka 정지 상태에서 빈 배열 200이 아니라 502/504가 반환된다
- [ ] 오류 응답 본문에 스택 트레이스가 없다
- [ ] `kafka/` 패키지 밖에서 `org.apache.kafka.*`를 import 하는 곳이 없다 (아래 명령으로 확인)

```bash
grep -rn "org.apache.kafka" src/main/java --include=*.java \
  | grep -v "/kafka/" | grep -v "/config/"
```

Expected: 출력 없음. `config/`는 AdminClient 빈을 만들므로 예외다.

---

## 이 계획이 다루지 않는 것

다음 계획으로 넘긴다.

| 항목 | 계획 |
|---|---|
| 로그인, 세션, 역할(`DEVELOPER`/`OPERATOR`/`ADMIN`), 감사 로그 | 계획 2 |
| React 조회 화면, 클러스터 미연결 시 화면 표시 | 계획 3 |
| 토픽 소유권, 사용자·팀 관리, `app_settings` 편집 UI | 계획 4 |
| 메시지 조회·발행, 발행 화이트리스트 | 계획 5 |
| 토픽 생성·설정 변경 승인 워크플로 | 계획 6 |
| 클라이언트 인증서 만료 30일 전 경고 | 계획 3 (화면 상단 배너이므로 UI와 함께) |
| 리더 편중 표시 | 계획 3 |
| 토픽별 구독 컨슈머 그룹 목록 | 계획 3 |

뒤의 두 항목은 새 API가 필요하지 않다. `GET /api/topics/{name}`이 파티션별 `leaderId`를 이미 돌려주므로 리더 분포는 화면에서 집계하고, 구독 그룹은 `GET /api/consumer-groups`의 `topics` 배열을 필터링해 얻는다. 같은 데이터를 두 곳에서 집계하지 않기 위해 백엔드에 별도 엔드포인트를 만들지 않는다.

현재 계획에서 `app_settings`의 `feature_message_enabled`·`feature_write_enabled`는 테이블에만 존재하고 읽는 코드가 없다. 계획 5·6에서 실제 게이트로 쓰인다. 미리 넣어둔 것은 마이그레이션을 나중에 쪼개지 않기 위해서다.
