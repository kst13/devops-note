# kafka-admin 2단계 (지표 수집 + 알림 이력 + 인증서 감시) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 랙/URP/디스크/브로커 수를 주기 수집해 H2에 이력을 쌓고, 임계치 초과·인증서 만료 임박·수집 연속 실패를 알림 이력으로 남기며, 화면에서 랙 추이와 알림을 확인할 수 있게 한다.

**Architecture:** `monitor/` 패키지가 신설된다: MetricsCollector(@Scheduled 주기 수집) → MetricSample(H2) → AlertEvaluator(임계치 비교 + 쿨다운) → AlertEvent(H2) + AlertNotifier(인터페이스, 2단계에서는 Noop — 외부 발송은 이력만). CertExpiryChecker는 브로커에 TLS 핸드셰이크로 인증서 만료일을 읽는다. 임계치는 DB가 아닌 설정(app.monitor.*)으로 관리한다(YAGNI).

**Tech Stack:** 1단계와 동일 (Spring Boot 4.1.0/Java 21, kafka-clients 4.2.x BOM 관리, JPA+H2, Vue 3+TS, Testcontainers 싱글턴 컨테이너).

## Global Constraints

- 코드 위치: `~/Documents/kafka-admin` (원격: origin=사내 GitLab, kkk=GitHub — 푸시는 사용자가 결정)
- Spring Boot 4.1.0 고정 (다운그레이드 금지). 테스트 슬라이스 import: `org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest`, `@MockitoBean`은 `org.springframework.test.context.bean.override.mockito.MockitoBean`
- 임계치 기본값: lag 1000, disk 80%, 인증서 D-30, 알림 쿨다운 30분, 수집 주기 60초, 이력 보존 7일 — 전부 env로 재정의 가능
- 수집 3회 연속 실패는 그 자체가 알림(COLLECTOR_FAILURE) — 스펙 "예외 처리" 절
- 스케줄러는 `app.monitor.enabled=true`일 때만 활성 (테스트 리소스는 false로 고정 — IT 중 백그라운드 수집이 돌면 안 됨)
- 알림 외부 발송 없음: `AlertNotifier` 인터페이스 + `NoopAlertNotifier`만. 웹훅/메일은 3단계 이후
- 차트(랙 추이)는 단일 시리즈 스파크라인: 범례 없음(제목이 시리즈명), 선 2px 단일 색 `#1d4ed8`, 값 텍스트는 본문 텍스트 색 유지, 마지막 값 직접 라벨, 호버 시 가장 가까운 점의 시각+값 표시, 이중 축 금지
- 주석 한국어, 커밋 메시지 짧은 명령형 영어
- 기존 파일 수정 범위: `application.yml`(monitor 블록 추가), `GroupDetailView.vue`/`ClusterView.vue`(카드·차트 추가), `router/index.ts`+`App.vue`(알림 메뉴) — 그 외 1단계 파일은 건드리지 않는다

## 파일 구조

```
was/src/main/java/com/osstem/kafkaadmin/
├── monitor/
│   ├── MonitorProperties.java      # app.monitor.* 바인딩
│   ├── MonitorConfig.java          # @EnableConfigurationProperties + @EnableScheduling
│   ├── MetricSample.java           # 엔티티 (지표 이력)
│   ├── MetricSampleRepository.java
│   ├── AlertEvent.java             # 엔티티 (알림 이력)
│   ├── AlertEventRepository.java
│   ├── AlertNotifier.java          # 인터페이스
│   ├── NoopAlertNotifier.java
│   ├── AlertEvaluator.java         # 임계치 비교 + 쿨다운 + 이력 저장
│   ├── MetricsCollector.java       # 수집 1회 실행 + 연속 실패 추적
│   ├── CertExpiryChecker.java      # TLS 핸드셰이크로 만료일 조회
│   └── MonitorScheduler.java       # @Scheduled 배선 + 보존기간 정리
├── kafka/MonitorQueryService.java  # URP 카운트 + 브로커별 디스크 사용률
└── api/MonitorController.java      # /api/metrics, /api/alerts, /api/monitor/status

web/src/
├── lib/trend.ts                    # 폴리라인 좌표 계산 (순수 함수)
├── components/TrendChart.vue       # SVG 스파크라인 (호버 포함)
└── views/AlertsView.vue            # 알림 이력 화면
```

---

### Task 1: 모니터 설정 + 엔티티/리포지토리

**Files:**
- Create: `monitor/MonitorProperties.java`, `monitor/MonitorConfig.java`, `monitor/MetricSample.java`, `monitor/MetricSampleRepository.java`, `monitor/AlertEvent.java`, `monitor/AlertEventRepository.java`
- Modify: `was/src/main/resources/application.yml` (app.monitor 블록), `was/src/test/resources/application.properties` (`app.monitor.enabled=false` 추가)
- Test: `was/src/test/java/com/osstem/kafkaadmin/monitor/MonitorPersistenceTest.java`

**Interfaces:**
- Produces:
  - `MonitorProperties(boolean enabled, long lagThreshold, int diskUsedPctThreshold, int certWarnDays, int cooldownMinutes, int retentionDays)` — `@ConfigurationProperties(prefix="app.monitor")`
  - `MetricSample(String metricType, String subjectKey, double value, Instant sampledAt)` 엔티티 (getter: getMetricType/getSubjectKey/getValue/getSampledAt)
  - `AlertEvent(String ruleType, String subjectKey, String message, double value, double threshold, Instant occurredAt)` 엔티티 (getter 동일 패턴)
  - `MetricSampleRepository`: `List<MetricSample> findByMetricTypeAndSubjectKeyAndSampledAtAfterOrderBySampledAt(String, String, Instant)`, `long deleteBySampledAtBefore(Instant)`
  - `AlertEventRepository`: `List<AlertEvent> findTop50ByOrderByOccurredAtDesc()`, `boolean existsByRuleTypeAndSubjectKeyAndOccurredAtAfter(String, String, Instant)`
  - metricType 문자열 상수: `LAG`, `URP`, `DISK_USED_PCT`, `BROKER_COUNT` / ruleType: `LAG_HIGH`, `DISK_HIGH`, `CERT_EXPIRY`, `COLLECTOR_FAILURE`

- [ ] **Step 1: 실패하는 테스트 작성**

`monitor/MonitorPersistenceTest.java`:

```java
package com.osstem.kafkaadmin.monitor;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Transactional
class MonitorPersistenceTest {

    @Autowired MetricSampleRepository samples;
    @Autowired AlertEventRepository alerts;
    @Autowired MonitorProperties props;

    @Test
    void 기간_내_지표를_시각순으로_조회하고_오래된_것을_삭제한다() {
        Instant now = Instant.now();
        samples.save(new MetricSample("LAG", "g1", 10, now.minus(2, ChronoUnit.HOURS)));
        samples.save(new MetricSample("LAG", "g1", 20, now.minus(1, ChronoUnit.HOURS)));
        samples.save(new MetricSample("LAG", "g2", 99, now.minus(1, ChronoUnit.HOURS)));
        samples.save(new MetricSample("LAG", "g1", 5, now.minus(10, ChronoUnit.DAYS)));

        List<MetricSample> found = samples
                .findByMetricTypeAndSubjectKeyAndSampledAtAfterOrderBySampledAt(
                        "LAG", "g1", now.minus(1, ChronoUnit.DAYS));
        assertThat(found).extracting(MetricSample::getValue).containsExactly(10.0, 20.0);

        long deleted = samples.deleteBySampledAtBefore(now.minus(7, ChronoUnit.DAYS));
        assertThat(deleted).isEqualTo(1);
    }

    @Test
    void 쿨다운_존재_확인과_최신_알림_조회가_동작한다() {
        Instant now = Instant.now();
        alerts.save(new AlertEvent("LAG_HIGH", "g1", "랙 초과", 1500, 1000,
                now.minus(10, ChronoUnit.MINUTES)));
        assertThat(alerts.existsByRuleTypeAndSubjectKeyAndOccurredAtAfter(
                "LAG_HIGH", "g1", now.minus(30, ChronoUnit.MINUTES))).isTrue();
        assertThat(alerts.existsByRuleTypeAndSubjectKeyAndOccurredAtAfter(
                "LAG_HIGH", "g2", now.minus(30, ChronoUnit.MINUTES))).isFalse();
        assertThat(alerts.findTop50ByOrderByOccurredAtDesc()).hasSize(1);
    }

    @Test
    void 모니터_설정_기본값이_바인딩된다() {
        assertThat(props.lagThreshold()).isEqualTo(1000);
        assertThat(props.diskUsedPctThreshold()).isEqualTo(80);
        assertThat(props.certWarnDays()).isEqualTo(30);
        assertThat(props.cooldownMinutes()).isEqualTo(30);
        assertThat(props.retentionDays()).isEqualTo(7);
        assertThat(props.enabled()).isFalse(); // 테스트 리소스에서 false
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `cd ~/Documents/kafka-admin/was && ./gradlew test --tests 'MonitorPersistenceTest'`
Expected: 컴파일 실패 (엔티티/리포지토리 없음)

- [ ] **Step 3: 구현**

`monitor/MonitorProperties.java`:

```java
package com.osstem.kafkaadmin.monitor;

import org.springframework.boot.context.properties.ConfigurationProperties;

// 임계치는 DB가 아닌 설정으로 관리한다(2단계 YAGNI). env로 재정의 가능.
@ConfigurationProperties(prefix = "app.monitor")
public record MonitorProperties(
        boolean enabled,
        long lagThreshold,
        int diskUsedPctThreshold,
        int certWarnDays,
        int cooldownMinutes,
        int retentionDays) {
}
```

`monitor/MonitorConfig.java`:

```java
package com.osstem.kafkaadmin.monitor;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(MonitorProperties.class)
public class MonitorConfig {
}
```

`monitor/MetricSample.java`:

```java
package com.osstem.kafkaadmin.monitor;

import jakarta.persistence.*;
import java.time.Instant;

// 지표 이력 1건. metricType: LAG | URP | DISK_USED_PCT | BROKER_COUNT
@Entity
@Table(name = "metric_sample", indexes =
        @Index(name = "idx_metric_lookup", columnList = "metricType,subjectKey,sampledAt"))
public class MetricSample {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String metricType;
    private String subjectKey;
    private double value;
    private Instant sampledAt;

    protected MetricSample() {}

    public MetricSample(String metricType, String subjectKey, double value, Instant sampledAt) {
        this.metricType = metricType;
        this.subjectKey = subjectKey;
        this.value = value;
        this.sampledAt = sampledAt;
    }

    public String getMetricType() { return metricType; }
    public String getSubjectKey() { return subjectKey; }
    public double getValue() { return value; }
    public Instant getSampledAt() { return sampledAt; }
}
```

`monitor/AlertEvent.java`:

```java
package com.osstem.kafkaadmin.monitor;

import jakarta.persistence.*;
import java.time.Instant;

// 알림 이력 1건. ruleType: LAG_HIGH | DISK_HIGH | CERT_EXPIRY | COLLECTOR_FAILURE
@Entity
@Table(name = "alert_event", indexes =
        @Index(name = "idx_alert_cooldown", columnList = "ruleType,subjectKey,occurredAt"))
public class AlertEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String ruleType;
    private String subjectKey;
    private String message;
    private double value;
    private double threshold;
    private Instant occurredAt;

    protected AlertEvent() {}

    public AlertEvent(String ruleType, String subjectKey, String message,
                      double value, double threshold, Instant occurredAt) {
        this.ruleType = ruleType;
        this.subjectKey = subjectKey;
        this.message = message;
        this.value = value;
        this.threshold = threshold;
        this.occurredAt = occurredAt;
    }

    public String getRuleType() { return ruleType; }
    public String getSubjectKey() { return subjectKey; }
    public String getMessage() { return message; }
    public double getValue() { return value; }
    public double getThreshold() { return threshold; }
    public Instant getOccurredAt() { return occurredAt; }
}
```

`monitor/MetricSampleRepository.java`:

```java
package com.osstem.kafkaadmin.monitor;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import java.time.Instant;
import java.util.List;

public interface MetricSampleRepository extends JpaRepository<MetricSample, Long> {
    List<MetricSample> findByMetricTypeAndSubjectKeyAndSampledAtAfterOrderBySampledAt(
            String metricType, String subjectKey, Instant after);

    @Modifying
    long deleteBySampledAtBefore(Instant before);
}
```

`monitor/AlertEventRepository.java`:

```java
package com.osstem.kafkaadmin.monitor;

import org.springframework.data.jpa.repository.JpaRepository;
import java.time.Instant;
import java.util.List;

public interface AlertEventRepository extends JpaRepository<AlertEvent, Long> {
    List<AlertEvent> findTop50ByOrderByOccurredAtDesc();
    boolean existsByRuleTypeAndSubjectKeyAndOccurredAtAfter(
            String ruleType, String subjectKey, Instant after);
}
```

`application.yml`의 `app:` 아래에 추가 (kafka 블록과 같은 깊이):

```yaml
  monitor:
    enabled: ${MONITOR_ENABLED:true}
    lag-threshold: ${MONITOR_LAG_THRESHOLD:1000}
    disk-used-pct-threshold: ${MONITOR_DISK_USED_PCT_THRESHOLD:80}
    cert-warn-days: ${MONITOR_CERT_WARN_DAYS:30}
    cooldown-minutes: ${MONITOR_COOLDOWN_MINUTES:30}
    retention-days: ${MONITOR_RETENTION_DAYS:7}
```

`src/test/resources/application.properties`에 추가:

```properties
app.monitor.enabled=false
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew test --tests 'MonitorPersistenceTest'`
Expected: 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: monitor entities, repositories, and properties"
```

---

### Task 2: MonitorQueryService (URP + 디스크 사용률)

**Files:**
- Create: `kafka/MonitorQueryService.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/kafka/MonitorQueryServiceIT.java`

**Interfaces:**
- Consumes: `Admin` 빈, `KafkaFutures.await`, `KafkaIntegrationTestBase`
- Produces: `MonitorQueryService.countUnderReplicatedPartitions() -> int`, `diskUsedPercentByBroker() -> Map<Integer, Double>` (0~100, 로그 디렉토리 total/usable 합산 기준; totalBytes 미제공 시 해당 브로커 제외)

- [ ] **Step 1: 실패하는 테스트 작성**

```java
package com.osstem.kafkaadmin.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;

class MonitorQueryServiceIT extends KafkaIntegrationTestBase {

    @Autowired Admin admin;
    @Autowired MonitorQueryService service;

    @Test
    void 단일_브로커_정상_클러스터는_URP가_0이다() throws Exception {
        if (!admin.listTopics().names().get().contains("monitor-topic")) {
            admin.createTopics(List.of(new NewTopic("monitor-topic", 2, (short) 1))).all().get();
        }
        assertThat(service.countUnderReplicatedPartitions()).isZero();
    }

    @Test
    void 브로커별_디스크_사용률을_0에서_100_사이로_반환한다() {
        Map<Integer, Double> disk = service.diskUsedPercentByBroker();
        assertThat(disk).isNotEmpty();
        assertThat(disk.values()).allSatisfy(pct ->
                assertThat(pct).isBetween(0.0, 100.0));
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew test --tests 'MonitorQueryServiceIT'`
Expected: 컴파일 실패 (`MonitorQueryService` 없음)

- [ ] **Step 3: 구현**

```java
package com.osstem.kafkaadmin.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

// 장애 감시용 조회: URP(복제 부족 파티션 수), 브로커별 디스크 사용률
@Service
public class MonitorQueryService {

    private final Admin admin;

    public MonitorQueryService(Admin admin) {
        this.admin = admin;
    }

    // 내부 토픽 포함 전체 기준. isr < replicas 인 파티션 수.
    public int countUnderReplicatedPartitions() {
        Set<String> names = KafkaFutures.await(
                admin.listTopics(new ListTopicsOptions().listInternal(true)).names());
        if (names.isEmpty()) {
            return 0;
        }
        return KafkaFutures.await(admin.describeTopics(names).allTopicNames()).values().stream()
                .flatMap(d -> d.partitions().stream())
                .mapToInt(p -> p.isr().size() < p.replicas().size() ? 1 : 0)
                .sum();
    }

    // 로그 디렉토리의 total/usable 바이트(KIP-827)로 사용률(%)을 계산한다.
    // totalBytes를 제공하지 않는 브로커(구버전/특수 FS)는 결과에서 제외한다.
    public Map<Integer, Double> diskUsedPercentByBroker() {
        List<Integer> brokerIds = KafkaFutures.await(admin.describeCluster().nodes()).stream()
                .map(n -> n.id())
                .toList();
        Map<Integer, Map<String, LogDirDescription>> all =
                KafkaFutures.await(admin.describeLogDirs(brokerIds).allDescriptions());
        Map<Integer, Double> result = new HashMap<>();
        all.forEach((brokerId, dirs) -> {
            long total = 0;
            long usable = 0;
            for (LogDirDescription d : dirs.values()) {
                if (d.totalBytes().isPresent() && d.usableBytes().isPresent()) {
                    total += d.totalBytes().getAsLong();
                    usable += d.usableBytes().getAsLong();
                }
            }
            if (total > 0) {
                result.put(brokerId, (total - usable) * 100.0 / total);
            }
        });
        return result;
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew test --tests 'MonitorQueryServiceIT'`
Expected: 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: monitor query service for urp and disk usage"
```

---

### Task 3: AlertEvaluator + AlertNotifier

**Files:**
- Create: `monitor/AlertNotifier.java`, `monitor/NoopAlertNotifier.java`, `monitor/AlertEvaluator.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/monitor/AlertEvaluatorTest.java`

**Interfaces:**
- Consumes: Task 1의 엔티티/리포지토리/MonitorProperties
- Produces:
  - `AlertNotifier { void send(AlertEvent event); }` — 외부 발송 확장점 (2단계는 Noop)
  - `AlertEvaluator.evaluate(List<MetricSample> samples)` — LAG > lagThreshold → LAG_HIGH, DISK_USED_PCT > diskUsedPctThreshold → DISK_HIGH
  - `AlertEvaluator.raise(String ruleType, String subjectKey, String message, double value, double threshold)` — 쿨다운(동일 ruleType+subjectKey가 cooldownMinutes 내 존재하면 무시) 후 저장 + notifier.send

- [ ] **Step 1: 실패하는 테스트 작성**

```java
package com.osstem.kafkaadmin.monitor;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Transactional
class AlertEvaluatorTest {

    @Autowired AlertEvaluator evaluator;
    @Autowired AlertEventRepository alerts;

    @Test
    void 랙_임계치_초과만_알림이_된다() {
        Instant now = Instant.now();
        evaluator.evaluate(List.of(
                new MetricSample("LAG", "g-high", 1500, now),
                new MetricSample("LAG", "g-ok", 500, now),
                new MetricSample("DISK_USED_PCT", "1", 95.5, now),
                new MetricSample("BROKER_COUNT", "cluster", 3, now)));

        List<AlertEvent> saved = alerts.findTop50ByOrderByOccurredAtDesc();
        assertThat(saved).hasSize(2);
        assertThat(saved).extracting(AlertEvent::getRuleType)
                .containsExactlyInAnyOrder("LAG_HIGH", "DISK_HIGH");
        assertThat(saved).extracting(AlertEvent::getSubjectKey)
                .containsExactlyInAnyOrder("g-high", "1");
    }

    @Test
    void 쿨다운_내_동일_알림은_중복_저장되지_않는다() {
        evaluator.raise("LAG_HIGH", "g1", "랙 초과", 2000, 1000);
        evaluator.raise("LAG_HIGH", "g1", "랙 초과", 2100, 1000); // 쿨다운 내 → 무시
        evaluator.raise("LAG_HIGH", "g2", "랙 초과", 2000, 1000); // 다른 그룹 → 저장

        assertThat(alerts.findTop50ByOrderByOccurredAtDesc()).hasSize(2);
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew test --tests 'AlertEvaluatorTest'`
Expected: 컴파일 실패

- [ ] **Step 3: 구현**

`monitor/AlertNotifier.java`:

```java
package com.osstem.kafkaadmin.monitor;

// 알림 외부 발송 확장점. 2단계는 이력만 남기고(Noop), 웹훅/메일은 이후 단계에서 구현체 추가.
public interface AlertNotifier {
    void send(AlertEvent event);
}
```

`monitor/NoopAlertNotifier.java`:

```java
package com.osstem.kafkaadmin.monitor;

import org.springframework.stereotype.Component;

@Component
public class NoopAlertNotifier implements AlertNotifier {
    @Override
    public void send(AlertEvent event) {
        // 의도적으로 아무 것도 하지 않는다 — 알림은 DB 이력으로만 남는다.
    }
}
```

`monitor/AlertEvaluator.java`:

```java
package com.osstem.kafkaadmin.monitor;

import org.springframework.stereotype.Service;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Service
public class AlertEvaluator {

    private final AlertEventRepository alerts;
    private final AlertNotifier notifier;
    private final MonitorProperties props;

    public AlertEvaluator(AlertEventRepository alerts, AlertNotifier notifier,
                          MonitorProperties props) {
        this.alerts = alerts;
        this.notifier = notifier;
        this.props = props;
    }

    public void evaluate(List<MetricSample> samples) {
        for (MetricSample s : samples) {
            switch (s.getMetricType()) {
                case "LAG" -> {
                    if (s.getValue() > props.lagThreshold()) {
                        raise("LAG_HIGH", s.getSubjectKey(),
                                "컨슈머 그룹 %s 랙 %.0f (임계치 %d)".formatted(
                                        s.getSubjectKey(), s.getValue(), props.lagThreshold()),
                                s.getValue(), props.lagThreshold());
                    }
                }
                case "DISK_USED_PCT" -> {
                    if (s.getValue() > props.diskUsedPctThreshold()) {
                        raise("DISK_HIGH", s.getSubjectKey(),
                                "브로커 %s 디스크 사용률 %.1f%% (임계치 %d%%)".formatted(
                                        s.getSubjectKey(), s.getValue(), props.diskUsedPctThreshold()),
                                s.getValue(), props.diskUsedPctThreshold());
                    }
                }
                default -> { /* URP, BROKER_COUNT 는 이력만 (증감 알림은 이후 단계) */ }
            }
        }
    }

    // 쿨다운: 동일 (ruleType, subjectKey) 알림이 cooldownMinutes 내에 있으면 중복 발생 억제
    public void raise(String ruleType, String subjectKey, String message,
                      double value, double threshold) {
        Instant cutoff = Instant.now().minus(props.cooldownMinutes(), ChronoUnit.MINUTES);
        if (alerts.existsByRuleTypeAndSubjectKeyAndOccurredAtAfter(ruleType, subjectKey, cutoff)) {
            return;
        }
        AlertEvent event = alerts.save(
                new AlertEvent(ruleType, subjectKey, message, value, threshold, Instant.now()));
        notifier.send(event);
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew test --tests 'AlertEvaluatorTest'`
Expected: 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: alert evaluator with cooldown and notifier port"
```

---

### Task 4: MetricsCollector (수집 1회 + 연속 실패 알림)

**Files:**
- Create: `monitor/MetricsCollector.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/monitor/MetricsCollectorIT.java`

**Interfaces:**
- Consumes: `GroupQueryService.listGroups()/describeGroup()`, `MonitorQueryService`, `ClusterQueryService.getClusterInfo()`, Task 1/3 산출물
- Produces: `MetricsCollector.collectOnce()` (성공 시 샘플 저장+평가, 실패 시 연속 실패 카운트 — 3회째에 COLLECTOR_FAILURE 알림), `lastSuccessAt() -> Instant`(없으면 null), `consecutiveFailures() -> int`

- [ ] **Step 1: 실패하는 테스트 작성**

```java
package com.osstem.kafkaadmin.monitor;

import com.osstem.kafkaadmin.kafka.KafkaIntegrationTestBase;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

class MetricsCollectorIT extends KafkaIntegrationTestBase {

    @Autowired Admin admin;
    @Autowired MetricsCollector collector;
    @Autowired MetricSampleRepository samples;

    @Test
    void 수집_1회로_URP_디스크_브로커수_샘플이_저장된다() throws Exception {
        if (!admin.listTopics().names().get().contains("collect-topic")) {
            admin.createTopics(List.of(new NewTopic("collect-topic", 1, (short) 1))).all().get();
        }
        collector.collectOnce();

        Instant after = Instant.now().minus(1, ChronoUnit.MINUTES);
        assertThat(samples.findByMetricTypeAndSubjectKeyAndSampledAtAfterOrderBySampledAt(
                "URP", "cluster", after)).isNotEmpty();
        assertThat(samples.findByMetricTypeAndSubjectKeyAndSampledAtAfterOrderBySampledAt(
                "BROKER_COUNT", "cluster", after))
                .last().satisfies(s -> assertThat(s.getValue()).isEqualTo(1.0));
        assertThat(collector.lastSuccessAt()).isNotNull();
        assertThat(collector.consecutiveFailures()).isZero();
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew test --tests 'MetricsCollectorIT'`
Expected: 컴파일 실패

- [ ] **Step 3: 구현**

```java
package com.osstem.kafkaadmin.monitor;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.GroupQueryService;
import com.osstem.kafkaadmin.kafka.MonitorQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.GroupSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

// 지표 수집 1회분. 스케줄링은 MonitorScheduler가 담당한다(테스트 용이성 분리).
@Service
public class MetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(MetricsCollector.class);
    private static final int FAILURE_ALERT_AT = 3;

    private final GroupQueryService groups;
    private final MonitorQueryService monitorQuery;
    private final ClusterQueryService cluster;
    private final MetricSampleRepository samples;
    private final AlertEvaluator evaluator;
    private final AtomicInteger failures = new AtomicInteger();
    private final AtomicReference<Instant> lastSuccess = new AtomicReference<>();

    public MetricsCollector(GroupQueryService groups, MonitorQueryService monitorQuery,
                            ClusterQueryService cluster, MetricSampleRepository samples,
                            AlertEvaluator evaluator) {
        this.groups = groups;
        this.monitorQuery = monitorQuery;
        this.cluster = cluster;
        this.samples = samples;
        this.evaluator = evaluator;
    }

    public void collectOnce() {
        try {
            Instant now = Instant.now();
            List<MetricSample> batch = new ArrayList<>();
            for (GroupSummary g : groups.listGroups()) {
                batch.add(new MetricSample("LAG", g.groupId(),
                        groups.describeGroup(g.groupId()).totalLag(), now));
            }
            batch.add(new MetricSample("URP", "cluster",
                    monitorQuery.countUnderReplicatedPartitions(), now));
            monitorQuery.diskUsedPercentByBroker().forEach((brokerId, pct) ->
                    batch.add(new MetricSample("DISK_USED_PCT", String.valueOf(brokerId), pct, now)));
            batch.add(new MetricSample("BROKER_COUNT", "cluster",
                    cluster.getClusterInfo().brokers().size(), now));

            samples.saveAll(batch);
            evaluator.evaluate(batch);
            failures.set(0);
            lastSuccess.set(now);
        } catch (RuntimeException e) {
            int count = failures.incrementAndGet();
            log.warn("지표 수집 실패 ({}회 연속): {}", count, e.getMessage());
            // 연속 실패는 클러스터 전면 장애 신호일 수 있다 — 스펙 예외 처리 절
            if (count == FAILURE_ALERT_AT) {
                evaluator.raise("COLLECTOR_FAILURE", "collector",
                        "지표 수집이 %d회 연속 실패: %s".formatted(count, e.getMessage()),
                        count, FAILURE_ALERT_AT);
            }
        }
    }

    public Instant lastSuccessAt() { return lastSuccess.get(); }
    public int consecutiveFailures() { return failures.get(); }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew test --tests 'MetricsCollectorIT'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: metrics collector with failure tracking"
```

---

### Task 5: CertExpiryChecker (TLS 만료일 조회)

**Files:**
- Create: `monitor/CertExpiryChecker.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/monitor/CertExpiryCheckerTest.java`

**Interfaces:**
- Consumes: `KafkaConnectionProperties`(securityProtocol 판단), `ClusterQueryService`, `AlertEvaluator.raise`
- Produces:
  - `record CertStatus(String broker, Instant notAfter, long daysRemaining)`
  - `CertExpiryChecker.runDailyCheck()` — SASL_SSL일 때만 브로커별 핸드셰이크, daysRemaining <= certWarnDays → CERT_EXPIRY 알림, 결과를 `lastStatuses()`에 보관
  - `lastStatuses() -> List<CertStatus>` (아직 실행 전이거나 PLAINTEXT면 빈 리스트)
  - `static long daysUntil(Instant notAfter, Instant now)` 순수 함수

- [ ] **Step 1: 실패하는 테스트 작성**

```java
package com.osstem.kafkaadmin.monitor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import static org.assertj.core.api.Assertions.assertThat;

class CertExpiryCheckerTest {

    @Test
    void 만료까지_남은_일수를_계산한다() {
        Instant now = Instant.parse("2026-08-07T00:00:00Z");
        assertThat(CertExpiryChecker.daysUntil(now.plus(30, ChronoUnit.DAYS), now)).isEqualTo(30);
        assertThat(CertExpiryChecker.daysUntil(now.plus(30, ChronoUnit.DAYS).plusSeconds(3600), now))
                .isEqualTo(30);
        assertThat(CertExpiryChecker.daysUntil(now.minus(1, ChronoUnit.DAYS), now)).isEqualTo(-1);
    }

    @Test
    void TLS_서버의_인증서_만료일을_읽는다(@TempDir Path dir) throws Exception {
        // keytool로 90일짜리 자가서명 키스토어 생성 (PATH가 아닌 실행 중 JDK의 keytool 사용)
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        Path ks = dir.resolve("server.jks");
        Process p = new ProcessBuilder(keytool, "-genkeypair", "-alias", "server",
                "-keyalg", "RSA", "-keysize", "2048", "-validity", "90",
                "-dname", "CN=localhost", "-keystore", ks.toString(),
                "-storepass", "changeit", "-keypass", "changeit").inheritIO().start();
        assertThat(p.waitFor()).isZero();

        KeyStore keyStore = KeyStore.getInstance("JKS");
        try (var in = new FileInputStream(ks.toFile())) {
            keyStore.load(in, "changeit".toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, "changeit".toCharArray());
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), null, null);

        try (SSLServerSocket server =
                     (SSLServerSocket) ctx.getServerSocketFactory().createServerSocket(0)) {
            Thread accepter = new Thread(() -> {
                try (var s = server.accept()) {
                    s.getInputStream().read(); // 핸드셰이크 완료까지 대기
                } catch (Exception ignored) {
                }
            });
            accepter.start();

            X509Certificate cert =
                    CertExpiryChecker.fetchServerCert("localhost", server.getLocalPort());
            long days = CertExpiryChecker.daysUntil(cert.getNotAfter().toInstant(), Instant.now());
            assertThat(days).isBetween(85L, 90L);
        }
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew test --tests 'CertExpiryCheckerTest'`
Expected: 컴파일 실패

- [ ] **Step 3: 구현**

```java
package com.osstem.kafkaadmin.monitor;

import com.osstem.kafkaadmin.config.KafkaConnectionProperties;
import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.BrokerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

// 브로커 TLS 인증서 만료 감시. SASL_SSL 리스너는 TLS 핸드셰이크가 SASL 인증보다
// 먼저 완료되므로, 계정 없이도 서버 인증서를 읽을 수 있다.
@Service
public class CertExpiryChecker {

    private static final Logger log = LoggerFactory.getLogger(CertExpiryChecker.class);

    private final KafkaConnectionProperties kafkaProps;
    private final ClusterQueryService cluster;
    private final AlertEvaluator evaluator;
    private final MonitorProperties props;
    private final AtomicReference<List<CertStatus>> last = new AtomicReference<>(List.of());

    public record CertStatus(String broker, Instant notAfter, long daysRemaining) {}

    public CertExpiryChecker(KafkaConnectionProperties kafkaProps, ClusterQueryService cluster,
                             AlertEvaluator evaluator, MonitorProperties props) {
        this.kafkaProps = kafkaProps;
        this.cluster = cluster;
        this.evaluator = evaluator;
        this.props = props;
    }

    public void runDailyCheck() {
        if (!"SASL_SSL".equals(kafkaProps.securityProtocol())) {
            return; // PLAINTEXT 구성에는 인증서가 없다
        }
        List<CertStatus> statuses = new ArrayList<>();
        for (BrokerInfo b : cluster.getClusterInfo().brokers()) {
            String key = b.host() + ":" + b.port();
            try {
                X509Certificate cert = fetchServerCert(b.host(), b.port());
                long days = daysUntil(cert.getNotAfter().toInstant(), Instant.now());
                statuses.add(new CertStatus(key, cert.getNotAfter().toInstant(), days));
                if (days <= props.certWarnDays()) {
                    evaluator.raise("CERT_EXPIRY", key,
                            "브로커 %s 인증서 만료 D-%d".formatted(key, days),
                            days, props.certWarnDays());
                }
            } catch (Exception e) {
                log.warn("인증서 조회 실패 {}: {}", key, e.getMessage());
            }
        }
        last.set(List.copyOf(statuses));
    }

    public List<CertStatus> lastStatuses() { return last.get(); }

    public static long daysUntil(Instant notAfter, Instant now) {
        return ChronoUnit.DAYS.between(now, notAfter);
    }

    // 만료일 조회 전용이라 신뢰 검증을 생략한다(데이터 교환 없음, 인증서 메타데이터만 읽음).
    // 실제 Kafka 통신은 AdminClient가 truststore로 정상 검증한다.
    static X509Certificate fetchServerCert(String host, int port) throws Exception {
        TrustManager trustAll = new X509TrustManager() {
            public void checkClientTrusted(X509Certificate[] chain, String authType) {}
            public void checkServerTrusted(X509Certificate[] chain, String authType) {}
            public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        };
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, new TrustManager[]{trustAll}, null);
        try (SSLSocket socket = (SSLSocket) ctx.getSocketFactory().createSocket(host, port)) {
            socket.setSoTimeout(5000);
            socket.startHandshake();
            return (X509Certificate) socket.getSession().getPeerCertificates()[0];
        }
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew test --tests 'CertExpiryCheckerTest'`
Expected: 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: broker certificate expiry checker"
```

---

### Task 6: MonitorScheduler (배선 + 보존기간 정리)

**Files:**
- Create: `monitor/MonitorScheduler.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/monitor/MonitorSchedulerTest.java`

**Interfaces:**
- Consumes: `MetricsCollector`, `CertExpiryChecker`, `MetricSampleRepository`, `MonitorProperties`
- Produces: `@Scheduled` 배선 — 수집(fixedDelay `${app.monitor.collect-interval-ms:60000}`), 일일 작업(cron `0 0 3 * * *`: 인증서 점검 + retentionDays 초과 샘플 삭제). `@ConditionalOnProperty(app.monitor.enabled)` 가드. `runDaily()`는 직접 호출 가능(테스트용 public)

- [ ] **Step 1: 실패하는 테스트 작성**

```java
package com.osstem.kafkaadmin.monitor;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import static org.assertj.core.api.Assertions.assertThat;

// 스케줄 트리거 자체(@Scheduled 타이밍)는 스프링의 책임이므로 검증하지 않는다.
// 여기서는 빈 가드(enabled=false면 미등록)와 일일 작업의 삭제 로직만 검증한다.
@SpringBootTest(properties = "app.monitor.enabled=true")
@TestPropertySource(properties = "app.monitor.collect-interval-ms=86400000")
@Transactional
class MonitorSchedulerTest {

    @Autowired ApplicationContext context;
    @Autowired MetricSampleRepository samples;
    @Autowired MonitorScheduler scheduler;

    @Test
    void enabled_true면_스케줄러_빈이_등록된다() {
        assertThat(context.getBeansOfType(MonitorScheduler.class)).hasSize(1);
    }

    @Test
    void 일일_작업이_보존기간_지난_샘플을_삭제한다() {
        Instant now = Instant.now();
        samples.save(new MetricSample("LAG", "old", 1, now.minus(10, ChronoUnit.DAYS)));
        samples.save(new MetricSample("LAG", "recent", 1, now.minus(1, ChronoUnit.DAYS)));

        scheduler.runDaily();

        assertThat(samples.findAll()).extracting(MetricSample::getSubjectKey)
                .contains("recent").doesNotContain("old");
    }
}
```

참고: 이 테스트는 `app.monitor.enabled=true`로 컨텍스트를 띄우지만 수집 주기를 24시간으로 늘려
IT 도중 백그라운드 수집이 실제 Kafka 없이 돌다 실패 로그를 쌓는 것을 방지한다. (collectOnce는
브로커가 없으면 실패 카운트만 올리고 예외를 삼키므로 테스트 자체는 안전하다.)

- [ ] **Step 2: 실패 확인**

Run: `./gradlew test --tests 'MonitorSchedulerTest'`
Expected: 컴파일 실패

- [ ] **Step 3: 구현**

```java
package com.osstem.kafkaadmin.monitor;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

// 스케줄 배선 전담. 로직은 collector/checker에 있고 여기는 주기만 정의한다.
@Component
@ConditionalOnProperty(prefix = "app.monitor", name = "enabled", havingValue = "true")
public class MonitorScheduler {

    private final MetricsCollector collector;
    private final CertExpiryChecker certChecker;
    private final MetricSampleRepository samples;
    private final MonitorProperties props;

    public MonitorScheduler(MetricsCollector collector, CertExpiryChecker certChecker,
                            MetricSampleRepository samples, MonitorProperties props) {
        this.collector = collector;
        this.certChecker = certChecker;
        this.samples = samples;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${app.monitor.collect-interval-ms:60000}")
    public void collect() {
        collector.collectOnce();
    }

    // 매일 03:00 — 인증서 점검 + 보존기간 지난 지표 정리
    @Scheduled(cron = "0 0 3 * * *")
    @Transactional
    public void runDaily() {
        certChecker.runDailyCheck();
        samples.deleteBySampledAtBefore(
                Instant.now().minus(props.retentionDays(), ChronoUnit.DAYS));
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew test --tests 'MonitorSchedulerTest'`
Expected: 2 tests PASS. 추가로 기존 테스트 회귀 확인: `./gradlew test --tests 'MonitorPersistenceTest' --tests 'AlertEvaluatorTest' --tests 'KafkaAdminApplicationTests'` (enabled=false 경로)

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: monitor scheduler with retention cleanup"
```

---

### Task 7: MonitorController (조회 API)

**Files:**
- Create: `api/MonitorController.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/api/MonitorControllerTest.java`

**Interfaces:**
- Consumes: Task 1/4/5 산출물
- Produces:
  - `GET /api/metrics?type=LAG&subject=g1&hours=24` → `[{"sampledAt": "...", "value": 12.0}, ...]` (hours 기본 24)
  - `GET /api/alerts` → AlertEvent 최신 50건 (occurredAt 내림차순)
  - `GET /api/monitor/status` → `{"lastCollectedAt": "...|null", "consecutiveFailures": 0, "certs": [{"broker","notAfter","daysRemaining"}]}`

- [ ] **Step 1: 실패하는 테스트 작성**

```java
package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.monitor.AlertEvent;
import com.osstem.kafkaadmin.monitor.AlertEventRepository;
import com.osstem.kafkaadmin.monitor.CertExpiryChecker;
import com.osstem.kafkaadmin.monitor.MetricSample;
import com.osstem.kafkaadmin.monitor.MetricSampleRepository;
import com.osstem.kafkaadmin.monitor.MetricsCollector;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import java.time.Instant;
import java.util.List;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(MonitorController.class)
class MonitorControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean MetricSampleRepository samples;
    @MockitoBean AlertEventRepository alerts;
    @MockitoBean MetricsCollector collector;
    @MockitoBean CertExpiryChecker certChecker;

    @Test
    @WithMockUser
    void 지표_이력을_조회한다() throws Exception {
        given(samples.findByMetricTypeAndSubjectKeyAndSampledAtAfterOrderBySampledAt(
                anyString(), anyString(), any()))
                .willReturn(List.of(new MetricSample("LAG", "g1", 12, Instant.now())));
        mvc.perform(get("/api/metrics?type=LAG&subject=g1&hours=24"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].value").value(12.0));
    }

    @Test
    @WithMockUser
    void 알림_이력과_모니터_상태를_조회한다() throws Exception {
        given(alerts.findTop50ByOrderByOccurredAtDesc()).willReturn(List.of(
                new AlertEvent("LAG_HIGH", "g1", "랙 초과", 1500, 1000, Instant.now())));
        given(collector.lastSuccessAt()).willReturn(Instant.parse("2026-08-07T01:00:00Z"));
        given(collector.consecutiveFailures()).willReturn(0);
        given(certChecker.lastStatuses()).willReturn(List.of());

        mvc.perform(get("/api/alerts"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].ruleType").value("LAG_HIGH"));
        mvc.perform(get("/api/monitor/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.consecutiveFailures").value(0))
                .andExpect(jsonPath("$.lastCollectedAt").value("2026-08-07T01:00:00Z"));
    }

    @Test
    void 미인증이면_401() throws Exception {
        mvc.perform(get("/api/alerts")).andExpect(status().isUnauthorized());
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew test --tests 'MonitorControllerTest'`
Expected: 컴파일 실패

- [ ] **Step 3: 구현**

```java
package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.monitor.AlertEvent;
import com.osstem.kafkaadmin.monitor.AlertEventRepository;
import com.osstem.kafkaadmin.monitor.CertExpiryChecker;
import com.osstem.kafkaadmin.monitor.CertExpiryChecker.CertStatus;
import com.osstem.kafkaadmin.monitor.MetricSampleRepository;
import com.osstem.kafkaadmin.monitor.MetricsCollector;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@RestController
@RequestMapping("/api")
public class MonitorController {

    public record SamplePoint(Instant sampledAt, double value) {}
    public record MonitorStatus(Instant lastCollectedAt, int consecutiveFailures,
                                List<CertStatus> certs) {}

    private final MetricSampleRepository samples;
    private final AlertEventRepository alerts;
    private final MetricsCollector collector;
    private final CertExpiryChecker certChecker;

    public MonitorController(MetricSampleRepository samples, AlertEventRepository alerts,
                             MetricsCollector collector, CertExpiryChecker certChecker) {
        this.samples = samples;
        this.alerts = alerts;
        this.collector = collector;
        this.certChecker = certChecker;
    }

    @GetMapping("/metrics")
    public List<SamplePoint> metrics(@RequestParam String type,
                                     @RequestParam String subject,
                                     @RequestParam(defaultValue = "24") int hours) {
        return samples.findByMetricTypeAndSubjectKeyAndSampledAtAfterOrderBySampledAt(
                        type, subject, Instant.now().minus(hours, ChronoUnit.HOURS)).stream()
                .map(s -> new SamplePoint(s.getSampledAt(), s.getValue()))
                .toList();
    }

    @GetMapping("/alerts")
    public List<AlertEvent> alerts() {
        return alerts.findTop50ByOrderByOccurredAtDesc();
    }

    @GetMapping("/monitor/status")
    public MonitorStatus status() {
        return new MonitorStatus(collector.lastSuccessAt(),
                collector.consecutiveFailures(), certChecker.lastStatuses());
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew test --tests 'MonitorControllerTest'`
Expected: 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: monitor REST API"
```

---

### Task 8: 프론트 (랙 추이 차트 + 알림 화면 + 상태 카드)

**Files:**
- Create: `web/src/lib/trend.ts`, `web/src/components/TrendChart.vue`, `web/src/views/AlertsView.vue`
- Modify: `web/src/router/index.ts` (route `/alerts` 추가), `web/src/App.vue` (nav에 "알림"), `web/src/views/GroupDetailView.vue` (랙 추이 차트), `web/src/views/ClusterView.vue` (모니터 상태 카드)
- Test: `web/src/lib/__tests__/trend.spec.ts`

**Interfaces:**
- Consumes: Task 7 API, 기존 `api<T>()`
- Produces: `toPolyline(values: number[], w: number, h: number, pad: number): string` (SVG points 문자열; 값 1개면 수평선, 전부 동일 값이면 중앙 수평선)

차트 규칙(디자인 가이드 반영): 단일 시리즈라 범례 없음 — 카드 제목("최근 24시간 랙 추이")이 시리즈명을 겸한다. 선 2px 단색 `#1d4ed8`, 이중 축 금지, 숫자 텍스트는 본문 텍스트 색. 마지막 값을 차트 우측에 직접 라벨. 호버 시 가장 가까운 점에 8px 마커 + 시각·값 표시.

- [ ] **Step 1: 실패하는 테스트 작성**

`web/src/lib/__tests__/trend.spec.ts`:

```ts
import { describe, it, expect } from 'vitest'
import { toPolyline } from '../trend'

describe('toPolyline', () => {
  it('값들을 좌우 패딩 안에서 x 등간격, y는 최대값 기준으로 배치한다', () => {
    // w=100, h=40, pad=10 → x: 10~90, y: 최대값=30(y=10), 최소값 0(y=30)
    const pts = toPolyline([0, 15, 30], 100, 40, 10)
    expect(pts).toBe('10,30 50,20 90,10')
  })
  it('값이 1개면 중앙 수평선을 만든다', () => {
    expect(toPolyline([7], 100, 40, 10)).toBe('10,20 90,20')
  })
  it('모든 값이 같으면 중앙 수평선', () => {
    expect(toPolyline([5, 5], 100, 40, 10)).toBe('10,20 90,20')
  })
  it('빈 배열은 빈 문자열', () => {
    expect(toPolyline([], 100, 40, 10)).toBe('')
  })
})
```

Run: `cd web && npm run test:unit -- --run`
Expected: FAIL (`../trend` 없음)

- [ ] **Step 2: trend.ts 구현 + 테스트 통과**

`web/src/lib/trend.ts`:

```ts
// SVG polyline points 문자열 생성. y축은 0 ~ max(values) 범위(패딩 안쪽).
export function toPolyline(values: number[], w: number, h: number, pad: number): string {
  if (values.length === 0) return ''
  const innerW = w - pad * 2
  const innerH = h - pad * 2
  const max = Math.max(...values)
  const min = Math.min(...values)
  if (values.length === 1 || max === min) {
    const y = pad + innerH / 2
    return `${pad},${y} ${w - pad},${y}`
  }
  return values
    .map((v, i) => {
      const x = pad + (innerW * i) / (values.length - 1)
      const y = pad + innerH - (innerH * v) / max
      return `${x},${y}`
    })
    .join(' ')
}
```

Run: `npm run test:unit -- --run`
Expected: trend 4건 + lag 2건 PASS

- [ ] **Step 3: TrendChart.vue 구현**

`web/src/components/TrendChart.vue`:

```vue
<script setup lang="ts">
import { computed, ref } from 'vue'
import { toPolyline } from '@/lib/trend'

const props = defineProps<{ points: { t: string; v: number }[] }>()

const W = 320
const H = 80
const PAD = 8
const hoverIndex = ref<number | null>(null)

const values = computed(() => props.points.map((p) => p.v))
const polyline = computed(() => toPolyline(values.value, W, H, PAD))
const last = computed(() => props.points.at(-1))
const hovered = computed(() =>
  hoverIndex.value === null ? null : props.points[hoverIndex.value],
)

function xOf(i: number): number {
  const n = props.points.length
  return n < 2 ? W / 2 : PAD + ((W - PAD * 2) * i) / (n - 1)
}
function yOf(i: number): number {
  const max = Math.max(...values.value)
  const v = values.value[i] ?? 0
  if (max === 0) return H / 2
  return PAD + (H - PAD * 2) - ((H - PAD * 2) * v) / max
}
function onMove(e: MouseEvent) {
  if (props.points.length === 0) return
  const rect = (e.currentTarget as SVGElement).getBoundingClientRect()
  const x = ((e.clientX - rect.left) / rect.width) * W
  const n = props.points.length
  const i = Math.round(((x - PAD) / (W - PAD * 2)) * (n - 1))
  hoverIndex.value = Math.min(Math.max(i, 0), n - 1)
}
</script>

<template>
  <div class="trend">
    <svg
      :viewBox="`0 0 ${W} ${H}`"
      role="img"
      aria-label="랙 추이 차트"
      @mousemove="onMove"
      @mouseleave="hoverIndex = null"
    >
      <polyline :points="polyline" fill="none" stroke="#1d4ed8" stroke-width="2" />
      <circle
        v-if="hoverIndex !== null"
        :cx="xOf(hoverIndex)"
        :cy="yOf(hoverIndex)"
        r="4"
        fill="#1d4ed8"
      />
    </svg>
    <p class="reading">
      <template v-if="hovered">
        {{ new Date(hovered.t).toLocaleTimeString() }} — {{ hovered.v.toLocaleString() }}
      </template>
      <template v-else-if="last"> 현재 {{ last.v.toLocaleString() }} </template>
      <template v-else> 데이터 없음 </template>
    </p>
  </div>
</template>

<style scoped>
.trend svg { width: 100%; max-width: 480px; display: block; }
.reading { margin: 0.25rem 0 0; font-size: 0.85rem; color: #555; }
</style>
```

- [ ] **Step 4: 화면 연결**

`web/src/views/AlertsView.vue`:

```vue
<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api } from '@/api/client'

interface AlertEvent {
  ruleType: string
  subjectKey: string
  message: string
  value: number
  threshold: number
  occurredAt: string
}

const alerts = ref<AlertEvent[]>([])
const error = ref('')

onMounted(async () => {
  try {
    alerts.value = await api<AlertEvent[]>('/alerts')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
})
</script>

<template>
  <main>
    <h1>알림 이력</h1>
    <p v-if="error" class="error">{{ error }}</p>
    <p v-else-if="alerts.length === 0">알림이 없습니다.</p>
    <table v-else>
      <thead>
        <tr><th>시각</th><th>유형</th><th>대상</th><th>내용</th></tr>
      </thead>
      <tbody>
        <tr v-for="a in alerts" :key="a.occurredAt + a.ruleType + a.subjectKey">
          <td>{{ new Date(a.occurredAt).toLocaleString() }}</td>
          <td>{{ a.ruleType }}</td>
          <td>{{ a.subjectKey }}</td>
          <td>{{ a.message }}</td>
        </tr>
      </tbody>
    </table>
  </main>
</template>
```

`router/index.ts`의 routes 배열에 추가 (`/groups/:groupId` 라우트 다음):

```ts
    { path: '/alerts', component: () => import('@/views/AlertsView.vue') },
```

`App.vue` nav에 추가 (컨슈머 그룹 링크 다음):

```vue
    <RouterLink to="/alerts">알림</RouterLink>
```

`GroupDetailView.vue` — script에 추가:

```ts
import TrendChart from '@/components/TrendChart.vue'

interface SamplePoint { sampledAt: string; value: number }
const trend = ref<{ t: string; v: number }[]>([])
```

onMounted의 try 블록 안, detail 로드 다음에 추가:

```ts
    const pts = await api<SamplePoint[]>(
      `/metrics?type=LAG&subject=${route.params.groupId}&hours=24`,
    )
    trend.value = pts.map((p) => ({ t: p.sampledAt, v: p.value }))
```

template의 상태/총랙 `<p>` 아래에 추가:

```vue
      <h2>최근 24시간 랙 추이</h2>
      <TrendChart :points="trend" />
```

`ClusterView.vue` — script에 추가:

```ts
interface CertStatus { broker: string; notAfter: string; daysRemaining: number }
interface MonitorStatus {
  lastCollectedAt: string | null
  consecutiveFailures: number
  certs: CertStatus[]
}
const monitor = ref<MonitorStatus | null>(null)
```

onMounted의 try 블록 안에 추가:

```ts
    monitor.value = await api<MonitorStatus>('/monitor/status')
```

template의 브로커 테이블 아래에 추가:

```vue
      <template v-if="monitor">
        <h2>감시 상태</h2>
        <p>
          마지막 수집:
          {{ monitor.lastCollectedAt ? new Date(monitor.lastCollectedAt).toLocaleString() : '없음' }}
          <span v-if="monitor.consecutiveFailures > 0" class="warn">
            (연속 실패 {{ monitor.consecutiveFailures }}회)
          </span>
        </p>
        <table v-if="monitor.certs.length > 0">
          <thead><tr><th>브로커</th><th>인증서 만료</th><th>남은 일수</th></tr></thead>
          <tbody>
            <tr v-for="c in monitor.certs" :key="c.broker">
              <td>{{ c.broker }}</td>
              <td>{{ new Date(c.notAfter).toLocaleDateString() }}</td>
              <td :class="{ warn: c.daysRemaining <= 30 }">D-{{ c.daysRemaining }}</td>
            </tr>
          </tbody>
        </table>
        <p v-else>인증서 정보 없음 (PLAINTEXT 구성이거나 아직 점검 전)</p>
      </template>
```

ClusterView.vue에 `.warn` 스타일이 없으므로 `<style scoped>` 추가:

```vue
<style scoped>
.warn { color: #c00; font-weight: bold; }
</style>
```

- [ ] **Step 5: 검증 + Commit**

Run: `npm run test:unit -- --run && npm run build`
Expected: 6 tests PASS(신규 4 + 기존 2), 빌드 성공

```bash
cd ~/Documents/kafka-admin && git add -A && git commit -m "feat: lag trend chart, alerts view, and monitor status card"
```

---

## 컨트롤러 최종 검증 (수동)

로컬 단일 브로커 + WAS + 프론트로 1단계와 동일하게 기동한 뒤:
1. 그룹 상세에 랙 추이 차트가 수집 주기(60초) 경과 후 점이 늘며 그려지는지
2. `MONITOR_LAG_THRESHOLD=0`으로 WAS를 재기동해 인위적으로 LAG_HIGH를 발생시켜 /alerts 화면에 뜨는지
3. /api/monitor/status의 lastCollectedAt이 갱신되는지

## 3단계 이후 (범위 밖)

- 웹훅/메일 AlertNotifier 구현체, 임계치 DB 관리 화면
- ops 모듈(조치 + 확인 + 감사 로그), URP/BROKER_COUNT 변화 감지 알림
