# Kafka 모니터링 — Prometheus와 Grafana

> Kafka는 브로커 1대가 죽어도 겉으로는 멀쩡하게 동작하도록 설계돼 있습니다. 그래서 **"아직 사고는 아니지만 안전 마진이 사라진 상태"** 는 로그에 안 나오고 메트릭으로만 보입니다. 이 문서는 무엇을 위해 모니터링하는지(목적), 다른 회사들은 어떻게 하는지(관행), 이 클러스터에는 어떻게 붙이는지(구축)를 순서대로 다룹니다.

## 1. 무엇을 위해 모니터링하나

| 목적 | 보는 것 | 안 보면 생기는 일 |
| --- | --- | --- |
| 장애 조기 감지 | `UnderReplicatedPartitions`, ISR 축소, 컨트롤러 수 | 복제본이 줄어든 채(무손실 보장이 약해진 채) 며칠 돌다가, 두 번째 장애에서 데이터 유실로 터짐 |
| 데이터 신선도 SLA | 컨슈머 그룹 **lag** | "주문 알림이 몇 분 늦게 나가는" 성능 저하를 사용자 신고로 처음 알게 됨 |
| 용량 계획 | 트래픽 추이, 디스크 증가율, 브로커 간 부하 편차 | 증설 시점을 감으로 결정, 디스크 풀로 클러스터 정지 |
| 원인의 계층 분리 | 앱 지표 · 브로커 지표 · 그룹 lag을 나란히 | "메시지가 늦어요" 신고에서 앱/클러스터/특정 그룹 중 어디 문제인지 구분 불가 |

- **lag은 비즈니스 언어로 "데이터가 지금 몇 분 늦고 있나"입니다.** 컨슈머 죽음, 처리 속도 부족, 핫 파티션이 전부 lag 그래프 하나에 나타납니다. 모니터링 대상 1순위.
- 사용 원칙([12](12-usage-principles.md) 1장)의 "관측: 컨슈머 lag·복제 상태 모니터링"을 실제로 수행하는 것이 이 문서의 스택입니다.

## 2. 다른 기업들은 어떻게 쓰나

**도구 조합은 사실상 표준입니다.** JMX Exporter + Prometheus + Grafana는 쿠버네티스용 Kafka 오퍼레이터(Strimzi)가 기본 대시보드로 내장할 정도의 업계 표준이고, Confluent Cloud 같은 관리형 서비스도 메트릭을 Prometheus 형식으로 내보내 고객이 자기 Grafana로 보게 합니다. 차이는 도구가 아니라 **운영 방식**에서 갈립니다.

- **LinkedIn**(Kafka 탄생지)은 lag 모니터링 전담 도구 **Burrow**를 만들었습니다. 핵심 아이디어: "lag > 5,000이면 알람" 같은 고정 임계치가 아니라 **lag이 계속 늘어나는 추세인지**를 봅니다. 트래픽 피크에 잠깐 쌓였다 빠지는 것은 정상이기 때문입니다. 이 철학이 현재 대부분 회사의 lag 알람 설계의 바탕입니다.
- **Uber**는 브로커 지표만으로는 "브로커는 건강한데 데이터가 안 온다"를 못 잡아서, 프로듀서→최종 컨슈머의 **end-to-end 지연·유실을 감사(audit)하는 파이프라인**(Chaperone)을 얹었습니다.
- **일반 규모 기업**(수십~수백 토픽, 공유 클러스터)의 가장 흔한 조직 패턴은 **책임 분리**입니다: 플랫폼팀이 브로커 대시보드와 클러스터 알람(URP·컨트롤러·디스크)을 소유하고, **각 서비스팀이 자기 컨슈머 그룹의 lag 대시보드·알람을 직접 소유**합니다. 새벽에 알람 받은 사람 = 바로 고칠 수 있는 사람이어야 하기 때문입니다.
- 알람도 긴급도로 계층화합니다: 클러스터 위험 신호는 즉시 호출, lag 임계치는 메신저 경고, 용량 추세는 주간 리뷰.

성숙도는 "브로커가 살아있나"를 넘어 **"데이터가 제때 흐르고 있나(lag, end-to-end)를 누가 소유하고 어떻게 알람 받느냐**에서 갈립니다.

## 3. 수집 구조 — 세 경로가 각각 다른 계층을 담당

Kafka는 Prometheus 형식을 직접 노출하지 않으므로 중간에 익스포터를 끼웁니다.

```text
브로커(JMX) ──[JMX Exporter 자바 에이전트]──▶ :7071/metrics ─┐
컨슈머 그룹 오프셋 ──[kafka_exporter 컨테이너]──▶ :9308/metrics ─┼──▶ Prometheus ──▶ Grafana
Spring Boot 앱 ──[Micrometer]──▶ /actuator/prometheus ────────┘
```

| 경로 | 담당 | 왜 따로 필요한가 |
| --- | --- | --- |
| ① JMX Exporter | 브로커 건강(복제·컨트롤러·처리량·지연) | 브로커 내부 상태는 JMX로만 나옴 |
| ② kafka_exporter | 컨슈머 그룹 **lag** | lag은 그룹 오프셋에서 계산해야 해서 브로커 JMX에 없음 |
| ③ Micrometer | 앱 관점(전송 실패율, 커밋 지연 등) | 같은 lag이라도 "앱이 느린 것"인지 교차 확인 |

## 4. 구축 방법

### ① 브로커 — JMX Exporter (javaagent)

각 브로커 JVM에 에이전트로 붙입니다. Docker Compose 기준:

```yaml
services:
  kafka1:
    environment:
      KAFKA_OPTS: >-
        -javaagent:/opt/jmx/jmx_prometheus_javaagent.jar=7071:/opt/jmx/kafka-rules.yml
    volumes:
      - ./jmx:/opt/jmx
```

- 규칙 파일(`kafka-rules.yml`)은 Prometheus 공식 jmx_exporter 저장소의 `kafka-2_0_0.yml` 예제를 시작점으로 씁니다.
- 브로커 3대 모두 같은 방식으로 붙이고, 포트(7071)는 내부망에서만 열어둡니다.

### ② lag — kafka_exporter

`danielqsj/kafka_exporter` 컨테이너 하나를 클러스터에 붙이면 `kafka_consumergroup_lag`이 그룹·토픽·파티션 단위로 나옵니다.

```yaml
services:
  kafka-exporter:
    image: danielqsj/kafka-exporter
    command:
      - --kafka.server=kafka1:9094
      - --kafka.server=kafka2:9094
      - --kafka.server=kafka3:9094
      - --sasl.enabled
      - --sasl.mechanism=scram-sha512
      - --sasl.username=${MONITOR_USER}
      - --sasl.password=${MONITOR_PASSWORD}
      - --tls.enabled
    ports:
      - "9308:9308"
```

- SASL_SSL 클러스터이므로 **모니터링 전용 계정**(모든 그룹·토픽 describe 권한)을 발급받아 씁니다. 서비스 계정을 돌려쓰지 마세요.

### ③ 애플리케이션 — Spring Boot Micrometer

서비스 앱에는 의존성 하나 + 노출 설정이면 끝입니다. Spring Kafka가 프로듀서/컨슈머 클라이언트 메트릭을 자동으로 내보냅니다.

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,prometheus
```

```text
implementation "io.micrometer:micrometer-registry-prometheus"
```

### Prometheus 스크레이프 설정

```yaml
scrape_configs:
  - job_name: kafka-broker
    static_configs:
      - targets: ["kafka1:7071", "kafka2:7071", "kafka3:7071"]
  - job_name: kafka-exporter
    static_configs:
      - targets: ["kafka-exporter:9308"]
  - job_name: service-apps
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ["order-service:8080", "notification-service:8080"]
```

### Grafana

- 데이터소스로 Prometheus를 추가하고, 대시보드는 만들지 말고 **가져옵니다**: kafka_exporter용은 커뮤니티 대시보드 ID `7589`, 브로커 JMX용은 Strimzi 프로젝트의 Kafka 대시보드 JSON이 잘 만들어져 있습니다.
- 가져온 뒤 우리 토픽·그룹 이름으로 변수만 조정하면 됩니다.

## 5. 무엇에 알람을 걸까

| 긴급도 | 조건 | 의미 |
| --- | --- | --- |
| 즉시 호출 | `kafka_controller_kafkacontroller_activecontrollercount` 클러스터 합계 ≠ 1 | 컨트롤러 부재/분열 — 클러스터 관리 기능 정지 |
| 즉시 호출 | `kafka_server_replicamanager_underreplicatedpartitions > 0` 5분 지속 | 복제본 부족 — 다음 장애가 곧 유실 |
| 즉시 호출 | `OfflinePartitionsCount > 0` | 읽기/쓰기 불가 파티션 존재 |
| 경고(메신저) | `kafka_consumergroup_lag` 이 서비스별 임계치 초과 **또는 계속 증가 추세** | 데이터 신선도 SLA 위반 진행 중 |
| 경고(메신저) | 디스크 사용률 > 70% | 증설 준비 시작 |
| 주간 리뷰 | BytesIn/Out 추이, 파티션 부하 편차 | 용량 계획 |

- lag 알람은 고정 임계치보다 **추세**(예: 10분 연속 증가)를 함께 보는 것이 오탐이 적습니다(LinkedIn Burrow의 교훈).
- **소유권 원칙**: 클러스터 알람(위 셋)은 플랫폼팀, lag 알람은 **그 토픽을 소비하는 서비스팀**이 소유합니다.

## 더 읽을거리

- 사용 원칙(관측이 왜 핵심 원칙인가): [12-usage-principles](12-usage-principles.md)
- lag이 늘어나는 원인별 해결: [13-message-key-and-ordering](13-message-key-and-ordering.md) · 사용 가이드의 자주 하는 실수
- 설정 레퍼런스: [08-configuration-reference](08-configuration-reference.md)
