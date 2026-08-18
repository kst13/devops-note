# Kafka 설정 레퍼런스 (KRaft 3노드 기준)

실서버 3대 KRaft 구성에서 쓰는 주요 설정을 **한곳에 모아** 정리합니다. 각 설정을 왜 그렇게 두는지의 맥락은 [실서버 3대 클러스터 설계 방식](06-cluster-design.md)과 [KRaft 3노드 클러스터 설치 및 설정 방법](07-kraft-cluster-installation.md)에 있고, 이 문서는 "무엇을 어디에 어떤 값으로" 빠르게 찾는 용도입니다.

## 설정이 들어가는 위치

Kafka 브로커/컨트롤러 설정의 실제 파일은 **`server.properties`** 하나입니다. 우리 Docker 구성에서는 이 파일을 직접 쓰지 않고, `apache/kafka` 이미지가 **`KAFKA_` 환경변수를 `server.properties`로 변환**합니다.

```text
docker-compose.yml (KAFKA_* env)  ─변환→  컨테이너 내부 server.properties
kafka-storage format 실행         ─생성→  log.dirs/meta.properties (cluster.id 등, 손대지 않음)
```

**환경변수 → 프로퍼티 변환 규칙**: `KAFKA_` 접두어를 떼고 나머지 `_`를 `.`로 바꿉니다.

```text
KAFKA_PROCESS_ROLES                   → process.roles
KAFKA_CONTROLLER_QUORUM_VOTERS        → controller.quorum.voters
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP  → listener.security.protocol.map
```

> 예외: 프로퍼티 이름에 진짜 `_`가 필요하면 env에서 `__`(더블), `-`가 필요하면 `___`(트리플)로 씁니다. 아래 표의 키들은 해당 없음.

아래 표의 "값" 열은 **우리 3대 MSA 구성 권장값**이고, 괄호 안은 Kafka 기본값입니다.

## 1. KRaft 역할과 쿼럼

| 프로퍼티 (compose env) | 값 (기본값) | 의미 |
| --- | --- | --- |
| `process.roles` (`KAFKA_PROCESS_ROLES`) | `broker,controller` (없음, 필수) | 이 노드가 맡는 역할. combined = 겸용 |
| `node.id` (`KAFKA_NODE_ID`) | `1`/`2`/`3` (없음, 필수) | 노드 고유 식별자. 3대가 서로 달라야 함 |
| `controller.quorum.voters` (`KAFKA_CONTROLLER_QUORUM_VOTERS`) | `1@kafka1:9093,2@kafka2:9093,3@kafka3:9093` | 컨트롤러 쿼럼 구성원. **3대 동일**하게 지정 |
| `controller.listener.names` (`KAFKA_CONTROLLER_LISTENER_NAMES`) | `CONTROLLER` | 컨트롤러 통신에 쓸 리스너 이름 |

## 2. 리스너 (네트워크)

| 프로퍼티 | 값 | 의미 |
| --- | --- | --- |
| `listeners` (`KAFKA_LISTENERS`) | `INTERNAL://:9092,CONTROLLER://:9093,CLIENT://:9094` | 이 노드가 **바인딩**하는 리스너 |
| `advertised.listeners` (`KAFKA_ADVERTISED_LISTENERS`) | `INTERNAL://<실IP>:9092,CLIENT://<실IP>:9094` | 클라이언트/다른 브로커에게 **광고**하는 주소. 실 IP 필수 |
| `listener.security.protocol.map` (`KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`) | `INTERNAL:SASL_SSL,CONTROLLER:SSL,CLIENT:SASL_SSL` | 리스너별 보안 프로토콜 |
| `inter.broker.listener.name` (`KAFKA_INTER_BROKER_LISTENER_NAME`) | `INTERNAL` | 브로커 간 통신에 쓸 리스너 |

주의: `advertised.listeners`에 컨트롤러 리스너는 넣지 않습니다(쿼럼 voters로 알림). 광고 주소는 인증서 SAN과 일치해야 합니다.

## 3. 보안 (SASL/SCRAM + TLS)

| 프로퍼티 | 값 | 의미 |
| --- | --- | --- |
| `sasl.enabled.mechanisms` | `SCRAM-SHA-512` | 허용할 SASL 메커니즘 |
| `sasl.mechanism.inter.broker.protocol` | `SCRAM-SHA-512` | 브로커 간 인증 메커니즘 |
| `ssl.keystore.location` / `...password` | 노드 keystore 경로/비밀번호 | 이 노드의 인증서(키스토어) |
| `ssl.key.password` | 키 비밀번호 | keystore 내 개인키 비밀번호 |
| `ssl.truststore.location` / `...password` | 공통 truststore 경로/비밀번호 | CA 신뢰 저장소 |
| `ssl.client.auth` (`KAFKA_SSL_CLIENT_AUTH`) | `required` (`none`) | 컨트롤러 mTLS: 클라이언트 인증서 필수 |
| `ssl.endpoint.identification.algorithm` | `https` (`https`) | 인증서 hostname 검증 유지 |

시크릿(비밀번호·키)은 파일이 아니라 Secret Manager/Vault로 주입합니다. SCRAM 관리자 계정은 `kafka-storage format --add-scram`으로 부트스트랩합니다([07 문서](07-kraft-cluster-installation.md) §7).

## 4. 복제와 무손실 (MSA 핵심)

근거는 [06 문서](06-cluster-design.md) §2 참고.

| 프로퍼티 | 값 (기본값) | 의미 |
| --- | --- | --- |
| `default.replication.factor` | `3` (`1`) | 새 토픽의 기본 복제 수 |
| `min.insync.replicas` | `2` (`1`) | acks=all 성공에 필요한 최소 ISR |
| `offsets.topic.replication.factor` | `3` (`3`) | 내부 오프셋 토픽 복제 수 |
| `transaction.state.log.replication.factor` | `3` (`3`) | 트랜잭션 상태 로그 복제 수 |
| `transaction.state.log.min.isr` | `2` (`2`) | 트랜잭션 로그 최소 ISR |
| `unclean.leader.election.enable` | `false` (`false`) | 뒤처진 replica의 리더 승격 차단 |
| `auto.create.topics.enable` | `false` (`true`) | 토픽 자동 생성 금지(명시적 생성) |

`min.insync.replicas`와 복제 수는 **토픽 단위 오버라이드**가 우선합니다. 중요한 토픽은 생성 시 다시 지정합니다.

## 5. 로그 저장과 보관

| 프로퍼티 | 값 (기본값) | 의미 |
| --- | --- | --- |
| `log.dirs` (`KAFKA_LOG_DIRS`) | `/var/lib/kafka/data` | 메시지·메타데이터 로그 저장 경로. 전용 디스크 권장 |
| `log.retention.hours` | 용도별 (`168` = 7일) | 시간 기준 보관 기간 |
| `log.retention.bytes` | 용도별 (`-1` = 무제한) | 파티션당 크기 기준 보관 |
| `log.segment.bytes` | (`1073741824` = 1GiB) | 세그먼트 파일 크기 |
| `num.partitions` | 용도별 (`1`) | 자동 생성 시 기본 파티션 수(우리는 auto-create off라 영향 적음) |
| `compression.type` (브로커·토픽) | `producer` (기본 `producer`) | 프로듀서가 보낸 압축 배치를 **그대로 저장**. `gzip`/`lz4` 등으로 고정하면 프로듀서 코덱과 다를 때 브로커가 풀어서 재압축하므로 CPU 낭비 — 코덱은 프로듀서에서 정한다 |
| `message.max.bytes` | (`1048588` ≈ 1MB) | 브로커가 받는 레코드 배치 최대 크기. **압축 후** 기준. 프로듀서 `max.request.size`, 컨슈머 `fetch.max.bytes`와 맞춘다 |

보관 설정은 토픽마다 특성이 다르므로(이벤트 소싱은 길게, 임시 큐는 짧게) 토픽 단위 오버라이드를 권장합니다. `compression.type`은 반대로 토픽에서 건드리지 않고 `producer`로 둡니다.

## 6. 성능·리소스 (선택, 기본값으로 시작 가능)

| 프로퍼티 | 기본값 | 의미 |
| --- | --- | --- |
| `num.network.threads` | `3` | 네트워크 처리 스레드 |
| `num.io.threads` | `8` | 디스크 I/O 스레드 |
| `socket.send.buffer.bytes` / `receive` | `102400` | 소켓 버퍼 |
| `num.recovery.threads.per.data.dir` | `1` | 시작 시 로그 복구 병렬도 |

처음에는 기본값으로 두고, 모니터링 후 병목이 보일 때 조정합니다.

## 7. 클라이언트(MSA 앱) 설정

브로커 설정과 짝을 이뤄야 무손실이 완성됩니다. 이건 `server.properties`가 아니라 **애플리케이션/CLI 쪽 설정**입니다.

Producer:

| 설정 | 값 | 의미 |
| --- | --- | --- |
| `acks` | `all` (3.0+ 기본 all) | ISR 전체 확인 후 성공 |
| `enable.idempotence` | `true` (3.0+ 기본 true) | 재시도 중복 방지 |
| `compression.type` | `lz4` (기본 `none`) | **레코드 배치 단위** 압축. 브로커는 그대로 저장, 컨슈머는 자동 해제 → 코드 변경 없이 네트워크·디스크·복제 트래픽 감소. `lz4`(처리량 우선) 또는 `zstd`(압축률 우선). `gzip`은 CPU 비용 큼 |
| `batch.size` | `65536` (기본 `16384`) | 파티션별 배치 최대 바이트. 압축은 배치 안에서 걸리므로 키울수록 압축률이 좋아짐 |
| `linger.ms` | `5`~`20` (기본 `0`) | 배치를 채우기 위해 기다리는 시간. 약간의 지연을 주고 배치·압축 효율을 얻는다 |
| `max.request.size` | (`1048576` = 1MB) | 요청 1건 최대 크기(**압축 후**). 브로커 `message.max.bytes` 이하 |
| `bootstrap.servers` | 3대 모두 나열 | `kafka1:9094,kafka2:9094,kafka3:9094` |

압축·배치 설정의 배경과 Spring 예시는 [12-usage-principles](12-usage-principles.md) 4장 참고.

Consumer:

| 설정 | 값 | 의미 |
| --- | --- | --- |
| `enable.auto.commit` | `false` | 처리 완료 후 수동 커밋(at-least-once) |
| `isolation.level` | `read_committed` | 트랜잭션 사용 시 |
| `fetch.max.bytes` | (`52428800` = 50MB) | 한 번에 가져올 최대 크기(압축 후). 브로커 `message.max.bytes`보다 커야 큰 배치를 받는다. 압축 해제는 클라이언트가 자동 처리 — 별도 설정 없음 |
| `bootstrap.servers` | 3대 모두 나열 | — |

## 8. 설정 확인·변경 방법

설정은 세 종류로 나뉩니다.

```text
read-only          : server.properties(=compose env) 변경 후 롤링 재시작 필요
                     예) process.roles, listeners, log.dirs
cluster-wide 동적   : kafka-configs 로 무중단 변경 (전 브로커 적용)
                     예) min.insync.replicas 기본값, log.retention.*
per-topic          : 토픽 단위 오버라이드 (가장 자주 씀)
                     예) 토픽별 min.insync.replicas, retention.ms
```

동적/토픽 설정 변경 예시:

```bash
# 클러스터 전역 기본값 변경 (무중단)
kafka-configs.sh --bootstrap-server kafka1:9094 --command-config $CFG \
  --alter --entity-type brokers --entity-default \
  --add-config log.retention.hours=72

# 토픽 단위 오버라이드
kafka-configs.sh --bootstrap-server kafka1:9094 --command-config $CFG \
  --alter --entity-type topics --entity-name order-created \
  --add-config min.insync.replicas=2,retention.ms=604800000

# 현재 적용값 확인
kafka-configs.sh --bootstrap-server kafka1:9094 --command-config $CFG \
  --describe --entity-type topics --entity-name order-created
```

read-only 설정을 바꿀 때는 한 번에 한 노드씩, ISR이 회복된 뒤 다음 노드로 넘어가는 롤링 재시작을 씁니다([07 문서](07-kraft-cluster-installation.md) §11).

## 참고한 공식 문서

- [Broker Configs](https://kafka.apache.org/documentation/#brokerconfigs)
- [Topic Configs](https://kafka.apache.org/documentation/#topicconfigs)
- [Producer Configs](https://kafka.apache.org/documentation/#producerconfigs) / [Consumer Configs](https://kafka.apache.org/documentation/#consumerconfigs)
- [Updating configs dynamically](https://kafka.apache.org/documentation/#dynamicbrokerconfigs)
- [apache/kafka Docker image (env 변환)](https://hub.docker.com/r/apache/kafka)
