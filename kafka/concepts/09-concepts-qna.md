# 개념 정리 Q&A (실무 관점)

앞의 개념 문서들([01](01-kafka-basics.md)~[08](08-configuration-reference.md))을 **자주 헷갈리는 질문 관점**에서 다시 묶고, 비교·구분·확인 방법을 보충합니다. 세부 설명은 각 원본 문서로 링크합니다.

## 1. broker는 "Kafka 인스턴스"인가?

거의 맞습니다. **broker = 실행 중인 Kafka 프로세스 1개 = Kafka 인스턴스 1개**입니다.

- "인스턴스 = 프로세스"이지 "= 장비"는 아닙니다. 보통 서버 1대에 broker 1개를 권장하지만, 포트·`log.dirs`를 달리하면 한 장비에 여러 개도 가능(테스트용).
- **KRaft에서는 "node"가 더 정확한 명칭**입니다. 한 프로세스가 `process.roles`로 broker·controller·둘 다를 맡습니다.

```text
process.roles = broker,controller  → 우리 3대 구성 (각 인스턴스가 broker이자 controller)
```

- ZooKeeper 모드에선 모든 프로세스 = broker이고 그중 1대가 controller를 겸함 → "broker = 인스턴스"가 거의 1:1.

## 2. Kafka 복제 vs Redis Cluster 복제

복제의 **단위**가 다릅니다.

| | Redis Cluster | Kafka |
| --- | --- | --- |
| 샤딩 단위 | hash slot (→ master 노드) | partition |
| 복제 단위 | **노드(master) 전체** | **파티션** |
| 복제본 위치 | **별도 replica 노드** | 같은 브로커들에 분산 |
| 3샤드 HA 최소 대수 | 6대 (3 master + 3 replica) | **3대** (RF3) |
| 노드 역할 | master 또는 replica (노드 단위 고정) | 모든 브로커가 일부는 리더·일부는 팔로워 |

- Redis는 "master마다 replica 노드를 **따로**" 붙입니다.
- Kafka는 별도 replica 노드가 없고, `replication.factor=3`이면 **각 파티션 사본이 기존 브로커들에 흩어져** 배치됩니다. 그래서 **3대만으로 3중 복제 + HA**가 됩니다.

## 3. 파티션(나누기)과 복제(복사)는 다른 축

두 개념을 분리해서 봐야 합니다. 자세히는 [01](01-kafka-basics.md), [02](02-producer-and-replication.md).

```text
① 파티셔닝(나눔)  : 토픽 데이터를 P0/P1/P2로 "나눠" 가짐   → 분할(샤딩), 처리량↑
② 복제(RF=3)      : 각 파티션을 여러 브로커에 "똑같이" 복사  → 이중화(무손실/HA)
```

3대 + RF3 데이터 분포:

```text
          서버1              서버2              서버3
 P0    ★리더[A,D]         팔로워[A,D]        팔로워[A,D]
 P1    팔로워[B,E]        ★리더[B,E]         팔로워[B,E]
 P2    팔로워[C]          팔로워[C]          ★리더[C]
       전체 사본 1벌        전체 사본 1벌        전체 사본 1벌
```

- **가로(파티션)**: 데이터를 나눔 — A·D는 P0, B·E는 P1, C는 P2.
- **세로(복제)**: 각 파티션을 3대에 복사 — 같은 파티션의 3사본은 **내용이 동일**(리더 1 + 팔로워 2).
- RF=3에서는 결국 **모든 서버가 전체 데이터의 완전한 사본**을 갖고, 리더십만 분산됩니다.

## 4. 순서 보장과 key (3파티션 순서 시뮬레이션)

**순서는 "파티션 안에서만" 보장**됩니다(offset 순서, 영구). 파티션 사이 전역 순서는 보장되지 않습니다. 그래서 **순서를 지켜야 하는 단위를 key로 지정**해 같은 파티션에 모읍니다.

예: 토픽 `order-events`(파티션 3개), key = 주문ID, 지켜야 할 순서 `생성 → 결제 → 배송`.

```text
 hash("1001") % 3 = 1 → 주문1001 이벤트는 항상 P1
 hash("1002") % 3 = 2 → 주문1002 이벤트는 항상 P2

 결과:
 P1 (주문1001): [생성, 결제, 배송]   ← 한 파티션 안에 순서대로 ✅
 P2 (주문1002): [생성, 결제, 배송]   ← ✅

 컨슈머: P1→A, P2→B 가 각자 순차 처리 → 주문별 순서 유지 + 병렬 처리
```

key 없이 보내면(=null) 같은 주문 이벤트가 여러 파티션에 흩어져 **순서가 깨집니다.**

주의:

- key 단위 안에서만 순서 보장(주문 간 순서는 X). 전역 순서가 필요하면 파티션 1개(처리량 trade-off).
- 재시도로 순서가 흐트러지지 않게 `enable.idempotence=true`(기본) 유지. 끄면 `max.in.flight.requests.per.connection=1`.
- **파티션 수를 나중에 바꾸면**(`hash % N`의 N 변경) 같은 key가 다른 파티션으로 갈 수 있어 순서 연속성이 깨집니다. 처음에 여유 있게 잡습니다.

## 5. 멱등 프로듀서(`enable.idempotence`)와 언제 끄나

ack 유실로 인한 **재전송 중복을 막는** 기능입니다(프로듀서에 PID 부여 + 파티션별 순번으로 중복 판별). 자세히는 [02](02-producer-and-replication.md).

- **켜면**: 재시도해도 딱 한 번 저장 + 순서 보존. Kafka 3.0+ 기본 `true`.
- 요구 조건: `acks=all`, `retries>0`, `max.in.flight ≤ 5`.

**false로 두는 경우** (특수 상황):

| 상황 | 이유 |
| --- | --- |
| 최고 처리량/유실·중복 허용 | `acks=0/1`로 빠르게 (멱등성은 `acks=all` 요구) |
| fire-and-forget | `retries=0` (멱등성은 `retries>0` 요구) |
| in-flight를 5보다 크게 | 고지연 링크 처리량 (순서·중복 보장 포기) |
| 아주 오래된 브로커(<0.11) | 호환 (요즘 거의 없음) |

MSA 무손실이 목표면 **끄지 말고 기본값(true) 유지**.

## 6. 설정 파일 지형과 "앱 코드 vs CLI"

### 설정 파일은 몇 개인가

고정된 총 개수는 없고 버전·컴포넌트에 따라 다릅니다. `config/` 표준 파일:

| 파일 | 대상 | 언제 |
| --- | --- | --- |
| `server.properties` | 브로커/컨트롤러 | 항상 |
| `producer.properties` | **CLI** 프로듀서 기본값 | CLI로 보낼 때 |
| `consumer.properties` | **CLI** 컨슈머 기본값 | CLI로 읽을 때 |
| `log4j2.yaml` | 브로커 로깅 | 필요시 |
| `zookeeper.properties` | ZooKeeper | ZK 모드에서만 (4.x 없음) |
| `connect-*.properties` | Kafka Connect | Connect 쓸 때 |

자동 생성/직접 생성: `meta.properties`(storage format이 `log.dirs`에 생성, 편집 X), JAAS 파일(SASL 쓸 때).

### 앱 코드 vs CLI — 프로듀서/컨슈머 설정을 넣는 두 주체

`acks`, `enable.idempotence` 같은 클라이언트 설정은 브로커(`server.properties`/compose)가 아니라 **메시지를 주고받는 쪽**에 둡니다.

| | 앱 코드 | CLI |
| --- | --- | --- |
| 주체 | 실행 중인 서비스(MSA 앱) | 사람이 터미널에서 |
| 설정 위치 | 코드 `Properties` / `application.yml` | `--producer-property` / `.properties` 파일 |
| 용도 | **실제 데이터 흐름**, 상시 운영 | 테스트·점검(일회성) |

```java
// 앱 코드 (운영 데이터는 여기서 흐름)
props.put("acks", "all");
props.put("enable.idempotence", "true");
```
```bash
# CLI (터미널에서 확인용)
kafka-console-producer.sh --bootstrap-server kafka1:9094 --topic order-created \
  --producer-property acks=all --producer-property enable.idempotence=true
```

### `producer.properties`에 대한 흔한 오해

`config/producer.properties`는 **처음부터 "CLI 도구용 기본 설정 파일"** 이지, 앱의 프로듀서 설정 방식이 아니었습니다. 앱은 예나 지금이나 코드(또는 `application.yml`)로 설정합니다. **방식이 바뀐 게 아니며**, 실제로 변한 것은 설정 기본값입니다(예: `acks` 1→all, `enable.idempotence` false→true가 3.0부터 기본).

## 7. 클러스터가 구성됐는지 확인하는 법

"클러스터 모드 스위치"는 없습니다. 같은 조정 계층(같은 KRaft 쿼럼 / 같은 ZooKeeper)에 서로 다른 id로 등록된 브로커들이 자동으로 한 클러스터가 됩니다. 확인 순서:

```bash
# 1) 등록된 브로커가 다 보이나 (KRaft/ZK 공통)
kafka-broker-api-versions.sh --bootstrap-server <host>:9092    # 3개 보이면 OK
kafka-cluster.sh cluster-id --bootstrap-server <host>:9092     # 클러스터 ID

# 2) ZooKeeper 모드면 직접 확인
zookeeper-shell.sh <zk>:2181 ls /brokers/ids                   # [1, 2, 3] 이면 OK

# 3) 복제가 실제 3대에 걸치나
kafka-topics.sh --bootstrap-server <host>:9092 --describe --topic <토픽>
#   → Replicas/Isr 에 1,2,3 이 보이면 정상
```

설정 레벨 필요조건: `broker.id`(또는 `node.id`)는 **노드마다 유일**, `zookeeper.connect`(또는 `controller.quorum.voters`)와 `cluster.id`는 **3대 동일**.

## 8. 압축은 누가 하고 누가 푸나 (`compression.type`)

"프로듀서에서 압축해서 보낼 수 있다"는 말은 맞습니다. 다만 압축의 **단위·위치·해제 주체**를 알아야 설정을 어디에 두는지 헷갈리지 않습니다.

```text
프로듀서                       브로커                          컨슈머
배치(같은 파티션행 묶음)  ──▶  받은 압축 배치를 그대로 저장  ──▶  압축 배치를 받아
를 lz4/zstd 로 압축            (compression.type=producer)         클라이언트가 자동 해제
```

| 질문 | 답 |
| --- | --- |
| 메시지 하나씩 압축되나? | 아니요. **레코드 배치**(같은 파티션으로 가는 묶음)를 통째로 압축합니다. 그래서 `batch.size`·`linger.ms`로 배치를 키울수록 압축률이 좋아지고, `send().get()`으로 한 건씩 동기 전송하면 이득이 거의 없습니다 |
| 브로커도 CPU를 쓰나? | 기본값(`compression.type=producer`)이면 **안 씁니다** — 받은 그대로 디스크에 쓰고 팔로워에 복제합니다. 브로커/토픽에 다른 코덱을 고정하면 풀어서 재압축하므로 CPU를 씁니다 |
| 컨슈머 설정이 필요한가? | 없습니다. 배치 헤더에 코덱이 적혀 있어 클라이언트가 자동으로 풉니다. 코드도 모릅니다 |
| 어느 코덱? | `lz4`(처리량 우선, 기본 추천) 또는 `zstd`(압축률 우선). `gzip`은 CPU 비용이 커서 대역폭이 정말 귀할 때만 |
| 크기 한도는 압축 전? 후? | **후**입니다. 프로듀서 `max.request.size`, 브로커 `message.max.bytes`, 컨슈머 `fetch.max.bytes` 모두 압축된 크기 기준 |
| 압축하면 보안이 되나? | 아니요. 압축은 크기만 줄입니다. 전송 보안은 TLS(SASL_SSL)가 담당 — [10](10-security-tls-and-auth.md) |
| 순서·멱등에 영향은? | 없습니다. 압축은 배치 안 내용을 바꿀 뿐, 파티션 배정(key)·시퀀스 번호(멱등)와 무관합니다 |

**설정 위치 한 줄 요약**: 프로듀서에 `compression.type=lz4`(+ `batch.size`, `linger.ms`), 브로커·토픽은 `producer`(기본) 유지, 컨슈머는 손대지 않음. 값과 Spring 예시는 [08](08-configuration-reference.md) 5·7장, [12](12-usage-principles.md) 4장.

## 9. 토픽 이름은 실무에서 어떻게 짓나 (업계 패턴 비교)

공개된 통계는 없지만(토픽 이름은 회사 내부 자산), 벤더 가이드·관리 도구 업체의 고객 관찰·도구 기본값·공개 기술 블로그가 모두 같은 방향을 가리킵니다: **소문자 + `.` 계층 + 도메인 우선 이벤트명**이 대다수이고, 규모가 커지면 거기에 분류 자리나 범위 접두사를 덧붙입니다. 이 클러스터의 규칙([12](12-usage-principles.md) 2장)은 그 다수 관행과 같습니다.

### 모두가 동의하는 원칙

| 원칙 | 이유 |
| --- | --- |
| 소문자만 | `Order`/`order`가 공존 가능해 사고 원인 |
| 구분자는 하나로 고정(계층은 `.`) | Kafka가 메트릭 이름에서 `.`을 `_`로 바꾸므로 `a.b`와 `a_b`가 JMX 메트릭에서 충돌(생성 시 경고) |
| "무슨 데이터인가"로 짓고 "누가 쓰는가"로 짓지 않는다 | 컨슈머·팀·서비스·제품명은 바뀌고 도메인·이벤트는 안 바뀜 |
| 바뀌는 메타데이터는 이름 밖으로 | 파티션 수·보존 기간·스키마·소유 팀 → 설정·카탈로그·태그로 |
| 첫 운영 배포 전에 정하고 자동 검증 | 나중에 바꾸면 데이터 마이그레이션 + 모든 앱 수정 |

### 실무 패턴 4종 (같은 "주문 결제 완료" 이벤트로 비교)

| 패턴 | 예 | 이름이 말해 주는 것 | 어울리는 조직 |
| --- | --- | --- | --- |
| **A. 도메인.엔티티.이벤트** | `order.paid` / `commerce.order.paid` | 도메인, 무슨 일(과거형) | 도메인 이벤트 위주, 토픽 수십~수백. **가장 흔함**. 이 클러스터 |
| **B. 도메인.분류.대상.버전** | `commerce.fct.order-paid.0` | + 메시지 성격: `fct` 사실/이벤트 · `cdc` 변경 캡처(현재 상태) · `cmd` 명령 · `sys` 내부용, + 스키마 세대 | 데이터 플랫폼·다팀 공유, CDC·명령·내부 토픽 혼재, 수백 개 이상 |
| **C. 범위/조직 접두사** | `public.commerce.order.paid` / `private.commerce.enrichment` / `blue.test-1` | + 남이 구독해도 되는 공식 계약인지, 또는 팀 자율 공간(prefixed ACL) | 팀 많고 개발 클러스터 공유, 데이터 계약 도입 |
| **D. 환경/리전/테넌트 접두사** | `prod.commerce.order.paid` / `us-east.orders` / `tenant-a.orders` | + 어느 환경·리전·테넌트 | 한 클러스터를 여러 환경이 공유할 때만, 멀티리전(MirrorMaker 2 기본 `<source-cluster>.<topic>`), 멀티테넌트 |

- **A의 핵심 결정**: 이벤트별 토픽(`order.created`, `order.paid`) vs 엔티티별 토픽(`order.events` + 타입 필드). 이벤트별로 나누면 구독은 정밀하지만 **같은 주문의 이벤트가 토픽 사이로 갈라져 순서를 잃습니다.** 순서가 필요하면 엔티티별 한 토픽 + 키=엔티티 ID.
- **B의 분류 자리는 정책의 손잡이**: `fct`는 재생 안전·보존 기간 기반, `cdc`는 `cleanup.policy=compact`·키=PK, `cmd`는 한 컨슈머 그룹만 처리, `sys`는 타 팀 구독 금지 → 분류별로 보존·compaction·ACL을 일괄 적용.
- **C의 팀 접두사**는 "바뀌는 것을 넣지 마라"와 충돌하므로 **개발 클러스터의 자율 공간에만**(이 클러스터의 `sandbox.`), 운영 토픽 이름에는 팀명을 넣지 않음.
- **D는 대부분 피함**: 환경별로 이름이 다르면 앱 설정이 환경마다 달라지고 운영에서 `dev.`를 구독하는 사고가 남. 환경은 클러스터(`bootstrap.servers`)로 분리하고 이름은 동일하게.

실제로는 **A를 기본으로 필요한 자리만 빌리는 혼합형**이 가장 많습니다: `commerce.order.paid`(이벤트) + `cdc.orders.orders`(CDC) + `sandbox.<팀>.*`(개발 자율) + 클러스터로 환경 분리.

### 진영이 갈리는 지점

| 쟁점 | 다수 | 이유 |
| --- | --- | --- |
| 환경(dev/prod)을 이름에? | 넣지 않음 | 위 D 참고 |
| 버전을 이름에? | Schema Registry 있으면 넣지 않음, 없으면 깨질 때만 `.v2` | 토픽 난립 방지 vs 호환 깨지는 변경의 명시 |
| 팀/서비스명을 이름에? | 넣지 않음 | 소유권은 태그·카탈로그·ACL로 |
| 구분자 | 계층 `.`, 단어 연결 `-` | `_`는 메트릭 충돌로 기피, camelCase는 드묾 |

### 도구가 만드는 파생 이름 (규칙이 이것들과 충돌하지 않게)

| 종류 | 관례 |
| --- | --- |
| 재시도 / DLQ | `<topic>.retry`, `<topic>.retry.5s`, `<topic>.dlq` — Uber 재처리 아키텍처(단계별 재시도 토픽), Spring Kafka 기본 `-retry-N`/`-dlt` |
| Kafka Streams 내부 | `<application.id>-<store>-changelog`, `<application.id>-<name>-repartition` |
| CDC (Debezium) | `<server>.<schema>.<table>` (`pg1.public.orders`) |
| Kafka Connect 내부 | `connect-configs`, `connect-offsets`, `connect-status` |
| MirrorMaker 2 | `<source-cluster>.<topic>` |

### 지키게 만드는 방법

- 브로커 `auto.create.topics.enable=false` + 토픽 생성은 GitOps/IaC(Strimzi `KafkaTopic`, Terraform 등)로만 → PR에서 이름 정규식·파티션 수·보존 한도 검사, 커밋 이력이 승인 기록.
- 개발 클러스터는 팀/`sandbox.` 접두사 + prefixed ACL로 자율 생성, 운영은 승인.
- 소유자·연락처·데이터 등급은 이름이 아니라 메타데이터(태그·카탈로그)로.

## 관련 문서

- [01. Kafka 핵심 개념](01-kafka-basics.md) · [02. Producer와 복제](02-producer-and-replication.md) · [03. Consumer와 Consumer Group](03-consumer-and-consumer-group.md)
- [04. 브로커 내부 구조](04-broker-internals.md) · [05. KRaft vs ZooKeeper](05-kraft-vs-zookeeper.md)
- [06. 클러스터 설계](06-cluster-design.md) · [07. 설치](07-kraft-cluster-installation.md) · [08. 설정 레퍼런스](08-configuration-reference.md)
- [12. 사용 원칙(개발자용)](12-usage-principles.md) · [13. 메시지 키와 순서 보장](13-message-key-and-ordering.md)

## 참고한 공식 문서

- [Kafka Design](https://kafka.apache.org/documentation/#design)
- [Producer Configs](https://kafka.apache.org/documentation/#producerconfigs) / [Consumer Configs](https://kafka.apache.org/documentation/#consumerconfigs)
- [KRaft Overview](https://kafka.apache.org/documentation/#kraft)
- 토픽 명명(업계 자료): [Confluent — Topic Naming Convention](https://www.confluent.io/learn/kafka-topic-naming-convention/) · [Conduktor — Rules & Restrictions](https://www.conduktor.io/kafka/kafka-topics-naming-convention) · [Kadeck — 5 recommendations](https://www.kadeck.com/blog/kafka-topic-naming-conventions-5-recommendations-with-examples) · [devshawn — Topic Naming Conventions](https://dev.to/devshawn/apache-kafka-topic-naming-conventions-3do6) · [Chris Riccomini — Kafka topic naming](https://cnr.sh/posts/2017-08-29-how-paint-bike-shed-kafka-topic-naming-conventions/) · [Uber — Reliable Reprocessing and DLQ](https://www.uber.com/us/en/blog/reliable-reprocessing/) · [IBM — Taming the Kafka topics wild west](https://community.ibm.com/community/user/blogs/dale-lane1/2024/09/17/taming-the-kafka-topics-wild-west)
