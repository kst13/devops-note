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

## 관련 문서

- [01. Kafka 핵심 개념](01-kafka-basics.md) · [02. Producer와 복제](02-producer-and-replication.md) · [03. Consumer와 Consumer Group](03-consumer-and-consumer-group.md)
- [04. 브로커 내부 구조](04-broker-internals.md) · [05. KRaft vs ZooKeeper](05-kraft-vs-zookeeper.md)
- [06. 클러스터 설계](06-cluster-design.md) · [07. 설치](07-kraft-cluster-installation.md) · [08. 설정 레퍼런스](08-configuration-reference.md)

## 참고한 공식 문서

- [Kafka Design](https://kafka.apache.org/documentation/#design)
- [Producer Configs](https://kafka.apache.org/documentation/#producerconfigs) / [Consumer Configs](https://kafka.apache.org/documentation/#consumerconfigs)
- [KRaft Overview](https://kafka.apache.org/documentation/#kraft)
