# 브로커·컨트롤러·주키퍼 프로세스 구조와 failover 원리

broker, controller, ZooKeeper가 **각각 프로세스로 어떻게 존재하는지**, KRaft의 combined/dedicated 모드가 이 프로세스를 어떻게 나누는지, 그리고 **ZooKeeper 대비 컨트롤러 failover가 왜 빠른지**를 한곳에 정리합니다. [05. KRaft vs ZooKeeper](05-kraft-vs-zookeeper.md)가 "왜 KRaft로 갔나"를 다뤘다면, 이 문서는 "그래서 프로세스가 어떻게 뜨고, 장애 시 무슨 일이 벌어지나"를 다룹니다. [06. 클러스터 설계](06-cluster-design.md)의 combined 모드 선택 근거와 이어집니다.

## 먼저: "프로세스"와 "역할"을 구분한다

가장 흔한 혼동은 컨트롤러를 독립 프로세스로 생각하는 것입니다. 아래 둘을 나눠야 합니다.

- **프로세스**: 실제로 실행 중인 프로그램 하나(JVM 인스턴스 하나). `ps`로 보이는 것.
- **역할(role)**: 그 프로세스가 맡은 일. 한 프로세스가 여러 역할을 겸할 수 있습니다.

핵심 원칙: **ZooKeeper만 항상 별도 프로그램**이고, **broker와 controller는 "Kafka"라는 하나의 프로그램이 맡는 역할**입니다. 모드에 따라 한 프로세스가 두 역할을 겸하기도, 프로세스를 나누기도 합니다.

## 세 가지 배치

### ① ZooKeeper 모드 (Kafka ~3.x)

```text
server1:  [ZooKeeper 프로세스]   [Kafka 프로세스(broker)] ★controller 역할 겸함
server2:  [ZooKeeper 프로세스]   [Kafka 프로세스(broker)]
server3:  [ZooKeeper 프로세스]   [Kafka 프로세스(broker)]
```

- 프로세스는 **2종류**: ZooKeeper + Kafka(broker).
- **controller는 별도 프로세스가 아니라** 브로커 중 하나가 ZooKeeper를 통해 선출되어 겸하는 역할(★)입니다.
- "컨트롤러 서버"는 없고 "지금 컨트롤러 역할을 맡은 브로커"가 있을 뿐입니다.

### ② KRaft combined 모드 (실서버 3대 표준, 현재 예제 구성)

```text
server1:  [Kafka 프로세스  roles=broker,controller] ★active controller
server2:  [Kafka 프로세스  roles=broker,controller]
server3:  [Kafka 프로세스  roles=broker,controller]
          ZooKeeper 없음
```

- 프로세스는 **1종류**(Kafka)이고 서버당 **1개**. 그 하나가 broker와 controller를 **동시에** 맡습니다.
- 3개의 controller 역할 중 하나가 active controller(리더), 나머지는 대기.
- 이 구성이 [PLAINTEXT 예제](../examples/compose-3node-kraft-plaintext/README.md), [SASL_SSL 예제](../examples/compose-3node-kraft/README.md)가 쓰는 방식입니다.

### ③ KRaft dedicated 모드 (대규모에서 분리)

```text
ctrl1:    [Kafka 프로세스  roles=controller] ★active
ctrl2:    [Kafka 프로세스  roles=controller]
ctrl3:    [Kafka 프로세스  roles=controller]
broker1:  [Kafka 프로세스  roles=broker]
broker2:  [Kafka 프로세스  roles=broker]
 ...                                        ZooKeeper 없음
```

- 여전히 프로세스는 **1종류**(전부 같은 Kafka). `process.roles` 값만 다릅니다.
- controller 전용 프로세스와 broker 전용 프로세스로 나뉘고, 여기서는 controller가 **독립 프로세스**가 됩니다.

### 한눈에 비교

| | ZooKeeper 모드 | KRaft combined | KRaft dedicated |
| --- | --- | --- | --- |
| ZooKeeper | **별도 프로세스 있음** | 없음 | 없음 |
| broker | Kafka 프로세스 | Kafka 프로세스의 역할 | broker 전용 Kafka 프로세스 |
| controller | 브로커가 **겸하는 역할** (별도 X) | 같은 프로세스가 겸하는 역할 | controller 전용 Kafka 프로세스 |
| 서버당 프로세스 | ZK 1 + Kafka 1 = **2개** | Kafka **1개** | 그 역할의 Kafka 1개 |
| 프로그램 종류 | 2종 (ZK + Kafka) | **1종** (Kafka) | **1종** (Kafka) |

포트로도 확인됩니다. combined는 Kafka 프로세스 하나가 `9092`(broker) + `9093`(controller)를 동시에 엽니다. dedicated는 controller 프로세스가 `9093`만, broker 프로세스가 `9092`만 엽니다. ZK 모드였다면 ZooKeeper가 `2181`을 따로 열었습니다.

## ZooKeeper가 하던 역할

ZooKeeper는 Kafka 전용이 아니라 범용 분산 조율 서비스(임시 노드, watch, 강한 일관성 제공)이고, Kafka는 그 클라이언트였습니다. Kafka가 맡기던 일:

| 역할 | 무슨 일 | 저장 위치(예) |
| --- | --- | --- |
| 컨트롤러 선출 | 브로커 중 누가 컨트롤러가 될지 | 임시 znode 경쟁 |
| 브로커 멤버십·생존 감지 | 지금 어떤 브로커가 살아있나 | `/brokers/ids/N` 임시 znode |
| 토픽·파티션 메타데이터 | 토픽·파티션·리더·ISR | `/brokers/topics/...` |
| 설정(Config) | 토픽별·동적 브로커 설정 | `/config/...` |
| ACL·쿼터 | 권한·처리량 제한 | `/kafka-acl/...` 등 |

즉 ZooKeeper는 Kafka의 **"관제탑 + 메타데이터 장부 + 합의 제공자"** 였습니다. KRaft는 이 기능들을 **내부 메타데이터 로그 `__cluster_metadata` + Raft 쿼럼**으로 흡수해, 별도 시스템 없이 스스로 처리합니다(선출→Raft 리더 선출, 생존 감지→heartbeat, 나머지 메타데이터→내부 로그 이벤트).

## dedicated 모드 상세

### 설정 차이 (combined에서 무엇을 바꾸나)

`roles`를 역할별로 쪼개고 리스너를 하나씩만 남기는 것이 핵심입니다.

| | controller 전용 | broker 전용 |
| --- | --- | --- |
| `process.roles` | `controller` | `broker` |
| 여는 리스너 | `CONTROLLER://:9093` 만 | `PLAINTEXT://:9092` 만 |
| `advertised.listeners` | 없음(클라이언트 안 받음) | 있음(앱 접속 주소) |
| `controller.quorum.voters` | 컨트롤러만 나열 | (여기도) 컨트롤러만 나열 |

지켜야 할 규칙:

- `node.id`는 컨트롤러·브로커 통틀어 **유일**(예: 컨트롤러 1·2·3, 브로커 101·102·103).
- `controller.quorum.voters`에는 **컨트롤러만** 나열(브로커 설정에도 컨트롤러만 적음).
- 컨트롤러 대수는 **홀수(3 또는 5)**. 3대면 1대 장애 허용. 브로커는 몇 대든 무관.

### 서버는 몇 대 필요한가

프로세스가 6개(컨트롤러 3 + 브로커 3)라면, **격리 목적을 살리려면 서버도 6대**로 나눕니다. 같은 서버에 컨트롤러와 브로커를 함께 올리면 그 서버가 죽을 때 둘이 동시에 사라져 combined와 다를 바 없어지기 때문입니다. 다만 두 가지 완화점이 있습니다.

- **컨트롤러 노드는 저사양이어도 됩니다.** 메타데이터 관리만 하므로 작고 싼 서버(또는 작은 VM/컨테이너) 3대로 충분합니다. 고사양은 브로커에만 씁니다.
- **숫자를 3+3으로 맞출 필요 없습니다.** 컨트롤러는 홀수 고정(보통 3), 브로커는 트래픽에 따라 N대. 예: 컨트롤러 3(소형) + 브로커 8(대형).

### 언제 쓰나

- **소규모(서버 3대)**: 컨트롤러 3 + 브로커까지 나누면 서버가 늘어 낭비 → **combined가 정답**.
- **대규모**: 브로커가 수십 대로 커져 combined의 부하 경합(한 프로세스에 broker·controller 부하가 섞임)이 문제 될 때 검토.
- **주의**: 운영 중 combined → dedicated 전환은 투표권(voter) 구성이 바뀌는 토폴로지 변경이라 단순 토글이 아닙니다. 규모 성장이 예상되면 처음부터 dedicated로 설계하는 편이 낫습니다.

## 컨트롤러 failover가 ZooKeeper보다 빠른 이유

dedicated KRaft(컨트롤러 3대)와 ZooKeeper(ZK 3대)는 둘 다 "별도 조율 서버 3대 + 브로커들"이라 겉모습이 같습니다. 그런데 failover 속도가 다릅니다. **핵심은 저장 위치(같은 서버냐 아니냐)가 아니라, "복제본을 가진 그 노드가 곧바로 컨트롤러가 될 수 있느냐"입니다.**

### 구조를 나란히 보면

```text
[ZooKeeper 모드]
  브로커 계층:   broker1 ★controller 역할   broker2   broker3
                     │ (ZK에 클라이언트로 접속)
  ZooKeeper 계층: zk1 ⇄ zk2 ⇄ zk3     ← 복제는 "여기 안에서만"

[dedicated KRaft]
  컨트롤러 계층:  ctrl1 ★리더 ⇄ ctrl2 ⇄ ctrl3   ← 복제 + 각자가 컨트롤러 후보
  브로커 계층:    broker1   broker2   broker3   (로그를 fetch)
```

- ZooKeeper도 **복제는 합니다** — 단, zk 노드들끼리만. 그런데 zk는 컨트롤러가 될 수 없습니다(다른 프로그램). 컨트롤러가 될 브로커들은 컨트롤러의 최신 작업 상태를 갖고 있지 않습니다.
- KRaft에서는 복제하는 ctrl1·2·3이 **곧 컨트롤러 후보 본인들**이고, 팔로워는 로그를 실시간 적용해 최신 상태를 이미 메모리에 들고 있습니다.

### 장애 순간에 벌어지는 일

ZooKeeper — 컨트롤러(broker1) 사망 시:

1. broker1의 임시 znode 소멸 → zk가 죽음 감지
2. 남은 브로커 중 하나가 새 컨트롤러로 선출됨
3. 새 컨트롤러는 **캐시가 비어 있어**, zk에서 토픽·파티션·리더·ISR·설정을 **전량 다시 읽어** 재구성
4. 그제서야 지휘 시작 → 파티션이 많을수록(수십만) 3번이 오래 걸림

dedicated KRaft — 리더(ctrl1) 사망 시:

1. 남은 컨트롤러가 감지하고 팔로워 하나가 리더로 승격
2. 그 팔로워는 **이미 최신 메타데이터를 메모리에 보유**(계속 복제·적용해 왔으므로)
3. 마지막 몇 항목만 확인하고 즉시 지휘 시작 → 다시 읽을 게 없음

### 정리 표

| | ZooKeeper | dedicated KRaft |
| --- | --- | --- |
| 조율 서버 3대 별도? | 예(zk 3대) | 예(controller 3대) |
| 그 3대가 복제하나? | **예** | 예 |
| 그 3대가 곧 컨트롤러 후보인가? | **아니오** (zk ≠ 컨트롤러) | **예** |
| 컨트롤러 죽으면 새 리더는? | 브로커에서 나와 **전량 재로딩** | 복제 중이던 팔로워가 **그대로 승격** |
| failover 속도 | 느림(파티션 수 비례) | 거의 즉시 |

## 한 줄 요약

- **프로세스 구조**: ZooKeeper만 별도 프로그램이고, broker·controller는 Kafka 한 프로그램의 역할입니다. combined는 한 프로세스가 겸하고, dedicated는 프로세스를 나누며, ZK 모드에선 controller가 브로커에 얹힌 역할이었습니다.
- **failover 속도 차이의 본질**: "복제본을 가진 노드가 곧바로 컨트롤러가 될 수 있는가"입니다. ZooKeeper는 복제하는 노드(zk)와 컨트롤러가 될 노드(broker)가 달라 다시 읽어야 하고, KRaft는 같아서 다시 읽을 필요가 없습니다.

## 관련 문서

- [01. Kafka 핵심 개념](01-kafka-basics.md) · [05. KRaft vs ZooKeeper](05-kraft-vs-zookeeper.md) · [06. 클러스터 설계](06-cluster-design.md)
- [07. KRaft 3노드 설치](07-kraft-cluster-installation.md) · [09. 개념 정리 Q&A](09-concepts-qna.md)

## 참고한 공식 문서

- [KRaft Overview](https://kafka.apache.org/documentation/#kraft)
- [KIP-500: Replace ZooKeeper with a Self-Managed Metadata Quorum](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500)
- [KIP-631: The Quorum-based Kafka Controller](https://cwiki.apache.org/confluence/display/KAFKA/KIP-631%3A+The+Quorum-based+Kafka+Controller)
