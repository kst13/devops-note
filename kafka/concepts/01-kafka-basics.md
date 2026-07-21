# Kafka 핵심 개념: 토픽, 파티션, 브로커, 컨트롤러

Kafka를 이해하기 위한 기본 개념을 정리합니다. 이 문서를 먼저 읽고 [Producer와 복제](02-producer-and-replication.md), [Consumer와 Consumer Group](03-consumer-and-consumer-group.md)으로 넘어갑니다.

## 한눈에 보기

| 용어 | 정의 | 역할 |
| --- | --- | --- |
| Topic | 메시지를 주제별로 분류하는 논리적 채널 | "무엇을" 분류 |
| Partition | Topic을 물리적으로 나눈 단위 | "어떻게 나눠서" 병렬 처리 |
| Broker | Kafka 서버 프로세스(노드 1대) | "어디에" 저장 |
| Controller | 클러스터 메타데이터와 리더 선출을 관리하는 역할 | 브로커 장애 감지와 복구의 핵심 |

## Topic (토픽)

메시지를 **주제별로 분류하는 논리적 채널**입니다. Producer는 토픽에 메시지를 쓰고, Consumer는 토픽을 구독해서 읽습니다.

```text
Producer → [order-created 토픽]      → Consumer
Producer → [payment-completed 토픽]  → Consumer
```

MSA 환경에서는 이벤트 종류별로 토픽을 나눕니다. 주문 이벤트는 `order-created`, 결제 이벤트는 `payment-completed` 토픽에 보내는 식입니다.

## Partition (파티션)

토픽을 **물리적으로 나눈 단위**입니다. 하나의 토픽은 1개 이상의 파티션으로 구성되며, 각 파티션은 메시지가 순서대로 추가되는 append-only 로그 파일입니다.

```text
order-created 토픽 (파티션 3개)
├── Partition 0: [msg0, msg3, msg6, ...]
├── Partition 1: [msg1, msg4, msg7, ...]
└── Partition 2: [msg2, msg5, msg8, ...]
```

파티션이 존재하는 이유는 **병렬 처리**입니다.

- 파티션 하나는 Consumer Group 내에서 **하나의 Consumer만** 읽습니다.
- 파티션이 3개면 Consumer 3개가 동시에 처리할 수 있습니다.
- 같은 파티션 안에서는 순서가 보장되지만, 파티션 간에는 순서가 보장되지 않습니다.

메시지가 어떤 파티션에 들어갈지는 **메시지 키(key)** 로 결정됩니다. 같은 키를 가진 메시지는 항상 같은 파티션에 들어가므로, 같은 주문 ID의 이벤트들은 순서가 유지됩니다.

주의할 점:

- 파티션 수는 나중에 늘릴 수는 있지만 줄일 수 없고, 늘리면 key 기반 순서 보장이 깨질 수 있습니다. 처음에 여유 있게 잡습니다.
- 파티션이 너무 많으면 메타데이터·파일 핸들 부담이 커집니다. 토픽당 수십~수백 수준에서 시작합니다.

## Broker (브로커)

Kafka가 실행되는 **서버 프로세스(노드) 한 대**입니다. 여러 브로커가 모여 클러스터를 구성합니다.

브로커가 하는 일:

- 파티션 데이터를 디스크에 저장
- Producer의 쓰기 요청, Consumer의 읽기 요청 처리
- 다른 브로커와 파티션 데이터 복제

```text
Kafka 클러스터 (3대)
├── Broker 1 (broker.id=1): Partition 0(리더), Partition 2(팔로워)
├── Broker 2 (broker.id=2): Partition 1(리더), Partition 0(팔로워)
└── Broker 3 (broker.id=3): Partition 2(리더), Partition 1(팔로워)
```

각 파티션에는 읽기/쓰기를 담당하는 **리더(Leader)** 1개와, 복제본인 **팔로워(Follower)** 가 있습니다. 리더 브로커가 죽으면 팔로워 중 하나가 리더로 승격되어 서비스가 계속됩니다.

## Controller (컨트롤러)

클러스터의 **메타데이터 관리와 리더 선출을 담당하는 역할**입니다. 브로커가 죽었을 때 해당 브로커의 파티션 리더를 다른 브로커로 재선출하고, 토픽 생성/삭제 같은 관리 작업을 처리합니다.

Controller가 하는 일:

- 브로커 장애 감지
- 파티션 리더 재선출
- 토픽 생성/삭제 처리
- 파티션 재할당 관리

### ZooKeeper 모드

브로커 중 **1대가 Controller로 선출**됩니다. 메타데이터는 별도의 ZooKeeper 앙상블에 저장됩니다.

```text
  Broker 1 (Controller ✓)  ← 브로커 중 1대가 Controller로 선출
  Broker 2
  Broker 3

  ZooKeeper 앙상블           ← 메타데이터 실제 저장 (별도 시스템)
  ├── ZK 1
  ├── ZK 2
  └── ZK 3
```

Controller 브로커가 죽으면 나머지 브로커 중 하나가 새 Controller가 됩니다. 이 과정에서 ZooKeeper가 선출을 중재합니다.

### KRaft 모드

ZooKeeper 없이 **Kafka 자체적으로** 메타데이터를 관리합니다. 여러 브로커가 Raft 합의 알고리즘으로 Controller 역할을 수행합니다.

```text
  Broker 1 (Controller 겸용) ─┐
  Broker 2 (Controller 겸용) ─┼─ 3대가 Raft 합의로 메타데이터 관리
  Broker 3 (Controller 겸용) ─┘

  ZooKeeper 불필요

  Controller 중 1대가 Active Controller(리더)
  나머지 2대는 팔로워로 대기
  Active Controller가 죽으면 팔로워 중 1대가 즉시 승격
```

소규모 클러스터(3대)에서는 브로커가 Controller를 **겸용(combined)** 하는 것이 일반적입니다. 대규모 클러스터에서는 Controller 전용 노드를 **분리(dedicated)** 할 수도 있습니다.

두 모드의 차이는 [KRaft 등장 배경과 ZooKeeper 대비 장단점](05-kraft-vs-zookeeper.md)에서 자세히 다룹니다.

## 세 개념의 관계

```text
클러스터
├── Broker 1 ─┐
├── Broker 2 ─┼── 물리적 서버
└── Broker 3 ─┘

토픽 "order-created" (replication.factor=3)
├── Partition 0: 리더=Broker1, 팔로워=Broker2, Broker3
├── Partition 1: 리더=Broker2, 팔로워=Broker1, Broker3
└── Partition 2: 리더=Broker3, 팔로워=Broker1, Broker2
```

## 전체 아키텍처

```text
┌─────────────────────────────────────────────────────────────────────┐
│                        Kafka Cluster                                │
│                                                                     │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐           │
│  │   Broker 1    │  │   Broker 2    │  │   Broker 3    │           │
│  │               │  │               │  │               │           │
│  │ ┌───────────┐ │  │ ┌───────────┐ │  │ ┌───────────┐ │           │
│  │ │ P0 리더   │ │  │ │ P0 팔로워 │ │  │ │ P0 팔로워 │ │           │
│  │ ├───────────┤ │  │ ├───────────┤ │  │ ├───────────┤ │           │
│  │ │ P1 팔로워 │ │  │ │ P1 리더   │ │  │ │ P1 팔로워 │ │           │
│  │ ├───────────┤ │  │ ├───────────┤ │  │ ├───────────┤ │           │
│  │ │ P2 팔로워 │ │  │ │ P2 팔로워 │ │  │ │ P2 리더   │ │           │
│  │ └───────────┘ │  │ └───────────┘ │  │ └───────────┘ │           │
│  └───────────────┘  └───────────────┘  └───────────────┘           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
         ↑ 쓰기                                    ↓ 읽기
         │                                         │
    ┌────┴────┐                              ┌─────┴─────┐
    │Producer │                              │ Consumer  │
    │ (앱/서비스)│                              │ Group     │
    └─────────┘                              └───────────┘
```

## 전체 프로세스 요약

```text
  ┌──────────┐    ┌──────────────────────────────────┐    ┌─────────────┐
  │          │    │         Kafka Cluster             │    │             │
  │ Producer │───►│                                    │───►│  Consumer   │
  │          │    │  Topic                             │    │  Group      │
  │  메시지   │    │  ├── Partition 0 ──► 리더 Broker1  │    │             │
  │  + key   │    │  ├── Partition 1 ──► 리더 Broker2  │    │  C1 ← P0   │
  │          │    │  └── Partition 2 ──► 리더 Broker3  │    │  C2 ← P1   │
  └──────────┘    │                                    │    │  C3 ← P2   │
                  │  각 파티션은 다른 브로커에 복제       │    │             │
                  │  (replication.factor=3)            │    │  오프셋 커밋  │
                  └──────────────────────────────────┘    └─────────────┘

  흐름: 쓰기 → 파티셔닝 → 리더에 저장 → 팔로워 복제 → Consumer Pull → 오프셋 커밋
```

쓰기 흐름과 복제 과정은 [Producer와 복제](02-producer-and-replication.md), 읽기 흐름은 [Consumer와 Consumer Group](03-consumer-and-consumer-group.md)에서 자세히 다룹니다.

## 참고한 공식 문서

- [Kafka Introduction](https://kafka.apache.org/documentation/#introduction)
- [Kafka Design — Replication](https://kafka.apache.org/documentation/#replication)
