# Kafka 기본 개념

클러스터 설계와 설치로 넘어가기 전에, 뒤 문서에서 계속 쓰이는 용어를 먼저 정리합니다. 이미 익숙하다면 [실서버 3대 클러스터 설계 방식](03-cluster-design.md)으로 바로 넘어가도 됩니다.

## 한눈에 보기

| 용어 | 정의 | 왜 중요한가 |
| --- | --- | --- |
| Broker | 메시지를 저장·전달하는 Kafka 서버 프로세스 | 실서버 1대에 broker 1개가 기본 |
| Topic | 메시지를 분류하는 논리적 채널 | MSA에서 이벤트 종류별로 나눔 |
| Partition | Topic을 나눈 물리적 단위. 순서 보장·병렬 처리의 기준 | 처리량과 순서 보장의 핵심 |
| Replica | Partition의 복제본 | 무손실·장애 조치의 기반 |
| Leader / Follower | Partition 복제본 중 읽고 쓰는 대상(Leader)과 따라오는 복제본(Follower) | Leader 장애 시 Follower가 승격 |
| ISR | In-Sync Replicas. Leader와 동기화된 복제본 집합 | `min.insync.replicas`의 판단 기준 |
| Consumer Group | 같은 그룹 ID로 묶여 Partition을 나눠 읽는 소비자 집합 | 소비 병렬성과 재처리 단위 |
| Offset | Consumer가 어디까지 읽었는지 나타내는 위치값 | 재시작·재처리 지점 |
| Controller | 메타데이터와 리더 선출을 관리하는 역할 | KRaft에서 broker가 겸할 수 있음 |

## Topic과 Partition

Topic은 이벤트를 분류하는 이름이고, 실제 데이터는 Partition에 나뉘어 저장됩니다.

```text
Topic: order-created (partition 3개)
  partition-0  ─ msg msg msg ...
  partition-1  ─ msg msg msg ...
  partition-2  ─ msg msg msg ...
```

- **순서 보장은 partition 단위**입니다. 같은 key를 가진 메시지는 같은 partition으로 가서 순서가 유지됩니다.
- Partition 수가 **소비 병렬성의 상한**입니다. Consumer Group 안에서 partition 하나는 소비자 하나에만 배정됩니다.

주의할 점:

- Partition 수는 나중에 늘릴 수는 있지만 줄일 수 없고, 늘리면 key 기반 순서 보장이 깨질 수 있습니다. 처음에 여유 있게 잡습니다.
- Partition이 너무 많으면 메타데이터·파일 핸들 부담이 커집니다. 토픽당 수십~수백 수준에서 시작합니다.

## Replication과 ISR (무손실의 핵심)

각 partition은 여러 broker에 복제됩니다. 복제본 수가 **replication factor(RF)** 입니다.

```text
partition-0 (RF=3)
  broker-1: Leader     ← 읽기/쓰기
  broker-2: Follower   ← 복제 따라옴 (ISR)
  broker-3: Follower   ← 복제 따라옴 (ISR)
```

- Producer는 Leader에만 씁니다. Follower가 Leader를 복제해 따라옵니다.
- Leader와 충분히 동기화된 복제본 집합이 **ISR**입니다.
- `min.insync.replicas=2` + producer `acks=all` 이면, **ISR이 2개 이상일 때만** 쓰기가 성공합니다. broker 1대가 죽어도 ISR 2개가 유지되면 쓰기가 계속됩니다.

이 조합이 [실서버 3대 클러스터 설계 방식](03-cluster-design.md)에서 다루는 무손실 구성의 기반입니다.

## KRaft vs ZooKeeper

과거 Kafka는 메타데이터(브로커 목록, 리더 정보 등)를 별도 **ZooKeeper** 앙상블에 저장했습니다. 최신 Kafka는 **KRaft(Kafka Raft)** 모드로, ZooKeeper 없이 Kafka 자체 controller가 Raft 합의로 메타데이터를 관리합니다.

| 구분 | ZooKeeper 모드 | KRaft 모드 |
| --- | --- | --- |
| 별도 컴포넌트 | ZooKeeper 앙상블 필요 | 없음 (Kafka 단독) |
| 운영 복잡도 | 두 시스템 운영 | 하나로 단순화 |
| 지원 버전 | ~3.x (deprecated) | 3.3+ 프로덕션, **4.x는 KRaft 전용** |
| 소규모 3대 구성 | ZK 3 + broker 3 필요 | broker+controller 겸용 3대로 충분 |

좋은 경우 (KRaft 선택):

- 새로 구축하는 클러스터. 2026년 기준 Kafka 4.x는 KRaft만 지원하므로 사실상 기본 선택입니다.
- 운영 컴포넌트를 줄이고 싶은 소규모 팀.

주의할 점:

- ZooKeeper 모드에서 KRaft로의 마이그레이션은 별도 절차가 필요합니다. 신규 구축이면 처음부터 KRaft로 시작합니다.
- KRaft에서 controller는 **broker와 겸용(combined)** 하거나 **분리(dedicated)** 할 수 있습니다. 3대 소규모에서는 겸용이 표준입니다. 자세한 기준은 다음 문서에서 다룹니다.

KRaft가 왜 등장했고 ZooKeeper 대비 장단점이 무엇인지는 [KRaft 등장 배경과 ZooKeeper 대비 장단점](02-kraft-vs-zookeeper.md)에서 자세히 다룹니다.

## Consumer Group

같은 group ID로 묶인 소비자들이 partition을 나눠 읽습니다.

```text
Topic order-created (partition 3개)
  ┌─ partition-0 → consumer A ┐
  ├─ partition-1 → consumer B ┤  group: order-service
  └─ partition-2 → consumer C ┘
```

- 그룹 안의 소비자 수를 partition 수까지 늘리면 병렬 소비가 선형으로 확장됩니다. 그 이상은 놀게 됩니다.
- Offset은 그룹 단위로 저장(내부 토픽 `__consumer_offsets`)되어, 재시작 시 이어서 읽습니다.
- 서로 다른 그룹은 같은 메시지를 **독립적으로** 각각 소비합니다. MSA에서 하나의 이벤트를 여러 서비스가 구독할 때 이 특성을 활용합니다.

## 참고한 공식 문서

- [Kafka Introduction](https://kafka.apache.org/documentation/#introduction)
- [Kafka Design — Replication](https://kafka.apache.org/documentation/#replication)
- [KRaft Overview](https://kafka.apache.org/documentation/#kraft)
