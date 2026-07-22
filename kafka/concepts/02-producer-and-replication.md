# Producer와 복제

Producer의 메시지 쓰기 흐름, 복제 메커니즘, acks 설정에 따른 성공 기준, 그리고 내부 토픽의 역할을 정리합니다.

## 메시지 쓰기 흐름 (Produce)

Producer가 메시지를 보내면 다음 순서로 처리됩니다.

```text
┌──────────┐         ① 메시지 전송 (key: 주문ID)
│ Producer │─────────────────────────────────────────┐
└──────────┘                                         │
                                                     ▼
                                          ┌─────────────────┐
                                          │    파티셔너       │
                                          │  (Partitioner)   │
                                          │                  │
                                          │  key 해시 계산    │
                                          │  hash("주문123") │
                                          │  = 1             │
                                          └────────┬─────────┘
                                                   │
                              ② 해당 파티션의 리더 브로커로 전송
                                                   │
                    ┌──────────────────────────────┼──────────────────┐
                    │                              │                  │
                    ▼                              ▼                  ▼
             ┌────────────┐                ┌────────────┐     ┌────────────┐
             │  Broker 1  │                │  Broker 2  │     │  Broker 3  │
             │            │                │            │     │            │
             │            │                │ P1 리더    │     │            │
             │            │                │ ③ 디스크   │     │            │
             │            │                │   에 저장   │     │            │
             │ P1 팔로워  │◄───④ 복제────── │            │────►│ P1 팔로워  │
             └────────────┘                └─────┬──────┘     └────────────┘
                                                 │
                                          ⑤ ISR 확인 완료
                                          acks=all이면
                                          팔로워 복제 확인 후
                                                 │
                                                 ▼
                                          ⑥ Producer에게
                                            성공 응답 반환
```

1. Producer가 메시지와 key를 전송합니다.
2. 파티셔너가 key의 해시값으로 파티션을 결정합니다. key가 없으면 기본 파티셔너가 sticky partitioning으로 분배합니다(배치가 찰 때까지 한 파티션에 모아 보내다가 다음 파티션으로 전환. Kafka 2.4 이전은 라운드 로빈).
3. 해당 파티션의 **리더 브로커**가 디스크에 저장합니다.
4. 팔로워 브로커가 리더로부터 데이터를 복제합니다.
5. `acks` 설정에 따라 복제 확인 범위가 달라집니다.
6. 조건 충족 시 Producer에게 성공 응답을 반환합니다.

## Replication(복제)과 ISR

각 파티션은 여러 브로커에 복제됩니다. 복제본 수가 **replication factor(RF)** 입니다.

```text
partition-0 (RF=3)
  Broker 1: Leader     ← 읽기/쓰기
  Broker 2: Follower   ← 복제 따라옴 (ISR)
  Broker 3: Follower   ← 복제 따라옴 (ISR)
```

- Producer는 Leader에만 씁니다. Follower가 Leader를 복제해 따라옵니다.
- Leader와 충분히 동기화된 복제본 집합이 **ISR(In-Sync Replicas)** 입니다.
- Follower가 복제 지연으로 뒤처지면 ISR에서 빠지고, 따라잡으면 다시 들어옵니다.

### 브로커 장애 시 복구 흐름

```text
── 정상 상태 ──────────────────────────────────

  Broker 1          Broker 2          Broker 3
  P0 리더 ✓         P0 팔로워 ✓       P0 팔로워 ✓
  ISR = [1, 2, 3]


── Broker 1 장애 ─────────────────────────────

  Broker 1          Broker 2          Broker 3
  P0 리더 ✗ 죽음    P0 팔로워 ✓       P0 팔로워 ✓
                    ISR = [2, 3]

                        │
                        ▼ 컨트롤러가 감지


── 리더 승격 ─────────────────────────────────

  Broker 1          Broker 2          Broker 3
     (없음)         P0 리더 ✓ 승격!   P0 팔로워 ✓
                    ISR = [2, 3]

                    클라이언트는 자동으로
                    Broker 2에 연결


── Broker 1 복구 ─────────────────────────────

  Broker 1          Broker 2          Broker 3
  P0 팔로워 ✓       P0 리더 ✓         P0 팔로워 ✓
  (복제 따라잡기)    ISR = [2, 3, 1]
```

`auto.leader.rebalance.enable=true`이면, 복구 후 원래 리더(Broker 1)로 자동 복귀합니다.

## acks 설정: 성공의 기준

Producer가 메시지를 보낸 뒤 **"성공 응답을 받는 기준"은 `acks` 설정**으로 결정됩니다. 이것은 Producer(클라이언트) 쪽 설정입니다.

### acks=0: 보내기만 하면 성공

```text
  Producer                    Broker 1 (리더)     Broker 2      Broker 3
     │                            │                 │             │
     │── 메시지 전송 ──►           │                 │             │
     │                            │                 │             │
     │  보내는 즉시 성공            │                 │             │
     │  (응답을 기다리지 않음)       │                 │             │
```

- 장점: 가장 빠릅니다.
- 단점: 메시지가 도착했는지, 저장됐는지 알 수 없습니다.
- 용도: 유실이 허용되는 메트릭, 로그 수집 등.

### acks=1: 리더 저장까지 확인

```text
  Producer                    Broker 1 (리더)     Broker 2      Broker 3
     │                            │                 │             │
     │── 메시지 전송 ──►           │                 │             │
     │                         디스크 저장 ✓         │             │
     │◄── 성공 응답 ───           │                 │             │
     │                            │── 복제 ──►      │             │
     │                            │── 복제 ──────────────►        │
```

- 장점: 리더 저장은 확인되므로 적당한 균형입니다.
- 단점: 리더가 응답 직후 죽으면, 팔로워에 복제되기 전이라 유실 가능합니다.

### acks=all (-1): ISR 전체 확인 (무손실)

```text
  Producer                    Broker 1 (리더)     Broker 2      Broker 3
     │                            │                 │             │
     │── 메시지 전송 ──►           │                 │             │
     │                         디스크 저장 ✓         │             │
     │                            │── 복제 ──►      │             │
     │                            │              디스크 저장 ✓     │
     │                            │── 복제 ──────────────►        │
     │                            │                            디스크 저장 ✓
     │                         ISR 전체 확인 ✓       │             │
     │◄── 성공 응답 ───           │                 │             │
```

- 장점: ISR에 속한 모든 복제본이 확인해야 성공이므로, 어떤 브로커가 죽어도 유실이 없습니다.
- 단점: 가장 느립니다.
- 용도: 주문, 결제 등 유실이 허용되지 않는 데이터.

## min.insync.replicas: acks=all의 안전장치

`acks=all`은 ISR 전체가 확인해야 성공이지만, **ISR이 몇 개 이상이어야 쓰기를 허용할지**를 `min.insync.replicas`(브로커 설정)가 결정합니다.

```text
── min.insync.replicas=2, acks=all ───────────────

  정상 (ISR=3):
  Broker 1 (리더) ✓  Broker 2 ✓  Broker 3 ✓  → ISR 3개 ≥ 2, 성공

  1대 장애 (ISR=2):
  Broker 1 (리더) ✓  Broker 2 ✓  Broker 3 ✗  → ISR 2개 ≥ 2, 성공

  2대 장애 (ISR=1):
  Broker 1 (리더) ✓  Broker 2 ✗  Broker 3 ✗  → ISR 1개 < 2, 쓰기 거부!


── min.insync.replicas=1, acks=all ───────────────

  2대 장애 (ISR=1):
  Broker 1 (리더) ✓  Broker 2 ✗  Broker 3 ✗  → ISR 1개 ≥ 1, 성공

  → 리더 혼자만 확인해도 성공, acks=1과 사실상 같음
```

정리하면 `acks`는 Producer가 정하는 **"나는 이 수준까지 확인받겠다"** 이고, `min.insync.replicas`는 브로커가 정하는 **"최소 이만큼은 살아있어야 쓰기를 받겠다"** 입니다. 둘을 조합해야 무손실이 완성됩니다.

MSA에서 권장하는 무손실 조합:

| 설정 | 위치 | 권장값 |
| --- | --- | --- |
| `acks` | Producer (클라이언트) | `all` |
| `min.insync.replicas` | Broker (서버) | `2` |
| `replication.factor` | 토픽 생성 시 | `3` |

`min.insync.replicas`와 `replication.factor`는 **토픽 단위 오버라이드**가 우선합니다. 중요한 토픽은 생성 시 다시 지정합니다.

## Kafka 내부 토픽

Kafka는 자체 운영을 위해 자동으로 생성하는 내부 토픽이 있습니다. 사용자 토픽과 달리, 각각 전용 설정으로 복제 수를 관리합니다.

```text
┌─────────────────────────────────────────────────────────────┐
│                        Kafka 내부 토픽                       │
│                                                             │
│  __consumer_offsets              __transaction_state         │
│  Consumer가 어디까지 읽었는지 저장   트랜잭션 상태 기록          │
│                                                             │
│  offsets.topic.                  transaction.state.log.      │
│  replication.factor=3            replication.factor=3        │
│                                  transaction.state.log.      │
│                                  min.isr=2 (권장)            │
│                                                             │
│  ── 사용자 토픽 ──────────────────────────────────────────   │
│                                                             │
│  order-created, payment-completed 등                        │
│  default.replication.factor 적용                             │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### `__consumer_offsets`

Consumer Group이 각 파티션에서 어디까지 읽었는지(오프셋)를 저장하는 토픽입니다. 이 토픽이 유실되면 Consumer가 읽은 위치를 잃어버립니다. `offsets.topic.replication.factor`로 복제 수를 지정합니다.

### `__transaction_state`

트랜잭션 Producer가 여러 토픽/파티션에 걸친 메시지를 원자적으로 보낼 때, 트랜잭션 진행 상태를 기록하는 토픽입니다.

```text
  트랜잭션 사용 예:

  Producer
  beginTransaction()
  ├── send(주문 토픽, "주문 생성")
  ├── send(재고 토픽, "재고 차감")
  └── send(알림 토픽, "알림 발송")
  commitTransaction()

  → 3개 모두 성공하면 커밋, 하나라도 실패하면 전부 취소

  __transaction_state에 기록되는 내용:
  txn-001: BEGIN   → 시작됨
  txn-001: PREPARE → 메시지 전송 완료
  txn-001: COMMIT  → 확정
```

이 기록이 있어야 장애 복구 시 "이 트랜잭션은 커밋됐는가, 취소됐는가"를 판단할 수 있습니다. `transaction.state.log.replication.factor`와 `transaction.state.log.min.isr`로 복제 수와 최소 ISR을 지정합니다.

### 내부 토픽이 별도 설정인 이유

내부 토픽은 `default.replication.factor`의 적용을 받지 않습니다.

```text
  사용자가 토픽 생성
    kafka-topics.sh --create --topic order-created
    → replication-factor 미지정 시 default.replication.factor 사용

  Kafka가 내부 토픽 자동 생성
    __consumer_offsets  → offsets.topic.replication.factor 사용
    __transaction_state → transaction.state.log.replication.factor 사용
```

만약 이 구분이 없으면, `default.replication.factor=1`로 두고 테스트하다가 내부 토픽까지 복제 없이 만들어져서 브로커 1대 장애 시 오프셋이나 트랜잭션 기록을 전부 잃는 사고가 날 수 있습니다.

## Producer 권장 설정 요약

브로커 설정과 짝을 이뤄야 무손실이 완성됩니다.

| 설정 | 값 | 의미 |
| --- | --- | --- |
| `acks` | `all` (Kafka 3.0+ 기본값) | ISR 전체 확인 후 성공 |
| `enable.idempotence` | `true` (Kafka 3.0+ 기본값) | 네트워크 재시도 시 중복 방지 |
| `bootstrap.servers` | 3대 모두 나열 | `kafka1:9094,kafka2:9094,kafka3:9094` |

## 참고한 공식 문서

- [Kafka Design — Replication](https://kafka.apache.org/documentation/#replication)
- [Producer Configs](https://kafka.apache.org/documentation/#producerconfigs)
- [Topic Configs](https://kafka.apache.org/documentation/#topicconfigs)
