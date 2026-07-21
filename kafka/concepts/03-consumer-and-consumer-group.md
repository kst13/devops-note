# Consumer와 Consumer Group

토픽에서 메시지를 읽어가는 Consumer의 동작 방식, Consumer Group의 파티션 분배, 오프셋 관리, 리밸런싱을 정리합니다.

## Consumer (컨슈머)

토픽에서 메시지를 **읽어가는 클라이언트 애플리케이션**입니다. Producer가 메시지를 쓰는 쪽이라면, Consumer는 읽는 쪽입니다.

```text
Producer → [토픽] → Consumer
(쓰기)               (읽기)
```

### Pull 방식

브로커가 메시지를 밀어주는(push) 것이 아니라, Consumer가 직접 가져갑니다(pull). Consumer가 자기 처리 속도에 맞춰 읽을 수 있어, 느린 Consumer가 있어도 브로커나 다른 Consumer에 영향을 주지 않습니다.

### Offset (오프셋)

각 파티션에서 "어디까지 읽었는지"를 나타내는 숫자입니다.

```text
Partition 0: [msg0, msg1, msg2, msg3, msg4, ...]
                              ↑
                         현재 오프셋=3 (여기부터 읽음)
```

Consumer가 메시지를 읽을 때마다 오프셋이 증가하고, 이 위치가 내부 토픽 `__consumer_offsets`에 저장됩니다. 장애 후 재시작해도 마지막 읽은 위치부터 이어서 읽을 수 있습니다.

## Consumer Group (컨슈머 그룹)

여러 Consumer를 하나의 그룹으로 묶어서 **파티션을 나눠 읽는 구조**입니다. 같은 그룹 내에서 하나의 파티션은 **하나의 Consumer만** 담당합니다.

```text
토픽 (파티션 3개), Consumer Group A (Consumer 3개)

Partition 0 → Consumer A-1
Partition 1 → Consumer A-2
Partition 2 → Consumer A-3
```

3개가 병렬로 처리하므로 처리량이 3배가 됩니다.

### 1파티션 : 1컨슈머 원칙

같은 그룹 안에서 하나의 파티션은 반드시 하나의 Consumer만 읽습니다. 두 Consumer가 같은 파티션을 동시에 읽으면 오프셋 관리가 꼬이고, 같은 메시지를 중복 처리하거나 빠뜨릴 수 있기 때문입니다.

따라서 Consumer 수가 파티션 수보다 많으면 남는 Consumer는 놀게 됩니다.

```text
파티션 3개, Consumer 4개

Partition 0 → Consumer 1
Partition 1 → Consumer 2
Partition 2 → Consumer 3
               Consumer 4 (할당 없음, 대기)
```

병렬 처리를 늘리려면 **파티션 수를 함께 늘려야** 합니다.

### 여러 Consumer Group

하나의 토픽에 여러 Consumer Group이 **독립적으로** 붙을 수 있습니다. 이것이 Kafka의 핵심 장점 중 하나입니다.

```text
                    ┌─────────────────────────────┐
                    │    Topic: order-created      │
                    │                             │
                    │  ┌─────┐ ┌─────┐ ┌─────┐   │
                    │  │ P0  │ │ P1  │ │ P2  │   │
                    │  └──┬──┘ └──┬──┘ └──┬──┘   │
                    └─────┼──────┼──────┼────────┘
                          │      │      │
              ┌───────────┼──────┼──────┼───────────┐
              │           │      │      │           │
              ▼           ▼      ▼      ▼           ▼
   ┌─────────────────┐              ┌─────────────────┐
   │ Consumer Group A │              │ Consumer Group B │
   │ (주문 처리 서비스)  │              │ (로그 분석 서비스)  │
   │                  │              │                  │
   │  C1 ← P0        │              │  C1 ← P0, P1    │
   │  C2 ← P1        │              │  C2 ← P2        │
   │  C3 ← P2        │              │                  │
   └─────────────────┘              └─────────────────┘
        오프셋: P0=120                  오프셋: P0=85
                P1=118                          P1=85
                P2=121                          P2=90
```

각 그룹은 완전히 독립적입니다.

- **오프셋을 따로 관리**합니다. Group A가 120번까지 읽었어도 Group B는 85번부터 읽을 수 있습니다.
- **읽는 속도가 달라도** 서로 영향이 없습니다.
- **그룹마다 Consumer 수가 달라도** 됩니다.

### Kafka는 읽어도 메시지를 삭제하지 않는다

이것이 여러 그룹이 독립적으로 읽을 수 있는 이유입니다. 전통적인 메시지 큐와 결정적으로 다른 부분입니다.

```text
── 전통적인 큐 (RabbitMQ 등) ──────────────────

  Queue: [msg1, msg2, msg3]

  Consumer A가 msg1 읽음 → Queue: [msg2, msg3]   ← msg1 삭제됨
  Consumer B가 msg1 읽으려 함 → 이미 없음 ✗


── Kafka ──────────────────────────────────────

  Partition: [msg1, msg2, msg3]

  Group A가 msg1 읽음 → Partition: [msg1, msg2, msg3]  ← 그대로 유지
  Group B가 msg1 읽음 → Partition: [msg1, msg2, msg3]  ← 여전히 유지
```

메시지는 보관 기간(`log.retention.hours`)이 지나야 삭제됩니다. 그 전까지는 몇 개의 그룹이든 자유롭게 읽을 수 있습니다. 데이터는 하나인데 **"어디까지 읽었는지"를 가리키는 포인터(오프셋)만 그룹마다 따로** 있는 구조입니다.

### 실제 활용 예시

```text
Topic: order-created (주문 생성 이벤트)
  │
  ├── Group A: 주문 처리 서비스     → 재고 차감, 배송 준비
  ├── Group B: 알림 서비스          → 고객에게 주문 확인 메일 발송
  ├── Group C: 분석 서비스          → 매출 통계 집계
  └── Group D: 감사 로그 서비스     → 주문 이력 장기 보관
```

같은 "주문 생성" 메시지 하나를 4개 시스템이 각자 용도에 맞게 소비합니다.

## 리밸런싱 (Rebalancing)

Consumer가 그룹에 추가되거나, 장애로 빠지면 파티션 할당이 **자동으로 재분배**됩니다.

```text
── 정상 상태 ──────────────────

  Partition 0 → Consumer A-1
  Partition 1 → Consumer A-2
  Partition 2 → Consumer A-3


── A-1 장애 발생 → 리밸런싱 ───

  Partition 0 → Consumer A-2  (대신 맡음)
  Partition 1 → Consumer A-2
  Partition 2 → Consumer A-3
```

리밸런싱 관련 브로커 설정:

| 설정 | 의미 |
| --- | --- |
| `group.initial.rebalance.delay.ms` | Consumer Group이 처음 구성될 때 리밸런싱 시작을 지연하는 시간. 여러 Consumer가 거의 동시에 접속하는 경우, 이 시간 동안 기다렸다가 한 번에 리밸런싱합니다 |

## Consumer 읽기 흐름

```text
┌──────────────────────────────────────────────────────┐
│                 Consumer Group A                      │
│                                                      │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐       │
│  │Consumer 1│    │Consumer 2│    │Consumer 3│       │
│  └────┬─────┘    └────┬─────┘    └────┬─────┘       │
│       │               │               │              │
└───────┼───────────────┼───────────────┼──────────────┘
        │               │               │
        │ ① Pull 요청    │               │
        ▼               ▼               ▼
   ┌─────────┐    ┌─────────┐    ┌─────────┐
   │Partition0│    │Partition1│    │Partition2│
   │ (리더)   │    │ (리더)   │    │ (리더)   │
   │         │    │         │    │         │
   │ offset  │    │ offset  │    │ offset  │
   │ 0: msg  │    │ 0: msg  │    │ 0: msg  │
   │ 1: msg  │    │ 1: msg  │    │ 1: msg  │
   │ 2: msg  │    │ 2: msg  │    │ 2: msg  │
   │ 3: msg ◄├─┐  │ 3: msg  │    │ 3: msg  │
   │ 4: msg  │ │  │ 4: msg  │    │ 4: msg  │
   └─────────┘ │  └─────────┘    └─────────┘
               │
          ② 현재 오프셋=3
             여기부터 읽음

        ③ 처리 완료 후 오프셋 커밋
        ┌──────────────────────────┐
        │  __consumer_offsets 토픽  │
        │                          │
        │  Group A, P0 → offset 5  │
        │  Group A, P1 → offset 3  │
        │  Group A, P2 → offset 4  │
        └──────────────────────────┘
```

1. Consumer가 담당 파티션의 리더 브로커에 Pull 요청을 보냅니다.
2. 저장된 오프셋 위치부터 메시지를 읽어옵니다.
3. 메시지 처리가 완료되면 오프셋을 커밋합니다.

## Consumer 권장 설정 요약

| 설정 | 값 | 의미 |
| --- | --- | --- |
| `enable.auto.commit` | `false` | 처리 완료 후 수동 커밋 (at-least-once 보장) |
| `isolation.level` | `read_committed` | 트랜잭션 사용 시 커밋된 메시지만 읽음 |
| `bootstrap.servers` | 3대 모두 나열 | `kafka1:9094,kafka2:9094,kafka3:9094` |

`enable.auto.commit=true`(기본값)이면 Consumer가 메시지를 처리하기 전에 오프셋이 커밋될 수 있어, 장애 시 메시지를 놓칠 수 있습니다.

## 참고한 공식 문서

- [Kafka Consumer](https://kafka.apache.org/documentation/#theconsumer)
- [Consumer Configs](https://kafka.apache.org/documentation/#consumerconfigs)
- [Consumer Group Protocol](https://kafka.apache.org/documentation/#consumerconfigs_group.protocol)
