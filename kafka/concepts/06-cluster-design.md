# 실서버 3대 클러스터 설계 방식

실서버 3대에 Kafka를 올려 **MSA 서비스 간 비동기 메시징** 백본으로 쓰는 상황을 기준으로, 어떤 토폴로지·복제·보안 설정을 선택해야 하는지와 그 이유를 정리합니다. 실제 설치 절차는 [KRaft 3노드 클러스터 설치 및 설정 방법](07-kraft-cluster-installation.md)에서 다룹니다.

용어가 낯설다면 [Kafka 기본 개념](01-kafka-basics.md)을 먼저 봅니다.

## 설계 목표

MSA 메시징 백본에서 가장 중요한 것은 처리량보다 **무손실**과 **가용성**입니다.

- 메시지는 한 번 성공 응답하면 잃지 않아야 한다 (무손실).
- 서버 1대가 죽어도 produce/consume이 멈추지 않아야 한다 (무중단).
- 위 두 가지를 3대라는 최소 HA 구성 안에서 달성한다.

## 1. 토폴로지: Combined 모드 (broker + controller 겸용)

KRaft에서 각 노드는 broker 역할, controller 역할, 또는 둘 다를 맡을 수 있습니다. 3대 소규모에서는 **각 노드가 broker와 controller를 겸하는 combined 모드**가 표준입니다.

```text
                (KRaft controller quorum: 3표, 과반 2 → 1대 장애 허용)
   server-1 (node.id=1)      server-2 (node.id=2)      server-3 (node.id=3)
  ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
  │ roles=broker,    │ ←→  │ roles=broker,    │ ←→  │ roles=broker,    │
  │       controller │     │       controller │     │       controller │
  └──────────────────┘     └──────────────────┘     └──────────────────┘
        /data/kafka             /data/kafka              /data/kafka
```

- **controller quorum이 3표**가 되어 과반(2표)만 유지되면 클러스터가 동작합니다. 즉 **1대 장애를 허용**합니다.
- controller quorum은 홀수여야 안전합니다. 3표는 1대 장애, 5표는 2대 장애를 허용합니다. 3대 구성이면 자연히 3표입니다.

좋은 경우:

- 노드 3~5대 규모의 MSA 메시징/이벤트 클러스터.
- 운영 노드 수를 최소화하고 싶은 경우.

주의할 점:

- combined 모드는 broker 부하와 controller 부하가 한 프로세스에 섞입니다. 트래픽이 커지면(수십 broker 규모) controller를 **분리(dedicated)** 하는 것을 검토합니다. 3대에서는 겸용이 맞습니다. combined/dedicated의 프로세스 구조와 서버 수 판단은 [11. 브로커·컨트롤러·주키퍼 프로세스 구조와 failover 원리](11-broker-controller-zookeeper-and-failover.md)를 참고합니다.
- controller quorum 정족수를 잃으면(3대 중 2대 다운) 메타데이터 쓰기가 멈춥니다. 3대를 서로 다른 랙/AZ에 배치해 동시 장애 확률을 낮춥니다.

## 2. 복제와 ISR: 무손실 설정 (가장 중요)

3대 구성에서 무손실·무중단을 만드는 핵심 조합입니다.

| 설정 | 값 | 이유 |
| --- | --- | --- |
| `default.replication.factor` | 3 | 파티션을 3대에 모두 복제 |
| `min.insync.replicas` | 2 | ISR 2개 이상일 때만 acks. 1대 죽어도 쓰기 가능 |
| producer `acks` | all | ISR 전체가 받은 뒤 성공 처리 |
| producer `enable.idempotence` | true | 재시도 중복 방지 (사실상 필수) |
| `unclean.leader.election.enable` | false | 뒤처진 replica가 leader 되어 유실되는 것 차단 |
| `offsets.topic.replication.factor` | 3 | 내부 오프셋 토픽도 3복제 |
| `transaction.state.log.replication.factor` | 3 | 트랜잭션 상태 로그 3복제 |
| `transaction.state.log.min.isr` | 2 | 트랜잭션 로그도 ISR 2 보장 |
| `auto.create.topics.enable` | false | MSA 토픽 거버넌스: 토픽은 명시적으로 생성 |

왜 RF3 + minISR2인가:

```text
정상: Leader + Follower + Follower = ISR 3  → 쓰기 OK
1대 다운: Leader + Follower       = ISR 2  → 여전히 min.insync.replicas=2 충족 → 쓰기 OK
2대 다운: Leader                  = ISR 1  → min.insync 미달 → 쓰기 거부(무손실 우선, 읽기는 가능)
```

- RF3 + minISR2 + acks=all 이면 **1대 장애까지는 무손실·무중단**, 2대 장애 시에는 데이터를 지키기 위해 쓰기를 막습니다(의도된 동작).
- `min.insync.replicas`를 3으로 올리면 1대만 죽어도 쓰기가 멈춥니다. 3대 구성에서는 **2가 정답**입니다.

주의할 점:

- `min.insync.replicas`는 broker 기본값으로 두되, 중요한 토픽은 토픽 단위로 다시 지정해 명확히 합니다.
- `acks=all`과 `enable.idempotence=true`는 **producer(클라이언트) 쪽 설정**입니다. broker 설정만으로는 무손실이 완성되지 않습니다. 아래 클라이언트 가이드를 반드시 함께 적용합니다.

## 3. 보안 리스너 설계 (SASL/SCRAM + TLS)

리스너를 역할별로 3종 분리합니다.

```text
CONTROLLER (9093)  : 컨트롤러 쿼럼 통신  → SSL (mTLS)
INTERNAL   (9092)  : 브로커 간 복제 통신  → SASL_SSL (SCRAM-SHA-512)
CLIENT     (9094)  : 애플리케이션 접속    → SASL_SSL (SCRAM-SHA-512)
```

왜 이렇게 나누는가:

- **컨트롤러 리스너는 SSL(mTLS)** 로 둡니다. SCRAM 자격증명은 메타데이터에 저장되는데, 그 메타데이터를 관리하는 controller 통신 자체가 SCRAM에 의존하면 부트스트랩 순환 문제가 생깁니다. 인증서 기반 mTLS가 이 의존을 끊어 줍니다.
- **브로커 간·클라이언트는 SASL_SSL + SCRAM-SHA-512** 로 인증(누구인지)과 암호화(엿보기 방지)를 모두 확보합니다.
- 클라이언트 리스너(9094)를 분리하면, 방화벽에서 외부에는 9094만 열고 9092/9093은 3노드 사이에서만 열 수 있습니다.

좋은 경우:

- 사내망이라도 서비스 계정별 권한 분리와 전송 암호화가 필요한 MSA 프로덕션.

주의할 점:

- TLS 인증서(사설 CA + 노드별 keystore/truststore)와 SCRAM 계정 부트스트랩이 설치의 가장 번거로운 부분입니다. 절차는 설치 문서와 `examples/compose-3node-kraft/certs/`에 정리합니다.
- 키스토어 비밀번호, SCRAM 비밀번호 등 시크릿은 **파일/저장소에 커밋하지 않습니다.** Vault나 Secret Manager로 주입합니다.
- 더 세밀한 접근 제어가 필요하면 SCRAM 인증 위에 **ACL**을 얹어 "이 서비스 계정은 이 토픽만" 식으로 제한합니다.

## 4. 네트워크와 스토리지

- 3대가 물리적으로 분리되어 있으므로, 각 broker는 **자신의 실제 IP/호스트명으로 advertised** 되어야 합니다. `advertised.listeners`가 잘못되면 클라이언트가 붙었다가 엉뚱한 주소로 재접속하며 실패합니다(가장 흔한 사고).
- 방화벽(firewalld): 9092·9093은 **3노드 사이에서만**, 9094는 애플리케이션 대역에 개방합니다.
- 데이터는 host의 **전용 디스크**(예: `/data/kafka` — 예제에서는 `.env` 의 `KAFKA_HOME_DIR` 아래 `data/`)에 저장하고 컨테이너에 bind mount 합니다. 컨테이너를 재생성해도 로그가 보존됩니다. Kafka는 순차 쓰기가 많으므로 가능하면 별도 디스크를 씁니다.

## 5. 클라이언트(MSA 앱) 설정 가이드

broker 설정과 짝을 이루는 클라이언트 설정입니다. 무손실은 양쪽이 맞아야 완성됩니다.

Producer:

- `acks=all`
- `enable.idempotence=true` (중복 없는 정확히 한 번 전송의 전제)
- `retries`를 충분히 크게, `delivery.timeout.ms`로 상한 관리
- `bootstrap.servers`에 3대를 모두 나열 (`server1:9094,server2:9094,server3:9094`)

Consumer:

- `enable.auto.commit=false` 후 처리 완료 시점에 수동 커밋 (at-least-once 보장)
- 트랜잭션(exactly-once)을 쓰면 `isolation.level=read_committed`
- `bootstrap.servers`에 3대 모두 나열

## 선택 기준 (요약 결정 트리)

```text
1. 노드 3~5대, 신규 구축, 운영 단순화가 중요
   -> KRaft combined 모드 (이 문서의 구성)
2. 무손실이 최우선인 MSA 메시징
   -> RF3 + min.insync.replicas=2 + acks=all + unclean 선출 off
3. 서비스 계정 인증 + 전송 암호화 필요
   -> 클라이언트/브로커간 SASL_SSL(SCRAM-SHA-512), 컨트롤러 SSL(mTLS)
4. broker 트래픽이 매우 커져 controller와 자원 경합
   -> controller dedicated 분리 검토 (3대 범위를 넘어설 때)
```

## 참고한 공식 문서

- [KRaft Configuration](https://kafka.apache.org/documentation/#kraft_config)
- [Replication and min.insync.replicas](https://kafka.apache.org/documentation/#design_ha)
- [Security — SSL & SASL/SCRAM](https://kafka.apache.org/documentation/#security)
- [Producer configs (acks, idempotence)](https://kafka.apache.org/documentation/#producerconfigs)
