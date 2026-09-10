# 배포 아키텍처: 브로커 사이징, 랙 인식, 멀티 데이터센터

클러스터를 "몇 대, 어떤 디스크, 어느 장소에" 놓을지 결정하는 기준을 정리합니다. 3대 클러스터의 토폴로지(combined·dedicated)와 무손실 설정은 [06-cluster-design](06-cluster-design.md)에, 컨트롤러 failover 원리는 [11-broker-controller-zookeeper-and-failover](11-broker-controller-zookeeper-and-failover.md)에 있으므로 여기서는 반복하지 않습니다. 이 문서는 그 위에서 **용량 산정, 랙·AZ 분산, 데이터센터 간 복제, 증설 시 파티션 재배치, 계층형 스토리지**를 다룹니다.

## 1. 브로커 사이징 — 무엇이 병목인가

Kafka 브로커의 자원은 네 가지 축으로 소모됩니다. 어느 축이 먼저 차는지 보고 대수를 정합니다.

| 자원 | 무엇에 비례하나 | 먼저 차는 경우 |
| --- | --- | --- |
| 디스크 용량 | 유입량 × 보관 기간 × RF | 보관 기간이 긴 토픽(7일 이상), 압축 안 한 대용량 이벤트 |
| 디스크 I/O | 유입량 + 복제 쓰기 + 페이지 캐시에 없는 읽기 | HDD, 컨슈머가 오래된 데이터를 자주 되감는 경우 |
| 네트워크 | 유입량 × (1 + 복제 팬아웃 + 컨슈머 그룹 수) | 컨슈머 그룹이 많은 토픽, 1GbE 환경 |
| 메모리·파일 핸들 | 파티션 수 | 파티션 수천 개 이상 |

### 계산 예시

전제: 피크 유입 10MB/s, RF 3, 보관 7일, 컨슈머 그룹 3개, 브로커 3대.

```text
디스크 총량   = 10MB/s × 86,400 × 7일 × RF 3 ≈ 18TB
브로커당      = 18TB / 3대 = 6TB → 여유 30% 더해 8TB 이상
네트워크 out  = 복제 (RF-1) × 10MB/s + 컨슈머 3그룹 × 10MB/s = 50MB/s ≈ 400Mbps
네트워크 in   = 유입 10MB/s + 복제 수신 (RF-1)/대수 비례 ≈ 17MB/s
```

디스크 용량은 여유 30%를 더해야 합니다. `log.retention.bytes`는 파티션 단위라 실제 사용량이 계산보다 크고, 세그먼트는 `log.segment.bytes`(기본 1GB) 단위로 삭제되어 한 세그먼트 분량이 항상 초과 상태로 남기 때문입니다. 디스크가 70%를 넘으면 증설을 시작하고 85%를 넘기지 않는 것이 운영 기준입니다.

네트워크는 **out이 in보다 항상 큽니다**. 리더는 팔로워에게 복제를 보내고, 컨슈머 그룹마다 한 번씩 또 보냅니다. 1GbE(약 120MB/s)에서 컨슈머 그룹이 8개면 유입 10MB/s만으로도 포화합니다. 10GbE를 권장하는 이유입니다.

### 파티션 수의 상한

파티션은 공짜가 아닙니다. 파티션 하나마다 열린 파일 핸들(세그먼트·인덱스 각각), 복제 fetch 요청, 리더 선출 시 처리할 메타데이터가 늘어납니다.

- 브로커당 **4,000개 이하**, 클러스터 전체 **20만 개 이하**가 ZooKeeper 시절부터의 권고입니다. KRaft는 메타데이터를 로그로 처리해 상한이 훨씬 높지만, 브로커 한 대의 파일 핸들과 복구 시간은 그대로 제약입니다.
- 브로커가 비정상 종료되면 재시작 시 모든 세그먼트 인덱스를 검사합니다. 파티션이 많을수록 `num.recovery.threads.per.data.dir`를 올려도 복구가 길어집니다.
- 파티션 수는 처리량이 아니라 **컨슈머 병렬도 요구**로 정합니다. 자세한 산정은 [usage-guide/02-topic-request](../usage-guide/02-topic-request.md)를 참고합니다.

### JVM 힙과 페이지 캐시

브로커 JVM 힙은 **6GB 안팎**에서 시작하고 그 이상 늘리지 않습니다. Kafka는 메시지를 힙에 오래 들고 있지 않습니다. 쓰기는 바로 OS 페이지 캐시로 가고, 읽기는 페이지 캐시에서 `sendfile`로 소켓에 직접 복사됩니다(zero-copy). 그래서 **남은 메모리 전부가 페이지 캐시**로 쓰이는 것이 성능에 유리합니다. 64GB 서버에서 힙 6GB, 캐시 50GB 이상이 일반적인 배분입니다.

```bash
# 예: 6GB 힙, G1GC
export KAFKA_HEAP_OPTS="-Xms6g -Xmx6g"
export KAFKA_JVM_PERFORMANCE_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35"
```

힙이 너무 크면 GC 정지가 길어져 팔로워가 ISR에서 빠지고, 컨트롤러 세션이 끊기는 **자기 유발 장애**가 생깁니다. 힙을 늘리기보다 `MaxGCPauseMillis` 안에 들어오는지 GC 로그로 확인합니다.

### OS 설정

| 항목 | 권장값 | 이유 |
| --- | --- | --- |
| `vm.swappiness` | `1` | 페이지 캐시 대신 힙이 스왑되면 GC 정지가 수십 초로 늘어남 |
| `vm.dirty_background_ratio` / `vm.dirty_ratio` | `5` / `60` | 더티 페이지를 일찍 내려쓰되 급격한 플러시 폭주는 막음 |
| 파일 핸들(`ulimit -n`) | `100000` 이상 | 파티션 × 세그먼트 수만큼 열림. 기본 1024면 곧 고갈 |
| 파일시스템 | **XFS** | 대용량 순차 쓰기와 많은 파일에서 ext4보다 안정적. 공식 권장 |
| 마운트 옵션 | `noatime` | 읽을 때마다 메타데이터 갱신하는 비용 제거 |

### 디스크 구성: JBOD와 RAID

| 구성 | 장점 | 단점 | 판단 |
| --- | --- | --- | --- |
| **JBOD** (`log.dirs`에 디스크 여러 개) | 용량·처리량을 디스크 수만큼 전부 사용 | 디스크 하나 고장이면 그 디스크의 파티션이 오프라인. 브로커는 계속 동작(1.1+) | 복제가 있으므로 **기본 선택**. 파티션 재배치로 복구 |
| **RAID 10** | 디스크 고장에도 브로커 무중단 | 용량 절반, 쓰기 성능 저하, 리빌드 중 I/O 경쟁 | 브로커 수가 적어 한 대 이탈이 부담일 때 |
| RAID 5/6 | 용량 효율 | 쓰기마다 패리티 계산, 순차 쓰기 성능 크게 하락 | 비권장 |
| NAS·NFS | 관리 편의 | 지연 불안정, 파일 잠금 문제 | **사용 금지** |

Kafka의 내구성은 디스크가 아니라 **복제**가 책임집니다. 그래서 RAID로 이중 보호하는 것보다 JBOD로 용량을 쓰고 RF 3을 유지하는 쪽이 비용 대비 낫습니다. 단, 브로커가 3대뿐인 환경에서는 디스크 한 장 고장이 곧 ISR 축소라 RAID 10도 합리적입니다.

SSD와 HDD는 용도로 나눕니다. 순차 쓰기와 최신 데이터 읽기만 있으면 HDD도 충분합니다. 컨슈머가 자주 되감거나 파티션이 많아 디스크 헤드가 여러 파일을 오가면 SSD가 필요합니다.

## 2. 컨트롤러 배치와 KRaft 메타데이터

토폴로지 선택(combined·dedicated)은 [06](06-cluster-design.md) 1장과 [11](11-broker-controller-zookeeper-and-failover.md)에 정리돼 있습니다. 여기서는 배포 시 놓치기 쉬운 점만 덧붙입니다.

- 컨트롤러는 **3 또는 5대**, 항상 홀수입니다. 과반(quorum)을 유지해야 하므로 3대면 1대, 5대면 2대 손실까지 견딥니다. 4대는 3대와 같은 내구성에 비용만 더 듭니다.
- 컨트롤러 메타데이터 로그(`__cluster_metadata`)는 `metadata.log.dir`로 **데이터 디스크와 분리**합니다. 데이터 디스크가 꽉 차도 메타데이터 쓰기는 계속돼야 합니다.
- 메타데이터 로그는 주기적으로 **스냅샷**으로 압축됩니다. `metadata.log.max.record.bytes.between.snapshots`(기본 20MB)마다 새 스냅샷을 만들고 오래된 로그를 지웁니다. 새 브로커나 재시작한 브로커는 최신 스냅샷 + 이후 로그로 빠르게 메타데이터를 복구합니다.
- Kafka 3.9부터는 `controller.quorum.voters`를 고정하지 않고 `controller.quorum.bootstrap.servers`로 **동적 쿼럼**을 쓸 수 있습니다. 컨트롤러 추가·교체를 재시작 없이 할 수 있습니다. 4.x 신규 구축이면 이 방식을 권장합니다.

### ZooKeeper에서 KRaft로 옮기는 절차 (개요)

기존 ZooKeeper 클러스터를 운영 중이라면 4.0 이전에 반드시 옮겨야 합니다. 4.0은 ZooKeeper 모드를 제거했고, 마이그레이션은 3.9에서 끝내야 합니다.

1. 브로커를 마이그레이션 지원 버전(3.6 이상, 권장 3.9)으로 올립니다. `inter.broker.protocol.version`도 함께 올립니다.
2. KRaft 컨트롤러 쿼럼을 새로 띄우면서 `zookeeper.metadata.migration.enable=true`와 ZooKeeper 접속 정보를 줍니다. 컨트롤러가 ZooKeeper의 메타데이터를 읽어 `__cluster_metadata`에 복사합니다.
3. 브로커를 한 대씩 재시작하며 같은 플래그와 `controller.quorum.voters`를 추가합니다. 이 단계에서 브로커는 ZooKeeper에도 쓰고 KRaft에도 쓰는 **이중 쓰기** 상태입니다. 문제가 생기면 여기서 되돌릴 수 있습니다.
4. 모든 브로커가 KRaft 모드로 돌아가면 컨트롤러에서 마이그레이션 플래그를 제거하고 재시작해 **확정**합니다. 이후에는 되돌릴 수 없습니다.
5. ZooKeeper를 내립니다.

신규 구축이라면 이 절차 자체가 불필요합니다. 그래서 [05](05-kraft-vs-zookeeper.md)가 "처음부터 KRaft"를 결론으로 둔 것입니다.

## 3. 랙 인식 (Rack Awareness)

같은 랙·같은 AZ·같은 전원 계통에 복제본 3개가 몰려 있으면 RF 3이 의미가 없습니다. 랙 하나가 내려가면 파티션 전체가 오프라인이 됩니다. `broker.rack`은 이를 막는 설정입니다.

```properties
# 브로커 1
broker.rack=rack-a
# 브로커 2
broker.rack=rack-b
# 브로커 3
broker.rack=rack-c
```

값은 임의 문자열입니다. 물리 랙, 클라우드 AZ(`ap-northeast-2a`), 데이터센터 구역 어느 것이든 "함께 죽을 단위"를 적습니다.

### 복제본 배치 규칙

토픽을 만들 때 컨트롤러는 각 파티션의 복제본을 **서로 다른 랙에 우선 배치**합니다. 랙이 3개, RF 3이면 파티션마다 복제본이 랙 하나씩 들어갑니다. 랙 하나가 전부 내려가도 모든 파티션에 복제본 2개가 남고, `min.insync.replicas=2`를 만족해 **쓰기가 계속됩니다**.

랙 수가 RF보다 적으면 한 랙에 복제본 두 개가 들어갑니다. 랙 2개에 RF 3이면 한 랙 손실 시 파티션의 절반은 복제본이 1개만 남아 `min.insync.replicas=2`에 걸려 쓰기가 막힙니다. **랙 수 ≥ RF**가 되도록 설계해야 랙 인식이 효과를 냅니다.

### 주의할 점

- 랙 인식은 **토픽 생성·파티션 추가 시점**에만 적용됩니다. 기존 토픽에 `broker.rack`을 나중에 추가해도 복제본이 자동으로 옮겨지지 않습니다. 4장의 파티션 재배치를 실행해야 합니다.
- 브로커 일부만 `broker.rack`을 설정하면 토픽 생성이 실패합니다. 전부 설정하거나 전부 비워야 합니다. `--disable-rack-aware` 옵션으로 무시하고 만들 수는 있지만 권장하지 않습니다.
- 랙 간 네트워크 지연이 복제 지연이 됩니다. 같은 데이터센터 안의 랙·AZ(1~2ms)는 문제없지만, 수십 ms가 넘는 데이터센터 간 거리는 4장의 멀티 DC 패턴으로 다뤄야 합니다.

### 팔로워에서 읽기 (KIP-392)

기본적으로 컨슈머는 파티션 **리더**에서만 읽습니다. 멀티 AZ에서는 컨슈머와 리더가 다른 AZ에 있으면 AZ 간 트래픽 비용이 발생합니다. 다음 두 설정으로 같은 랙의 팔로워에서 읽게 할 수 있습니다.

```properties
# 브로커
replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector
```

```properties
# 컨슈머
client.rack=rack-a
```

팔로워는 리더보다 약간 늦을 수 있으므로 지연 민감한 컨슈머에는 쓰지 않습니다. 프로듀서는 항상 리더에만 씁니다.

Kafka 3.4부터는 컨슈머 그룹 파티션 배정도 랙을 고려합니다. `client.rack`이 설정된 컨슈머에게 같은 랙에 복제본이 있는 파티션을 우선 배정해 팔로워 읽기 효과를 높입니다.

## 4. 증설과 교체: 파티션 재배치

브로커를 추가하면 **새 브로커는 비어 있습니다**. Kafka는 기존 파티션을 자동으로 옮기지 않습니다. 새로 만드는 토픽만 새 브로커를 포함합니다. 기존 토픽의 부하를 나누려면 `kafka-reassign-partitions.sh`로 직접 옮겨야 합니다.

### 절차

```bash
# 1) 옮길 토픽 목록
cat > topics.json <<'EOF'
{"topics": [{"topic": "commerce.order.created"}, {"topic": "commerce.payment.completed"}], "version": 1}
EOF

# 2) 배치안 생성 — 브로커 1,2,3,4에 고르게 (랙 인식 반영됨)
kafka-reassign-partitions.sh --bootstrap-server kafka1:9094 --command-config client.properties \
  --topics-to-move-json-file topics.json --broker-list "1,2,3,4" --generate
#   출력의 "Proposed partition reassignment configuration" 부분을 reassignment.json 으로 저장
#   "Current partition replica assignment" 는 롤백용으로 보관

# 3) 실행 — 복제 대역폭 제한(바이트/초)을 반드시 걸기
kafka-reassign-partitions.sh --bootstrap-server kafka1:9094 --command-config client.properties \
  --reassignment-json-file reassignment.json --execute --throttle 50000000

# 4) 진행 확인 — 완료되면 이 명령이 스로틀도 제거한다
kafka-reassign-partitions.sh --bootstrap-server kafka1:9094 --command-config client.properties \
  --reassignment-json-file reassignment.json --verify
```

### 왜 스로틀이 필수인가

재배치는 새 복제본이 리더에서 **전체 데이터를 처음부터 복사**하는 작업입니다. 파티션 수십 개, 수백 GB를 제한 없이 복사하면 복제 트래픽이 일반 복제를 밀어내 ISR이 축소되고 프로듀서 지연이 튑니다. `--throttle`은 브로커의 `leader.replication.throttled.rate`와 `follower.replication.throttled.rate`를 설정합니다. 네트워크 여유의 절반 이하로 잡고, 진행이 느리면 `--execute --throttle`을 더 큰 값으로 다시 실행해 올립니다.

**`--verify`를 끝까지 실행해야 합니다.** 스로틀은 재배치가 끝나도 자동 해제되지 않습니다. `--verify`가 완료를 확인하면서 스로틀 설정을 지웁니다. 이를 빠뜨리면 이후 일반 복제도 제한된 속도로 동작합니다.

### 브로커 교체(디스크 손실 포함)

브로커 한 대를 같은 `node.id`로 다시 올리면 복제본이 비어 있으므로 리더에서 자동으로 다시 받습니다. 재배치가 필요 없습니다. 다른 `node.id`로 교체하면 위 절차로 옛 브로커의 파티션을 새 브로커로 옮긴 뒤 옛 브로커를 제거합니다. 절차 상세는 [troubleshooting/cluster-total-outage](../troubleshooting/cluster-total-outage.md)의 "디스크를 잃은 노드" 절을 참고합니다.

리더만 고르게 다시 배분하려면 `kafka-leader-election.sh --election-type PREFERRED --all-topic-partitions`를 씁니다. `auto.leader.rebalance.enable=true`(기본)면 주기적으로 자동 실행되지만, 장애 직후 바로 정리할 때 수동으로 호출합니다.

## 5. 멀티 데이터센터

데이터센터(DC) 간 구성은 목적부터 정합니다. 목적이 다르면 정답이 다릅니다.

| 목적 | 요구 | 맞는 패턴 |
| --- | --- | --- |
| 재해 복구(DR) | DC 하나가 사라져도 데이터 보존, 몇 분 안에 재개 | Active-Passive 복제 |
| 무중단 DR | 데이터 유실 0, 전환 시간 0 | Stretch 클러스터 |
| 지역 분산 서비스 | 각 지역 사용자를 가까운 DC에서 처리, 데이터는 공유 | Active-Active 복제 |
| 중앙 집계 | 여러 지역의 이벤트를 분석용 DC 한 곳에 모음 | 허브-스포크(집계) |

### 패턴 A — Stretch 클러스터

클러스터 **하나**를 여러 DC에 걸쳐 배치합니다. `broker.rack`에 DC 이름을 넣고 RF 3, `min.insync.replicas=2`를 쓰면 DC 하나 손실 시에도 쓰기가 계속됩니다. 동기 복제이므로 **RPO 0**(유실 없음), 전환 작업이 없어 **RTO 0**입니다.

조건이 까다롭습니다.

- DC 간 지연이 **수 ms에서 최대 수십 ms** 이내여야 합니다. 모든 `acks=all` 쓰기가 DC 간 왕복을 기다리므로 지연이 그대로 프로듀서 지연이 됩니다. 같은 도시 안의 DC나 클라우드의 한 리전 내 AZ가 적합합니다.
- **DC가 3곳** 필요합니다. DC 2곳에 컨트롤러 3대를 나누면(2+1) 2대 있는 DC가 죽으면 쿼럼을 잃습니다. 브로커만 두 DC에 두고 컨트롤러 하나를 제3의 작은 사이트에 두는 "2.5 DC" 구성이 절충안입니다.
- 랙 수가 2라면 앞서 본 대로 한 DC 손실 시 파티션 절반이 `min.insync.replicas`에 걸립니다. DC 2곳이면 RF 4, `min.insync.replicas=2`로 각 DC에 복제본 2개씩 두는 방법이 있습니다. 이때 한 DC의 복제본 2개만으로 ISR을 채워 커밋될 수 있어, 그 DC가 바로 죽으면 유실 가능성이 생깁니다. 완전한 RPO 0은 DC 3곳에서만 성립합니다.

### 패턴 B — Active-Passive (MirrorMaker 2)

운영 DC의 클러스터를 **비동기로** DR DC의 별도 클러스터에 복제합니다. 프로듀서·컨슈머는 평소 운영 DC만 봅니다. 운영 DC가 죽으면 앱의 `bootstrap.servers`를 DR로 바꿔 재개합니다.

```text
DC-A (active)                           DC-B (passive)
 orders ──▶ MirrorMaker 2 ──비동기──▶ A.orders
 앱 (produce/consume)                    앱 (대기, 장애 시 전환)
```

- **RPO > 0**입니다. 복제 지연분(보통 초 단위)은 장애 시 유실됩니다. 지연은 MM2의 lag 지표로 감시합니다.
- **RTO는 전환 절차 시간**입니다. DNS·설정 변경, 컨슈머 오프셋 변환, 앱 재시작이 포함됩니다. 수 분에서 수십 분입니다.
- DC 간 지연 제약이 없습니다. 대륙 간 복제도 가능합니다.
- 비용은 클러스터 두 세트와 MM2 워커입니다.

### 패턴 C — Active-Active

두 DC 모두 쓰기를 받고 **서로 양방향으로** 복제합니다. 각 DC의 앱은 자기 DC 토픽에 쓰고, 상대 DC에서 온 토픽도 함께 읽습니다.

```text
DC-A: orders (로컬 쓰기), B.orders (B에서 복제)
DC-B: orders (로컬 쓰기), A.orders (A에서 복제)
컨슈머는 두 토픽을 모두 구독 → 전체 주문 흐름
```

같은 키에 대한 쓰기가 두 DC에서 동시에 일어날 수 있습니다. Kafka는 이를 병합하지 않습니다. **충돌 방지는 설계 책임**입니다. 보통 사용자·지역을 기준으로 "이 키는 항상 DC-A에서만 쓴다"로 나눕니다. 복제 루프는 MM2가 토픽 접두사로 막습니다. `A.orders`는 다시 B로 복제되지 않습니다.

### 패턴 비교

| | Stretch | Active-Passive | Active-Active |
| --- | --- | --- | --- |
| RPO | 0 | 초 단위 | 초 단위 |
| RTO | 0 | 분 단위 | 0 (트래픽 전환만) |
| DC 간 지연 요구 | 수십 ms 이내 | 제약 없음 | 제약 없음 |
| DC 수 | 3 (또는 2.5) | 2 | 2 이상 |
| 클러스터 수 | 1 | 2 | 2 이상 |
| 앱 복잡도 | 없음 | 전환 절차 필요 | 토픽 두 개 구독, 키 소유권 설계 |
| 비용 | 브로커만 | 클러스터 ×2 + MM2 | 클러스터 ×N + MM2 양방향 |

## 6. MirrorMaker 2 동작 원리

MirrorMaker 2(MM2)는 [Kafka Connect](17-kafka-connect.md) 위에서 도는 커넥터 세 개의 묶음입니다. 레거시 MirrorMaker 1은 단순 컨슈머+프로듀서라 오프셋 변환과 설정 동기화가 없어 더 이상 쓰지 않습니다.

| 커넥터 | 하는 일 |
| --- | --- |
| `MirrorSourceConnector` | 토픽 데이터와 토픽 설정(파티션 수, 보관 기간), ACL을 복제 |
| `MirrorCheckpointConnector` | 컨슈머 그룹 오프셋을 **대상 클러스터 기준으로 변환**해 체크포인트 토픽에 기록 |
| `MirrorHeartbeatConnector` | 복제 경로가 살아 있는지 heartbeat 토픽으로 확인, 다단계 복제 추적 |

### 오프셋은 그대로 복사되지 않는다

원본 토픽의 오프셋 1,000번 메시지가 대상 토픽에서도 1,000번이라는 보장이 없습니다. 대상 토픽은 MM2 프로듀서가 새로 쓰는 것이라 오프셋이 독립적이고, 중간에 압축·재시도가 끼면 어긋납니다. 그래서 DR 전환 시 컨슈머가 "원본에서 읽던 오프셋"을 그대로 대상에 적용하면 엉뚱한 위치에서 시작합니다.

`MirrorCheckpointConnector`가 원본 오프셋 ↔ 대상 오프셋 대응표(offset sync)를 유지하고, 주기적으로 컨슈머 그룹 오프셋을 변환해 **대상 클러스터의 `__consumer_offsets`에 직접 써 줍니다**(`sync.group.offsets.enabled=true`, 2.7+). 전환 후 컨슈머가 같은 `group.id`로 대상에 붙으면 변환된 위치부터 이어서 읽습니다. 이 기능 덕에 Active-Passive 전환이 실용적이 됩니다.

### 토픽 이름 정책

기본 `DefaultReplicationPolicy`는 대상 토픽에 **원본 클러스터 별칭을 접두사**로 붙입니다. `A.orders`처럼 됩니다. 장점은 루프 방지와 출처 구분이고, 단점은 전환 시 앱이 구독하는 토픽 이름이 바뀐다는 점입니다.

`IdentityReplicationPolicy`(3.0+)는 같은 이름으로 복제합니다. Active-Passive DR에서 앱 수정 없이 전환하려면 이 정책을 씁니다. 단, 양방향 복제에 쓰면 루프가 생기므로 Active-Active에서는 쓰지 않습니다.

### 설정 예시 (Active-Passive)

```properties
# mm2.properties — 전용 MM2 클러스터 모드 (connect-mirror-maker.sh)
clusters = A, B
A.bootstrap.servers = kafka-a1:9094,kafka-a2:9094,kafka-a3:9094
B.bootstrap.servers = kafka-b1:9094,kafka-b2:9094,kafka-b3:9094

# A → B 단방향
A->B.enabled = true
A->B.topics = commerce\..*
A->B.groups = .*
B->A.enabled = false

# DR용: 같은 이름으로 복제
replication.policy.class = org.apache.kafka.connect.mirror.IdentityReplicationPolicy

# 컨슈머 오프셋을 B 의 __consumer_offsets 에 동기화
sync.group.offsets.enabled = true
sync.group.offsets.interval.seconds = 60
emit.checkpoints.interval.seconds = 60

# 토픽 설정·ACL 동기화
sync.topic.configs.enabled = true
sync.topic.acls.enabled = true
refresh.topics.interval.seconds = 300

replication.factor = 3
checkpoints.topic.replication.factor = 3
heartbeats.topic.replication.factor = 3
offset-syncs.topic.replication.factor = 3

# 보안 (클러스터별 접두사)
A.security.protocol = SASL_SSL
A.sasl.mechanism = SCRAM-SHA-512
A.sasl.jaas.config = org.apache.kafka.common.security.scram.ScramLoginModule required username="mm2" password="${MM2_PASSWORD_A}";
B.security.protocol = SASL_SSL
B.sasl.mechanism = SCRAM-SHA-512
B.sasl.jaas.config = org.apache.kafka.common.security.scram.ScramLoginModule required username="mm2" password="${MM2_PASSWORD_B}";
```

```bash
connect-mirror-maker.sh mm2.properties
```

### 배치 위치

MM2 워커는 **대상 클러스터 쪽 DC**에 둡니다. MM2는 원본에서 읽어(consume) 대상에 씁니다(produce). 네트워크가 끊기거나 느릴 때 컨슈머 쪽은 fetch 재시도로 버티지만, 프로듀서 쓰기가 원거리면 타임아웃과 재시도로 중복이 늘어납니다. 원본을 원거리에서 읽고 대상에는 근거리로 쓰는 배치가 안정적입니다.

## 7. 계층형 스토리지 (Tiered Storage)

Kafka 3.6에서 얼리 액세스, 3.9에서 정식이 된 기능입니다. 오래된 세그먼트를 브로커 로컬 디스크 대신 **오브젝트 스토리지(S3, MinIO 등)로** 내보냅니다.

```text
로컬 디스크:  최근 N시간 (local.retention.ms)  ← 빠른 읽기, 복제 대상
원격 저장소:  나머지 보관 기간 (retention.ms)   ← 저렴, 느린 읽기
```

```properties
# 브로커
remote.log.storage.system.enable=true
remote.log.storage.manager.class.name=<플러그인 클래스>
remote.log.metadata.manager.class.name=org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager
```

```bash
# 토픽
kafka-configs.sh --alter --topic commerce.order.created \
  --add-config remote.storage.enable=true,local.retention.ms=86400000,retention.ms=2592000000
```

| 장점 | 제약 |
| --- | --- |
| 보관 기간을 늘려도 브로커 디스크가 늘지 않음 | **compacted 토픽에는 사용 불가** (`cleanup.policy=compact`) |
| 브로커 교체·재배치 시 복사할 로컬 데이터가 줄어 빨라짐 | 토픽에 한 번 켜면 끌 수 없음 (3.9 기준) |
| 오래된 데이터 읽기가 브로커 페이지 캐시를 밀어내지 않음 | 원격 읽기는 느림. 되감기가 잦은 워크로드에는 부적합 |
| 스토리지 비용 절감 | 원격 저장소 플러그인(RemoteStorageManager)을 별도로 선택·운영해야 함. Apache Kafka에 기본 구현은 없음 |

이 저장소의 MinIO 구성([minio/](../../minio/README.md))이 있다면 그 버킷을 원격 저장소로 쓰는 것이 자연스러운 조합입니다. 다만 운영 도입 전에 플러그인의 성숙도와 원격 읽기 지연을 실측해야 합니다.

## 8. 시험 포인트 요약

- 디스크 용량 = 유입량 × 보관 기간 × RF. 네트워크 out > in (복제 + 컨슈머 그룹 수).
- 브로커당 파티션 4,000개 안팎이 전통적 권고. 파티션이 많으면 파일 핸들·복구 시간이 늘어남.
- JVM 힙은 6GB 정도로 작게. 나머지는 페이지 캐시. zero-copy(`sendfile`)가 성능의 핵심.
- XFS, `noatime`, `vm.swappiness=1`, 파일 핸들 10만 이상.
- JBOD가 기본. 내구성은 RAID가 아니라 복제가 책임. NAS 금지.
- 컨트롤러는 3 또는 5대(홀수). 메타데이터 로그 디렉터리는 데이터 디스크와 분리.
- `broker.rack`은 토픽 생성 시점에만 적용. 기존 토픽은 재배치 필요. 랙 수 ≥ RF여야 효과.
- 팔로워 읽기는 `replica.selector.class` + `client.rack`. 프로듀서는 항상 리더.
- 브로커 추가 시 자동 재배치 없음. `kafka-reassign-partitions.sh --generate/--execute --throttle/--verify`. `--verify`가 스로틀 해제.
- Stretch = RPO 0, 지연 수십 ms 이내, DC 3곳. Active-Passive = MM2 비동기, RPO > 0.
- MM2 = Connect 커넥터 3개. 오프셋은 그대로 복사되지 않으며 Checkpoint 커넥터가 변환. 기본 토픽 이름은 `<원본별칭>.<토픽>`.
- MM2 워커는 대상 클러스터 쪽에 배치.
- Tiered Storage: compacted 토픽 불가, `local.retention.ms`로 로컬 보관량 조절.

## 관련 문서

- 3대 클러스터 토폴로지와 무손실 설정: [06-cluster-design](06-cluster-design.md)
- 컨트롤러 failover 원리: [11-broker-controller-zookeeper-and-failover](11-broker-controller-zookeeper-and-failover.md)
- KRaft 선택 배경: [05-kraft-vs-zookeeper](05-kraft-vs-zookeeper.md)
- MM2의 기반인 Connect: [17-kafka-connect](17-kafka-connect.md)
- 디스크 손실 복구 절차: [troubleshooting/cluster-total-outage](../troubleshooting/cluster-total-outage.md)
- 생태계 배치 방안: [15-deployment-layout-options](15-deployment-layout-options.md)

## 참고한 공식 문서

- Apache Kafka Documentation — Operations (Hardware and OS, Filesystem Selection, Datacenters, Geo-Replication, Expanding your cluster, Tiered Storage)
- KIP-36: Rack aware replica assignment
- KIP-392: Allow consumers to fetch from closest replica
- KIP-382: MirrorMaker 2.0
- KIP-545: Support automated consumer offset sync across clusters in MM 2.0
- KIP-405: Kafka Tiered Storage
- KIP-853: KRaft Controller Membership Changes (동적 쿼럼)
- KIP-866: ZooKeeper to KRaft Migration
