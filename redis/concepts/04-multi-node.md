# Redis 여러 대 구성 방식

## 학습 목표

- 복제, 자동 장애 조치, 읽기 확장, 쓰기 샤딩이 각각 다른 문제임을 구분합니다.
- Sentinel quorum과 failover 승인의 조건을 설명합니다.
- Redis Cluster의 client, hash slot, 네트워크 제약을 배포 전에 점검합니다.

Redis를 여러 대 묶는 방법은 하나가 아닙니다. 어떤 방식이 좋은지는 "고가용성만 필요한지", "읽기 부하를 나누고 싶은지", "데이터를 여러 노드에 나눠 담아야 하는지", "멀티 리전 쓰기까지 필요한지"에 따라 달라집니다.

## 한눈에 보기

| 방식 | 해결하려는 문제 | 쓰기 확장 | 읽기 확장 | 자동 장애 조치 | 샤딩 | 추천 상황 |
| --- | --- | --- | --- | --- | --- | --- |
| Primary-Replica | 복제, 읽기 분산 | 아니오 | stale read 허용 시 가능 | 단독으로는 불가 | 아니오 | 단순 복제, 읽기 부하 분산 |
| Sentinel | 비샤딩 Redis의 고가용성 | 아니오 | stale read 허용 시 가능 | 가능 | 아니오 | 데이터가 한 노드에 들어가고 자동 failover가 필요할 때 |
| Redis Cluster | 샤딩과 고가용성 | 가능 | replica `READONLY` 사용 시 가능 | 가능 | 가능 | 데이터나 트래픽이 한 Redis를 넘을 때 |
| Client-side Sharding | 애플리케이션 주도 분산 | 가능 | 가능 | 직접 구현 | 가능 | 단순 캐시를 앱이 직접 나누고 싶을 때 |
| Proxy Sharding | 프록시 주도 분산 | 가능 | 가능 | 제품/구성 의존 | 가능 | 앱을 cluster-aware하게 바꾸기 어려울 때 |
| Managed Redis | 운영 부담 감소 | 상품 구성 의존 | 상품 구성 의존 | 보통 가능 | 상품 구성 의존 | 장애 조치, 백업, 패치를 직접 운영하고 싶지 않을 때 |
| Active-Active | 멀티 리전 양방향 쓰기 | 가능 | 가능 | 가능 | 제품 의존 | 글로벌 쓰기, 낮은 지연 시간이 필요할 때 |

## 1. Primary-Replica

가장 기본적인 여러 대 구성입니다. Primary가 쓰기를 받고, Replica는 Primary 데이터를 비동기로 복제합니다.

```text
App -> Primary
        ├─ Replica 1
        └─ Replica 2
```

Replica는 읽기 부하 분산, 백업 생성 부하 분리, 장애 시 승격 후보로 사용할 수 있습니다. 하지만 replica 자체는 백업이 아닙니다. 잘못된 삭제나 손상도 복제되므로 과거 시점으로 복구하려면 별도 백업이 필요합니다. Primary-Replica만으로는 자동 장애 조치도 되지 않습니다. Primary가 죽으면 누가 새 Primary가 될지 Sentinel, 관리형 서비스, 별도 자동화가 처리해야 합니다.

Replica 설정은 보통 Replica 쪽 설정 파일에 작성합니다.

```conf
replicaof 10.0.1.10 6379
masteruser replica-user
masterauth replace-with-replica-password
```

확인은 `INFO replication`으로 합니다.

```bash
redis-cli INFO replication
```

좋은 경우:

- 읽기 요청 일부를 Replica로 보내고 싶을 때
- 장애 시 승격할 복제본을 준비하고 싶을 때
- 백업이나 분석 용도로 Primary 부하를 줄이고 싶을 때

주의할 점:

- Redis 복제는 기본적으로 비동기입니다. 장애 순간 일부 쓰기 데이터가 Replica에 도착하지 않았을 수 있습니다.
- Replica 읽기는 Primary보다 뒤처질 수 있습니다.
- 쓰기 처리량은 Primary 하나에 묶입니다.
- persistence가 없는 primary를 빈 데이터로 자동 재시작하면 그 상태가 replica로 전파될 수 있으므로 재시작 정책과 persistence를 함께 설계합니다.

복제 지연이 큰 상태에서 쓰기를 제한해 유실 창을 줄일 수 있지만 가용성과 trade-off가 생깁니다.

```conf
min-replicas-to-write 1
min-replicas-max-lag 10
```

특정 쓰기 뒤 replica 확인 응답을 기다릴 때는 `WAIT`를 사용할 수 있습니다.

```bash
SET order:123 paid
WAIT 1 1000
```

`WAIT`도 강한 일관성이나 무손실을 보장하지 않는 best-effort 수단입니다. 업무 내구성이 필요하면 저장소 선택과 트랜잭션 경계를 함께 검토합니다.

## 2. Sentinel

Sentinel은 Primary-Replica 구조에 감시와 자동 장애 조치를 붙이는 방식입니다. Sentinel은 Redis 노드의 상태를 감시하다가 Primary 장애를 감지하면 Replica 중 하나를 새 Primary로 승격합니다.

```text
App -> Sentinel에게 현재 Primary 조회
              |
        Primary Redis
        ├─ Replica 1
        └─ Replica 2

Sentinel 1, Sentinel 2, Sentinel 3
```

Sentinel은 샤딩을 하지 않습니다. 데이터가 한 Redis Primary에 들어가는 규모에서 고가용성을 얻는 방식입니다.

예시 설정:

```conf
port 26379

sentinel monitor mymaster 10.0.1.10 6379 2
sentinel auth-user mymaster sentinel-user
sentinel auth-pass mymaster replace-with-sentinel-password
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 60000
sentinel parallel-syncs mymaster 1
```

마지막 숫자 `2`는 quorum입니다. 한 Sentinel의 주관적 장애 판단은 SDOWN이고 quorum만큼 동의하면 ODOWN이 됩니다. 실제 failover 수행에는 알려진 Sentinel 과반수의 승인도 별도로 필요합니다. Sentinel 3개와 quorum 2 구성에서는 장애 판단과 승인 모두 2개가 필요합니다. 실무에서는 Sentinel을 3개 이상, 가능하면 서로 다른 장애 도메인에 배치합니다.

```bash
redis-cli -p 26379 SENTINEL CKQUORUM mymaster
```

좋은 경우:

- Redis 데이터가 한 Primary에 들어갈 때
- 샤딩은 필요 없고 자동 failover만 필요할 때
- Redis Cluster보다 단순한 운영 모델을 원할 때

주의할 점:

- 애플리케이션 클라이언트가 Sentinel을 지원해야 합니다.
- Sentinel 설정 파일은 failover 중 재작성될 수 있으므로 컨테이너에서 읽기 전용으로만 마운트하면 문제가 될 수 있습니다.
- 자동 failover가 있어도 비동기 복제 특성상 장애 직전 쓰기는 유실될 수 있습니다.

## 3. Redis Cluster

Redis Cluster는 Redis 공식 샤딩 방식입니다. 전체 keyspace를 16384개 hash slot으로 나누고, 여러 master가 slot을 나눠 가집니다.

```text
App
 |
Redis Cluster
├─ Master A: hash slots 0-5460
│  └─ Replica A
├─ Master B: hash slots 5461-10922
│  └─ Replica B
└─ Master C: hash slots 10923-16383
   └─ Replica C
```

Cluster-aware 클라이언트는 키가 어느 slot에 속하는지 계산하고 해당 master로 요청을 보냅니다. slot 위치가 바뀌면 Redis가 `MOVED` 또는 `ASK` 응답을 보내고, 클라이언트는 새 노드로 다시 요청합니다.

각 노드는 Cluster mode와 쓰기 가능한 고유 상태 파일을 먼저 설정합니다. `nodes.conf`는 Redis가 관리하므로 직접 편집하거나 노드 간 공유하지 않습니다.

```conf
port 6379
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 5000
appendonly yes
```

Cluster 생성 예시:

```bash
redis-cli --cluster create \
  10.0.1.11:6379 10.0.1.12:6379 10.0.1.13:6379 \
  10.0.1.14:6379 10.0.1.15:6379 10.0.1.16:6379 \
  --cluster-replicas 1
```

접속할 때는 cluster mode를 이해하는 클라이언트가 필요합니다. `redis-cli`는 `-c` 옵션을 사용합니다.

```bash
redis-cli -c -h 10.0.1.11 -p 6379
```

확인 명령어:

```bash
redis-cli -c CLUSTER INFO
redis-cli -c CLUSTER NODES
redis-cli -c CLUSTER SHARDS
redis-cli -c CLUSTER KEYSLOT 'cart:{user:123}'
```

좋은 경우:

- 데이터가 단일 Redis 메모리 한도를 넘을 때
- 쓰기 처리량을 여러 master로 나누고 싶을 때
- 샤딩과 자동 failover를 Redis 자체 기능으로 가져가고 싶을 때

주의할 점:

- 공식적으로 최소 3개 master가 필요하고, 운영에서는 보통 3 master + 3 replica부터 시작합니다.
- 모든 클라이언트가 Redis Cluster를 잘 지원하는 것은 아닙니다.
- 여러 키를 한 번에 다루는 명령은 같은 hash slot에 있어야 합니다.
- 데이터 포트와 cluster bus 포트(기본값은 데이터 포트 + 10000)를 노드 간 열어야 합니다.
- Redis Cluster는 노드가 광고한 주소와 포트가 그대로 도달 가능해야 하며 포트가 remap되는 NAT 환경을 공식 지원하지 않습니다.
- Cluster는 DB 0만 사용하며 `SELECT`를 지원하지 않습니다.
- replica 읽기는 클라이언트가 replica 연결에서 `READONLY`를 사용하고 stale read를 허용할 때만 적용합니다.

multi-key 명령이 필요하면 hash tag로 같은 slot에 묶을 수 있습니다.

```text
cart:{user:123}
profile:{user:123}
session:{user:123}
```

중괄호 안의 `user:123`이 hash slot 계산에 사용되므로 위 세 키는 같은 slot에 들어갑니다.

## 4. Client-side Sharding

Client-side sharding은 Redis Cluster를 쓰지 않고 애플리케이션이나 클라이언트 라이브러리가 직접 키를 여러 Redis에 나눠 보내는 방식입니다.

```text
App
├─ key A -> Redis 1
├─ key B -> Redis 2
└─ key C -> Redis 3
```

보통 consistent hashing을 사용합니다. Redis 노드들은 서로를 모르는 독립 인스턴스이고, 애플리케이션이 "이 키는 어느 Redis로 보낼지"를 결정합니다.

좋은 경우:

- 단순 캐시를 여러 노드로 나누고 싶을 때
- Redis Cluster의 multi-key 제약이나 클러스터 프로토콜을 피하고 싶을 때
- 이미 애플리케이션에 sharding 로직이 있을 때

주의할 점:

- 노드 추가, 제거, 리밸런싱을 직접 설계해야 합니다.
- 장애 조치도 직접 구현하거나 각 shard마다 Sentinel/관리형 구성을 붙여야 합니다.
- 같은 기능을 여러 서비스가 각자 다르게 구현하면 운영이 어려워집니다.

## 5. Proxy Sharding

Proxy sharding은 애플리케이션과 Redis 사이에 프록시를 두고, 프록시가 키를 여러 Redis로 라우팅하는 방식입니다.

```text
App -> Proxy -> Redis 1
             -> Redis 2
             -> Redis 3
```

애플리케이션은 Redis가 하나처럼 보이지만, 실제로는 프록시가 뒤쪽 Redis들을 나눠 씁니다. 예전에는 Twemproxy 같은 구성이 많이 쓰였고, 일부 조직은 자체 프록시나 서비스 메시 계층을 사용하기도 합니다.

좋은 경우:

- 애플리케이션 클라이언트를 Redis Cluster-aware하게 바꾸기 어려울 때
- 라우팅 정책을 중앙에서 통제하고 싶을 때
- 여러 언어의 애플리케이션이 같은 sharding 정책을 공유해야 할 때

주의할 점:

- 프록시 자체가 추가 장애 지점이 됩니다.
- Redis의 모든 명령을 그대로 지원하지 못할 수 있습니다.
- 요즘 새 구성에서는 Redis Cluster나 관리형 Redis를 먼저 검토하는 경우가 많습니다.

## 6. Managed Redis

클라우드 관리형 Redis는 별도 Redis 배포 방식이라기보다 운영 선택지입니다. AWS ElastiCache, Azure Cache for Redis, Google Cloud Memorystore, Redis Cloud 같은 서비스가 여기에 해당합니다.

관리형 서비스는 보통 아래를 대신 처리합니다.

- primary-replica 구성
- 자동 failover
- 백업과 복원
- 패치와 버전 업그레이드
- 모니터링 지표
- TLS, 인증, 네트워크 접근 제어
- Cluster mode 또는 shard 구성

좋은 경우:

- Redis 운영보다 애플리케이션 개발에 집중하고 싶을 때
- 장애 조치, 백업, 패치를 표준화하고 싶을 때
- 운영팀 규모가 작거나 Redis 장애 경험이 많지 않을 때

주의할 점:

- 제공자마다 Redis 버전, 명령어, 모듈, Cluster mode, 백업 정책이 다릅니다.
- 장애 조치 시간과 데이터 유실 가능성은 서비스 등급과 설정에 따라 달라집니다.
- 비용은 직접 운영보다 높을 수 있지만, 운영 리스크와 인건비를 함께 비교해야 합니다.

## 7. Active-Active와 멀티 리전

일반적인 Redis OSS의 Primary-Replica, Sentinel, Cluster는 "한 시점에 한 쪽이 쓰기를 받는" 구조로 이해하는 것이 안전합니다. 여러 리전에서 동시에 쓰고 자동 병합하는 active-active 구조는 Redis Enterprise, Redis Cloud의 특수 기능, 또는 CRDT 기반 제품 영역으로 보는 것이 맞습니다.

좋은 경우:

- 글로벌 서비스에서 여러 리전의 쓰기 지연 시간을 줄이고 싶을 때
- 한 리전 장애에도 다른 리전에서 쓰기를 계속 받아야 할 때

주의할 점:

- 충돌 해결 정책이 필요합니다.
- Redis 자료구조와 명령어가 모두 동일하게 동작한다고 가정하면 안 됩니다.
- 일반적인 캐시나 세션 저장소에는 과한 선택일 수 있습니다.

## 선택 기준

실무에서는 보통 아래 순서로 고릅니다.

```text
1. 단일 Redis로 충분하고 장애 영향이 작다
   -> 단일 Redis 또는 Primary-Replica

2. 데이터는 한 Redis에 들어가지만 자동 장애 조치가 필요하다
   -> Sentinel 또는 관리형 Redis의 non-cluster HA 구성

3. 데이터나 쓰기 처리량이 한 Redis를 넘는다
   -> Redis Cluster 또는 관리형 Redis의 cluster mode

4. 애플리케이션이 Redis Cluster를 지원하지 못한다
   -> 관리형 non-cluster, Proxy Sharding, Client-side Sharding 검토

5. 여러 리전에서 동시에 쓰기를 받아야 한다
   -> Redis Enterprise/Redis Cloud active-active 같은 특수 기능 검토
```

## 자주 하는 실수

- Primary-Replica만 구성하고 자동 failover가 된다고 착각합니다.
- Sentinel을 쓰면서 클라이언트는 여전히 고정 Primary 주소만 바라봅니다.
- Redis Cluster를 쓰면서 multi-key 명령의 hash slot 제약을 고려하지 않습니다.
- 컨테이너나 Kubernetes에서 Cluster 노드가 내부 주소와 외부 주소를 잘못 광고합니다.
- replica를 시점 복구 가능한 백업으로 착각합니다.
- Redis를 단독 영구 저장소처럼 쓰면서 비동기 복제와 장애 시 유실 가능성을 검토하지 않습니다.
- `maxmemory`와 eviction 정책 없이 여러 대를 늘리는 것으로 문제를 해결하려 합니다.

## 운영 확인 명령어

복제 상태:

```bash
redis-cli INFO replication
```

Sentinel 상태:

```bash
redis-cli -p 26379 SENTINEL masters
redis-cli -p 26379 SENTINEL replicas mymaster
redis-cli -p 26379 SENTINEL sentinels mymaster
redis-cli -p 26379 SENTINEL CKQUORUM mymaster
```

Cluster 상태:

```bash
redis-cli -c CLUSTER INFO
redis-cli -c CLUSTER NODES
redis-cli -c CLUSTER SHARDS
```

메모리와 eviction 상태:

```bash
redis-cli INFO memory
redis-cli INFO stats
redis-cli CONFIG GET maxmemory maxmemory-policy
```

## 장애 훈련 체크리스트

1. 클라이언트가 고정 primary 주소가 아니라 Sentinel/Cluster topology를 사용합니다.
2. primary 중지 후 감지 시간, 승격 시간, 애플리케이션 오류율을 측정합니다.
3. 장애 직전 쓰기 중 유실되거나 중복된 범위를 확인합니다.
4. 기존 primary가 돌아왔을 때 split-brain 없이 replica로 합류하는지 확인합니다.
5. backup 복구와 failover를 별도 훈련으로 수행합니다.
6. 목표 RTO/RPO와 측정 결과가 다르면 timeout, quorum, 배치 위치, 클라이언트 재시도를 조정합니다.

## 참고한 공식 문서

- [Redis replication](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)
- [High availability with Redis Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/)
- [Scale with Redis Cluster](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/)
- [Redis cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)
- [WAIT](https://redis.io/docs/latest/commands/wait/)
- [CLUSTER SHARDS](https://redis.io/docs/latest/commands/cluster-shards/)
