# Redis Cluster 설치 및 설정 방법

이 문서는 [Redis 여러 대 구성 방식](04-multi-node.md)의 선택 기준 중 "데이터나 쓰기 처리량이 한 Redis를 넘는다"에 해당할 때, Redis Cluster를 직접 설치하고 설정하는 절차를 정리합니다.

Redis Cluster는 Redis Open Source의 공식 샤딩 방식입니다. 전체 keyspace를 16384개 hash slot으로 나누고, 여러 master가 slot을 나눠 가집니다. 각 master에 replica를 붙이면 일부 노드 장애에도 서비스를 계속할 수 있습니다.

## 언제 Redis Cluster를 선택할까

Redis Cluster는 아래 조건에 맞을 때 검토합니다.

- 단일 Redis 인스턴스의 메모리 한도를 넘습니다.
- 쓰기 처리량을 여러 master로 나누고 싶습니다.
- 캐시, 세션, 랭킹, feature flag, rate limit처럼 key 단위로 나누기 쉬운 데이터입니다.
- 애플리케이션에서 Redis Cluster를 지원하는 클라이언트를 사용할 수 있습니다.
- 운영팀이 slot, failover, resharding, cluster-aware client의 제약을 이해하고 운영할 수 있습니다.

아래 조건이라면 Redis Cluster보다 다른 선택지가 나을 수 있습니다.

- 데이터가 한 Redis에 들어가고 자동 장애 조치만 필요합니다. 이 경우 Sentinel 또는 관리형 Redis의 non-cluster HA 구성이 단순합니다.
- multi-key 명령, Lua script, transaction이 서로 다른 key를 자주 함께 다룹니다.
- 클라이언트 라이브러리가 Redis Cluster를 제대로 지원하지 않습니다.
- 강한 일관성과 무손실 쓰기가 필요합니다. Redis Cluster는 비동기 복제 기반이라 장애 상황에서 쓰기 유실 가능성이 있습니다.

## 최소 구성과 권장 구성

Redis Cluster는 최소 3개의 master가 필요합니다. 운영 배포에서는 3 master + 3 replica 구성을 기본 출발점으로 잡는 것이 일반적입니다.

```text
redis-1  master A  slots 0-5460
redis-2  master B  slots 5461-10922
redis-3  master C  slots 10923-16383
redis-4  replica of redis-1
redis-5  replica of redis-2
redis-6  replica of redis-3
```

가능하면 master와 그 replica는 서로 다른 서버, rack, availability zone에 둡니다. 같은 서버에 master와 replica를 함께 두면 서버 장애 시 복제본까지 같이 사라집니다.

## 네트워크 요구사항

각 Redis Cluster 노드는 두 종류의 TCP 포트를 사용합니다.

```text
6379   Redis client port
16379  Cluster bus port
```

기본 cluster bus port는 Redis client port에 10000을 더한 값입니다. 예를 들어 Redis가 `6379`로 떠 있으면 cluster bus는 `16379`입니다. `cluster-port` 설정으로 바꿀 수 있지만, 모든 노드가 서로의 client port와 cluster bus port에 접근할 수 있어야 합니다.

방화벽 기준:

- 애플리케이션 서버 -> 모든 Redis 노드의 client port
- Redis 노드들끼리 -> 모든 Redis 노드의 client port
- Redis 노드들끼리 -> 모든 Redis 노드의 cluster bus port

Docker 포트 매핑, NAT, Kubernetes Service처럼 실제 노드 주소와 Redis가 광고하는 주소가 달라지는 환경에서는 Cluster가 꼬이기 쉽습니다. 공식 문서는 Docker에서 Redis Cluster를 다룰 때 host networking을 권장합니다. 컨테이너나 Kubernetes에서 직접 운영해야 한다면 `cluster-announce-ip`, `cluster-announce-port`, `cluster-announce-bus-port`를 반드시 검토합니다.

## 설치 전 체크리스트

설치 전에 아래를 먼저 정합니다.

- Redis 버전은 모든 노드에서 동일한 major/minor로 맞춥니다.
- 각 노드는 고정 IP 또는 안정적인 DNS 이름을 사용합니다.
- OS 시간 동기화가 되어 있습니다.
- Redis client port와 cluster bus port가 열려 있습니다.
- 데이터 디렉터리는 노드별로 분리합니다.
- `maxmemory`, eviction 정책, persistence 정책을 미리 정합니다.
- 애플리케이션 클라이언트가 Redis Cluster를 지원하는지 확인합니다.
- 운영에서 사용할 ACL, TLS, 보안 그룹, 방화벽 정책을 정합니다.

## 설치 방식 선택

운영에서 직접 Redis Cluster를 구성한다면 VM 또는 bare metal에 Redis를 설치하고 systemd로 관리하는 방식이 가장 이해하기 쉽습니다. Docker를 사용할 수는 있지만, Redis Cluster는 NAT와 포트 재매핑에 민감하므로 단순 `-p 6379:6379` 방식은 피합니다.

운영 권장 순서:

```text
1. 관리형 Redis Cluster
2. VM 또는 bare metal에 Redis Open Source 설치
3. Docker host network 기반 구성
4. Kubernetes 직접 운영
```

Kubernetes에서 직접 Redis Cluster를 운영하는 경우 StatefulSet, Headless Service, persistent volume, Pod DNS, announce 설정, 장애 조치 절차까지 함께 설계해야 합니다. 운영 부담을 줄이고 싶다면 관리형 Redis나 검증된 operator/Helm chart를 먼저 검토합니다.

## Redis 설치와 버전 확인

각 노드에 Redis server와 `redis-cli`를 설치합니다. 설치 방식은 OS와 회사 표준에 맞추되, 모든 노드의 Redis major/minor 버전을 맞추는 것을 우선합니다.

설치 후 각 노드에서 버전을 확인합니다.

```bash
redis-server --version
redis-cli --version
```

설치 경로도 확인합니다.

```bash
which redis-server
which redis-cli
```

운영에서는 배포 자동화 도구로 모든 노드에 동일한 Redis 패키지, 동일한 설정 템플릿, 동일한 systemd unit을 배포합니다. 손으로 한 대씩 수정하면 cluster 구성 이후 노드별 설정 차이가 숨어 있다가 장애 때 드러나기 쉽습니다.

## VM 기준 디렉터리 구조

노드 하나에 Redis 인스턴스 하나를 둔다면 아래처럼 단순하게 가져갑니다.

```text
/etc/redis/redis.conf
/etc/redis/users.acl
/var/lib/redis/
/var/log/redis/
```

한 서버에서 학습용으로 여러 인스턴스를 띄울 때는 포트별 디렉터리를 나눕니다.

```text
cluster-test/
├─ 7000/redis.conf
├─ 7001/redis.conf
├─ 7002/redis.conf
├─ 7003/redis.conf
├─ 7004/redis.conf
└─ 7005/redis.conf
```

운영에서는 한 서버에 여러 master를 몰아두는 구성을 피합니다.

## redis.conf 기본 예시

아래 설정은 각 Redis 노드에 들어가는 기본 예시입니다. 실제 운영에서는 `bind`, `cluster-announce-ip`, `maxmemory`, ACL, TLS, persistence를 환경에 맞게 조정합니다.

```conf
bind 0.0.0.0
protected-mode yes
port 6379

dir /var/lib/redis
logfile /var/log/redis/redis.log

cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 5000
cluster-require-full-coverage yes
cluster-allow-reads-when-down no

appendonly yes
appendfsync everysec

maxmemory 4gb
maxmemory-policy allkeys-lfu

aclfile /etc/redis/users.acl
```

중요한 설정:

- `cluster-enabled yes`: Redis Cluster 모드를 켭니다.
- `cluster-config-file nodes.conf`: Cluster 상태를 Redis가 자동으로 저장하는 파일입니다. 사람이 직접 수정하지 않습니다.
- `cluster-node-timeout 5000`: 노드를 장애로 판단하기까지의 시간입니다. 너무 짧으면 네트워크 흔들림에 민감하고, 너무 길면 failover가 늦습니다.
- `cluster-require-full-coverage yes`: 일부 hash slot이 사라지면 클러스터가 쓰기를 멈춥니다. 기본값을 유지하는 편이 안전합니다.
- `appendonly yes`: 장애 후 재시작 복구를 위해 AOF를 켭니다.
- `maxmemory`: 컨테이너나 VM 전체 메모리보다 낮게 잡아 OS와 Redis 내부 버퍼 여유를 둡니다.

컨테이너나 NAT 환경에서 Redis가 실제 접근 가능한 주소를 제대로 광고하지 못하면 아래 설정을 추가로 검토합니다.

```conf
cluster-announce-ip 10.0.1.11
cluster-announce-port 6379
cluster-announce-bus-port 16379
```

노드마다 `cluster-announce-ip`는 자기 노드의 실제 접근 가능한 IP 또는 DNS로 바꿔야 합니다.

## ACL 예시

초기 cluster 생성과 운영 명령은 관리자 권한이 필요합니다. 운영 앱 사용자는 key pattern과 command를 제한합니다.

```conf
user default off
user cluster-admin on >replace-with-admin-password ~* +@all
user app on >replace-with-app-password ~app:* +@read +@write +@connection +cluster|slots +cluster|nodes
```

실제 운영에서는 애플리케이션이 사용하는 명령을 확인하고 최소 권한으로 줄입니다. 비밀번호는 파일에 직접 커밋하지 않고 Secret Manager, Vault, Kubernetes Secret 같은 별도 시크릿 저장소에서 관리합니다.

## 6노드 Cluster 생성 절차

아래 예시는 6대의 노드가 있고, 각 노드가 `6379` 포트로 Redis를 실행한다고 가정합니다.

```text
redis-1.example.internal
redis-2.example.internal
redis-3.example.internal
redis-4.example.internal
redis-5.example.internal
redis-6.example.internal
```

각 노드에서 Redis를 시작합니다.

```bash
redis-server /etc/redis/redis.conf
```

systemd로 운영한다면 배포 방식에 맞게 서비스를 등록한 뒤 모든 노드를 실행합니다.

```bash
sudo systemctl start redis
sudo systemctl enable redis
```

모든 노드가 비어 있고 cluster mode로 떠 있는지 확인합니다.

```bash
redis-cli -h redis-1.example.internal -p 6379 CLUSTER INFO
```

아직 cluster를 만들기 전이면 `cluster_state:fail`이 보일 수 있습니다. 노드들이 비어 있고 cluster mode로 실행 중이면 다음 단계로 넘어갑니다.

Cluster를 생성합니다.

```bash
redis-cli --cluster create \
  redis-1.example.internal:6379 \
  redis-2.example.internal:6379 \
  redis-3.example.internal:6379 \
  redis-4.example.internal:6379 \
  redis-5.example.internal:6379 \
  redis-6.example.internal:6379 \
  --cluster-replicas 1
```

`redis-cli`가 master/replica 배치와 slot 할당 계획을 보여주면 내용을 확인하고 `yes`를 입력합니다. 성공하면 모든 16384개 hash slot이 커버된다는 메시지가 나옵니다.

ACL을 사용한다면 cluster 생성에 필요한 관리자 사용자로 접속합니다.

```bash
redis-cli --user cluster-admin -a replace-with-admin-password --cluster create \
  redis-1.example.internal:6379 \
  redis-2.example.internal:6379 \
  redis-3.example.internal:6379 \
  redis-4.example.internal:6379 \
  redis-5.example.internal:6379 \
  redis-6.example.internal:6379 \
  --cluster-replicas 1
```

명령어 히스토리에 비밀번호가 남지 않게 운영 환경에서는 환경 변수, 시크릿 주입, 제한된 터미널 사용 정책을 함께 적용합니다.

## 로컬 학습용 6인스턴스 구성

한 장비에서 동작을 이해하기 위한 학습용 예시입니다. 운영 구성으로 사용하지 않습니다.

```bash
mkdir -p cluster-test/{7000,7001,7002,7003,7004,7005}
```

각 디렉터리에 포트만 바꾼 `redis.conf`를 둡니다.

```conf
port 7000
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 5000
appendonly yes
dir ./
```

각 인스턴스를 별도 터미널에서 실행합니다.

```bash
cd cluster-test/7000
redis-server ./redis.conf
```

6개 인스턴스가 모두 뜨면 cluster를 생성합니다.

```bash
redis-cli --cluster create \
  127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 \
  127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 \
  --cluster-replicas 1
```

접속할 때는 `-c` 옵션을 사용합니다.

```bash
redis-cli -c -p 7000
```

## 설정 확인

Cluster 상태를 확인합니다.

```bash
redis-cli -c -h redis-1.example.internal -p 6379 CLUSTER INFO
redis-cli -c -h redis-1.example.internal -p 6379 CLUSTER NODES
redis-cli -c -h redis-1.example.internal -p 6379 CLUSTER SLOTS
```

전체 Cluster 검사는 `redis-cli --cluster check`를 사용합니다.

```bash
redis-cli --cluster check redis-1.example.internal:6379
```

간단한 읽기/쓰기 테스트:

```bash
redis-cli -c -h redis-1.example.internal -p 6379 SET app:{user:1}:name alice
redis-cli -c -h redis-1.example.internal -p 6379 GET app:{user:1}:name
```

`-c` 옵션이 없거나 cluster-aware client가 아니면 `MOVED` 응답을 직접 받게 됩니다.

## 애플리케이션 연결 기준

애플리케이션은 반드시 Redis Cluster를 지원하는 클라이언트를 사용합니다. 클라이언트는 보통 초기 seed node 목록을 받고, `CLUSTER SLOTS` 정보를 캐시해 key가 속한 master로 요청을 보냅니다.

연결 설정에 포함할 것:

- seed node를 2개 이상 등록합니다.
- 인증 사용자와 비밀번호를 설정합니다.
- TLS를 사용한다면 CA와 SNI 설정을 확인합니다.
- timeout, retry, backoff, reconnect 정책을 설정합니다.
- replica read를 사용할지 여부를 명시합니다.

multi-key 명령이 필요하면 hash tag를 사용해 같은 slot에 묶습니다.

```text
cart:{user:123}
profile:{user:123}
session:{user:123}
```

중괄호 안의 값이 같으면 같은 hash slot으로 계산됩니다.

## 장애 조치 테스트

운영 투입 전에는 테스트 환경에서 master 하나를 중지하고 failover가 동작하는지 확인합니다.

```bash
sudo systemctl stop redis
```

다른 노드에서 상태를 확인합니다.

```bash
redis-cli -c -h redis-2.example.internal -p 6379 CLUSTER NODES
redis-cli -c -h redis-2.example.internal -p 6379 CLUSTER INFO
```

확인할 것:

- 중지한 master의 replica가 master로 승격됐는지
- `cluster_state:ok`로 돌아오는지
- 애플리케이션 클라이언트가 새 slot map을 갱신하는지
- 장애 시간 동안 쓰기 실패나 retry가 어떻게 기록되는지

테스트가 끝나면 중지한 Redis를 다시 시작하고 replica로 재합류하는지 확인합니다.

```bash
sudo systemctl start redis
redis-cli -c -h redis-1.example.internal -p 6379 CLUSTER NODES
```

## 노드 추가와 resharding

새 master 후보를 추가합니다.

```bash
redis-cli --cluster add-node \
  redis-7.example.internal:6379 \
  redis-1.example.internal:6379
```

slot을 새 노드로 옮깁니다.

```bash
redis-cli --cluster reshard redis-1.example.internal:6379
```

새 replica를 특정 master에 붙일 때는 master node id를 확인한 뒤 `--cluster-master-id`를 사용합니다.

```bash
redis-cli -c -h redis-1.example.internal -p 6379 CLUSTER NODES

redis-cli --cluster add-node \
  redis-8.example.internal:6379 \
  redis-1.example.internal:6379 \
  --cluster-slave \
  --cluster-master-id <master-node-id>
```

## 노드 제거

master를 제거하려면 먼저 해당 master가 가진 slot을 다른 master로 옮겨 비워야 합니다. 그 뒤 node id를 확인하고 제거합니다.

```bash
redis-cli -c -h redis-1.example.internal -p 6379 CLUSTER NODES

redis-cli --cluster del-node \
  redis-1.example.internal:6379 \
  <node-id>
```

replica는 slot을 갖지 않으므로 master보다 제거 절차가 단순하지만, 제거 후 각 master에 replica가 충분히 남는지 확인해야 합니다.

## 자주 만나는 문제

### cluster_state:fail

원인 후보:

- 모든 hash slot이 커버되지 않았습니다.
- master 과반이 서로 통신하지 못합니다.
- cluster bus port가 방화벽에 막혔습니다.
- NAT 또는 포트 매핑 때문에 노드가 잘못된 주소를 광고합니다.

확인:

```bash
redis-cli -c CLUSTER INFO
redis-cli -c CLUSTER NODES
redis-cli --cluster check redis-1.example.internal:6379
```

### MOVED 응답이 애플리케이션에 노출됨

원인 후보:

- cluster-aware client를 사용하지 않습니다.
- 클라이언트의 cluster mode 옵션이 꺼져 있습니다.
- 프록시나 로드밸런서가 Redis Cluster redirect를 이해하지 못합니다.

해결:

- Redis Cluster 지원 클라이언트로 교체합니다.
- seed node 목록과 cluster mode 옵션을 확인합니다.
- Redis Cluster 앞에 일반 L4/L7 로드밸런서를 단순 배치하지 않습니다.

### CROSSSLOT 오류

서로 다른 hash slot의 key를 한 명령에서 사용했습니다.

해결:

- multi-key 명령이 필요한 key는 hash tag를 사용합니다.
- 데이터 모델을 key 단위 작업에 맞게 조정합니다.

예시:

```text
order:{1001}:items
payment:{1001}:status
```

### 새 노드가 추가되지 않음

원인 후보:

- 새 Redis에 기존 데이터가 남아 있습니다.
- `nodes.conf`가 이전 cluster 정보를 들고 있습니다.
- Redis가 cluster mode로 뜨지 않았습니다.

해결:

- 운영 데이터가 아닌지 확인한 뒤 빈 노드로 준비합니다.
- 테스트 환경에서는 `FLUSHALL`과 `nodes.conf` 삭제 후 재시작으로 초기화할 수 있습니다.
- 운영에서는 임의 삭제하지 말고 백업과 변경 절차를 먼저 확인합니다.

## 운영 전 최종 점검

- 모든 노드의 Redis 버전이 맞습니다.
- 모든 노드의 client port와 cluster bus port가 열려 있습니다.
- `CLUSTER INFO`가 `cluster_state:ok`입니다.
- `redis-cli --cluster check`가 모든 slot coverage를 확인합니다.
- `maxmemory`와 `maxmemory-policy`가 설정되어 있습니다.
- AOF/RDB, 백업, 복구 절차가 정리되어 있습니다.
- 클라이언트가 Redis Cluster를 지원하고 `MOVED`/`ASK` redirect를 처리합니다.
- master와 replica가 서로 다른 장애 도메인에 배치되어 있습니다.
- 모니터링에서 memory, evicted keys, connected clients, rejected connections, replication delay, cluster state를 봅니다.

## 참고한 공식 문서

- [Scale with Redis Cluster](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/)
- [Redis cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)
- [Redis configuration](https://redis.io/docs/latest/operate/oss_and_stack/management/config/)
