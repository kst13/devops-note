# Redis 운영 설정 시작하기

2026-06-08 기준으로, 운영에서 많이 쓰는 Redis 설정 방식은 "기본값으로 띄우기"가 아니라 목적별 `redis.conf`를 명시하고, 네트워크와 인증을 닫고, 메모리 상한과 persistence 방식을 먼저 정하는 흐름입니다.

## 먼저 결정할 것

Redis 설정은 아래 네 가지를 먼저 정한 뒤 작성합니다.

1. 이 Redis가 캐시인지, 세션/락/큐처럼 유실에 민감한 저장소인지 정합니다.
2. 직접 운영할지, 클라우드 관리형 Redis 또는 Redis 호환 서비스를 쓸지 정합니다.
3. 단일 인스턴스, primary-replica + Sentinel, Redis Cluster 중 하나를 정합니다.
4. 메모리 한도, eviction 정책, persistence, 백업, 모니터링 기준을 정합니다.

운영에서는 가능하면 관리형 서비스를 먼저 검토합니다. 패치, 장애 조치, 백업, 모니터링, TLS, 접근 제어를 직접 운영하지 않아도 되기 때문입니다. 직접 운영해야 한다면 Docker든 VM이든 Kubernetes든 `redis.conf`와 데이터 디렉터리를 명시적으로 관리합니다.

## 현재 많이 쓰는 설정 방식

### 1. 설정 파일을 명시한다

Redis는 설정 파일 없이도 실행되지만, 이 방식은 테스트와 개발용으로 보는 것이 안전합니다. 운영에서는 `redis.conf`를 준비하고 다음처럼 실행합니다.

```bash
redis-server /etc/redis/redis.conf
```

Docker에서는 공식 이미지를 사용하되 설정 파일과 데이터 디렉터리를 마운트합니다.

```bash
docker run --name redis \
  -v ./conf:/usr/local/etc/redis \
  -v redis-data:/data \
  redis:<version> \
  redis-server /usr/local/etc/redis/redis.conf
```

Redis 8부터는 서버 설정만 담은 `redis.conf`와 Search, JSON, time series, probabilistic data structures 같은 구성 요소까지 포함하는 `redis-full.conf`를 구분합니다. 일반 캐시나 세션 저장소라면 먼저 `redis.conf`로 시작하고, Redis Stack 계열 기능이 필요할 때만 `redis-full.conf`를 검토합니다.

### 2. 외부 노출을 막고 인증을 켠다

Redis는 애플리케이션 내부망에서만 접근하도록 둡니다. 퍼블릭 인터넷에 `6379`를 열지 않고, 방화벽과 보안 그룹으로 접근 가능한 클라이언트를 제한합니다.

```conf
bind 127.0.0.1 10.0.1.10
protected-mode yes
port 6379
```

Redis 6 이상에서는 단순 `requirepass`보다 ACL을 기준으로 사용자와 권한을 분리하는 구성이 좋습니다.

```conf
aclfile /etc/redis/users.acl
```

```conf
user default off
user app on >replace-with-long-random-password ~app:* +@read +@write +@connection -@dangerous
user ops on >replace-with-another-long-random-password ~* +@all
```

네트워크를 신뢰할 수 없거나 클러스터, 복제, 클라이언트 연결이 호스트 밖으로 나간다면 TLS도 함께 검토합니다. 인증 정보는 평문 네트워크에서 보호되지 않는다고 보고, 시크릿 관리 도구나 클라우드 Secret Manager에 둡니다.

### 3. 메모리 한도를 반드시 둔다

Redis는 메모리 기반 저장소이므로 운영에서는 `maxmemory`를 명시합니다. 컨테이너나 VM의 전체 메모리와 같게 잡지 말고, 복제 버퍼, AOF 버퍼, OS 여유 메모리를 남깁니다.

```conf
maxmemory 1gb
maxmemory-policy allkeys-lfu
```

정책 선택 기준은 다음처럼 잡습니다.

- 일반 캐시: `allkeys-lfu` 또는 `allkeys-lru`
- TTL이 있는 키만 지우고 싶을 때: `volatile-ttl` 또는 `volatile-lru`
- 세션, 락, 큐처럼 임의 삭제가 위험할 때: `noeviction`

`noeviction`은 메모리가 꽉 찼을 때 쓰기 요청이 실패할 수 있으므로 애플리케이션에서 에러 처리를 준비해야 합니다.

## persistence 선택

Redis를 순수 캐시로만 쓰고 원본 데이터가 다른 DB에 있다면 persistence를 끄거나 최소화할 수 있습니다.

```conf
appendonly no
save ""
```

재시작 후 빠른 복구나 일정 수준의 데이터 보존이 필요하면 RDB snapshot 또는 AOF를 켭니다. 운영에서 많이 쓰는 기본 선택지는 AOF `everysec`입니다. 성능과 안정성의 균형이 좋지만, 장애 시 최대 약 1초의 쓰기 유실 가능성을 받아들이는 설정입니다.

```conf
appendonly yes
appendfsync everysec

save 900 1
save 300 10
save 60 10000

dir /data
```

데이터 유실이 거의 허용되지 않는 업무라면 Redis 단독 저장을 다시 검토합니다. Redis 복제와 Cluster는 기본적으로 비동기 복제 특성이 있으므로, 강한 내구성이 필요한 데이터는 RDBMS, 로그 기반 큐, 관리형 Redis의 durability 옵션 등을 함께 비교합니다.

## 고가용성 선택

단일 Redis는 로컬 개발, 학습, 장애 영향이 작은 캐시에 적합합니다.

운영에서 단일 primary 구조를 유지하되 자동 장애 조치가 필요하면 primary-replica 복제와 Sentinel을 검토합니다. Sentinel은 비샤딩 Redis의 고가용성을 제공하며, 견고한 배포에는 최소 3개 Sentinel 인스턴스와 Sentinel을 지원하는 클라이언트가 필요합니다.

수평 확장과 샤딩이 필요하면 Redis Cluster를 사용합니다. 공식 문서는 최소 3개 master가 필요하고, 배포에는 3 master + 3 replica 구성을 강하게 권장합니다. Cluster는 NAT나 포트 매핑 환경에서 주의가 필요하므로 Docker 포트 매핑이나 Kubernetes Service 설계까지 함께 확인합니다.

## 운영 기준 예시

아래 설정은 그대로 복사하기보다 용도에 맞게 조정하는 출발점입니다.

```conf
bind 127.0.0.1 10.0.1.10
protected-mode yes
port 6379

aclfile /etc/redis/users.acl

maxmemory 1gb
maxmemory-policy allkeys-lfu

appendonly yes
appendfsync everysec
save 900 1
save 300 10
save 60 10000
dir /data

timeout 0
tcp-keepalive 300

slowlog-log-slower-than 10000
slowlog-max-len 128
```

## 확인 명령어

설정이 의도대로 적용됐는지 `redis-cli`로 확인합니다.

```bash
redis-cli INFO server
redis-cli INFO memory
redis-cli INFO persistence
redis-cli CONFIG GET maxmemory maxmemory-policy appendonly appendfsync
redis-cli ACL LIST
redis-cli SLOWLOG LEN
```

메모리 문제를 볼 때는 아래 명령어도 자주 사용합니다.

```bash
redis-cli --bigkeys
redis-cli --memkeys
redis-cli INFO stats
```

## 참고한 공식 문서

- [Redis configuration](https://redis.io/docs/latest/operate/oss_and_stack/management/config/)
- [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- [Redis security](https://redis.io/docs/latest/operate/oss_and_stack/management/security/)
- [Redis ACL](https://redis.io/docs/latest/operate/oss_and_stack/management/security/acl/)
- [Redis key eviction](https://redis.io/docs/latest/develop/reference/eviction/)
- [Run Redis Open Source on Docker](https://redis.io/docs/latest/operate/oss_and_stack/install/install-stack/docker/)
- [High availability with Redis Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/)
- [Scale with Redis Cluster](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/)
- [Redis Official Image on Docker Hub](https://hub.docker.com/_/redis/)
