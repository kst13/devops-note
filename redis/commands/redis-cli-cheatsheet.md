# Redis CLI 운영 치트시트

## 접속

```bash
redis-cli -h <host> -p 6379 --tls --user <user>
REDISCLI_AUTH='<password>' redis-cli -h <host> -p 6379 PING
```

비밀번호를 `-a` 인자나 셸 기록에 남기지 않습니다. 가능하면 시크릿 관리 도구에서 환경 변수나 클라이언트 설정으로 주입합니다.

## 키와 TTL

```bash
TYPE <key>
GET <key>
TTL <key>
PTTL <key>
EXPIRE <key> 300
UNLINK <key>
```

큰 키의 `DEL`은 메모리 해제를 동기 처리해 지연을 만들 수 있으므로 비동기 해제인 `UNLINK`를 검토합니다.

운영 keyspace는 `KEYS *` 대신 cursor 기반으로 순회합니다.

```bash
redis-cli --scan --pattern 'app:*' --count 100
SCAN 0 MATCH 'app:*' COUNT 100
```

`SCAN`은 중복을 반환할 수 있으며 `COUNT`는 정확한 개수가 아닙니다.

## 자료구조 확인

```bash
HGETALL <hash>
HLEN <hash>
LLEN <list>
SCARD <set>
ZCARD <sorted-set>
XLEN <stream>
```

큰 collection에서 전체 원소를 한 번에 반환하는 명령은 피하고 `HSCAN`, `SSCAN`, `ZSCAN`, 범위 조회를 사용합니다.

## 서버 상태

```bash
redis-cli INFO server clients memory stats persistence replication
redis-cli INFO commandstats latencystats
redis-cli DBSIZE
redis-cli MEMORY STATS
redis-cli MEMORY DOCTOR
redis-cli MEMORY USAGE <key> SAMPLES 5
```

## 느린 명령과 지연 시간

```bash
redis-cli SLOWLOG LEN
redis-cli SLOWLOG GET 20
redis-cli LATENCY LATEST
redis-cli LATENCY DOCTOR
```

Slow Log는 서버에서 명령을 실행한 시간만 기록하며 네트워크 왕복 시간은 포함하지 않습니다. `MONITOR`는 모든 명령을 스트리밍해 부하와 정보 노출을 만들 수 있으므로 운영에서 상시 사용하지 않습니다.

## 메모리 조사

```bash
redis-cli --bigkeys -i 0.1
redis-cli --memkeys -i 0.1
redis-cli INFO memory
redis-cli INFO stats
```

`--bigkeys`와 `--memkeys`는 `SCAN`을 사용하지만 전체 keyspace를 읽습니다. 트래픽이 낮을 때 간격을 두고 실행하고 애플리케이션 지연을 관찰합니다.

## persistence

```bash
redis-cli INFO persistence
redis-cli LASTSAVE
redis-cli BGSAVE
redis-cli BGREWRITEAOF
```

`BGSAVE`와 `BGREWRITEAOF`는 메모리와 디스크에 영향을 줍니다. 운영에서는 상태와 여유 자원을 확인한 뒤 실행합니다.

## 복제, Sentinel, Cluster

```bash
redis-cli INFO replication

redis-cli -p 26379 SENTINEL CKQUORUM mymaster
redis-cli -p 26379 SENTINEL masters
redis-cli -p 26379 SENTINEL replicas mymaster

redis-cli -c CLUSTER INFO
redis-cli -c CLUSTER NODES
redis-cli -c CLUSTER SHARDS
redis-cli -c CLUSTER KEYSLOT 'cart:{user:42}'
```

## 위험 명령

다음 명령은 데이터 삭제, 장시간 block, 설정 불일치를 만들 수 있습니다.

```text
FLUSHALL / FLUSHDB
KEYS *
CONFIG SET / CONFIG REWRITE
DEBUG / SHUTDOWN
MONITOR
```

운영 계정 ACL에서 불필요한 위험 명령을 제거하고, 실행 전 대상 인스턴스와 복구 계획을 확인합니다.

## 참고한 공식 문서

- [Redis CLI](https://redis.io/docs/latest/develop/tools/cli/)
- [SCAN](https://redis.io/docs/latest/commands/scan/)
- [Redis latency monitoring](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/latency-monitor/)
