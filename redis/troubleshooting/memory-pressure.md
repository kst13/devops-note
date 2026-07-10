# Redis memory pressure와 OOM

## 증상

- 쓰기 요청이 `OOM command not allowed when used memory > 'maxmemory'`로 실패합니다.
- `evicted_keys`가 빠르게 증가하고 cache hit ratio가 떨어집니다.
- 지연 시간이 증가하거나 Redis/호스트 프로세스가 OOM kill 됩니다.
- `used_memory_rss`가 `used_memory`보다 크게 보이거나 persistence 작업 중 메모리가 급증합니다.

## 먼저 할 일

데이터를 지우거나 서버를 재시작하기 전에 상태를 보존합니다.

```bash
redis-cli INFO memory
redis-cli INFO stats
redis-cli INFO persistence
redis-cli INFO replication
redis-cli MEMORY STATS
redis-cli CONFIG GET maxmemory maxmemory-policy
```

운영 장애 중 원인을 모른 채 `FLUSHALL`, 큰 범위 `DEL`, persistence 비활성화를 실행하지 않습니다.

## 핵심 지표 해석

| 지표 | 확인할 내용 |
| --- | --- |
| `used_memory` | Redis allocator가 데이터와 내부 구조에 사용한 메모리 |
| `used_memory_rss` | OS가 Redis 프로세스에 할당한 실제 resident memory |
| `maxmemory` | eviction 또는 write 거부를 판단하는 설정값 |
| `mem_not_counted_for_evict` | 복제/AOF 버퍼처럼 eviction 계산에서 제외된 메모리 |
| `mem_fragmentation_ratio` | RSS와 allocator 메모리 차이의 신호, 단독 판단 금지 |
| `evicted_keys` | 메모리 정책으로 제거된 누적 키 수 |
| `expired_keys` | TTL로 만료된 누적 키 수 |
| `keyspace_hits/misses` | 캐시 적중과 실패 추세 |

`maxmemory`는 프로세스 전체 RSS의 완전한 상한이 아닙니다. 복제/AOF 버퍼, allocator fragmentation, `fork()` 시 copy-on-write 메모리와 OS 메모리까지 여유를 둡니다.

## 자주 보는 원인

- `maxmemory`가 없거나 컨테이너/VM 전체 메모리에 너무 가깝습니다.
- 캐시 키에 TTL이 빠졌거나 eviction 정책이 용도와 맞지 않습니다.
- 하나의 큰 String 또는 element가 많은 Hash, Set, Sorted Set, Stream이 생겼습니다.
- 캐시 데이터와 임의 삭제가 불가능한 세션·큐 데이터를 한 인스턴스에 섞었습니다.
- RDB/AOF rewrite 중 쓰기가 많아 copy-on-write 메모리가 증가했습니다.
- replica/AOF backlog나 느린 replica 때문에 버퍼가 커졌습니다.
- 호스트가 swap을 사용하거나 다른 프로세스와 메모리를 경쟁합니다.

`volatile-*` 정책은 TTL이 설정된 키만 후보로 사용합니다. TTL 키가 없다면 메모리를 비울 수 없어 사실상 `noeviction`처럼 쓰기가 실패할 수 있습니다.

## 큰 키 찾기

```bash
redis-cli --bigkeys -i 0.1
redis-cli --memkeys -i 0.1
redis-cli MEMORY USAGE <suspected-key> SAMPLES 5
redis-cli TYPE <suspected-key>
```

이 명령은 `SCAN`을 사용하지만 전체 keyspace를 조사합니다. 트래픽이 낮을 때 간격을 두고 실행하고 지연 시간을 관찰합니다.

## 해결 방법

### 순수 캐시

- 원본에서 재생성 가능한지 확인한 뒤 `allkeys-lfu` 또는 `allkeys-lru`를 검토합니다.
- cache hit ratio와 eviction rate가 함께 나쁘면 메모리를 늘리거나 캐시할 대상을 줄입니다.
- TTL에 임의 오차를 주고 cache stampede 방지 전략을 적용합니다.

### 세션, 큐, 락, 상태 저장

- 임의 eviction이 위험하면 `noeviction`을 사용하고 쓰기 실패를 애플리케이션에서 처리합니다.
- 캐시와 별도 인스턴스로 분리해 서로 다른 정책을 적용합니다.
- persistence, 백업, replica 상태와 데이터 유실 허용 범위를 다시 확인합니다.

### big key

- collection을 시간, 사용자, shard 단위로 나누고 최대 element 수를 정합니다.
- 큰 키 삭제는 `UNLINK`나 점진적 삭제를 검토하고 지연 시간을 측정합니다.
- 값 직렬화 형식과 중복 데이터를 줄이되 CPU 비용도 함께 측정합니다.

## 해결 후 검증

```bash
redis-cli INFO memory
redis-cli INFO stats
redis-cli SLOWLOG GET 20
redis-cli LATENCY DOCTOR
```

- 쓰기 오류가 멈췄는지 확인합니다.
- `evicted_keys` 증가율과 애플리케이션 지연 시간이 안정됐는지 확인합니다.
- 재시작이나 persistence 작업 중에도 호스트 메모리 여유가 유지되는지 부하 테스트합니다.
- 원인, 임계값, 조치와 재발 방지 항목을 기록합니다.

## 참고한 공식 문서

- [Key eviction](https://redis.io/docs/latest/develop/reference/eviction/)
- [Memory optimization](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/memory-optimization/)
- [Diagnosing latency issues](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/latency/)
