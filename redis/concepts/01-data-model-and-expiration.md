# Redis 데이터 모델과 만료

## 학습 목표

- Redis를 단순한 문자열 캐시가 아니라 자료구조 서버로 이해합니다.
- 데이터 성격에 맞는 자료구조와 키 이름을 선택합니다.
- TTL, 원자적 명령, keyspace 순회 시 주의점을 설명합니다.

## Redis를 먼저 어떻게 사용할지 정한다

Redis는 메모리를 중심으로 동작하는 key-value 자료구조 서버입니다. 빠른 조회가 장점이지만 사용 목적에 따라 허용 가능한 데이터 유실, eviction, persistence, 고가용성 구성이 달라집니다.

대표 용도는 다음과 같습니다.

- 원본 DB 결과를 잠시 저장하는 캐시
- 만료가 필요한 세션과 임시 토큰
- 원자적 카운터와 rate limit
- 순위표, 작업 대기열, Pub/Sub

강한 트랜잭션 일관성, 복잡한 관계 질의, 장기 보존이 핵심이라면 RDBMS나 전용 메시지 브로커가 더 적합할 수 있습니다. Redis를 원본 데이터 저장소로 쓸 때는 persistence와 복구 목표를 먼저 검증합니다.

## 주요 자료구조 선택

| 자료구조 | 대표 명령 | 적합한 예 | 주의점 |
| --- | --- | --- | --- |
| String | `SET`, `GET`, `INCR` | 캐시 값, 토큰, 카운터 | 한 값에 큰 객체를 계속 덮어쓰지 않기 |
| Hash | `HSET`, `HGET`, `HINCRBY` | 사용자 속성, 작은 객체 | field 수가 무제한으로 커지지 않게 관리 |
| List | `LPUSH`, `RPOP`, `BLPOP` | 단순 FIFO/LIFO 작업 | 재처리·확인이 중요하면 Streams 검토 |
| Set | `SADD`, `SISMEMBER` | 중복 없는 태그, 권한 | 전체 `SMEMBERS`는 큰 집합에서 부담 |
| Sorted Set | `ZADD`, `ZRANGE` | 순위표, 시간 기반 정렬 | score가 `double`임을 고려 |
| Stream | `XADD`, `XREADGROUP`, `XACK` | 소비자 그룹이 있는 이벤트 처리 | retention과 pending 메시지 운영 필요 |

자료구조가 제공하는 원자적 명령을 우선 사용하면 애플리케이션의 읽기-수정-쓰기 경쟁 조건을 줄일 수 있습니다.

```text
나쁜 흐름: GET counter -> 애플리케이션에서 +1 -> SET counter
권장 흐름: INCR counter
```

Redis 명령 하나는 원자적으로 처리되지만 여러 명령의 묶음이 자동으로 원자적인 것은 아닙니다. 조건부 쓰기에는 `SET ... NX`, 여러 단계에는 transaction이나 Lua/Functions가 필요한지 검토합니다.

## 키 이름 설계

Redis에는 내장 namespace가 없으므로 콜론으로 계층을 표현하는 관례를 사용합니다.

```text
<service>:<environment>:<entity>:<id>[:<attribute>]

shop:prod:user:42:session
shop:prod:product:1001
shop:prod:rate-limit:login:203.0.113.10
```

- 지나치게 긴 키는 메모리를 소비하지만 `u:42`처럼 의미가 없는 축약도 운영을 어렵게 합니다.
- 개인정보나 시크릿을 키 이름에 직접 넣지 않습니다. 키 이름은 로그와 진단 결과에 노출될 수 있습니다.
- Cluster에서 여러 키를 한 명령으로 다뤄야 한다면 hash tag 설계를 별도로 검토합니다.
- 하나의 키나 collection이 무제한으로 커지는 big key가 되지 않도록 상한과 분할 기준을 정합니다.

## TTL과 만료

캐시와 임시 데이터는 생성 시점에 TTL을 함께 설정해 만료 누락 구간을 줄입니다.

```bash
SET session:42 '{"userId":42}' EX 1800
TTL session:42
GET session:42
```

`TTL` 결과는 초 단위 남은 시간이며 `-1`은 키가 있지만 만료가 없고, `-2`는 키가 없음을 뜻합니다.

기존 키를 일반 `SET`으로 덮어쓰면 TTL이 제거될 수 있습니다. TTL을 의도적으로 유지할 때만 `KEEPTTL`을 사용합니다.

```bash
SET session:42 '{"userId":42,"role":"admin"}' KEEPTTL
```

TTL에 임의 오차를 더하면 많은 키가 같은 순간에 만료되어 원본 DB로 요청이 몰리는 cache stampede를 줄이는 데 도움이 됩니다.

## 기본 실습

먼저 [로컬 캐시 실습](../examples/basic-cache/README.md)을 실행한 뒤 Redis CLI에서 확인합니다.

```bash
SET page:home '<html>...</html>' EX 60
GET page:home
TTL page:home

HSET user:42 name galaxy role admin
HGETALL user:42

ZADD leaderboard 120 alice 95 bob
ZREVRANGE leaderboard 0 -1 WITHSCORES

INCR page:home:views
```

키를 찾을 때는 전체 서버를 오래 막을 수 있는 `KEYS *` 대신 cursor 기반 `SCAN`을 사용합니다.

```bash
SCAN 0 MATCH 'user:*' COUNT 100
```

`COUNT`는 정확한 반환 개수가 아니라 힌트입니다. cursor가 다시 `0`이 될 때까지 반복해야 전체 순회가 끝나며, 순회 중 데이터가 바뀌면 중복이 나올 수 있습니다.

## 확인 질문

1. 사용자 조회 결과 캐시와 순위표에 각각 어떤 자료구조가 적합한가요?
2. `SET key value`를 다시 실행했을 때 기존 TTL을 확인해야 하는 이유는 무엇인가요?
3. 애플리케이션에서 `GET` 후 값을 증가시켜 `SET`하는 것보다 `INCR`이 안전한 이유는 무엇인가요?
4. 운영 환경에서 `KEYS *` 대신 `SCAN`을 사용하는 이유는 무엇인가요?

## 참고한 공식 문서

- [Redis data types](https://redis.io/docs/latest/develop/data-types/)
- [Keys and values](https://redis.io/docs/latest/develop/using-commands/keyspace/)
- [EXPIRE](https://redis.io/docs/latest/commands/expire/)
- [SCAN](https://redis.io/docs/latest/commands/scan/)
