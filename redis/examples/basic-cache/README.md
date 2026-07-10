# Redis 로컬 캐시 실습

로컬 전용 Redis를 실행하고 TTL, 자료구조, 원자적 카운터, 메모리 정책을 확인합니다.

## 실행

이 디렉터리에서 실행합니다.

```bash
docker compose config -q
docker compose up -d
docker compose ps
docker compose exec redis redis-cli PING
```

`PONG`과 `healthy` 상태가 보이면 준비됐습니다. 호스트 포트는 `127.0.0.1`에만 바인딩되어 외부 인터페이스로 공개하지 않습니다.

## TTL 캐시

```bash
docker compose exec redis redis-cli SET product:1001 '{"name":"keyboard"}' EX 60
docker compose exec redis redis-cli GET product:1001
docker compose exec redis redis-cli TTL product:1001
```

60초 안에는 값과 남은 TTL이 보이고 시간이 지나면 `GET` 결과가 nil이 됩니다.

## 자료구조와 원자적 명령

```bash
docker compose exec redis redis-cli HSET user:42 name galaxy role admin
docker compose exec redis redis-cli HGETALL user:42

docker compose exec redis redis-cli ZADD leaderboard 120 alice 95 bob
docker compose exec redis redis-cli ZREVRANGE leaderboard 0 -1 WITHSCORES

docker compose exec redis redis-cli INCR page:home:views
docker compose exec redis redis-cli INCR page:home:views
docker compose exec redis redis-cli GET page:home:views
```

마지막 카운터 값은 `2`가 됩니다. `INCR` 하나로 읽기와 증가를 처리하므로 여러 클라이언트가 동시에 실행해도 증가 연산 자체는 원자적입니다.

## keyspace와 메모리 확인

```bash
docker compose exec redis redis-cli SCAN 0 MATCH 'user:*' COUNT 100
docker compose exec redis redis-cli INFO memory
docker compose exec redis redis-cli INFO stats
docker compose exec redis redis-cli CONFIG GET maxmemory maxmemory-policy
```

이 예제는 순수 캐시를 가정해 persistence를 끄고 `allkeys-lfu`를 사용합니다. 컨테이너가 새로 만들어지면 데이터가 사라지는 것이 정상입니다.

## 재생성 확인

```bash
docker compose down
docker compose up -d
docker compose exec redis redis-cli DBSIZE
```

`DBSIZE`가 0이면 persistence 없는 캐시의 수명을 확인한 것입니다. 원본 데이터가 없거나 재생성 비용이 너무 크다면 이 구성을 사용하면 안 됩니다.

## 정리

```bash
docker compose down
```

## 확인 질문

1. 캐시 키를 만들 때 TTL을 같은 명령에 넣는 이유는 무엇인가요?
2. 이 Compose 구성이 세션이나 작업 큐에 부적합할 수 있는 이유는 무엇인가요?
3. `maxmemory`보다 컨테이너 메모리 제한을 더 크게 잡아야 하는 이유는 무엇인가요?
