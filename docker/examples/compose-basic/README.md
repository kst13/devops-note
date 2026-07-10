# Compose 기본 실습

Nginx와 Redis를 함께 실행하고 서비스 상태, 기본 네트워크, named volume을 확인합니다.

## 설정 검증

이 디렉터리에서 실행합니다.

```bash
docker compose config -q
docker compose config
```

첫 명령이 출력 없이 종료 코드 0을 반환하면 문법이 유효합니다.

## 실행과 확인

```bash
docker compose up -d
docker compose ps -a
curl http://127.0.0.1:8080
docker compose logs checker
docker compose exec redis redis-cli PING
```

- `web`과 `redis`가 `healthy` 상태가 됩니다.
- `checker` 로그에는 서비스명 `redis`로 접속한 결과인 `PONG`이 보입니다.
- `checker`는 작업을 마친 one-shot 컨테이너이므로 종료 코드 0인 `Exited` 상태가 정상입니다.

Compose는 이 프로젝트의 서비스가 공유하는 기본 네트워크를 만들고 서비스명을 DNS 이름으로 제공합니다. 호스트에 공개하지 않은 Redis 6379 포트도 같은 네트워크의 `checker`에서는 `redis:6379`로 접근할 수 있습니다.

## 데이터 수명 확인

```bash
docker compose exec redis redis-cli SET lesson compose
docker compose restart redis
docker compose exec redis redis-cli GET lesson
docker volume ls --filter name=compose-basic
```

Redis 데이터를 named volume에 저장하므로 컨테이너를 다시 만들어도 값이 남습니다.

## 정리

컨테이너와 네트워크만 제거하고 데이터를 남기려면 다음을 실행합니다.

```bash
docker compose down
```

학습 데이터를 포함한 named volume까지 제거할 때만 `-v`를 사용합니다.

```bash
docker compose down -v
```

`down -v`는 복구할 데이터가 없는지 확인한 뒤 실행해야 합니다.
