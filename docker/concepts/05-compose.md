# Docker Compose

## 학습 목표

- 여러 컨테이너의 원하는 상태를 하나의 Compose 파일로 선언합니다.
- 서비스명 DNS, 포트 공개, volume 수명을 구분합니다.
- 시작 순서와 애플리케이션 준비 상태가 다르다는 점을 이해합니다.

## 핵심 개념

Docker Compose는 서비스, 네트워크, 볼륨을 YAML로 정의하고 하나의 프로젝트 단위로 관리합니다.

```yaml
services:
  web:
    image: nginx:alpine
    ports:
      - "127.0.0.1:8080:80"
```

Compose는 기본 네트워크를 만들고 같은 프로젝트의 서비스명을 DNS 이름으로 제공합니다. 컨테이너 간에는 컨테이너 포트를 사용하고, 호스트에서 접근할 때만 공개된 호스트 포트를 사용합니다.

```text
host -> 127.0.0.1:8080 -> web container:80
app container -> http://web:80
```

## 준비 상태를 기다리는 의존성

짧은 `depends_on`은 컨테이너 시작 순서만 제어하며 데이터베이스가 요청을 받을 준비까지 기다리지 않습니다. 준비 상태가 중요하다면 healthcheck와 `service_healthy` 조건을 사용합니다.

```yaml
services:
  app:
    image: my-app
    depends_on:
      redis:
        condition: service_healthy

  redis:
    image: redis:8-alpine
    healthcheck:
      test: ["CMD", "redis-cli", "PING"]
      interval: 5s
      timeout: 3s
      retries: 5
```

애플리케이션에도 연결 재시도와 제한 시간을 구현해야 합니다. healthcheck 하나가 일시적인 네트워크 장애를 없애지는 않습니다.

## 자주 쓰는 명령어

```bash
docker compose config -q
docker compose up -d
docker compose ps -a
docker compose logs -f --tail 100
docker compose exec <service> sh
docker compose stop
docker compose down
```

- `config -q`는 병합과 변수 치환이 끝난 구성이 유효한지 확인합니다.
- `stop`은 컨테이너를 중지하지만 프로젝트의 컨테이너와 네트워크를 남깁니다.
- `down`은 Compose가 만든 컨테이너와 기본 네트워크를 제거합니다.
- `down -v`는 선언된 named volume과 anonymous volume까지 제거하므로 데이터 삭제 의도를 확인한 뒤 사용합니다.

## 설정값과 시크릿

```yaml
services:
  app:
    environment:
      API_BASE_URL: http://api:8080
      APP_MODE: ${APP_MODE:-local}
```

`docker compose config` 출력에는 치환된 값이 보일 수 있습니다. 비밀번호를 Compose 파일이나 Git에 저장하지 말고 시크릿 관리 기능이나 배포 환경에서 주입합니다. 필수 변수는 `${PASSWORD:?PASSWORD is required}`처럼 누락 시 즉시 실패하게 만들 수 있습니다.

## 실습

[Compose 기본 실습](../examples/compose-basic/README.md)에서 Nginx, Redis, one-shot checker를 실행해 다음을 확인합니다.

1. 서비스명으로 Redis에 연결되는지 확인합니다.
2. healthcheck를 통과한 뒤 의존 서비스가 시작되는지 확인합니다.
3. 컨테이너 재시작 후 named volume 데이터가 남는지 확인합니다.
4. `down`과 `down -v`의 결과를 비교합니다.

## 확인 질문

1. `ports` 없이도 `app`이 `redis:6379`에 접근할 수 있는 이유는 무엇인가요?
2. `depends_on: [redis]`만으로 준비 완료를 보장할 수 없는 이유는 무엇인가요?
3. `docker compose down -v`를 운영 데이터가 있는 환경에서 주의해야 하는 이유는 무엇인가요?

## 참고한 공식 문서

- [Compose networking](https://docs.docker.com/compose/how-tos/networking/)
- [Control startup order](https://docs.docker.com/compose/how-tos/startup-order/)
- [docker compose down](https://docs.docker.com/reference/cli/docker/compose/down/)
