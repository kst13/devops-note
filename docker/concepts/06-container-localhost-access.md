# 컨테이너에서 localhost로 다른 서비스 접근하기

## 학습 목표

- 컨테이너 안의 `localhost`가 가리키는 대상을 설명합니다.
- 호스트, 같은 Docker 네트워크의 서비스, 공유 네트워크 네임스페이스에 맞는 주소를 선택합니다.
- host network의 편의성과 격리·이식성 비용을 함께 판단합니다.

## 핵심 개념

컨테이너 안에서 `localhost`는 호스트 서버가 아니라 컨테이너 자기 자신입니다.

예를 들어 컨테이너 내부 애플리케이션이 `http://localhost:8080`으로 요청하면, 기본적으로는 호스트 서버의 8080 포트나 다른 컨테이너의 8080 포트가 아니라 현재 컨테이너 내부의 8080 포트로 요청합니다.

그래서 컨테이너에서 외부 대상에 접근할 때는 대상에 따라 접근 방법을 다르게 선택해야 합니다.

## 상황별 접근 방법

| 대상 | 권장 접근 방법 | 예시 |
| --- | --- | --- |
| 호스트 서버에서 실행 중인 서비스 | `host.docker.internal` 또는 host network | `http://host.docker.internal:8080` |
| 다른 Docker 컨테이너 | 사용자 정의 Docker 네트워크 + 컨테이너명 또는 Compose 서비스명 | `http://api:8080` |
| 반드시 `localhost:<port>`를 유지해야 하는 경우 | host network 또는 네트워크 네임스페이스 공유 | `network_mode: host`, `network_mode: "service:api"` |

## 호스트 서버의 서비스로 요청하기

호스트 서버에서 실행 중인 서비스에 컨테이너가 접근해야 한다면 `localhost` 대신 `host.docker.internal`을 사용합니다.

```bash
curl http://host.docker.internal:8080
```

Linux 서버에서는 Docker 실행 시 `host.docker.internal`을 직접 추가해야 하는 경우가 많습니다.

```bash
docker run \
  --add-host=host.docker.internal:host-gateway \
  <image>
```

Docker Compose에서는 다음처럼 설정합니다.

```yaml
services:
  app:
    image: my-app
    extra_hosts:
      - "host.docker.internal:host-gateway"
```

이후 컨테이너 내부에서는 다음 주소로 호스트 서버의 서비스에 접근합니다.

```bash
curl http://host.docker.internal:8080
```

주의할 점은 호스트의 서비스가 `127.0.0.1`에만 바인딩되어 있으면 컨테이너에서 접근하지 못할 수 있다는 것입니다. 이 경우 서비스가 `0.0.0.0` 또는 Docker 브리지 게이트웨이 주소에서 요청을 받을 수 있도록 바인딩 설정을 확인해야 합니다.

## 다른 컨테이너로 요청하기

다른 컨테이너에 접근할 때는 `localhost`를 사용하지 않는 것이 일반적입니다. 두 컨테이너를 같은 사용자 정의 네트워크에 넣고, 컨테이너명이나 Compose 서비스명을 호스트명처럼 사용합니다.

```bash
docker network create app-network

docker run -d \
  --name api \
  --network app-network \
  my-api

docker run --rm \
  --network app-network \
  curlimages/curl \
  http://api:8080
```

Compose에서는 같은 `services` 아래에 있는 서비스끼리 서비스명으로 접근할 수 있습니다.

```yaml
services:
  app:
    image: my-app
    depends_on:
      - api
    environment:
      API_BASE_URL: http://api:8080

  api:
    image: my-api
    expose:
      - "8080"
```

`expose`는 컨테이너가 사용하는 포트를 설명하지만 같은 네트워크의 접근을 허용하거나 차단하는 보안 규칙은 아닙니다. 서비스가 실제로 해당 포트에서 리슨하면 같은 네트워크의 다른 컨테이너가 접근할 수 있습니다.

이때 `app` 컨테이너는 `http://api:8080`으로 `api` 컨테이너에 접근합니다. 여기서 사용하는 포트는 호스트에 공개한 포트가 아니라 컨테이너 내부에서 서비스가 실제로 리슨하는 포트입니다.

## 예시: host network Spring Boot에서 Compose PostgreSQL 접속하기

다음과 같은 구성을 생각해볼 수 있습니다.

- PostgreSQL과 Adminer는 Docker Compose로 실행합니다.
- Spring Boot 애플리케이션은 `docker run --network host`로 단일 컨테이너 실행합니다.
- Spring Boot 애플리케이션은 PostgreSQL에 연결해야 합니다.

이 경우 Spring Boot 컨테이너는 Compose 네트워크에 직접 붙어 있는 것이 아닙니다. 대신 호스트 서버의 네트워크를 그대로 사용합니다. 따라서 PostgreSQL 컨테이너의 포트를 호스트에 공개하고, Spring Boot에서는 `localhost:<호스트 포트>`로 접근합니다.

### Compose 설정

```yaml
services:
  postgres:
    image: postgres:16
    container_name: local-postgres
    environment:
      POSTGRES_DB: appdb
      POSTGRES_USER: appuser
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}
    ports:
      - "127.0.0.1:5432:5432"

  adminer:
    image: adminer
    container_name: local-adminer
    ports:
      - "127.0.0.1:8080:8080"
```

PostgreSQL 설정에서 중요한 부분은 `ports`입니다.

```yaml
ports:
  - "127.0.0.1:5432:5432"
```

이 설정은 PostgreSQL 컨테이너의 `5432` 포트를 호스트 서버의 `127.0.0.1:5432`로 공개합니다. Spring Boot 컨테이너는 host network를 사용하므로 컨테이너 안에서 `localhost:5432`로 요청했을 때 호스트 서버의 `127.0.0.1:5432`에 접근합니다.

흐름은 다음과 같습니다.

```text
Spring Boot container (--network host)
  -> localhost:5432
  -> host server 127.0.0.1:5432
  -> compose postgres container 5432
```

### Spring Boot datasource 설정

`application.properties`를 사용한다면 다음처럼 설정합니다.

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/appdb
spring.datasource.username=appuser
spring.datasource.password=${POSTGRES_PASSWORD}
```

환경 변수로 넘긴다면 다음처럼 실행할 수 있습니다.

```bash
docker run --network host \
  -e SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/appdb \
  -e SPRING_DATASOURCE_USERNAME=appuser \
  -e SPRING_DATASOURCE_PASSWORD="${POSTGRES_PASSWORD}" \
  my-spring-app
```

Spring Boot가 `server.port=8080`으로 실행된다면 `--network host` 환경에서는 호스트 서버의 8080 포트를 직접 사용합니다. 이때 `docker run -p 8080:8080` 같은 포트 매핑은 의미가 없습니다.

### 왜 postgres 서비스명으로 접속하지 않을까

Compose 내부 컨테이너끼리는 다음처럼 서비스명으로 접근할 수 있습니다.

```text
jdbc:postgresql://postgres:5432/appdb
```

하지만 `--network host`로 실행한 Spring Boot 컨테이너는 Compose가 만든 Docker 네트워크에 붙어 있지 않습니다. 그래서 기본적으로 Compose 서비스명인 `postgres`를 Docker DNS로 해석하지 못합니다.

이 구성에서는 다음 주소를 사용합니다.

```text
jdbc:postgresql://localhost:5432/appdb
```

즉, `postgres` 서비스명으로 붙는 방식이 아니라 호스트에 공개된 포트를 통해 PostgreSQL 컨테이너로 들어가는 방식입니다.

### 확인 명령어

Compose 서비스를 먼저 실행합니다.

```bash
docker compose up -d
```

호스트 서버에서 PostgreSQL 포트가 열렸는지 확인합니다.

```bash
docker compose ps
lsof -i :5432
```

Spring Boot 컨테이너와 같은 조건에서 접속을 테스트하려면 host network로 임시 컨테이너를 실행해 확인할 수 있습니다.

```bash
docker run --rm --network host \
  -e PGPASSWORD="${POSTGRES_PASSWORD}" \
  postgres:16 \
  psql -h localhost -U appuser -d appdb
```

### 주의할 점

- PostgreSQL compose 서비스에 `ports`가 없고 `expose`만 있으면 host network Spring Boot 컨테이너에서 `localhost:5432`로 접근할 수 없습니다.
- Spring Boot 컨테이너에서는 `jdbc:postgresql://postgres:5432/appdb`가 아니라 `jdbc:postgresql://localhost:5432/appdb`를 사용합니다.
- PostgreSQL을 `"127.0.0.1:5432:5432"`로 공개하면 같은 서버 내부에서만 접근하기 좋습니다.
- 외부 서버에서도 PostgreSQL에 접근해야 한다면 `"5432:5432"`처럼 열 수 있지만, 보안상 방화벽과 계정 권한을 반드시 함께 제한해야 합니다.
- host network는 Linux 서버에서 주로 사용하는 방식입니다. Docker Desktop 환경에서는 동작 방식이 다를 수 있습니다.

## 반드시 localhost로 요청해야 하는 경우

애플리케이션 설정을 바꾸기 어렵거나 라이브러리가 `localhost:<port>`로만 접근하도록 되어 있다면 네트워크 구조를 바꿔야 합니다.

### 1. host network 사용

Linux 서버에서는 컨테이너를 호스트 네트워크 네임스페이스에서 실행할 수 있습니다.

```bash
docker run --network host <image>
```

Compose에서는 다음처럼 설정합니다.

```yaml
services:
  app:
    image: my-app
    network_mode: host
```

이 경우 컨테이너 안의 `localhost`는 호스트 서버의 `localhost`와 같은 네트워크를 바라봅니다. 따라서 컨테이너에서 `http://localhost:8080`으로 요청하면 호스트 서버의 8080 포트로 접근할 수 있습니다. host mode에서는 `-p` 또는 `ports` 포트 매핑이 무시되며 경고가 발생합니다.

다만 host network는 포트 격리가 약해지고 같은 포트를 여러 컨테이너가 동시에 사용할 수 없습니다. Docker Desktop 4.34 이상은 설정에서 host networking을 활성화한 Linux 컨테이너에 한해 이 기능을 지원하지만, Linux Docker Engine과 세부 동작이 같다고 가정하지 말고 대상 환경에서 검증해야 합니다. 운영 환경에서는 필요한 경우에만 제한적으로 사용합니다.

### 2. 다른 컨테이너의 네트워크 네임스페이스 공유

컨테이너 A가 컨테이너 B와 같은 네트워크 네임스페이스를 공유하면, A에서 `localhost:<port>`로 B의 프로세스에 접근할 수 있습니다.

```bash
docker run -d --name api my-api

docker run --rm \
  --network container:api \
  curlimages/curl \
  http://localhost:8080
```

Compose에서는 다음처럼 특정 서비스의 네트워크 네임스페이스를 공유할 수 있습니다.

```yaml
services:
  api:
    image: my-api

  app:
    image: my-app
    network_mode: "service:api"
```

이 방식은 sidecar 패턴처럼 두 컨테이너가 거의 한 묶음으로 동작해야 할 때 사용할 수 있습니다. 대신 `app`은 독립적인 포트 매핑이나 네트워크 설정을 갖기 어렵고, 같은 네트워크 공간을 공유하므로 포트 충돌에 주의해야 합니다.

## 어떤 방법을 선택할까

대부분의 경우에는 다음 순서로 선택합니다.

1. 다른 컨테이너로 접근한다면 `http://서비스명:포트`를 사용합니다.
2. 호스트 서버 서비스로 접근한다면 `host.docker.internal`을 사용합니다.
3. 애플리케이션이 반드시 `localhost`만 써야 한다면 `network_mode: host`나 `network_mode: "service:<name>"`를 검토합니다.

## 자주 하는 실수

- 컨테이너 안에서 `localhost`를 호스트 서버라고 생각하는 것
- 컨테이너끼리 통신할 때 호스트에 공개한 포트를 사용하는 것
- Compose 서비스 간 통신에서 `localhost`를 사용하는 것
- 호스트 서비스가 `127.0.0.1`에만 바인딩되어 있는데 컨테이너에서 접근하려는 것
- host network를 편하다는 이유로 모든 컨테이너에 적용하는 것

## 빠른 예시

호스트 서버의 8080 포트로 접근:

```yaml
services:
  app:
    image: my-app
    extra_hosts:
      - "host.docker.internal:host-gateway"
    environment:
      API_BASE_URL: http://host.docker.internal:8080
```

다른 컨테이너의 8080 포트로 접근:

```yaml
services:
  app:
    image: my-app
    environment:
      API_BASE_URL: http://api:8080

  api:
    image: my-api
    expose:
      - "8080"
```

반드시 `localhost:8080`으로 접근:

```yaml
services:
  api:
    image: my-api

  app:
    image: my-app
    network_mode: "service:api"
```

## 참고한 공식 문서

- [Networking overview](https://docs.docker.com/engine/network/)
- [Host network driver](https://docs.docker.com/engine/network/drivers/host/)
- [Control Compose startup order](https://docs.docker.com/compose/how-tos/startup-order/)
